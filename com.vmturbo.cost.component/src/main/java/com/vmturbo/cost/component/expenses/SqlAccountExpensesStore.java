package com.vmturbo.cost.component.expenses;

import static com.vmturbo.cost.component.db.tables.AccountExpenses.ACCOUNT_EXPENSES;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Condition;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record1;
import org.jooq.Record6;
import org.jooq.Result;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.Builder;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.TierExpenses;
import com.vmturbo.common.protobuf.cost.Cost.GetCurrentAccountExpensesRequest.AccountExpenseQueryScope;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.tables.records.AccountExpensesRecord;
import com.vmturbo.cost.component.util.CostFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.CurrencyAmount;
import com.vmturbo.sql.utils.DbException;

/**
 * {@link AccountExpensesStore} that stores expenses to the SQL database.
 */
public class SqlAccountExpensesStore implements AccountExpensesStore {

    private static final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    private final Clock clock;

    private final int chunkSize;

    private final AtomicReference<LocalDateTime> inProgressBatchTime =
        new AtomicReference<>(null);

    public SqlAccountExpensesStore(@Nonnull final DSLContext dsl,
                                   @Nonnull final Clock clock,
                                   final int chunkSize) {
        this.dsl = Objects.requireNonNull(dsl);
        this.clock = Objects.requireNonNull(clock);
        this.chunkSize = chunkSize;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public void persistAccountExpenses(final long associatedAccountId,
                                       @Nonnull final AccountExpensesInfo accountExpensesInfo)
            throws DbException {
        Objects.requireNonNull(accountExpensesInfo);
        final LocalDateTime curTime = LocalDateTime.now(clock);
        inProgressBatchTime.set(curTime);
        try {
            // We chunk the transactions for speed, and to avoid overloading the DB buffers
            // on large topologies. Ideally this should be one transaction.
            //
            // TODO (Sept 6 2018): Try to handle transaction failure (e.g. by deleting all
            // committed data).
            // persist service expenses
            Lists.partition(accountExpensesInfo.getServiceExpensesList(), chunkSize).forEach(chunk -> {
                dsl.transaction(transaction -> {
                    final BatchBindStep batch = getBatchBindStep(curTime, transaction);

                    // Bind values to the batch insert statement. Each "bind" should have values for
                    // all fields set during batch initialization.
                    chunk.forEach(expense ->
                            batch.bind(curTime,
                                    associatedAccountId,
                                    expense.getAssociatedServiceId(), // Service id
                                    EntityType.CLOUD_SERVICE_VALUE, // Cloud service
                                    expense.getExpenses().getCurrency(),
                                    BigDecimal.valueOf(expense.getExpenses().getAmount())));
                    // Actually execute the batch insert.
                    batch.execute();
                });
            });

            // persist tier expenses
            Lists.partition(accountExpensesInfo.getTierExpensesList(), chunkSize).forEach(chunk -> {
                dsl.transaction(transaction -> {
                    final BatchBindStep batch = getBatchBindStep(curTime, transaction);
                    chunk.forEach(expense ->
                            batch.bind(curTime,
                                    associatedAccountId,
                                    expense.getAssociatedTierId(), // Tier id
                                    EntityType.COMPUTE_TIER_VALUE, // computer tier
                                    expense.getExpenses().getCurrency(),
                                    BigDecimal.valueOf(expense.getExpenses().getAmount())));

                    // Actually execute the batch insert.
                    batch.execute();
                });
            });
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        } finally {
            inProgressBatchTime.set(null);
        }
    }

    // build batch bind step
    private BatchBindStep getBatchBindStep(@Nonnull final LocalDateTime curTime,
                                           @Nonnull final Configuration transaction) {
        final DSLContext transactionContext = DSL.using(transaction);
        return transactionContext.batch(
                //have to provide dummy values for jooq
                transactionContext.insertInto(ACCOUNT_EXPENSES)
                        .set(ACCOUNT_EXPENSES.SNAPSHOT_TIME, curTime)
                        .set(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID, 0L)
                        .set(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID, 0L)
                        .set(ACCOUNT_EXPENSES.ENTITY_TYPE, 0)
                        .set(ACCOUNT_EXPENSES.CURRENCY, 0)
                        .set(ACCOUNT_EXPENSES.AMOUNT, BigDecimal.valueOf(0)));
    }

    @VisibleForTesting
    Optional<LocalDateTime> findRecentSnapshot() {
        // Find the most recent snapshot, but if we're in the process of saving another batch
        // don't return the time of that batch (since it is not completely written to the DB yet).
        LocalDateTime inProgressTime = inProgressBatchTime.get();
        if (inProgressTime == null) {
            inProgressTime = LocalDateTime.now(clock);
        }

        // First find the most recent snapshot time.
        final Optional<LocalDateTime> recentSnapshotTime =
            dsl.selectDistinct(ACCOUNT_EXPENSES.SNAPSHOT_TIME)
                .from(ACCOUNT_EXPENSES)
                .where(ACCOUNT_EXPENSES.SNAPSHOT_TIME.lessThan(inProgressTime))
                .orderBy(ACCOUNT_EXPENSES.SNAPSHOT_TIME.desc())
                .limit(1)
                .fetch()
                .stream()
                .findFirst()
                .map(Record1::value1);

        if (!recentSnapshotTime.isPresent()) {
            logger.warn("No recent snapshot found with time less than {} " +
                "for current account expense query", inProgressTime);
        }

        return recentSnapshotTime;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Collection<AccountExpenses> getCurrentAccountExpenses(@Nonnull final AccountExpenseQueryScope queryScope)
            throws DbException {
        try {
            Optional<LocalDateTime> recentSnapshotTime = findRecentSnapshot();
            if (!recentSnapshotTime.isPresent()) {
                return Collections.emptyList();
            }

            final List<Condition> conditions = new ArrayList<>();
            conditions.add(ACCOUNT_EXPENSES.SNAPSHOT_TIME.eq(recentSnapshotTime.get()));
            if (!queryScope.getAll()) {
                conditions.add(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID.in(
                    queryScope.getSpecificAccounts().getAccountIdsList()));
            }

            final Result<Record6<Long, LocalDateTime, Long, Integer, Integer, BigDecimal>> records = dsl
                    .select(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID,
                        ACCOUNT_EXPENSES.SNAPSHOT_TIME, ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID,
                        ACCOUNT_EXPENSES.ENTITY_TYPE, ACCOUNT_EXPENSES.CURRENCY, ACCOUNT_EXPENSES.AMOUNT)
                    .from(ACCOUNT_EXPENSES)
                    .where(conditions)
                    .fetch();
            final Map<Long, Map<Long, Cost.AccountExpenses>> expensesByTimeAndAccountId =
                constructExpensesMap(records);

            if (expensesByTimeAndAccountId.size() != 1) {
                // This shouldn't happen, since we know the snapshot is in the database, and
                // we ask for it by SNAPSHOT_TIME!
                logger.warn("Expense map didn't return exactly one record with " +
                    "expected timestamp: {}. Expense Map: {}", recentSnapshotTime.get(),
                    expensesByTimeAndAccountId);
            }
            return expensesByTimeAndAccountId.values().stream()
                .findFirst()
                .map(Map::values)
                .orElse(Collections.emptyList());
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAccountExpensesByAssociatedAccountId(final long associatedAccountId)
            throws AccountExpenseNotFoundException, DbException {
        try {
            if (dsl.deleteFrom(ACCOUNT_EXPENSES)
                    .where(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID.eq(associatedAccountId))
                    .execute() == 0) {
                throw new AccountExpenseNotFoundException("Account association id " + associatedAccountId +
                        " is not found. Could not delete account expenses");
            }
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        }
    }

    @Override
    public Map<Long, Map<Long, AccountExpenses>> getAccountExpenses(@Nonnull final CostFilter filter) throws DbException {
        try {
            final Field<Long> entityId = (Field<Long>) filter
                    .getTable()
                    .field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID.getName());
            final Field<LocalDateTime> createdTime = (Field<LocalDateTime>) filter
                    .getTable()
                    .field(ACCOUNT_EXPENSES.SNAPSHOT_TIME.getName());
            final Field<Long> acccountId = (Field<Long>) filter
                    .getTable()
                    .field(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID.getName());
            final Field<Integer> entityType = (Field<Integer>) filter
                    .getTable()
                    .field(ACCOUNT_EXPENSES.ENTITY_TYPE.getName());
            final Field<Integer> currency = (Field<Integer>) filter
                    .getTable()
                    .field(ACCOUNT_EXPENSES.CURRENCY.getName());
            final Field<BigDecimal> amount = (Field<BigDecimal>) filter
                    .getTable()
                    .field(ACCOUNT_EXPENSES.AMOUNT.getName());
            final Result<Record6<Long, LocalDateTime, Long, Integer, Integer, BigDecimal>> records = dsl
                    .select(acccountId, createdTime, entityId, entityType, currency, amount)
                    .from(filter.getTable())
                    .where(filter.getConditions())
                    .fetch();
            return constructExpensesMap(records);
        } catch (DataAccessException e) {
            throw new DbException("Failed to get entity costs from DB" + e.getMessage());
        }
    }


    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public List<Cost.AccountExpenses> getAllAccountExpenses() throws DbException {
        try {
            return dsl.selectFrom(ACCOUNT_EXPENSES).fetch().map(this::toDTO);
        } catch (DataAccessException dataAccessException) {
            throw new DbException(dataAccessException.getMessage());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<Long, Map<Long, Cost.AccountExpenses>> getLatestExpenses(@Nonnull final Set<Long> entityIds,
                                                                        @Nonnull final Set<Integer> entityTypeIds) throws DbException {
        try {
            final List<Condition> conditions = new ArrayList<>();
            if (!entityTypeIds.isEmpty()) {
                conditions.add(ACCOUNT_EXPENSES.field(ACCOUNT_EXPENSES.ENTITY_TYPE.getName()).in(entityTypeIds));
            }

            if (!entityIds.isEmpty()) {
                conditions.add(ACCOUNT_EXPENSES.field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID.getName()).in(entityIds));
            }


            final Result<Record6<Long, LocalDateTime, Long, Integer, Integer, BigDecimal>> results = dsl.select(
                    ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID,
                    ACCOUNT_EXPENSES.SNAPSHOT_TIME,
                    ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID,
                    ACCOUNT_EXPENSES.ENTITY_TYPE,
                    ACCOUNT_EXPENSES.CURRENCY,
                    ACCOUNT_EXPENSES.AMOUNT)
                    .from(ACCOUNT_EXPENSES)
                    .where(conditions.toArray(new Condition[conditions.size()]))
                    .and(ACCOUNT_EXPENSES.SNAPSHOT_TIME.eq(dsl.select(ACCOUNT_EXPENSES.SNAPSHOT_TIME
                            .max())
                            .from(ACCOUNT_EXPENSES)
                            .where(conditions.toArray(new Condition[conditions.size()]))))

                    .fetch();
            return constructExpensesMap(results);
        } catch (DataAccessException e) {
            throw new DbException("Failed to get entity costs from DB" + e.getMessage());
        }
    }

    /**
     * Construct Account expense map. Key is timestamp in long, Values are Map of AccountId -> AccountExpense
     * It will first group the records by timestamp, and combine the account expense with same id.
     *
     * @param accountExpensesRecords account expense records in db
     * @return Account expenses map, key is timestamp in long, values are Map of AccountId -> AccountExpense.
     */
    private Map<Long, Map<Long, AccountExpenses>> constructExpensesMap(
            @Nonnull final Result<Record6<Long, LocalDateTime, Long, Integer, Integer, BigDecimal>> accountExpensesRecords) {
        final Map<Long, Map<Long, Cost.AccountExpenses>> records = new HashMap<>();
        accountExpensesRecords.forEach(expense -> {
            final Map<Long, AccountExpenses> costsForTimestamp = records
                    .computeIfAbsent(TimeUtil.localDateTimeToMilli(expense.value2(), clock), k -> new HashMap<>());
            //TODO: optimize to avoid building newExpense
            final AccountExpenses newExpense = toDTO(new RecordWrapper(expense));
            costsForTimestamp.compute(newExpense.getAssociatedAccountId(),
                    (id, existingExpense) -> existingExpense == null ?
                            newExpense :
                            existingExpense.toBuilder().setAccountExpensesInfo(existingExpense.getAccountExpensesInfo().toBuilder()
                                    .addAllServiceExpenses(newExpense.getAccountExpensesInfo().getServiceExpensesList())
                                    .addAllTierExpenses(newExpense.getAccountExpensesInfo().getTierExpensesList())
                            ).build());
        });
        return records;
    }

    private AccountExpenses toDTO(final RecordWrapper recordWrapper) {
        final boolean isCloudSerivce = EntityType.CLOUD_SERVICE_VALUE == recordWrapper.getAssociatedEntityType();
        AccountExpensesInfo info = isCloudSerivce ? AccountExpensesInfo.newBuilder()
                .addServiceExpenses(ServiceExpenses.newBuilder()
                        .setAssociatedServiceId(recordWrapper.getAssociatedEntityId())
                        .setExpenses(CurrencyAmount.newBuilder()
                                .setAmount(recordWrapper.getAmount().doubleValue())
                                .setCurrency(recordWrapper.getCurrency())
                                .build()))
                .build()
                : AccountExpensesInfo.newBuilder()
                .addTierExpenses(TierExpenses
                        .newBuilder()
                        .setAssociatedTierId(recordWrapper.getAssociatedEntityId())
                        .setExpenses(CurrencyAmount.newBuilder()
                                .setAmount(recordWrapper.getAmount().doubleValue())
                                .setCurrency(recordWrapper.getCurrency()))
                        .build())
                .build();
        return Cost.AccountExpenses.newBuilder()
                .setAssociatedAccountId(recordWrapper.gettAssociatedAccountId())
                .setExpenseReceivedTimestamp(localDateTimeToDate(recordWrapper.getSnapshotTime()))
                .setAccountExpensesInfo(info)
                .build();
    }

    //Convert accountExpensesRecord DB record to AccountExpenses proto DTO
    private Cost.AccountExpenses toDTO(@Nonnull final AccountExpensesRecord accountExpensesRecord) {
        final boolean isCloudSerivce = EntityType.CLOUD_SERVICE_VALUE == accountExpensesRecord.getEntityType();
        final Builder accountExpensesInfoBuilder = AccountExpensesInfo.newBuilder();
        final AccountExpensesInfo info = isCloudSerivce ? accountExpensesInfoBuilder
                .addServiceExpenses(ServiceExpenses.newBuilder()
                        .setAssociatedServiceId(accountExpensesRecord.getAssociatedEntityId())
                        .setExpenses(CurrencyAmount.newBuilder()
                                .setAmount(accountExpensesRecord.getAmount().doubleValue())
                                .setCurrency(accountExpensesRecord.getCurrency())
                                .build()))
                .build()
                : accountExpensesInfoBuilder
                .addTierExpenses(TierExpenses.newBuilder()
                        .setAssociatedTierId(accountExpensesRecord.getAssociatedEntityId())
                        .setExpenses(CurrencyAmount.newBuilder()
                                .setAmount(accountExpensesRecord.getAmount().doubleValue())
                                .setCurrency(accountExpensesRecord.getCurrency()))
                        .build())
                .build();
        return Cost.AccountExpenses.newBuilder()
                .setAssociatedAccountId(accountExpensesRecord.getAssociatedAccountId())
                .setExpenseReceivedTimestamp(localDateTimeToDate(accountExpensesRecord.getSnapshotTime()))
                .setAccountExpensesInfo(info)
                .build();
    }

    /**
     * Convert local date time to long.
     *
     * @param startOfDay start of date with LocalDateTime type.
     * @return date time in long type.
     */
    private long localDateTimeToDate(LocalDateTime startOfDay) {
        return Date.from(startOfDay.atZone(ZoneId.systemDefault()).toInstant()).getTime();
    }


    /**
     * A wrapper class to wrap {@link Record6} class, to make it more readable
     */
    private class RecordWrapper {
        final Record6<Long, LocalDateTime, Long, Integer, Integer, BigDecimal> record6;

        RecordWrapper(Record6 record6) {
            this.record6 = record6;
        }

        long gettAssociatedAccountId() {
            return record6.value1();
        }

        LocalDateTime getSnapshotTime() {
            return record6.value2();
        }

        long getAssociatedEntityId() {
            return record6.value3();
        }

        int getAssociatedEntityType() {
            return record6.value4();
        }

        int getCurrency() {
            return record6.value5();
        }

        BigDecimal getAmount() {
            return record6.value6();
        }
    }
}

