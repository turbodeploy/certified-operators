package com.vmturbo.cost.component.expenses;

import static com.vmturbo.cost.component.db.tables.AccountExpenses.ACCOUNT_EXPENSES;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.BatchBindStep;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record6;
import org.jooq.Result;
import org.jooq.SelectJoinStep;
import org.jooq.SelectSelectStep;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.Builder;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.ServiceExpenses;
import com.vmturbo.common.protobuf.cost.Cost.AccountExpenses.AccountExpensesInfo.TierExpenses;
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

    @Nonnull
    @Override
    public Map<Long, Map<Long, AccountExpenses>> getAccountExpenses(@Nonnull final CostFilter filter)
            throws DbException {
        Table<?> table = filter.getTable();
        try {
            final Field<Long> entityId = (Field<Long>)table
                    .field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID.getName());
            final Field<LocalDateTime> createdTime = (Field<LocalDateTime>)table
                    .field(ACCOUNT_EXPENSES.SNAPSHOT_TIME.getName());
            final Field<Long> accountId = (Field<Long>)table
                    .field(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID.getName());
            final Field<Integer> entityType = (Field<Integer>)table
                    .field(ACCOUNT_EXPENSES.ENTITY_TYPE.getName());
            final Field<Integer> currency = (Field<Integer>)table
                    .field(ACCOUNT_EXPENSES.CURRENCY.getName());
            final Field<BigDecimal> amount = (Field<BigDecimal>)table
                    .field(ACCOUNT_EXPENSES.AMOUNT.getName());

            SelectSelectStep<Record6<Long, LocalDateTime, Long, Integer, Integer, BigDecimal>> select = dsl
                .select(accountId, createdTime, entityId, entityType, currency, amount);

            SelectJoinStep<Record6<Long, LocalDateTime, Long, Integer, Integer, BigDecimal>> selectFrom;

            // Since the information about different account can have different timestamp, we
            // find the latest timestamp for each account and inner join it original select
            // results. That way, we get latest info for each account.
            if (filter.isLatestTimeStampRequested()) {
                selectFrom =
                    select.from(table.innerJoin(dsl
                    .select(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID,
                        DSL.max(ACCOUNT_EXPENSES.SNAPSHOT_TIME).as(ACCOUNT_EXPENSES.SNAPSHOT_TIME))
                    .from(table)
                    .groupBy(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID))
                    .using(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID,
                        ACCOUNT_EXPENSES.SNAPSHOT_TIME));
            } else {
                selectFrom = select.from(table);
            }

            final Result<Record6<Long, LocalDateTime, Long, Integer, Integer, BigDecimal>> records =
                selectFrom.where(filter.getConditions()).fetch();
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
        final boolean isCloudService = EntityType.CLOUD_SERVICE_VALUE == recordWrapper.getAssociatedEntityType();
        AccountExpensesInfo info = isCloudService ? AccountExpensesInfo.newBuilder()
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

