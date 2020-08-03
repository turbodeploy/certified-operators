package com.vmturbo.cost.component.util;

import static com.vmturbo.cost.component.db.Tables.ACCOUNT_EXPENSES;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import com.vmturbo.commons.TimeFrame;
import com.vmturbo.cost.component.db.Tables;

/**
 * A filter to restrict the account expenses records from the
 * {@link com.vmturbo.cost.component.expenses.AccountExpensesStore}.
 * It provider a easier way to define simple search over account expense records
 * in the tables.
 */
public class AccountExpensesFilter extends CostFilter {

    private static final Logger logger = LogManager.getLogger();

    private static final String EXPENSE_DATE = ACCOUNT_EXPENSES.EXPENSE_DATE.getName();

    private final Set<Long> accountIds;
    private final List<Condition> conditions;

    @Nullable
    private final CostGroupBy costGroupBy;

    AccountExpensesFilter(@Nullable Set<Long> entityFilter,
                          @Nullable Set<Integer> entityTypeFilter,
                          @Nullable final Long startDateMillis,
                          @Nullable final Long endDateMillis,
                          @Nullable final TimeFrame timeFrame,
                          @Nonnull Set<String> groupByFields,
                          @Nullable final Set<Long> accountIds,
                          final boolean latestTimeStampRequested,
                          long realtimeTopologyContextId) {
        super(entityFilter, entityTypeFilter, startDateMillis, endDateMillis, timeFrame,
                EXPENSE_DATE, latestTimeStampRequested, null, realtimeTopologyContextId);
        this.accountIds = accountIds;
        this.conditions = generateConditions();
        this.costGroupBy = createGroupByFieldString(groupByFields);
    }

    @Nullable
    private CostGroupBy createGroupByFieldString(@Nonnull Set<String> items ) {
        Set<String> listOfFields = Sets.newHashSet(items);
        listOfFields.add(getTable().field(EXPENSE_DATE).getName());
        return items.isEmpty() ?
                null :
                new CostGroupBy(listOfFields.stream().map(columnName -> columnName.toLowerCase(Locale.getDefault()))
                        .collect(Collectors.toSet()),
                        timeFrame, realtimeTopologyContextId);
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @return a list of {@link Condition}.
     */
    public List<Condition> generateConditions() {
        final List<Condition> conditions = new ArrayList<>();


        final Table<?> table = getTable();

        if (startDateMillis != null) {
            LocalDateTime localStart = LocalDateTime.ofInstant(Instant.ofEpochMilli(this.startDateMillis),
                ZoneId.from(ZoneOffset.UTC));
            LocalDateTime localEnd =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(this.endDateMillis),
                    ZoneId.from(ZoneOffset.UTC));
            conditions.add(((Field<LocalDateTime>)table.field(snapshotTime))
                .between(localStart, localEnd));
        }

        if (entityTypeFilters != null) {
            conditions.add((table.field(ACCOUNT_EXPENSES.ENTITY_TYPE.getName()))
                    .in(entityTypeFilters));
        }
        if (entityFilters != null) {
            conditions.add((table.field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID.getName()))
                    .in(entityFilters));
        } else {
            // In case there is no filter on entity IDs, ignore records where the
            // associated entity ID is 0.
            // This can happen when the expense is for a cloud service which wasn't
            // discovered because it doesn't appear in the CloudService enum.
            conditions.add(table.field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID).notEqual(0L));
        }

        if (accountIds != null) {
            conditions.add((table.field(ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID.getName()))
                .in(accountIds));
        }

        return conditions;
    }

    @Override
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    @Override
    public Table<?> getTable() {
        switch (this.timeFrame) {
            case MONTH:
                return Tables.ACCOUNT_EXPENSES_BY_MONTH;
            case LATEST:
            case DAY:
                return Tables.ACCOUNT_EXPENSES;
            default:
                logger.error("Cannot get account expenses for Timeframe {} getting the daily " +
                                "expenses", this.timeFrame.name());
                return Tables.ACCOUNT_EXPENSES;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            final AccountExpensesFilter other = (AccountExpensesFilter)obj;
            return Objects.equals(accountIds, other.accountIds);
        }
        return false;
    }

    @Override
    public int hashCode() {
        Function<Set<?>, Integer> setHashCode = (set) -> (set == null) ? 0 : set.stream()
            .map(Object::hashCode).collect(Collectors.summingInt(Integer::intValue));
        return Objects.hash(setHashCode.apply(accountIds), super.hashCode());
    }

    @Override
    @Nonnull
    public String toString() {
        StringBuilder builder = new StringBuilder(super.toString());
        builder.append("\n account ids: ");
        builder.append((accountIds == null) ? "NOT SET" :
            accountIds.stream().map(String::valueOf).collect(Collectors.joining(",")));
        builder.append("\n conditions: ");
        builder.append(
            conditions.stream().map(Condition::toString).collect(Collectors.joining(" AND ")));
        return builder.toString();
    }

    /**
     * The builder class for {@link AccountExpensesFilter} class.
     */
    public static class AccountExpenseFilterBuilder extends CostFilterBuilder<AccountExpenseFilterBuilder,
        AccountExpensesFilter> {
        private Set<Long> accountIds;

        private AccountExpenseFilterBuilder(@Nonnull TimeFrame timeFrame,
                long realtimeTopologyContextId) {
            super(realtimeTopologyContextId);
            this.timeFrame = timeFrame;
        }

        /**
         * Factory method.
         * @param timeFrame  the time frame that we are making this query for.
         * @param realtimeTopologyContextId RT topology context ID.
         * @return a new instance of builder class.
         */
        @Nonnull
        public static AccountExpenseFilterBuilder newBuilder(@Nonnull TimeFrame timeFrame,
                long realtimeTopologyContextId) {
            return new AccountExpenseFilterBuilder(timeFrame, realtimeTopologyContextId);
        }

        /**
         * Sets the account ids to include in the cost.
         *
         * @param accountIds the account ids to include in the cost.
         * @return the builder.
         */
        @Nonnull
        public AccountExpenseFilterBuilder accountIds(@Nonnull Collection<Long> accountIds) {
            this.accountIds = new HashSet<>(accountIds);
            return this;
        }

        @Nonnull
        @Override
        public AccountExpensesFilter build() {
            return new AccountExpensesFilter(entityIds, entityTypeFilters, startDateMillis,
                endDateMillis, timeFrame, groupByFields, accountIds, latestTimeStampRequested,
                    realtimeTopologyContextId);
        }
    }

    @Override
    public CostGroupBy getCostGroupBy() {
        return costGroupBy;
    }
}
