package com.vmturbo.cost.component.util;

import static com.vmturbo.cost.component.db.Tables.ACCOUNT_EXPENSES;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.Table;

import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.reserved.instance.TimeFrameCalculator.TimeFrame;

/**
 * A filter to restrict the account expenses records from the
 * {@link com.vmturbo.cost.component.expenses.AccountExpensesStore}.
 * It provider a easier way to define simple search over account expense records
 * in the tables.
 */
public class AccountExpensesFilter extends CostFilter {

    private static final String CREATED_TIME = ACCOUNT_EXPENSES.SNAPSHOT_TIME.getName();

    private final List<Condition> conditions;

    public AccountExpensesFilter(@Nonnull Set<Long> entityFilter,
                                 @Nonnull Set<Integer> entityTypeFilter,
                                 final long startDateMillis,
                                 final long endDateMillis,
                                 @Nullable final TimeFrame timeFrame) {
        super(entityFilter, entityTypeFilter, startDateMillis, endDateMillis, timeFrame, CREATED_TIME);
        this.conditions = generateConditions();
    }

    /**
     * Generate a list of {@link Condition} based on different fields.
     *
     * @return a list of {@link Condition}.
     */
    public List<Condition> generateConditions() {
        final List<Condition> conditions = new ArrayList<>();


        final Table<?> table = getTable();

        if (startDateMillis > 0 && endDateMillis > 0) {
            conditions.add(((Field<Timestamp>) table.field(snapshotTime))
                    .between(new Timestamp(this.startDateMillis), new Timestamp(this.endDateMillis)));
        }

        if (!entityTypeFilters.isEmpty()) {
            conditions.add((table.field(ACCOUNT_EXPENSES.ENTITY_TYPE.getName()))
                    .in(entityTypeFilters));
        }
        if (!entityFilters.isEmpty()) {
            conditions.add((table.field(ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID.getName()))
                    .in(entityFilters));
        }
        return conditions;
    }

    @Override
    public Condition[] getConditions() {
        return this.conditions.toArray(new Condition[conditions.size()]);
    }

    @Override
    public Table<?> getTable() {
        if (this.timeFrame == null || this.timeFrame.equals(TimeFrame.LATEST)) {
            return Tables.ACCOUNT_EXPENSES;
        } else if (this.timeFrame.equals(TimeFrame.HOUR)) {
            return Tables.ACCOUNT_EXPENSES_BY_HOUR;
        } else if (this.timeFrame.equals(TimeFrame.DAY)) {
            return Tables.ACCOUNT_EXPENSES_BY_DAY;
        } else {
            return Tables.ACCOUNT_EXPENSES_BY_MONTH;
        }
    }
}
