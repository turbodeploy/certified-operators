package com.vmturbo.cost.component.expenses;

import static com.vmturbo.cost.component.db.Tables.ACCOUNT_EXPENSES;
import static com.vmturbo.cost.component.db.Tables.ACCOUNT_EXPENSES_BY_MONTH;

import java.time.LocalDate;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;

import com.vmturbo.sql.utils.jooq.UpsertBuilder;

/**
 * Class responsible for rolling up records found in 'account_expense'.
 */
public class AccountExpenseRollup {

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dsl;

    /**
     * AccountExpenseRollup constructor.
     * @param dsl the dsl object.
     */
    public AccountExpenseRollup(@Nonnull DSLContext dsl) {
        this.dsl = dsl;
    }

    /**
     * Execute the rollup that's surrounded by a try catch block.
     * (To prevent cancelling scheduled executors).
     */
    public void execute() {
        try {
            logger.info("Start AccountExpense rollup procedure.");
            rollup().run();
            logger.info("End AccountExpense rollup procedure.");
        } catch (Throwable e) {
            logger.error("Exception was thrown in the AccountExpensesRollup:", e);
        }
    }

    /**
     * Perform a rollup from 'account_expense' table to 'account_expense_by_month'. This
     * is done on a daily interval.
     *
     * <P> 1. Read all records from account_expense table that have not yet been aggregated.
     * 2. Group records by the month ('expense_date')
     *  2.1. Aggregated fields such as 'samples' and 'amount'
     * 3. Insert records captured in steps 1 and 2 into 'account_expense_by_month'
     * 4. Aggregate records again whenever there's a conflict in 'account_expense_by_month'
     * (almost ever record being inserted into 'account_expense_by_month' will cause a conflict
     *  and we perform another aggregation. This is apart of the rollup)
     * 5. set all records that were rolled up from 'account_expense' as aggregated </P>
     *
     * @return runnable to execute rollup.
     */
    private Runnable rollup() {
        return () -> dsl.transaction(configuration -> {
            final DSLContext context = configuration.dsl();
            final Field<LocalDate> aggregationDate = previousDay();
            final Field<LocalDate> lastDayOfMonth = (Field<LocalDate>)lastDayOfMonth(
                    ACCOUNT_EXPENSES.EXPENSE_DATE);
            final UpsertBuilder upsertBuilder = new UpsertBuilder().withTargetTable(
                            ACCOUNT_EXPENSES_BY_MONTH)
                    .withSourceTable(ACCOUNT_EXPENSES)
                    .withInsertFields(ACCOUNT_EXPENSES_BY_MONTH.ASSOCIATED_ACCOUNT_ID,
                            ACCOUNT_EXPENSES_BY_MONTH.EXPENSE_DATE, ACCOUNT_EXPENSES_BY_MONTH.ASSOCIATED_ENTITY_ID,
                            ACCOUNT_EXPENSES_BY_MONTH.ENTITY_TYPE, ACCOUNT_EXPENSES_BY_MONTH.CURRENCY,
                            ACCOUNT_EXPENSES_BY_MONTH.AMOUNT, ACCOUNT_EXPENSES_BY_MONTH.SAMPLES)
                    .withInsertValue(ACCOUNT_EXPENSES_BY_MONTH.EXPENSE_DATE, lastDayOfMonth)
                    .withInsertValue(ACCOUNT_EXPENSES_BY_MONTH.AMOUNT, DSL.avg(ACCOUNT_EXPENSES.AMOUNT).as("amount"))
                    .withInsertValue(ACCOUNT_EXPENSES_BY_MONTH.SAMPLES, DSL.count().as("samples"))
                    .withInsertValue(ACCOUNT_EXPENSES_BY_MONTH.ENTITY_TYPE, DSL.min(ACCOUNT_EXPENSES.ENTITY_TYPE))
                    .withInsertValue(ACCOUNT_EXPENSES_BY_MONTH.CURRENCY, DSL.min(ACCOUNT_EXPENSES.CURRENCY))
                    // ORDER HERE MATTERS
                    // 'Amount' must be updated before 'samples' is updated.
                    .withUpdateValue(ACCOUNT_EXPENSES_BY_MONTH.AMOUNT, UpsertBuilder.avg(ACCOUNT_EXPENSES_BY_MONTH.SAMPLES))
                    .withUpdateValue(ACCOUNT_EXPENSES_BY_MONTH.SAMPLES, UpsertBuilder::sum)
                    .withSourceCondition(ACCOUNT_EXPENSES.EXPENSE_DATE.lessOrEqual(aggregationDate)
                            .and(ACCOUNT_EXPENSES.AGGREGATED.eq((byte)0))) //toData is made up for now, not sure the case to byte make sense
                    .withSourceGroupBy(lastDayOfMonth, ACCOUNT_EXPENSES.ASSOCIATED_ACCOUNT_ID,
                            ACCOUNT_EXPENSES.ASSOCIATED_ENTITY_ID)
                    .withConflictColumns(ACCOUNT_EXPENSES_BY_MONTH.EXPENSE_DATE,
                            ACCOUNT_EXPENSES_BY_MONTH.ASSOCIATED_ACCOUNT_ID,
                            ACCOUNT_EXPENSES_BY_MONTH.ASSOCIATED_ENTITY_ID);
            context.execute(upsertBuilder.getUpsert(context));

            // Update all records in "account_expense" that were rolled up:
            // set 'aggregated' field to true (1).
            context.update(ACCOUNT_EXPENSES)
                    .set(ACCOUNT_EXPENSES.AGGREGATED, (byte)1)
                    .where(ACCOUNT_EXPENSES.AGGREGATED.eq((byte)0)
                            .and(ACCOUNT_EXPENSES.EXPENSE_DATE.lessThan(aggregationDate)))
                    .execute();
        });
    }

    /**
     * get the previous day.
     *
     * @return the previous day
     */
    private Field<LocalDate> previousDay() {
        return dsl.dialect() == SQLDialect.POSTGRES ? DSL.field("date_trunc('day', now()::date - 1)", LocalDate.class)
                : DSL.field("subdate(curdate(), 1)", LocalDate.class);
    }

    /**
     * returns last day of month given a reference day.
     *
     * @param field date field
     * @return last day of month
     */
    private Field<?> lastDayOfMonth(Field<?> field) {
        return dsl.dialect() == SQLDialect.POSTGRES ? DSL.field(
                "date_trunc('month', {0}) + '1 month - 1 day'::interval", field.getType(), field)
                : DSL.field("last_day({0})", field.getType(), field);
    }
}