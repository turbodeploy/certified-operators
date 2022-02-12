package com.vmturbo.cost.component.expenses;

import static com.vmturbo.cost.component.db.Tables.ACCOUNT_EXPENSES;
import static com.vmturbo.cost.component.db.Tables.ACCOUNT_EXPENSES_BY_MONTH;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.util.List;

import org.hamcrest.Matchers;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.AccountExpensesByMonth;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

@RunWith(Parameterized.class)
public class AccountExpensesRollupTest extends MultiDbTestBase {

    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.POSTGRES_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @throws SQLException if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    public AccountExpensesRollupTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /**
     * Rule chain to manage db provisioning and lifecycle.
     */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    /**
     * test that records are being rolled up from 'account_expense' to 'account_expense_by_month'.
     */
    @Test
    @CleanupOverrides(truncate = {AccountExpensesByMonth.class})
    public void testAccountExpenseRecordsAreRolledUp() {
        AccountExpenseRollup accountExpensesRollup = new AccountExpenseRollup(dsl);
        dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields()).values(0, "2000-01-01",
                0, 1, 0, 75, 0).execute();
        dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields()).values(0, "2000-01-05",
                0, 2, 0, 25, 0).execute();
        accountExpensesRollup.execute();
        List<Record> records = dsl.fetch("SELECT * from " + ACCOUNT_EXPENSES_BY_MONTH.getName());
        Assert.assertFalse(records.isEmpty());
        Record record = records.get(0);
        Assert.assertEquals(2,
                record.get(ACCOUNT_EXPENSES_BY_MONTH.SAMPLES.getName(), Integer.class).intValue());
        Assert.assertEquals("2000-01-31",
                record.get(ACCOUNT_EXPENSES_BY_MONTH.EXPENSE_DATE.getName(), Date.class)
                        .toString());
        Assert.assertThat(BigDecimal.valueOf(50),
                Matchers.comparesEqualTo(record.getValue(ACCOUNT_EXPENSES_BY_MONTH.AMOUNT)));
    }

    /**
     * test that 'account_expense' records are marked as aggregated.
     */
    @Test
    @CleanupOverrides(truncate = {AccountExpensesByMonth.class})
    public void testAccountExpenseRecordsAreMarkedAsAggregated() {
        AccountExpenseRollup accountExpensesRollup = new AccountExpenseRollup(dsl);
        dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields()).values(0, "2000-01-01",
                0, 1, 0, 75, 0).execute();
        dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields()).values(0, "2000-01-05",
                0, 2, 0, 25, 0).execute();
        accountExpensesRollup.execute();
        List<Record> records = dsl.fetch("SELECT * from " + ACCOUNT_EXPENSES.getName());
        Assert.assertFalse(records.isEmpty());
        for (Record record : records) {
            Assert.assertEquals((Integer)1, record.get(ACCOUNT_EXPENSES.AGGREGATED, Integer.class));
        }
    }

    /**
     * test multiple day rollup performs correctly.
     */
    @Test
    @CleanupOverrides(truncate = {AccountExpensesByMonth.class})
    public void testAccountExpenseMultiDayRollup() {
        AccountExpenseRollup accountExpensesRollup = new AccountExpenseRollup(dsl);
        dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields()).values(0, "2000-01-01",
                0, 1, 0, 75, 0).execute();
        dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields()).values(0, "2000-01-05",
                0, 2, 0, 25, 0).execute();
        accountExpensesRollup.execute();
        dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields()).values(0, "2000-01-06",
                0, 1, 0, 150, 0).execute();
        dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields()).values(0, "2000-01-07",
                0, 2, 0, 150, 0).execute();
        accountExpensesRollup.execute();
        List<Record> records = dsl.fetch("SELECT * from " + ACCOUNT_EXPENSES_BY_MONTH.getName());
        Assert.assertFalse(records.isEmpty());
        Record record = records.get(0);
        Assert.assertEquals(4,
                record.get(ACCOUNT_EXPENSES_BY_MONTH.SAMPLES.getName(), Integer.class).intValue());
        Assert.assertEquals("2000-01-31",
                record.get(ACCOUNT_EXPENSES_BY_MONTH.EXPENSE_DATE.getName(), Date.class)
                        .toString());
        Assert.assertThat(BigDecimal.valueOf(100),
                Matchers.comparesEqualTo(record.getValue(ACCOUNT_EXPENSES_BY_MONTH.AMOUNT)));
    }
}
