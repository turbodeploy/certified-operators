package com.vmturbo.cost.component.db;

import static com.vmturbo.cost.component.db.Tables.ACCOUNT_EXPENSES_BY_MONTH;
import static com.vmturbo.cost.component.db.tables.AccountExpenses.ACCOUNT_EXPENSES;
import static com.vmturbo.cost.component.db.tables.AccountExpensesRetentionPolicies.ACCOUNT_EXPENSES_RETENTION_POLICIES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import java.sql.SQLException;
import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.jooq.DSLContext;
import org.jooq.Record1;
import org.jooq.SQLDialect;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;
import com.vmturbo.sql.utils.jooq.PurgeEvent;

/**
 * Test that all the purge procedures delete data correctly based on retention policy period.
 */
@RunWith(Parameterized.class)
public class StoredProcedureConfigTest extends MultiDbTestBase {

    private static final LocalDate pastTime1 = LocalDate.of(2021, 2, 10);
    private static final LocalDate pastTime2 = LocalDate.of(2021, 4, 10);
    private static final LocalDate futureTime = LocalDate.of(3000, 2, 10);

    /**
     * Provide test parameters. It doesn't support the legacy mariadb.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return new Object[][]{ DBENDPOINT_MARIADB_PARAMS, DBENDPOINT_POSTGRES_PARAMS };
    }

    private final DSLContext dsl;
    private final StoredProcedureConfig storedProcedureConfig;

    /**
     * Construct a rule chaining for a given scenario.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect DB dialect to use
     * @throws SQLException if there's a provisioning problem
     * @throws UnsupportedDialectException if the dialect is bogus
     * @throws InterruptedException if we're interrupted
     */
    public StoredProcedureConfigTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
        this.storedProcedureConfig = new TestStoredProcedureConfig(dsl);
    }

    /**
     * Test that account expense data is purged correctly.
     */
    @Test
    public void testPurgeExpiredAccountExpensesData() {
        // initialize records
        Stream.of(pastTime1, pastTime2, futureTime).forEach(timestamp -> {
            dsl.insertInto(ACCOUNT_EXPENSES).columns(ACCOUNT_EXPENSES.fields())
                    .values(1, timestamp, 2, 1, 1, 1, 0).execute();
            dsl.insertInto(ACCOUNT_EXPENSES_BY_MONTH).columns(ACCOUNT_EXPENSES_BY_MONTH.fields())
                    .values(1, timestamp, 2, 1, 1, 1, 4).execute();
        });

        // before
        assertThat(dsl.fetchCount(ACCOUNT_EXPENSES), is(3));
        assertThat(dsl.fetchCount(ACCOUNT_EXPENSES_BY_MONTH), is(3));

        // remove retention policy and verify the purge doesn't delete anything
        deleteRetentionPolicy("retention_days");
        deleteRetentionPolicy("retention_months");
        // purge
        storedProcedureConfig.purgeExpiredAccountExpensesData().getProcedure().run();
        assertThat(dsl.fetchCount(ACCOUNT_EXPENSES), is(3));
        assertThat(dsl.fetchCount(ACCOUNT_EXPENSES_BY_MONTH), is(3));

        // set retention period to 7 days, so pastTime1 and pastTime2 are removed
        setRetentionPeriod("retention_days", 60);
        setRetentionPeriod("retention_months", 24);
        // purge
        storedProcedureConfig.purgeExpiredAccountExpensesData().getProcedure().run();
        List<LocalDate> rest = dsl.select(ACCOUNT_EXPENSES.EXPENSE_DATE)
                .from(ACCOUNT_EXPENSES)
                .stream()
                .map(Record1::value1)
                .collect(Collectors.toList());
        assertThat(rest, containsInAnyOrder(futureTime));
    }

    private void deleteRetentionPolicy(String policyName) {
        dsl.deleteFrom(ACCOUNT_EXPENSES_RETENTION_POLICIES)
                .where(ACCOUNT_EXPENSES_RETENTION_POLICIES.POLICY_NAME.eq(policyName))
                .execute();
    }

    private void setRetentionPeriod(String policyName, Integer value) {
        dsl.insertInto(ACCOUNT_EXPENSES_RETENTION_POLICIES)
                .columns(ACCOUNT_EXPENSES_RETENTION_POLICIES.POLICY_NAME,
                        ACCOUNT_EXPENSES_RETENTION_POLICIES.RETENTION_PERIOD)
                .values(policyName, value).execute();
    }

    /**
     * Test config.
     */
    static class TestStoredProcedureConfig extends StoredProcedureConfig {

        private final DSLContext dsl;

        TestStoredProcedureConfig(DSLContext dsl) {
            this.dsl = dsl;
        }

        @Override
        public DSLContext dsl()
                throws SQLException, UnsupportedDialectException, InterruptedException {
            return dsl;
        }

        @Override
        public PurgeEvent createPurgeEvent(String name, Runnable runnable) {
            PurgeEvent purgeEvent = spy(super.createPurgeEvent(name, runnable));
            // avoid actual invoke by scheduler
            doReturn(purgeEvent).when(purgeEvent).schedule(anyInt(), anyLong());
            return purgeEvent;
        }
    }
}