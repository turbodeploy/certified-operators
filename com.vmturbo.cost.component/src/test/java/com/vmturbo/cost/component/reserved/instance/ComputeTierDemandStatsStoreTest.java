package com.vmturbo.cost.component.reserved.instance;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.ImmutableList;

import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.TestCostDbEndpointConfig;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.MultiDbTestBase;

@RunWith(Parameterized.class)
public class ComputeTierDemandStatsStoreTest extends MultiDbTestBase {
    /**
     * Provide test parameters.
     *
     * @return test parameters
     */
    @Parameters
    public static Object[][] parameters() {
        return MultiDbTestBase.DBENDPOINT_CONVERTED_PARAMS;
    }

    private final DSLContext dsl;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public ComputeTierDemandStatsStoreTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(Cost.COST, configurableDbDialect, dialect, "cost",
                TestCostDbEndpointConfig::costEndpoint);
        this.dsl = super.getDslContext();
    }

    /** Rule chain to manage db provisioning and lifecycle. */
    @Rule
    public TestRule multiDbRules = super.ruleChain;

    @Test
    public void testPersistence() {

        // Construct 2 records across different hours
        final ComputeTierTypeHourlyByWeekRecord recordA = new ComputeTierTypeHourlyByWeekRecord(
                (byte)1, (byte)2, 3L, 4L, 5L, (byte)6, (byte)7, new BigDecimal("8.000"),
                new BigDecimal("9.000"));
        final ComputeTierTypeHourlyByWeekRecord recordB = new ComputeTierTypeHourlyByWeekRecord(
                (byte)1, (byte)3, 3L, 4L, 5L, (byte)6, (byte)7, new BigDecimal("8.000"),
                new BigDecimal("9.000"));

        final ComputeTierDemandStatsStore store = new ComputeTierDemandStatsStore(dsl, 1, 1);

        // persist the records
        store.persistComputeTierDemandStats(ImmutableList.of(recordA, recordB), false);

        // verify the records match
        final List<ComputeTierTypeHourlyByWeekRecord> actualRecords = dsl.selectFrom(Tables.COMPUTE_TIER_TYPE_HOURLY_BY_WEEK)
                .fetchInto(ComputeTierTypeHourlyByWeekRecord.class);

        assertThat(actualRecords, hasSize(2));
        assertThat(actualRecords, containsInAnyOrder(recordA, recordB));
    }

}
