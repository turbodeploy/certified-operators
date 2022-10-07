package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;
import java.util.List;

import com.google.common.collect.ImmutableSet;

import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.tables.BilledSavingsByDay;
import com.vmturbo.cost.component.db.tables.EntityCloudScope;
import com.vmturbo.cost.component.db.tables.EntitySavingsByDay;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.savings.bottomup.AggregatedSavingsStats;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsStats;
import com.vmturbo.cost.component.savings.bottomup.SqlEntitySavingsStore;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Tests for data retention processor.
 */
@RunWith(Parameterized.class)
@CleanupOverrides(truncate = {EntityCloudScope.class, EntitySavingsByDay.class,
        BilledSavingsByDay.class})
public class BillBasedDataRetentionProcessorTest extends DataRetentionProcessorTest {
    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public BillBasedDataRetentionProcessorTest(boolean configurableDbDialect, SQLDialect dialect)
            throws SQLException, UnsupportedDialectException, InterruptedException {
        super(configurableDbDialect, dialect);
    }

    /**
     * Setting up stuff.
     *
     * @throws Exception Thrown on init exception.
     */
    @Before
    public void setup() throws Exception {
        statsSavingsStore = new SqlEntitySavingsStore(dsl, clock, 5, true);
        addScopeRecords();
    }

    /**
     * Inserts some hourly stats and then tests retention settings by deleting older stats data
     * to verify data is getting deleted as per requested config.
     *
     * @throws EntitySavingsException Thrown on DB access error.
     */
    @Test
    public void dailyRetention() throws EntitySavingsException {
        long timestampOld = TimeUtil.localDateTimeToMilli(timeOld, clock);
        final EntitySavingsStats stats1 = new EntitySavingsStats(vmOid1, timestampOld,
                EntitySavingsStatsType.REALIZED_SAVINGS, 10.02);

        long timestampNew = TimeUtil.localDateTimeToMilli(timeNew, clock);
        final EntitySavingsStats stats2 = new EntitySavingsStats(vmOid2, timestampNew,
                EntitySavingsStatsType.REALIZED_INVESTMENTS, 20.03);

        // Check counts before insertion, should be 0.
        checkDailyCounts(0, 0);

        // Add to bill-based daily stats table
        statsSavingsStore.addDailyStats(ImmutableSet.of(stats1, stats2), dsl, true);

        // Add to bottom-up daily stats table
        statsSavingsStore.addDailyStats(ImmutableSet.of(stats1, stats2), dsl, false);

        // Confirm there are 2 rows for each table now.
        checkDailyCounts(2, 2);

        // Delete anything older than 3 days, only newer stats is retained, older one is deleted.
        long daysBack = 3;
        // Create new processor here so that lastRunTime is still null the 2nd time below.
        retentionProcessor = new BillBasedDataRetentionProcessor(statsSavingsStore, clock,
                1, daysBack * 24L);
        retentionProcessor.process();

        // Bottom-up daily should still have 2 rows, but only 1 for bill-based daily.
        checkDailyCounts(2, 1);

        // // Confirm with stats query to bill-based table.
        long timestampEnd = TimeUtil.localDateTimeToMilli(timeEnd, clock);
        List<AggregatedSavingsStats> stats = fetchStats(timestampOld, timestampEnd,
                RollupDurationType.DAILY);
        assertNotNull(stats);
        // The older timestampOld should now be deleted, only timestampNew should be retained.
        assertEquals(1, stats.size());
        assertEquals(timestampNew, stats.get(0).getTimestamp());

        // Delete anything older than 1 day, both stats should be gone now.
        daysBack = 1;
        retentionProcessor = new BillBasedDataRetentionProcessor(statsSavingsStore, clock,
                1, daysBack * 24L);
        retentionProcessor.process();

        // No rows should be present for bill-based, but still 2 rows for bottom-up.
        checkDailyCounts(2, 0);

        // Confirm with stats query to bill-based table.
        stats = fetchStats(timestampOld, timestampEnd, RollupDurationType.DAILY);
        assertNotNull(stats);
        // Both timestampOld and timestampNew should be deleted.
        assertEquals(0, stats.size());
    }
}
