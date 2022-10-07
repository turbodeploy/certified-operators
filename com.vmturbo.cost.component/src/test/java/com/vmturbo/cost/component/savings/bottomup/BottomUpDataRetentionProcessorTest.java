package com.vmturbo.cost.component.savings.bottomup;

import static com.vmturbo.cost.component.db.Tables.ENTITY_CLOUD_SCOPE;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_AUDIT_EVENTS;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_DAY;
import static com.vmturbo.cost.component.db.Tables.ENTITY_SAVINGS_BY_MONTH;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.commons.collections4.CollectionUtils;
import org.jooq.SQLDialect;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.db.tables.BilledSavingsByDay;
import com.vmturbo.cost.component.db.tables.EntityCloudScope;
import com.vmturbo.cost.component.db.tables.EntitySavingsAuditEvents;
import com.vmturbo.cost.component.db.tables.EntitySavingsByDay;
import com.vmturbo.cost.component.db.tables.EntitySavingsByHour;
import com.vmturbo.cost.component.db.tables.EntitySavingsByMonth;
import com.vmturbo.cost.component.rollup.RollupDurationType;
import com.vmturbo.cost.component.savings.DataRetentionProcessorTest;
import com.vmturbo.cost.component.savings.EntitySavingsException;
import com.vmturbo.cost.component.savings.bottomup.EntitySavingsRetentionConfig.DataRetentionSettings;
import com.vmturbo.cost.component.savings.bottomup.TopologyEvent.EventType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbCleanupRule.CleanupOverrides;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Tests for data retention processor.
 */
@RunWith(Parameterized.class)
@CleanupOverrides(truncate = {EntityCloudScope.class, EntitySavingsByHour.class,
        EntitySavingsByDay.class, EntitySavingsByMonth.class, BilledSavingsByDay.class,
        EntitySavingsAuditEvents.class})
public class BottomUpDataRetentionProcessorTest extends DataRetentionProcessorTest {
    /**
     * Mock config for retention.
     */
    private final EntitySavingsRetentionConfig retentionConfig = mock(
            EntitySavingsRetentionConfig.class);

    /**
     * For testing audit log DB cleanup.
     */
    private AuditLogWriter auditLogWriter;

    /**
     * Create a new instance with given parameters.
     *
     * @param configurableDbDialect true to enable POSTGRES_PRIMARY_DB feature flag
     * @param dialect         DB dialect to use
     * @throws SQLException                if a DB operation fails
     * @throws UnsupportedDialectException if dialect is bogus
     * @throws InterruptedException        if we're interrupted
     */
    public BottomUpDataRetentionProcessorTest(boolean configurableDbDialect, SQLDialect dialect)
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
        statsSavingsStore = new SqlEntitySavingsStore(dsl, clock, 5, false);
        auditLogWriter = new SqlAuditLogWriter(dsl, clock, 5, true);
        addScopeRecords();
    }

    /**
     * Inserts some hourly stats and then tests retention settings by deleting older stats data
     * to verify data is getting deleted as per requested config.
     *
     * @throws EntitySavingsException Thrown on DB access error.
     */
    @Test
    public void statsRetention() throws EntitySavingsException {
        // Verify scope entries are present
        assertEquals(2, dsl.fetchCount(ENTITY_CLOUD_SCOPE));

        long timestampOlder = TimeUtil.localDateTimeToMilli(timeOlder, clock);
        final EntitySavingsStats stats1 = new EntitySavingsStats(vmOid1, timestampOlder,
                EntitySavingsStatsType.REALIZED_SAVINGS, 10.01);

        long timestampOld = TimeUtil.localDateTimeToMilli(timeOld, clock);
        final EntitySavingsStats stats2 = new EntitySavingsStats(vmOid1, timestampOld,
                EntitySavingsStatsType.MISSED_SAVINGS, 20.02);

        long timestampNew = TimeUtil.localDateTimeToMilli(timeNew, clock);
        final EntitySavingsStats stats3 = new EntitySavingsStats(vmOid2, timestampNew,
                EntitySavingsStatsType.REALIZED_INVESTMENTS, 30.03);

        final Set<EntitySavingsStats> allStats = ImmutableSet.of(stats1, stats2, stats3);

        long timestampStart = TimeUtil.localDateTimeToMilli(timeStart, clock);
        long timestampEnd = TimeUtil.localDateTimeToMilli(timeEnd, clock);

        // Bottom-up hourly table insertion.
        statsSavingsStore.addHourlyStats(allStats, dsl);

        // Check hourly stats counts before.
        List<AggregatedSavingsStats> hourlyStats = fetchStats(timestampStart, timestampEnd,
                RollupDurationType.HOURLY);
        assertNotNull(hourlyStats);
        // All 3 records should be present initially.
        assertEquals(3, hourlyStats.size());

        // Add to bottom-up daily stats table
        statsSavingsStore.addDailyStats(allStats, dsl, false);

        // Add to bill-based daily stats table. Check to make sure we don't end up deleting this.
        statsSavingsStore.addDailyStats(allStats, dsl, true);

        // Check daily table counts for both daily tables.
        checkDailyCounts(3, 3);

        // Copy same data to monthly as well.
        dsl.insertInto(ENTITY_SAVINGS_BY_MONTH)
                .select(dsl.selectFrom(ENTITY_SAVINGS_BY_DAY))
                .execute();
        // Confirm 3 rows in monthly table.
        assertEquals(3, dsl.fetchCount(ENTITY_SAVINGS_BY_MONTH));

        // Delete anything older than 3 days, only newer stats is retained, older one is deleted.
        setRetentionSettings(3, 1);

        // Create new processor here so that lastRunTime is still null the 2nd time below.
        retentionProcessor = new BottomUpDataRetentionProcessor(statsSavingsStore, auditLogWriter,
                retentionConfig, clock, 1, null);
        retentionProcessor.process();
        hourlyStats = fetchStats(timestampStart, timestampEnd, RollupDurationType.HOURLY);
        assertNotNull(hourlyStats);
        // The older timestampOld & timestampOlder should now be deleted, so that only timestampNew
        // should be retained.
        assertEquals(1, hourlyStats.size());
        assertEquals(timestampNew, hourlyStats.get(0).timestamp);

        // Daily should be the same way, only the timestampNew should be left.
        List<AggregatedSavingsStats> dailyStats = fetchStats(timestampStart, timestampEnd,
                RollupDurationType.DAILY);
        assertNotNull(dailyStats);
        assertEquals(1, dailyStats.size());
        assertEquals(timestampNew, dailyStats.get(0).timestamp);

        // Verify we haven't done anything to bill-based daily stats, so still 3.
        checkDailyCounts(1, 3);

        // For monthly, retention is 1 month, so we should only delete timestampOldest (40 days old),
        // keeping 2 records intact timestampOld and timestampNew.
        List<AggregatedSavingsStats> monthlyStats = fetchStats(timestampStart, timestampEnd,
                RollupDurationType.MONTHLY);
        assertNotNull(monthlyStats);
        assertEquals(2, monthlyStats.size());
        final List<Long> actualMonthlyTimes = monthlyStats
                .stream()
                .map(AggregatedSavingsStats::getTimestamp).collect(Collectors.toList());
        final List<Long> expectedMonthlyTimes = ImmutableList.of(timestampOld, timestampNew);
        assertTrue(CollectionUtils.isEqualCollection(expectedMonthlyTimes, actualMonthlyTimes));

        // Delete anything older than 1 day and 0 for monthly, all stats should be gone now.
        setRetentionSettings(1, 0);

        retentionProcessor = new BottomUpDataRetentionProcessor(statsSavingsStore, auditLogWriter,
                retentionConfig, clock, 1, null);
        retentionProcessor.process();

        // Check hourly table, everything should be deleted.
        hourlyStats = fetchStats(timestampStart, timestampEnd, RollupDurationType.HOURLY);
        assertNotNull(hourlyStats);
        assertEquals(0, hourlyStats.size());

        // Same for monthly, all rows should be gone.
        monthlyStats = fetchStats(timestampStart, timestampEnd,
                RollupDurationType.MONTHLY);
        assertNotNull(monthlyStats);
        assertEquals(0, monthlyStats.size());

        // Confirm daily counts, bill-based should still have 3.
        checkDailyCounts(0, 3);
    }

    /**
     * Checks retention for audit log DB table.
     */
    @Test
    public void auditRetention() {
        // Verify table is empty.
        assertEquals(0, dsl.fetchCount(ENTITY_SAVINGS_AUDIT_EVENTS));

        SavingsEvent event1 = createSavingsEvent(TimeUtil.localDateTimeToMilli(timeOlder, clock),
                vmOid1);
        SavingsEvent event2 = createSavingsEvent(TimeUtil.localDateTimeToMilli(timeOld, clock),
                vmOid2);
        SavingsEvent event3 = createSavingsEvent(TimeUtil.localDateTimeToMilli(timeNew, clock),
                vmOid1);

        auditLogWriter.write(ImmutableList.of(event1, event2, event3));

        // Verify table has 3 entries now.
        assertEquals(3, dsl.fetchCount(ENTITY_SAVINGS_AUDIT_EVENTS));

        // Delete anything older than 3 days.
        setRetentionSettings(3, 1);
        retentionProcessor = new BottomUpDataRetentionProcessor(statsSavingsStore, auditLogWriter,
                retentionConfig, clock, 1, null);
        retentionProcessor.process();

        // 2 records should get deleted, leaving only 1.
        assertEquals(1, dsl.fetchCount(ENTITY_SAVINGS_AUDIT_EVENTS));

        // Delete anything older than 1 day back.
        setRetentionSettings(1, 1);
        retentionProcessor = new BottomUpDataRetentionProcessor(statsSavingsStore, auditLogWriter,
                retentionConfig, clock, 1, null);
        retentionProcessor.process();

        // Verify all records are gone.
        assertEquals(0, dsl.fetchCount(ENTITY_SAVINGS_AUDIT_EVENTS));
    }

    /**
     * Helper to create a savings audit event.
     *
     * @param timestamp Event timestamp.
     * @param entityOid Entity OID.
     * @return Instance of SavingsEvent.
     */
    private SavingsEvent createSavingsEvent(long timestamp, long entityOid) {
        return new SavingsEvent.Builder()
                .entityId(entityOid)
                .timestamp(timestamp)
                .topologyEvent(new TopologyEvent.Builder()
                        .eventType(EventType.STATE_CHANGE.getValue())
                        .timestamp(timestamp)
                        .entityOid(entityOid)
                        .entityType(EntityType.VIRTUAL_MACHINE.getValue())
                        .poweredOn(true)
                        .build())
                .build();
    }

    /**
     * Helper to get mock set data retention settings.
     *
     * @param daysBack How long in days to retain data, used for hourly and daily setting.
     * @param monthsBack Months back setting, 0 to be used for deleting all monthly data.
     */
    private void setRetentionSettings(long daysBack, long monthsBack) {
        when(retentionConfig.fetchDataRetentionSettings())
                .thenReturn(new DataRetentionSettings(daysBack * 24L,
                        daysBack * 24L, daysBack, monthsBack,
                        168));
    }
}
