package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import com.google.common.collect.ImmutableSet;

import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.savings.EntitySavingsRetentionConfig.DataRetentionSettings;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Tests for data retention processor.
 */
public class DataRetentionProcessorTest {
    /**
     * Handle to store for stats DB table access.
     */
    private EntitySavingsStore statsSavingsStore;

    /**
     * Instance of retention processor.
     */
    private DataRetentionProcessor retentionProcessor;

    /**
     * Config providing access to DB. Also ClassRule to init Db and upgrade to latest.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to clean up temp test DB.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Clock to keeping track of times. UTC: June 4, 2021 12:00:00 AM
     */
    private final MutableFixedClock clock = new MutableFixedClock(1622764800000L);

    /**
     * Context to execute DB queries and inserts.
     */
    private final DSLContext dsl = dbConfig.getDslContext();

    /**
     * OIDs for VMs for testing.
     */
    private final long vmOid1 = 101L;

    /**
     * VM2 oid.
     */
    private final long vmOid2 = 202L;

    /**
     * Time that is old and will get purged as part of retention processing.
     */
    private final LocalDateTime timeOld = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().minusDays(4).truncatedTo(ChronoUnit.HOURS);

    /**
     * Newer time that won't get purged.
     */
    private final LocalDateTime timeNew = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().minusDays(2).truncatedTo(ChronoUnit.HOURS);

    /**
     * End time for queries.
     */
    private final LocalDateTime timeEnd = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().plusDays(1).truncatedTo(ChronoUnit.HOURS);

    /**
     * Mock config for retention.
     */
    private final EntitySavingsRetentionConfig retentionConfig = mock(
            EntitySavingsRetentionConfig.class);

    /**
     * Setting up stuff.
     *
     * @throws Exception Thrown on init exception.
     */
    @Before
    public void setup() throws Exception {
        statsSavingsStore = new SqlEntitySavingsStore(dsl, clock, 5);
        AuditLogWriter auditLogWriter = new SqlAuditLogWriter(dsl, clock, 5);
        retentionProcessor = new DataRetentionProcessor(statsSavingsStore, auditLogWriter,
                retentionConfig, clock, 1);
    }

    /**
     * Inserts some hourly stats and then tests retention settings by deleting older stats data
     * to verify data is getting deleted as per requested config.
     *
     * @throws EntitySavingsException Thrown on DB access error.
     */
    @Test
    public void verifyDataRetention() throws EntitySavingsException {
        long timestampOld = TimeUtil.localDateTimeToMilli(timeOld, clock);
        final EntitySavingsStats stats1 = new EntitySavingsStats(vmOid1, timestampOld,
                EntitySavingsStatsType.MISSED_SAVINGS, 10.02);

        long timestampNew = TimeUtil.localDateTimeToMilli(timeNew, clock);
        final EntitySavingsStats stats2 = new EntitySavingsStats(vmOid2, timestampNew,
                EntitySavingsStatsType.REALIZED_INVESTMENTS, 20.03);

        long timestampEnd = TimeUtil.localDateTimeToMilli(timeEnd, clock);

        statsSavingsStore.addHourlyStats(ImmutableSet.of(stats1, stats2), dsl);

        List<AggregatedSavingsStats> hourlyStats = fetchHourlyStats(timestampOld, timestampEnd);
        assertNotNull(hourlyStats);
        // Both timestampOld and timestampNew should be present initially.
        assertEquals(2, hourlyStats.size());

        // Delete anything older than 3 days, only newer stats is retained, older one is deleted.
        long daysBack = 3;
        DataRetentionSettings hourlySettings = new DataRetentionSettings(1L,
                daysBack * 24L, 1L, 1L);
        when(retentionConfig.fetchDataRetentionSettings()).thenReturn(hourlySettings);


        retentionProcessor.process(true);
        hourlyStats = fetchHourlyStats(timestampOld, timestampEnd);
        assertNotNull(hourlyStats);
        // The older timestampOld should now be deleted, only timestampNew should be retained.
        assertEquals(1, hourlyStats.size());
        assertEquals(timestampNew, hourlyStats.get(0).timestamp);


        // Delete anything older than 1 day, both stats should be gone now.
        daysBack = 1;
        hourlySettings = new DataRetentionSettings(1L,
                daysBack * 24L, 1L, 1L);
        when(retentionConfig.fetchDataRetentionSettings()).thenReturn(hourlySettings);

        retentionProcessor.process(true);
        hourlyStats = fetchHourlyStats(timestampOld, timestampEnd);
        assertNotNull(hourlyStats);
        // Both timestampOld and timestampNew should be deleted.
        assertEquals(0, hourlyStats.size());
    }

    /**
     * Convenience function query hourly stats.
     *
     * @param startTime Stats start time.
     * @param endTime Stats end time.
     * @return Queried hourly stats.
     * @throws EntitySavingsException Thrown on DB access error.
     */
    private List<AggregatedSavingsStats> fetchHourlyStats(long startTime, long endTime)
            throws EntitySavingsException {
        final Set<EntitySavingsStatsType> statsTypes = ImmutableSet.of(
                EntitySavingsStatsType.MISSED_SAVINGS,
                EntitySavingsStatsType.REALIZED_INVESTMENTS);
        final Set<Long> entityOids = ImmutableSet.of(vmOid1, vmOid2);
        return statsSavingsStore.getHourlyStats(statsTypes, startTime, endTime,
                entityOids, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList());
    }
}
