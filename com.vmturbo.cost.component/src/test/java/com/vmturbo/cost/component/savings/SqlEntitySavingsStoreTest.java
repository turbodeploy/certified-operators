package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.records.EntityCloudScopeRecord;
import com.vmturbo.cost.component.savings.EntitySavingsStore.LastRollupTimes;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Used to test savings related DB read/write codebase.
 */
public class SqlEntitySavingsStoreTest {
    private final Logger logger = LogManager.getLogger();

    /**
     * Handle to store for DB access.
     */
    private SqlEntitySavingsStore store;

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
     * Clock to keeping track of times.
     */
    private final MutableFixedClock clock = new MutableFixedClock(1_000_000_000);

    /**
     * Context to execute DB queries and inserts.
     */
    private final DSLContext dsl = dbConfig.getDslContext();

    /**
     * For double checks.
     */
    private static final double EPSILON_PRECISION = 0.0000001d;

    /**
     * ID of VM 1.
     */
    private static final long vm1Id = 100L;

    /**
     * ID of VM 2.
     */
    private static final long vm2Id = 200L;

    /**
     * Realized savings test value.
     */
    private static final double realizedSavings = 1.0d;

    /**
     * Missed savings test value.
     */
    private static final double missedSavings = 2.0d;

    /**
     * Realized investments test value.
     */
    private static final double realizedInvestments = 4.0d;

    /**
     * Missed investments test value.
     */
    private static final double missedInvestments = 8.0d;

    /**
     * Get exact time, strip of the minute part: 1990-01-12T13:00
     */
    private final LocalDateTime timeExact1PM = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().plusYears(20).truncatedTo(ChronoUnit.HOURS);
    private final LocalDateTime timeExact0PM = timeExact1PM.minusHours(1); // 1990-01-12T12:00
    private final LocalDateTime timeExact2PM = timeExact1PM.plusHours(1); // 1990-01-12T14:00
    private final LocalDateTime timeExact3PM = timeExact1PM.plusHours(2); // 1990-01-12T15:00
    private final LocalDateTime timeExact4PM = timeExact1PM.plusHours(3); // 1990-01-12T16:00

    /**
     * For testing rollup processing.
     */
    private RollupSavingsProcessor rollupProcessor;

    /**
     * Initializing store.
     *
     * @throws Exception Throw on DB init errors.
     */
    @Before
    public void setup() throws Exception {
        store = new SqlEntitySavingsStore(dsl, clock, 5);
        rollupProcessor = new RollupSavingsProcessor(store, clock);
    }

    /**
     * Testing reads/writes for hourly stats table.
     *
     * @throws EntitySavingsException Thrown on DB error.
     * @throws IOException Thrown on insert scope DB error.
     */
    @Test
    public void testHourlyStats() throws EntitySavingsException, IOException {
        final Set<EntitySavingsStats> hourlyStats = new HashSet<>();

        setStatsValues(hourlyStats, vm1Id, timeExact1PM, 10, null); // VM1 at 1PM.
        setStatsValues(hourlyStats, vm2Id, timeExact1PM, 20, null); // VM2 at 1PM.

        setStatsValues(hourlyStats, vm1Id, timeExact2PM, 30, null); // VM1 at 2PM.
        setStatsValues(hourlyStats, vm2Id, timeExact2PM, 40, null); // VM2 at 2PM.

        setStatsValues(hourlyStats, vm1Id, timeExact3PM, 50, null); // VM1 at 3PM.
        setStatsValues(hourlyStats, vm2Id, timeExact3PM, 50, null); // VM2 at 3PM.

        // Write it.
        store.addHourlyStats(hourlyStats, dsl);

        final Set<EntitySavingsStatsType> allStatsTypes = Arrays.stream(EntitySavingsStatsType
                .values()).collect(Collectors.toSet());

        // Read back and verify.
        Collection<Long> entityOids = ImmutableSet.of(vm1Id, vm2Id);
        Collection<Integer> entityTypes = Collections.singleton(EntityType.VIRTUAL_MACHINE_VALUE);
        Collection<Long> billingFamilies = new HashSet<>();
        Collection<Long> resourceGroups = new HashSet<>();

        List<AggregatedSavingsStats> statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact0PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                entityOids, entityTypes, billingFamilies, resourceGroups);
        // 0 stats sets, for 12-1 PM range, because end time (1 PM) is exclusive.
        assertEquals(0, statsReadBack.size());

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                entityOids, entityTypes, billingFamilies, resourceGroups);
        // 1 stats sets of 4 stats types, aggregated for both VMs, both 1PM, for 1-2 PM range.
        assertEquals(4, statsReadBack.size());

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                entityOids, entityTypes, billingFamilies, resourceGroups);
        // 1 stats sets of 4 stats types, aggregated for both VMs, for 2-3 PM range.
        assertEquals(4, statsReadBack.size());

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact4PM, clock),
                entityOids, entityTypes, billingFamilies, resourceGroups);
        // 1 stats sets of 4 stats types, aggregated for both VMs, both 3PM, for 3-4 PM range.
        assertEquals(4, statsReadBack.size());
        checkStatsValues(statsReadBack, vm1Id, timeExact3PM, 50, vm2Id);

        // Verify data for 1 VM.
        entityOids = ImmutableSet.of(vm1Id);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact4PM, clock),
                entityOids, entityTypes, billingFamilies, resourceGroups);
        // 1 stats sets of 4 stats types, for the 1 VM, both 3PM, for 3-4 PM range.
        assertEquals(4, statsReadBack.size());
        checkStatsValues(statsReadBack, vm1Id, timeExact3PM, 50, null);
    }

    @Test
    public void rollupToDailyAndMonthly() throws IOException, EntitySavingsException {
        final Set<EntitySavingsStats> hourlyStats = new HashSet<>();

        final Set<EntitySavingsStatsType> statsTypes = ImmutableSet.of(
                EntitySavingsStatsType.REALIZED_SAVINGS);

        // Start of day range query.
        final long dayRangeStart = TimeUtil.localDateTimeToMilli(timeExact1PM.minusDays(1), clock);
        final long monthRangeStart = 0L;

        final LocalDateTime startTime = timeExact1PM.plusHours(12); // 01/13 1:00 AM
        final List<Long> hourlyTimes = new ArrayList<>();

        // Times being inserted are like this:
        // Jan-13: 23 hrs - Starting at 1:00 AM (inclusive), up to midnight 12:00 AM (exclusive)
        // Jan-14: 24 hrs - 12:00 AM (inclusive) up to 12:00 AM midnight (exclusive)
        // Jan-15: 03 hrs - 12:00 AM (inclusive) up to 3:00 AM (exclusive)

        // This results in following rollup scenario:
        // Hourly to Daily: All 50 hours get rolled up to their respective daily table, as these
        //      hourly stats are 'complete' and hence can be safely rolled up to daily.
        // Daily to Monthly: 'Complete' days get rolled up to monthly (end of month time).
        //      Jan-13: Done as we noticed next day Jan-14.
        //      Jan-14: Done as we noticed next day Jan-15
        // Jan-15: Day is still 'in progress', so doesn't get rolled up to monthly yet.
        for (int hour = 0; hour < 50; hour++) {
            final LocalDateTime thisTime = startTime.plusHours(hour);
            setStatsValues(hourlyStats, vm1Id, thisTime, 1, statsTypes);

            long timestamp = TimeUtil.localDateTimeToMilli(thisTime, clock);
            hourlyTimes.add(timestamp);
        }

        // Start of day range query.
        final long dayRangeEnd = TimeUtil.localDateTimeToMilli(timeExact1PM.plusDays(7), clock);
        final long monthRangeEnd = SavingsUtil.getMonthEndTime(timeExact1PM.plusMonths(2), clock);

        store.addHourlyStats(hourlyStats, dsl);
        rollupProcessor.process(hourlyTimes);

        final LastRollupTimes newLastTimes = store.getLastRollupTimes();
        assertNotNull(newLastTimes);
        logger.info("New last rollup times: {}", newLastTimes);
        // First hour inserted into DB - Thu Jan 13 1990 01:00:00
        final long firstHourExpected = 632192400000L;

        // Last hourly data that was rolled up - Thu Jan 15 1990 02:00:00
        final long lastHourExpected = 632368800000L;

        // Last day that was rolled up - Wed Jan 14 1990 00:00:00
        // Jan 15 is still 'in progress', so its daily rollup is not yet completed.
        final long lastDayExpected = 632275200000L;

        // Month end to which daily data has been rolled up - Sat Jan 31 1990 00:00:00
        final long lastMonthExpected = 633744000000L;
        assertEquals(lastHourExpected, newLastTimes.getLastTimeByHour());
        assertEquals(lastHourExpected, (long)hourlyTimes.get(hourlyTimes.size() - 1));
        assertEquals(lastDayExpected, newLastTimes.getLastTimeByDay());
        assertEquals(lastMonthExpected, newLastTimes.getLastTimeByMonth());

        Collection<Long> entityOids = ImmutableSet.of(vm1Id);
        Collection<Integer> entityTypes = Collections.singleton(EntityType.VIRTUAL_MACHINE_VALUE);
        Collection<Long> billingFamilies = new HashSet<>();
        Collection<Long> resourceGroups = new HashSet<>();
        final List<AggregatedSavingsStats> statsByHour = store.getHourlyStats(statsTypes,
                firstHourExpected, (lastHourExpected + 1), entityOids, entityTypes, billingFamilies,
                resourceGroups);
        assertNotNull(statsByHour);
        int sizeHourly = statsByHour.size();
        assertEquals(50, sizeHourly);
        assertEquals(firstHourExpected, statsByHour.get(0).getTimestamp());
        assertEquals(lastHourExpected, statsByHour.get(sizeHourly - 1).getTimestamp());

        final List<AggregatedSavingsStats> statsByDay = store.getDailyStats(statsTypes,
                dayRangeStart, dayRangeEnd, entityOids, entityTypes, billingFamilies, resourceGroups);
        assertNotNull(statsByDay);
        assertEquals(3, statsByDay.size());
        // 23 hours Jan-13, $100/hr
        assertEquals(2300.0, statsByDay.get(0).getValue(), EPSILON_PRECISION);
        // 24 hours Jan-14, $100/hr
        assertEquals(2400.0, statsByDay.get(1).getValue(), EPSILON_PRECISION);
        // 3 hours Jan-15, $100/hr
        assertEquals(300.0, statsByDay.get(2).getValue(), EPSILON_PRECISION);

        final List<AggregatedSavingsStats> statsByMonth = store.getMonthlyStats(statsTypes,
                monthRangeStart, monthRangeEnd, entityOids, entityTypes, billingFamilies, resourceGroups);
        assertNotNull(statsByMonth);
        assertEquals(1, statsByMonth.size());
        assertEquals(4700.0, statsByMonth.get(0).getValue(), EPSILON_PRECISION);
    }

    /**
     * Check if query based on various time frames (hour/day/month/year) works as expected.
     *
     * @throws IOException On IO error.
     * @throws EntitySavingsException On DB access error.
     */
    @Test
    public void timeframeStatsQuery() throws IOException, EntitySavingsException {
        final Set<EntitySavingsStats> hourlyStats = new HashSet<>();
        final List<Long> hourlyTimes = new ArrayList<>();

        final Set<EntitySavingsStatsType> statsTypes = ImmutableSet.of(
                EntitySavingsStatsType.REALIZED_INVESTMENTS);

        final LocalDateTime year1monthBack = timeExact1PM.minusMonths(11);
        final LocalDateTime anHourLater = timeExact1PM.plusHours(1);

        // Start at 2 AM.
        final LocalDateTime startTime = year1monthBack.toLocalDate().atStartOfDay().plusHours(2);
        int hour = 0;
        long timestamp;
        long endTimeMillis = TimeUtil.localDateTimeToMilli(anHourLater, clock);
        do {
            final LocalDateTime thisTime = startTime.plusHours(hour++);
            setStatsValues(hourlyStats, vm1Id, thisTime, 1, statsTypes);

            timestamp = TimeUtil.localDateTimeToMilli(thisTime, clock);
            hourlyTimes.add(timestamp);
        } while (timestamp < endTimeMillis);

        store.addHourlyStats(hourlyStats, dsl);
        rollupProcessor.process(hourlyTimes);

        final LastRollupTimes newLastTimes = store.getLastRollupTimes();
        assertNotNull(newLastTimes);
        logger.info("Total {} times. New last rollup times: {}", hourlyTimes.size(),
                newLastTimes);

        Collection<Long> entityOids = ImmutableSet.of(vm1Id);
        Collection<Integer> entityTypes = Collections.singleton(EntityType.VIRTUAL_MACHINE_VALUE);
        Collection<Long> billingFamilies = new HashSet<>();
        Collection<Long> resourceGroups = new HashSet<>();

        long startTimeMillis = TimeUtil.localDateTimeToMilli(year1monthBack.minusHours(1), clock);
        final List<AggregatedSavingsStats> hourlyResult = store.getSavingsStats(TimeFrame.HOUR,
                statsTypes,
                startTimeMillis, endTimeMillis + 1000,
                entityOids, entityTypes, billingFamilies, resourceGroups);
        assertNotNull(hourlyResult);

        final List<AggregatedSavingsStats> dailyResult = store.getSavingsStats(TimeFrame.DAY,
                statsTypes,
                startTimeMillis, endTimeMillis + 1000,
                entityOids, entityTypes, billingFamilies, resourceGroups);
        assertNotNull(dailyResult);

        final List<AggregatedSavingsStats> monthlyResult = store.getSavingsStats(TimeFrame.MONTH,
                statsTypes,
                startTimeMillis, endTimeMillis + 1000,
                entityOids, entityTypes, billingFamilies, resourceGroups);
        assertNotNull(monthlyResult);

        logger.info("Hourly result count: {}, Daily count: {}, Monthly count: {}",
                hourlyResult.size(), dailyResult.size(), monthlyResult.size());

        assertEquals(8018, hourlyResult.size());
        // 400.0 each day.
        AggregatedSavingsStats result = hourlyResult.get(0);
        assertEquals(400.0, result.value, EPSILON_PRECISION);
        assertEquals(603288000000L, result.getTimestamp());

        result = hourlyResult.get(hourlyResult.size() - 2);
        assertEquals(400.0, result.value, EPSILON_PRECISION);
        assertEquals(632149200000L, result.getTimestamp());

        result = hourlyResult.get(hourlyResult.size() - 1);
        assertEquals(400.0, result.value, EPSILON_PRECISION);
        assertEquals(632152800000L, result.getTimestamp());

        assertEquals(334, dailyResult.size());
        // 400.0 per hour over 24 hours.
        result = dailyResult.get(0);
        assertEquals(9600.0, result.value, EPSILON_PRECISION);
        assertEquals(603331200000L, result.getTimestamp());

        // 400.0 per hour over 24 hours.
        result = dailyResult.get(dailyResult.size() - 2);
        assertEquals(9600.0, result.value, EPSILON_PRECISION);
        assertEquals(632016000000L, result.getTimestamp());

        // Last day, only 15, not full 24 hours.
        result = dailyResult.get(dailyResult.size() - 1);
        assertEquals(6000.0, result.value, EPSILON_PRECISION);
        assertEquals(632102400000L, result.getTimestamp());

        assertEquals(11, monthlyResult.size());
        // Month totals varies by days in the month.
        result = monthlyResult.get(0);
        assertEquals(162400.0, result.value, EPSILON_PRECISION);
        assertEquals(604627200000L, result.getTimestamp());

        result = monthlyResult.get(monthlyResult.size() - 2);
        assertEquals(288000.0, result.value, EPSILON_PRECISION);
        assertEquals(628387200000L, result.getTimestamp());

        result = monthlyResult.get(monthlyResult.size() - 1);
        assertEquals(297600.0, result.value, EPSILON_PRECISION);
        assertEquals(631065600000L, result.getTimestamp());
    }

    /**
     * Util method to add a new stats object to the input set.
     *
     * @param hourlyStats Set of stats to be inserted into DB.
     * @param vmId ID of VM.
     * @param dateTime Stats time.
     * @param multiple Multiple value used for dummy stats data.
     * @param requiredStatsTypes Stats types to set values for.
     */
    private void setStatsValues(@Nonnull final Set<EntitySavingsStats> hourlyStats, long vmId,
            @Nonnull final LocalDateTime dateTime, int multiple,
            @Nullable Set<EntitySavingsStatsType> requiredStatsTypes) {
        long timestamp = TimeUtil.localDateTimeToMilli(dateTime, clock);
        EntitySavingsStatsType statsType = EntitySavingsStatsType.REALIZED_SAVINGS;
        if (requiredStatsTypes == null || requiredStatsTypes.contains(statsType)) {
            hourlyStats.add(new EntitySavingsStats(vmId, timestamp, statsType,
                    getDummyValue(statsType, multiple, vmId)));
        }

        statsType = EntitySavingsStatsType.MISSED_SAVINGS;
        if (requiredStatsTypes == null || requiredStatsTypes.contains(statsType)) {
            hourlyStats.add(new EntitySavingsStats(vmId, timestamp, statsType,
                    getDummyValue(statsType, multiple, vmId)));
        }

        statsType = EntitySavingsStatsType.REALIZED_INVESTMENTS;
        if (requiredStatsTypes == null || requiredStatsTypes.contains(statsType)) {
            hourlyStats.add(new EntitySavingsStats(vmId, timestamp, statsType,
                    getDummyValue(statsType, multiple, vmId)));
        }

        statsType = EntitySavingsStatsType.MISSED_INVESTMENTS;
        if (requiredStatsTypes == null || requiredStatsTypes.contains(statsType)) {
            hourlyStats.add(new EntitySavingsStats(vmId, timestamp, statsType,
                    getDummyValue(statsType, multiple, vmId)));
        }
    }

    /**
     * Verifies savings/investments fields in stats object to verify they are as expected.
     *
     * @param statsReadBack Stats object read back from DB.
     * @param vmId Id of VM.
     * @param dateTime Stats time.
     * @param multiple Multiple value used for dummy stats data.
     * @param vm2Id Id of 2nd VM, if non-null.
     */
    private void checkStatsValues(@Nonnull final List<AggregatedSavingsStats> statsReadBack,
            long vmId, @Nonnull final LocalDateTime dateTime, int multiple, @Nullable Long vm2Id) {
        assertNotNull(statsReadBack);
        statsReadBack.forEach(stats -> {
                    assertEquals(TimeUtil.localDateTimeToMilli(dateTime, clock),
                            stats.getTimestamp());

                    double expectedValue = getDummyValue(stats.getType(), multiple, vmId);
                    if (vm2Id != null) {
                        expectedValue += getDummyValue(stats.getType(), multiple, vm2Id);
                    }
                    assertEquals(expectedValue, stats.getValue(), EPSILON_PRECISION);
                });
    }

    /**
     * Creates dummy values based on stats type, that get inserted into DB, and they are verified
     * when value is read back.
     *
     * @param statsType Type of stats.
     * @param multiple Multiple value constant.
     * @param vmId Id of VM.
     * @return Stats dummy value.
     */
    private double getDummyValue(EntitySavingsStatsType statsType, int multiple, long vmId) {
        switch (statsType) {
            case REALIZED_SAVINGS:
                return realizedSavings * multiple * vmId;
            case MISSED_SAVINGS:
                return missedSavings * multiple * vmId;
            case REALIZED_INVESTMENTS:
                return realizedInvestments * multiple * vmId;
            case MISSED_INVESTMENTS:
                return missedInvestments * multiple * vmId;
        }
        return 0d;
    }

    /**
     * Test queries for savings stats that require a join with the entity_cloud_scope table.
     *
     * @throws EntitySavingsException errors when writing to database
     */
    @Test
    public void testGetStatsUsingScopeTable() throws EntitySavingsException {
        Long entityOid1 = 1L;
        Long entityOid2 = 2L;
        Long accountId = 100L;
        Long regionOid1 = 1000L;
        Long regionOid2 = 2000L;
        Long zoneOid1 = 10L;
        Long zoneOid2 = 20L;
        Long serviceProviderOid = 10000L;
        Long billingFamilyOid1 = 300000L;
        Long billingFamilyOid2 = 400000L;
        Long resourceGroupOid1 = 100000L;
        Long resourceGroupOid2 = 200000L;
        EntityCloudScopeRecord r1 = createEntityCloudScopeRecord(entityOid1, accountId, regionOid1,
                zoneOid1, serviceProviderOid, billingFamilyOid1, resourceGroupOid1);

        EntityCloudScopeRecord r2 = createEntityCloudScopeRecord(entityOid2, accountId, regionOid2,
                zoneOid2, serviceProviderOid, billingFamilyOid2, resourceGroupOid2);

        // Insert 2 records into the scope table. Both are VMs in the same account.
        dsl.insertInto(Tables.ENTITY_CLOUD_SCOPE).set(r1).execute();
        dsl.insertInto(Tables.ENTITY_CLOUD_SCOPE).set(r2).execute();

        // Insert stats into entity_savings_by_hour table.
        final Set<EntitySavingsStats> hourlyStats = new HashSet<>();
        hourlyStats.add(new EntitySavingsStats(entityOid1, TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                EntitySavingsStatsType.REALIZED_SAVINGS, 10d));
        hourlyStats.add(new EntitySavingsStats(entityOid2, TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                EntitySavingsStatsType.REALIZED_SAVINGS, 20d));
        store.addHourlyStats(hourlyStats, dsl);

        // 1. Query by account ID
        // Both VMs are in the same account. Realized savings value equal to the same of the 2.
        Collection<Integer> entityTypes = Collections.singleton(EntityType.BUSINESS_ACCOUNT_VALUE);
        final Set<EntitySavingsStatsType> allStatsTypes = Arrays.stream(EntitySavingsStatsType
                .values()).collect(Collectors.toSet());
        Collection<Long> billingFamilies = new HashSet<>();
        Collection<Long> resourceGroups = new HashSet<>();

        List<AggregatedSavingsStats> statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                ImmutableSet.of(accountId), entityTypes, billingFamilies, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(30), statsReadBack.get(0).getValue());

        // 2. Query by Region ID
        // VM1 is in region 1, and VM2 is in region 2. The query ask for region 1. So only savings
        // of VM1 is returned.
        entityTypes = Collections.singleton(EntityType.REGION_VALUE);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                ImmutableSet.of(regionOid1), entityTypes, billingFamilies, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(10), statsReadBack.get(0).getValue());

        // 3. Query by Availability Zone ID
        // VM1 is in AZ 1, and VM2 is in AZ 2. The query ask for AZ 2. So only savings
        // of VM2 is returned.
        entityTypes = Collections.singleton(EntityType.AVAILABILITY_ZONE_VALUE);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                ImmutableSet.of(zoneOid2), entityTypes, billingFamilies, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(20), statsReadBack.get(0).getValue());

        // 4. Query by service provider
        // Both VMs are in the same service provider. Realized savings value equal to the same of the 2.
        entityTypes = Collections.singleton(EntityType.SERVICE_PROVIDER_VALUE);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                ImmutableSet.of(serviceProviderOid), entityTypes, billingFamilies, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(30), statsReadBack.get(0).getValue());

        // 5. Query by resource group
        // VM2 is in resource group 2. Savings of VM2 will be returned.
        resourceGroups.add(resourceGroupOid2);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                Collections.emptySet(), Collections.emptySet(), billingFamilies, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(20), statsReadBack.get(0).getValue());

        // 6a. Query by billing family
        // Query for billing Family OID 1. VM1 stats should be returned.
        billingFamilies.add(billingFamilyOid1);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                Collections.emptySet(), Collections.emptySet(), billingFamilies, new HashSet<>());
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(10), statsReadBack.get(0).getValue());

        // 6b. Now query for both billing families. Stats for VM1 and VM2 are both returned.
        billingFamilies.add(billingFamilyOid2);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                Collections.emptySet(), Collections.emptySet(), billingFamilies, new HashSet<>());
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(30), statsReadBack.get(0).getValue());
    }

    private EntityCloudScopeRecord createEntityCloudScopeRecord(Long entityOid, Long accountOid,
            Long regionOid, Long availabilityZoneOid, Long serviceProviderOid, Long billingFamilyOid,
            Long resourceGroupOid) {
        EntityCloudScopeRecord record = new EntityCloudScopeRecord();
        record.setAccountOid(accountOid);
        record.setEntityOid(entityOid);
        record.setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        record.setRegionOid(regionOid);
        record.setAvailabilityZoneOid(availabilityZoneOid);
        record.setServiceProviderOid(serviceProviderOid);
        record.setBillingFamilyOid(billingFamilyOid);
        record.setResourceGroupOid(resourceGroupOid);
        record.setCreationTime(LocalDateTime.now());
        return record;
    }
}
