package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
import com.vmturbo.cost.component.db.Cost;
import com.vmturbo.cost.component.db.Tables;
import com.vmturbo.cost.component.db.tables.EntitySavingsByHour;
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
     * Config providing access to DB. Also, ClassRule to init Db and upgrade to latest.
     */
    @ClassRule
    public static DbConfigurationRule dbConfig = new DbConfigurationRule(Cost.COST);

    /**
     * Rule to clean up temp test DB.
     */
    @Rule
    public DbCleanupRule dbCleanup = dbConfig.cleanupRule();

    /**
     * Fixed clock to keep track of times. Pinned to shortly after 1pm on 1/12/1970.
     */
    private final Clock clock = Clock.fixed(Instant.ofEpochMilli(1_000_000_000L),
            ZoneId.from(ZoneOffset.UTC));

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
    private static final double realizedSavings = 0.0001d;

    /**
     * Missed savings test value.
     */
    private static final double missedSavings = 0.0002;

    /**
     * Realized investments test value.
     */
    private static final double realizedInvestments = 0.0004d;

    /**
     * Missed investments test value.
     */
    private static final double missedInvestments = 0.0008d;

    /**
     * Get some exact times in a day based on our fixed clock.
     */
    private final LocalDateTime timeExact0PM = Instant.now(clock).atZone(ZoneOffset.UTC)
            .toLocalDateTime().plusYears(20).truncatedTo(ChronoUnit.DAYS).plusHours(12);
    private final LocalDateTime timeExact1PM = timeExact0PM.plusHours(1);
    private final LocalDateTime timeExact2PM = timeExact1PM.plusHours(1);
    private final LocalDateTime timeExact3PM = timeExact1PM.plusHours(2);
    private final LocalDateTime timeExact4PM = timeExact1PM.plusHours(3);

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
    public void setup() {
        // some of our conversions depend on timezone being set to UTC
        TimeZone.setDefault(TimeZone.getTimeZone(ZoneOffset.UTC));
        store = new SqlEntitySavingsStore(dsl, clock, 1000);
        rollupProcessor = new RollupSavingsProcessor(store, clock);
    }

    /**
     * Testing reads/writes for hourly stats table.
     *
     * @throws EntitySavingsException Thrown on DB error.
     */
    @Test
    public void testHourlyStats() throws EntitySavingsException {
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
        Collection<Long> resourceGroups = new HashSet<>();

        List<AggregatedSavingsStats> statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact0PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                entityOids, entityTypes, resourceGroups);
        // 0 stats sets, for 12-1 PM range, because end time (1 PM) is exclusive.
        assertEquals(0, statsReadBack.size());

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                entityOids, entityTypes, resourceGroups);
        // 1 stats sets of 4 stats types, aggregated for both VMs, both 1PM, for 1-2 PM range.
        assertEquals(4, statsReadBack.size());

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                entityOids, entityTypes, resourceGroups);
        // 1 stats sets of 4 stats types, aggregated for both VMs, for 2-3 PM range.
        assertEquals(4, statsReadBack.size());

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact4PM, clock),
                entityOids, entityTypes, resourceGroups);
        // 1 stats sets of 4 stats types, aggregated for both VMs, both 3PM, for 3-4 PM range.
        assertEquals(4, statsReadBack.size());
        checkStatsValues(statsReadBack, vm1Id, timeExact3PM, 50, vm2Id);

        // Verify data for 1 VM.
        entityOids = ImmutableSet.of(vm1Id);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact4PM, clock),
                entityOids, entityTypes, resourceGroups);
        // 1 stats sets of 4 stats types, for the 1 VM, both 3PM, for 3-4 PM range.
        assertEquals(4, statsReadBack.size());
        checkStatsValues(statsReadBack, vm1Id, timeExact3PM, 50, null);
    }

    /**
     * Test that newly added savings data is correctly rolled up.
     *
     * @throws EntitySavingsException if there's a problem with the store
     */
    @Test
    public void rollupToDailyAndMonthly() throws EntitySavingsException {
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

        // add new data to the store, and compute eligible rollups
        store.addHourlyStats(hourlyStats, dsl);
        rollupProcessor.process(hourlyTimes);

        final LastRollupTimes newLastTimes = store.getLastRollupTimes();
        assertNotNull(newLastTimes);
        logger.info("New last rollup times: {}", newLastTimes);

        // Start of day range query.
        final long dayRangeEnd = TimeUtil.localDateTimeToMilli(timeExact1PM.plusDays(7), clock);
        final long monthRangeEnd = SavingsUtil.getMonthEndTime(timeExact1PM.plusMonths(2), clock);

        // First hour inserted into DB - Thu Jan 13 1990 01:00:00
        final long firstHourExpected = Instant.parse("1990-01-13T01:00:00Z").toEpochMilli();

        // Last hourly data that was rolled up - Thu Jan 15 1990 02:00:00
        final long lastHourExpected = Instant.parse("1990-01-15T02:00:00Z").toEpochMilli();

        // Last day that was rolled up - Wed Jan 14 1990 00:00:00
        // Jan 15 is still 'in progress', so its daily rollup is not yet completed.
        final long lastDayExpected = Instant.parse("1990-01-14T00:00:00Z").toEpochMilli();

        // Month end to which daily data has been rolled up - Sat Jan 31 1990 00:00:00
        final long lastMonthExpected = Instant.parse("1990-01-31T00:00:00Z").toEpochMilli();

        // check rollup metadata values
        assertEquals(lastHourExpected, newLastTimes.getLastTimeByHour());
        assertEquals(lastDayExpected, newLastTimes.getLastTimeByDay());
        assertEquals(lastMonthExpected, newLastTimes.getLastTimeByMonth());

        // perform hourly, daily and monthly queries to the store and check results
        Collection<Long> entityOids = ImmutableSet.of(vm1Id);
        Collection<Integer> entityTypes = Collections.singleton(EntityType.VIRTUAL_MACHINE_VALUE);
        Collection<Long> billingFamilies = new HashSet<>();
        Collection<Long> resourceGroups = new HashSet<>();
        final List<AggregatedSavingsStats> statsByHour = store.getHourlyStats(statsTypes,
                firstHourExpected, (lastHourExpected + 1), entityOids, entityTypes,
                resourceGroups);
        assertNotNull(statsByHour);
        int sizeHourly = statsByHour.size();
        assertEquals(50, sizeHourly);
        assertEquals(firstHourExpected, statsByHour.get(0).getTimestamp());
        assertEquals(lastHourExpected, statsByHour.get(sizeHourly - 1).getTimestamp());

        final List<AggregatedSavingsStats> statsByDay = store.getDailyStats(statsTypes,
                dayRangeStart, dayRangeEnd, entityOids, entityTypes, resourceGroups);
        assertNotNull(statsByDay);
        assertEquals(3, statsByDay.size());
        // 23 hours Jan-13, $100/hr
        assertEquals(0.23, statsByDay.get(0).getValue(), EPSILON_PRECISION);
        // 24 hours Jan-14, $100/hr
        assertEquals(0.24, statsByDay.get(1).getValue(), EPSILON_PRECISION);
        // 3 hours Jan-15, $100/hr
        assertEquals(0.03, statsByDay.get(2).getValue(), EPSILON_PRECISION);

        final List<AggregatedSavingsStats> statsByMonth = store.getMonthlyStats(statsTypes,
                monthRangeStart, monthRangeEnd, entityOids, entityTypes, resourceGroups);
        assertNotNull(statsByMonth);
        assertEquals(1, statsByMonth.size());
        assertEquals(0.47, statsByMonth.get(0).getValue(), EPSILON_PRECISION);
    }

    /**
     * Check if query based on various time frames (hour/day/month/year) works as expected.
     *
     * @throws EntitySavingsException On DB access error.
     */
    @Test
    public void testTimeframeStatsQueries() throws EntitySavingsException {
        final Set<EntitySavingsStats> hourlyStats = new HashSet<>();
        final List<Long> hourlyTimes = new ArrayList<>();

        final Set<EntitySavingsStatsType> statsTypes = ImmutableSet.of(
                EntitySavingsStatsType.REALIZED_INVESTMENTS);

        long vmId = 10L;

        // Add hourly stats records every hour, starting at 2am 11 months ago, and up to and
        // including 2pm today.
        LocalDateTime startTime = timeExact1PM.minusMonths(11).toLocalDate().atStartOfDay().plusHours(2);
        long startTimeMillis = TimeUtil.localDateTimeToMilli(startTime, clock);
        LocalDateTime endTime = timeExact1PM.plusHours(1);
        long endTimeMillis = TimeUtil.localDateTimeToMilli(endTime, clock);
        for (long tMillis = startTimeMillis; tMillis <= endTimeMillis; tMillis += TimeUnit.HOURS.toMillis(1)) {
            LocalDateTime time = LocalDateTime.ofInstant(Instant.ofEpochMilli(tMillis), ZoneOffset.UTC);
            setStatsValues(hourlyStats, vmId, time, 1, statsTypes);
            hourlyTimes.add(tMillis);
        }
        // all the stats have the same stats value - remember it for expected rollup results
        final double baseValue = hourlyStats.iterator().next().getValue();
        // save all the hourly records to the hourly table
        store.addHourlyStats(hourlyStats, dsl);
        int count = dsl.fetchCount(EntitySavingsByHour.ENTITY_SAVINGS_BY_HOUR);
        assertEquals(hourlyStats.size(), count);
        // and perform daily and monthly rollups
        rollupProcessor.process(hourlyTimes);

        final LastRollupTimes newLastTimes = store.getLastRollupTimes();
        assertNotNull(newLastTimes);
        logger.info("Total {} times. New last rollup times: {}", hourlyTimes.size(),
                newLastTimes);

        Collection<Long> entityOids = ImmutableSet.of(vmId);
        Collection<Integer> entityTypes = Collections.singleton(EntityType.VIRTUAL_MACHINE_VALUE);
        Collection<Long> billingFamilies = new HashSet<>();
        Collection<Long> resourceGroups = new HashSet<>();

        // query hourly, daily, and monthly data with buffers on both ends of the time span to
        // ensure all records are selected
        final List<AggregatedSavingsStats> hourlyResult = store.getSavingsStats(TimeFrame.HOUR,
                statsTypes,
                startTimeMillis - TimeUnit.HOURS.toMillis(1), endTimeMillis + TimeUnit.HOURS.toMillis(1),
                entityOids, entityTypes, resourceGroups);
        assertNotNull(hourlyResult);

        final List<AggregatedSavingsStats> dailyResult = store.getSavingsStats(TimeFrame.DAY,
                statsTypes,
                startTimeMillis - TimeUnit.DAYS.toMillis(1), endTimeMillis + TimeUnit.DAYS.toMillis(1),
                entityOids, entityTypes, resourceGroups);
        assertNotNull(dailyResult);

        final List<AggregatedSavingsStats> monthlyResult = store.getSavingsStats(TimeFrame.MONTH,
                statsTypes,
                startTimeMillis - TimeUnit.DAYS.toMillis(31), endTimeMillis + TimeUnit.DAYS.toMillis(31),
                entityOids, entityTypes, resourceGroups);
        assertNotNull(monthlyResult);

        logger.info("Hourly result count: {}, Daily count: {}, Monthly count: {}",
                hourlyResult.size(), dailyResult.size(), monthlyResult.size());

        // check the hourly results
        // we should have a record for each of the timestamps we collected
        assertEquals(hourlyStats.size(), hourlyResult.size());
        // each record should have our base value
        hourlyResult.forEach(result -> assertEquals("Failed for hour " + Instant.ofEpochMilli(result.getTimestamp()),
                baseValue, result.value, EPSILON_PRECISION));
        // first record is at our start time, and then they're separated by 1 hour
        assertEquals(startTimeMillis, hourlyResult.get(0).getTimestamp());
        IntStream.range(1, hourlyResult.size()).forEach(i ->
                assertEquals("Failed for hour " + Instant.ofEpochMilli(hourlyResult.get(i - 1).getTimestamp()),
                        TimeUnit.HOURS.toMillis(1),
                        hourlyResult.get(i).getTimestamp() - hourlyResult.get(i - 1).getTimestamp()));

        // check daily results
        // we should have a record for every day on which an hourly timestamp fell
        long expectedDays = endTime.toLocalDate().toEpochDay() - startTime.toLocalDate().toEpochDay() + 1;
        assertEquals(expectedDays, dailyResult.size());
        // baseValue per hour over 24 hours for most days, but first day only has (02-23), and final
        // day only has 15 (00-14)
        IntStream.range(1, dailyResult.size() - 1).forEach(i ->
                assertEquals("Failed for day " + Instant.ofEpochMilli(dailyResult.get(i).getTimestamp()),
                        24 * baseValue, dailyResult.get(i).value, EPSILON_PRECISION));
        int firstDayHours = 24 - startTime.getHour();
        assertEquals(firstDayHours * baseValue, dailyResult.get(0).value, EPSILON_PRECISION);
        int lastDayHours = endTime.getHour() + 1;
        assertEquals(lastDayHours * baseValue, dailyResult.get(dailyResult.size() - 1).value, EPSILON_PRECISION);
        // first record timestamp should be midnight of day containing first hourly timestamp, and
        // then they should be spaced by one day.
        assertEquals(TimeUnit.DAYS.toMillis(startTime.atZone(ZoneOffset.UTC).toLocalDate().toEpochDay()),
                dailyResult.get(0).getTimestamp());
        IntStream.range(1, dailyResult.size()).forEach(i ->
                assertEquals("Failed for day " + Instant.ofEpochMilli(dailyResult.get(i - 1).getTimestamp()),
                        TimeUnit.DAYS.toMillis(1),
                        dailyResult.get(i).getTimestamp() - dailyResult.get(i - 1).getTimestamp()));

        // check monthly results
        // we have a record for every distinct month in which an hourly timestamp appeared
        assertEquals(12L * (endTime.getYear() - startTime.getYear())
                        + endTime.getMonthValue() - startTime.getMonthValue() + 1,
                monthlyResult.size());
        // first record is at midnight of last day in month of start record, and likewise for
        // succeeding months
        LocalDateTime startOfFirstMonth = startTime.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS);
        IntStream.range(0, monthlyResult.size()).forEach(i ->
                assertEquals("Failed for month starting on " + startOfFirstMonth.plusMonths(i),
                        startOfFirstMonth.plusMonths(i + 1).minusDays(1),
                        SavingsUtil.getLocalDateTime(monthlyResult.get(i).getTimestamp(), clock)));
        // Month totals varies by days in the month. First and last are handled specially.
        IntStream.range(1, monthlyResult.size() - 1).forEach(i -> {
            // midnight on last day of month
            LocalDateTime monthDate = SavingsUtil.getLocalDateTime(monthlyResult.get(i).getTimestamp(), clock);
            assertEquals("Failed for month ending on " + monthDate,
                    monthDate.getDayOfMonth() * 24 * baseValue, monthlyResult.get(i).getValue(), EPSILON_PRECISION);
        });
        // first and last months are adjusted by day/hour of start/end times respectively
        int firstMonthHours =
                SavingsUtil.getLocalDateTime(monthlyResult.get(0).getTimestamp(), clock).getDayOfMonth() * 24
                        - (startTime.getDayOfMonth() - 1) * 24 - startTime.getHour();
        assertEquals(firstMonthHours * baseValue, monthlyResult.get(0).getValue(), EPSILON_PRECISION);
        // last active day in month is not yet rolled up into monthly since the day may not be complete
        int lastMonthHours = (endTime.getDayOfMonth() - 1) * 24;
        assertEquals(lastMonthHours * baseValue, monthlyResult.get(monthlyResult.size() - 1).getValue(), EPSILON_PRECISION);
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
        long entityOid1 = 1L;
        long entityOid2 = 2L;
        long accountId = 100L;
        long regionOid1 = 1000L;
        long regionOid2 = 2000L;
        long zoneOid1 = 10L;
        long zoneOid2 = 20L;
        long serviceProviderOid = 10000L;
        long resourceGroupOid1 = 100000L;
        long resourceGroupOid2 = 200000L;
        EntityCloudScopeRecord r1 = createEntityCloudScopeRecord(entityOid1, accountId, regionOid1,
                zoneOid1, serviceProviderOid, resourceGroupOid1);

        EntityCloudScopeRecord r2 = createEntityCloudScopeRecord(entityOid2, accountId, regionOid2,
                zoneOid2, serviceProviderOid, resourceGroupOid2);

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
        Collection<Long> resourceGroups = new HashSet<>();

        List<AggregatedSavingsStats> statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                ImmutableSet.of(accountId), entityTypes, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(30), statsReadBack.get(0).getValue());

        // 2. Query by Region ID
        // VM1 is in region 1, and VM2 is in region 2. The query ask for region 1. So only savings
        // of VM1 is returned.
        entityTypes = Collections.singleton(EntityType.REGION_VALUE);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                ImmutableSet.of(regionOid1), entityTypes, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(10), statsReadBack.get(0).getValue());

        // 3. Query by Availability Zone ID
        // VM1 is in AZ 1, and VM2 is in AZ 2. The query ask for AZ 2. So only savings
        // of VM2 is returned.
        entityTypes = Collections.singleton(EntityType.AVAILABILITY_ZONE_VALUE);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                ImmutableSet.of(zoneOid2), entityTypes, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(20), statsReadBack.get(0).getValue());

        // 4. Query by service provider
        // Both VMs are in the same service provider. Realized savings value equal to the same of the 2.
        entityTypes = Collections.singleton(EntityType.SERVICE_PROVIDER_VALUE);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                ImmutableSet.of(serviceProviderOid), entityTypes, resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(30), statsReadBack.get(0).getValue());

        // 5. Query by resource group
        // VM2 is in resource group 2. Savings of VM2 will be returned.
        resourceGroups.add(resourceGroupOid2);
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                Collections.<Long>emptySet(), Collections.<Integer>emptySet(), resourceGroups);
        assertEquals(1, statsReadBack.size());
        assertEquals(Double.valueOf(20), statsReadBack.get(0).getValue());
    }

    private EntityCloudScopeRecord createEntityCloudScopeRecord(Long entityOid, Long accountOid,
            Long regionOid, Long availabilityZoneOid, Long serviceProviderOid,
            Long resourceGroupOid) {
        EntityCloudScopeRecord record = new EntityCloudScopeRecord();
        record.setAccountOid(accountOid);
        record.setEntityOid(entityOid);
        record.setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        record.setRegionOid(regionOid);
        record.setAvailabilityZoneOid(availabilityZoneOid);
        record.setServiceProviderOid(serviceProviderOid);
        record.setResourceGroupOid(resourceGroupOid);
        record.setCreationTime(LocalDateTime.now());
        return record;
    }
}
