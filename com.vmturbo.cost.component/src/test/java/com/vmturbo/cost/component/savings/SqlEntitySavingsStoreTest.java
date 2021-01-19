package com.vmturbo.cost.component.savings;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import com.vmturbo.sql.utils.DbCleanupRule;
import com.vmturbo.sql.utils.DbConfigurationRule;

/**
 * Used to test savings related DB read/write codebase.
 */
public class SqlEntitySavingsStoreTest {
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
    private static final long vm1Id = 101L;

    /**
     * ID of VM 2.
     */
    private static final long vm2Id = 201L;

    /**
     * Realized savings test value.
     */
    private static final double realizedSavings = 10.532d;

    /**
     * Missed savings test value.
     */
    private static final double missedSavings = 5.21d;

    /**
     * Realized investments test value.
     */
    private static final double realizedInvestments = 35.2d;

    /**
     * Missed investments test value.
     */
    private static final double missedInvestments = 551.34d;

    /**
     * Initializing store.
     *
     * @throws Exception Throw on DB init errors.
     */
    @Before
    public void setup() throws Exception {
        store = new SqlEntitySavingsStore(dsl, clock, 1);
    }

    /**
     * Testing reads/writes for hourly stats table.
     *
     * @throws EntitySavingsException Thrown on DB error.
     */
    @Test
    public void testHourlyStats() throws EntitySavingsException {
        final Set<EntitySavingsStats> hourlyStats = new HashSet<>();

        // Get exact time, strip of the hour part.
        final LocalDateTime timeExact1PM = Instant.now(clock).atZone(ZoneOffset.UTC)
                .toLocalDateTime().truncatedTo(ChronoUnit.HOURS); // 1970-01-12T13:00
        final LocalDateTime timeExact0PM = timeExact1PM.minusHours(1); // 1970-01-12T12:00
        final LocalDateTime timeExact2PM = timeExact1PM.plusHours(1); // 1970-01-12T14:00
        final LocalDateTime timeExact3PM = timeExact1PM.plusHours(2); // 1970-01-12T15:00
        final LocalDateTime timeExact4PM = timeExact1PM.plusHours(3); // 1970-01-12T16:00

        setStatsValues(hourlyStats, vm1Id, timeExact1PM, 10); // VM1 at 1PM.
        setStatsValues(hourlyStats, vm2Id, timeExact1PM, 20); // VM2 at 1PM.

        setStatsValues(hourlyStats, vm1Id, timeExact2PM, 30); // VM1 at 2PM.
        setStatsValues(hourlyStats, vm2Id, timeExact2PM, 40); // VM2 at 2PM.

        setStatsValues(hourlyStats, vm1Id, timeExact3PM, 50); // VM1 at 3PM.
        setStatsValues(hourlyStats, vm2Id, timeExact3PM, 60); // VM2 at 3PM.

        // Write it.
        store.addHourlyStats(hourlyStats);

        final Set<EntitySavingsStatsType> allStatsTypes = Arrays.stream(EntitySavingsStatsType
                .values()).collect(Collectors.toSet());
        // Read back and verify.
        final Set<Long> bothVms = ImmutableSet.of(vm1Id, vm2Id);
        Set<EntitySavingsStats> statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact0PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                bothVms);
        assertEquals(0, statsReadBack.size()); // 0 stats sets, for 12-1 PM range.

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact1PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                bothVms);
        assertEquals(16, statsReadBack.size()); // 2 stats sets, both 1PM, for 1-2 PM range.

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact2PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                bothVms);
        assertEquals(16, statsReadBack.size()); // 2 stats sets, both 2PM, for 2-3 PM range.

        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact4PM, clock),
                bothVms);
        assertEquals(16, statsReadBack.size()); // 2 stats sets, both 3PM, for 3-4 PM range.

        // Verify data for 1 VM.
        statsReadBack = store.getHourlyStats(allStatsTypes,
                TimeUtil.localDateTimeToMilli(timeExact3PM, clock),
                TimeUtil.localDateTimeToMilli(timeExact4PM, clock),
                ImmutableSet.of(vm1Id));
        assertEquals(8, statsReadBack.size()); // 1 stats set, for vm1, for 3-4 PM range.
        final EntitySavingsStats stats = statsReadBack.iterator().next();
        checkStatsValues(statsReadBack, vm1Id, timeExact3PM, 50);
    }

    /**
     * Util method to add a new stats object to the input set.
     *
     * @param hourlyStats Set of stats to be inserted into DB.
     * @param vmId ID of VM.
     * @param dateTime Stats time.
     * @param multiple Multiple value used for dummy stats data.
     */
    private void setStatsValues(@Nonnull final Set<EntitySavingsStats> hourlyStats, long vmId,
            @Nonnull final LocalDateTime dateTime, int multiple) {
        long timestamp = TimeUtil.localDateTimeToMilli(dateTime, clock);
        EntitySavingsStatsType statsType = EntitySavingsStatsType.REALIZED_SAVINGS;
        hourlyStats.add(new EntitySavingsStats(vmId, timestamp,
                statsType, getDummyValue(statsType, multiple, vmId)));

        statsType = EntitySavingsStatsType.MISSED_SAVINGS;
        hourlyStats.add(new EntitySavingsStats(vmId, timestamp,
                statsType, getDummyValue(statsType, multiple, vmId)));

        statsType = EntitySavingsStatsType.REALIZED_INVESTMENTS;
        hourlyStats.add(new EntitySavingsStats(vmId, timestamp,
                statsType, getDummyValue(statsType, multiple, vmId)));

        statsType = EntitySavingsStatsType.MISSED_INVESTMENTS;
        hourlyStats.add(new EntitySavingsStats(vmId, timestamp,
                statsType, getDummyValue(statsType, multiple, vmId)));

        statsType = EntitySavingsStatsType.CUMULATIVE_REALIZED_SAVINGS;
        hourlyStats.add(new EntitySavingsStats(vmId, timestamp,
                statsType, getDummyValue(statsType, multiple, vmId)));

        statsType = EntitySavingsStatsType.CUMULATIVE_MISSED_SAVINGS;
        hourlyStats.add(new EntitySavingsStats(vmId, timestamp,
                statsType, getDummyValue(statsType, multiple, vmId)));

        statsType = EntitySavingsStatsType.CUMULATIVE_REALIZED_INVESTMENTS;
        hourlyStats.add(new EntitySavingsStats(vmId, timestamp,
                statsType, getDummyValue(statsType, multiple, vmId)));

        statsType = EntitySavingsStatsType.CUMULATIVE_MISSED_INVESTMENTS;
        hourlyStats.add(new EntitySavingsStats(vmId, timestamp,
                statsType, getDummyValue(statsType, multiple, vmId)));
    }

    /**
     * Verifies savings/investments fields in stats object to verify they are as expected.
     *
     * @param statsReadBack Stats object read back from DB.
     * @param vmId Id of VM.
     * @param dateTime Stats time.
     * @param multiple Multiple value used for dummy stats data.
     */
    private void checkStatsValues(@Nonnull final Set<EntitySavingsStats> statsReadBack, long vmId,
            @Nonnull final LocalDateTime dateTime, int multiple) {
        assertNotNull(statsReadBack);
        statsReadBack.forEach(stats -> {
                    assertEquals(stats.getEntityId(), vmId);
                    assertEquals(TimeUtil.localDateTimeToMilli(dateTime, clock),
                            stats.getTimestamp());

                    double expectedValue = getDummyValue(stats.getType(), multiple, vmId);
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
                return realizedSavings + multiple * vmId;
            case MISSED_SAVINGS:
                return missedSavings + multiple * vmId;
            case REALIZED_INVESTMENTS:
                return realizedInvestments + multiple * vmId;
            case MISSED_INVESTMENTS:
                return missedInvestments + multiple * vmId;
            case CUMULATIVE_REALIZED_SAVINGS:
                return realizedSavings * multiple * vmId;
            case CUMULATIVE_MISSED_SAVINGS:
                return missedSavings * multiple * vmId;
            case CUMULATIVE_REALIZED_INVESTMENTS:
                return realizedInvestments * multiple * vmId;
            case CUMULATIVE_MISSED_INVESTMENTS:
                return missedInvestments * multiple * vmId;
        }
        return 0d;
    }
}
