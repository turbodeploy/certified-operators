package com.vmturbo.cost.component.savings;

import static org.mockito.Mockito.mock;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;
import com.google.common.math.DoubleMath;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.apache.commons.collections4.MultiValuedMap;
import org.jooq.DSLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.cost.component.savings.EventInjector.ScriptEvent;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Tests to verify operation of the savings algorithm.
 */
public class SavingsCalculatorTest {

    /**
     * Test setup.
     *
     * @throws Exception if there's an error.
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * Ensure that the algorithm is generating the correct savings entries.
     *
     * @throws FileNotFoundException if the test events file cannot be opened.
     * @throws ParseException if the events file contains invalid JSON.
     */
    @Ignore
    @Test
    public void entityRemoved() throws FileNotFoundException, ParseException {
        EntityStateStore entityStateStore = new SqlEntityStateStore(mock(DSLContext.class), 1000);
        EntityEventsJournal entityEventsJournal = new InMemoryEntityEventsJournal();
        EntitySavingsStore entitySavingsStore = new SavingsCapture();
        AuditLogWriter auditLogWriter = mock(AuditLogWriter.class);
        EntitySavingsTracker tracker = new EntitySavingsTracker(entitySavingsStore,
                entityEventsJournal, entityStateStore, Clock.systemUTC(), auditLogWriter, 1000);
        // Inject some events
        addTestEvents("src/test/resources/savings/unit-test.json", entityEventsJournal);
        tracker.processEvents(roundTime(entityEventsJournal.getNewestEventTime(), true).getTimeInMillis()
                + 3600000L);
        // Verify the results
        //Assert.assertEquals(0, entityStateCache.size());
    }

    /**
     * Ensure that Algorithm-2 is generating the correct savings entries.
     *
     *  @throws FileNotFoundException if the events file is missing.
     */
    @Test
    public void testAlgorithm2() throws FileNotFoundException {
        // Add events
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal();
        addTestEvents("src/test/resources/savings/alg2-test.json", eventsJournal);

        // Run the algorithm. Run a single period of one hour
        SavingsCalculator savingsCalculator = new SavingsCalculator();
        Map<Long, EntityState> entityStates = new HashMap<>();
        savingsCalculator.calculate(entityStates, eventsJournal.removeAllEvents(), 0, 3600000L);

        // Verify the results
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertEquals(ImmutableList.of(-2d, 4d, -8d), entityState.getActionList());
        Assert.assertFalse(entityState.isDeletePending());
        Assert.assertTrue(DoubleMath.fuzzyEquals(4.6666d, entityState.getRealizedSavings(), .0001d));
        Assert.assertTrue(DoubleMath.fuzzyEquals(4.1333d, entityState.getRealizedInvestments(), .0001d));
        Assert.assertTrue(DoubleMath.fuzzyEquals(0.1333d, entityState.getMissedSavings(), .0001d));
        Assert.assertTrue(DoubleMath.fuzzyEquals(0.4d, entityState.getMissedInvestments(), .0001d));
    }

    /**
     * Round the indicated time up or down to the nearest top of the hour.
     *
     * @param time time in ms to round
     * @param roundUp true to round up (future), else round down (past).
     * @return rounded time
     */
    private Calendar roundTime(long time, boolean roundUp) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if (roundUp) {
            calendar.add(Calendar.HOUR_OF_DAY, 1);
        }
        // Set time to the top of the hour.
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar;

    }

    private void addTestEvents(String eventFileName, EntityEventsJournal entityEventsJournal)
            throws FileNotFoundException {
        // Open the script file, convert the events to SavingsEvents, and add them to the event
        // journal.
        Gson gson = new Gson();
        JsonReader reader = new JsonReader(new FileReader(eventFileName));
        List<ScriptEvent> events = Arrays.asList(gson.fromJson(reader, ScriptEvent[].class));
        events.forEach(event -> EventInjector.addEvent(event, entityEventsJournal));
    }

    /**
     * Stub savings store to capture generated savings events.
     */
    class SavingsCapture implements EntitySavingsStore {
        private List<EntitySavingsStats> stats;

        SavingsCapture() {
            this.stats = new ArrayList<>();
        }

        @Override
        public void addHourlyStats(@Nonnull Set<EntitySavingsStats> hourlyStats)
                throws EntitySavingsException {
            stats.addAll(hourlyStats);
        }

        List<EntitySavingsStats> getStats() {
            return this.stats;
        }

        @Nonnull
        @Override
        public List<AggregatedSavingsStats> getHourlyStats(
                @Nonnull Set<EntitySavingsStatsType> statsTypes, @Nonnull Long startTime,
                @Nonnull Long endTime, @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
                throws EntitySavingsException {
            // Not used.
            return new ArrayList<>();
        }

        @Nonnull
        @Override
        public List<AggregatedSavingsStats> getDailyStats(
                @Nonnull Set<EntitySavingsStatsType> statsTypes, @Nonnull Long startTime,
                @Nonnull Long endTime, @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
                throws EntitySavingsException {
            // Not used.
            return new ArrayList<>();
        }

        @Nonnull
        @Override
        public List<AggregatedSavingsStats> getMonthlyStats(
                @Nonnull Set<EntitySavingsStatsType> statsTypes, @Nonnull Long startTime,
                @Nonnull Long endTime, @Nonnull MultiValuedMap<EntityType, Long> entitiesByType)
                throws EntitySavingsException {
            // Not used.
            return new ArrayList<>();
        }

        @Nullable
        @Override
        public Long getMaxStatsTime() {
            return null;
        }

        @Nonnull
        @Override
        public LastRollupTimes getLastRollupTimes() {
            return null;
        }

        @Override
        public void setLastRollupTimes(@Nonnull LastRollupTimes rollupTimes) {

        }

        @Override
        public void performRollup(@Nonnull RollupTimeInfo rollupTimeInfo) {

        }
    }
}