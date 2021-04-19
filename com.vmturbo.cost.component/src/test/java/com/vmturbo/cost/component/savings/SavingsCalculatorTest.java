package com.vmturbo.cost.component.savings;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.math.DoubleMath;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cost.component.savings.EventInjector.ScriptEvent;

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
        savingsCalculator.calculate(entityStates, entityStates.values(),
                eventsJournal.removeAllEvents(), 0, 3600000L);

        // Verify the results
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertEquals(ImmutableList.of(2d, 2d, -2d, 6d, -4d, -6d, 4d, -8d, 6d, -4d, 10d, -8d), entityState.getActionList());
        Assert.assertFalse(entityState.isDeletePending());
        Assert.assertTrue(DoubleMath.fuzzyEquals(4.6666d, entityState.getRealizedSavings(), .0001d));
        Assert.assertTrue(DoubleMath.fuzzyEquals(4.1333d, entityState.getRealizedInvestments(), .0001d));
        Assert.assertTrue(DoubleMath.fuzzyEquals(0.1333d, entityState.getMissedSavings(), .0001d));
        Assert.assertTrue(DoubleMath.fuzzyEquals(0.4d, entityState.getMissedInvestments(), .0001d));
    }

    /**
     * Ensure that Algorithm-2 is generating the correct savings entries.
     *
     *  @throws FileNotFoundException if the events file is missing.
     */
    @Test
    public void testAlgorithm2WithExpirations() throws FileNotFoundException {
        // Add events
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal();
        addTestEvents("src/test/resources/savings/action-aging-simple.json", eventsJournal);

        // Run the algorithm. Run for four one-hour periods
        SavingsCalculator savingsCalculator = new SavingsCalculator();
        Map<Long, EntityState> entityStates = new HashMap<>();
        long baseTime = 1617681600000L; // 4-6-2021 00:00:00

        /**
         * Define the expected results after different runs of the savings calculator.
         */
        class Result {
            public int numStates;
            public boolean deletePending;
            public List<Double> actions;
            public Double rs;
            public Double ri;

            Result(int numStates, boolean deletePending, List<Double> actions,
                    Double rs, Double ri) {
                this.numStates = numStates;
                this.deletePending = deletePending;
                this.actions = actions;
                this.rs = rs;
                this.ri = ri;
            }
        }

        Result[] results = {
                new Result(1, false, ImmutableList.of(1d), null, 0.5),
                new Result(1, false, ImmutableList.of(), null, 0.5),
                new Result(1, false, ImmutableList.of(), null, 0.0),
                new Result(1, false, ImmutableList.of(), null, 0.0)
        };
        for (int period = 0; period < 4; period++) {
            long start = baseTime + period * 3600000L;
            long end = start + 3600000L;
            savingsCalculator.calculate(entityStates, entityStates.values(),
                    eventsJournal.removeEventsBetween(start, end), start, end);
            // Verify the results for this period
            Assert.assertEquals(results[period].numStates, entityStates.size());
            EntityState entityState = entityStates.values().iterator().next();
            Assert.assertEquals(results[period].actions, entityState.getActionList());
            Assert.assertEquals(results[period].deletePending, entityState.isDeletePending());
            Assert.assertEquals(results[period].rs, entityState.getRealizedSavings());
            Assert.assertEquals(results[period].ri, entityState.getRealizedInvestments());
        }
    }


    /**
     * Ensure that Algorithm-2 is generating the correct savings entries.
     *
     *  @throws FileNotFoundException if the events file is missing.
     */
    @Test
    public void testAlgorithm2WithExpirationsMoreComplex() throws FileNotFoundException {
        // Add events
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal();
        addTestEvents("src/test/resources/savings/action-aging-rolling.json", eventsJournal);

        // Run the algorithm. Run for four one-hour periods
        SavingsCalculator savingsCalculator = new SavingsCalculator();
        Map<Long, EntityState> entityStates = new HashMap<>();
        long baseTime = 1617681600000L; // 4-6-2021 00:00:00

        Result[] results = {
                new Result(1, false, ImmutableList.of(1d, 1d, 1d), null, 1.5),
                new Result(1, false, ImmutableList.of(1d, 1d, 1d, 1d, 1d, 1d), null, 4.5),
                new Result(1, false, ImmutableList.of(1d, 1d, 1d, 1d, 1d, 1d), null, 6.0),
                new Result(1, false, ImmutableList.of(1d, 1d, 1d, 1d, 1d, 1d), null, 6.0),
                new Result(1, false, ImmutableList.of(1d, 1d, 1d, 1d, 1d, 1d), null, 6.0),
                new Result(1, false, ImmutableList.of(1d, 1d, 1d, 1d, 1d, 1d), null, 6.0),
                new Result(1, false, ImmutableList.of(1d, 1d, 1d), null, 4.5),
                new Result(1, false, ImmutableList.of(), null, 1.5),
                new Result(1, false, ImmutableList.of(), null, 0.0),
                new Result(1, false, ImmutableList.of(), null, 0.0)
        };
        for (int period = 0; period < results.length; period++) {
            long start = baseTime + period * 3600000L;
            long end = start + 3600000L;
            savingsCalculator.calculate(entityStates, entityStates.values(),
                    eventsJournal.removeEventsBetween(start, end), start, end);
            // Verify the results for this period
            Assert.assertEquals("period " + period, results[period].numStates, entityStates.size());
            EntityState entityState = entityStates.values().iterator().next();
            Assert.assertEquals("period " + period, results[period].actions, entityState.getActionList());
            Assert.assertEquals("period " + period, results[period].deletePending, entityState.isDeletePending());
            Assert.assertEquals("period " + period, results[period].rs, entityState.getRealizedSavings());
            Assert.assertEquals("period " + period, results[period].ri, entityState.getRealizedInvestments());
        }
    }

    /**
     * Verify the updated flag on the state object is set correctly.
     *
     * @throws FileNotFoundException if the events file is missing.
     */
    @Test
    public void testUpdateFlag() throws FileNotFoundException {
        // Add events
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal();
        addTestEvents("src/test/resources/savings/alg2-test.json", eventsJournal);

        long entityUpdatedInLastPeriod = 5555555L;
        EntityState stateFromLastPeriod = new EntityState(entityUpdatedInLastPeriod);
        Map<Long, EntityState> entityStates = new HashMap<>();
        entityStates.put(entityUpdatedInLastPeriod, stateFromLastPeriod);

        // Run the algorithm. Run a single period of one hour
        SavingsCalculator savingsCalculator = new SavingsCalculator();
        savingsCalculator.calculate(entityStates, new ArrayList<>(), eventsJournal.removeAllEvents(), 0, 3600000L);

        // Verify the results
        Assert.assertEquals(2, entityStates.size());
        entityStates.values().forEach(state -> {
            if (state.getEntityId() == 74766790688767L) {
                // the state of the entity associated with the event from the journal should be marked updated
                Assert.assertTrue(state.isUpdated());
            } else if (state.getEntityId() == entityUpdatedInLastPeriod) {
                Assert.assertFalse(state.isUpdated());
            } else {
                Assert.fail("Unexpected entity ID");
            }
        });
    }

    /**
     * Verify that we can delete a volume and that we stop tracking it after the configured
     * expiration time.
     *
     * @throws FileNotFoundException if the events file is missing.
     */
    @Test
    public void testVolumeDelete() throws FileNotFoundException {
        // Add events
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal();
        addTestEvents("src/test/resources/savings/delete-volume.json", eventsJournal);

        // Run the algorithm. Run for four one-hour periods
        SavingsCalculator savingsCalculator = new SavingsCalculator();
        Map<Long, EntityState> entityStates = new HashMap<>();
        long baseTime = 1618545600000L; // 4-16-2021 00:00:00

        Result[] results = {
                new Result(0, false, null, null, null),
                new Result(0, false, null, null, null),
                new Result(0, false, null, null, null),
                new Result(0, false, null, null, null),
                new Result(0, false, null, null, null),
                new Result(0, false, null, null, null),
                new Result(0, false, null, null, null),
                new Result(2, false, ImmutableList.of(-60d), 30d, null),
                new Result(2, false, ImmutableList.of(-60d), 60d, null),
                new Result(2, false, ImmutableList.of(), 30d, null),
                new Result(2, false, ImmutableList.of(), 0d, null),
                new Result(3, false, ImmutableList.of(), 0d, null)
        };
        for (int period = 0; period < results.length; period++) {
            long start = baseTime + period * 3600000L;
            long end = start + 3600000L;
            savingsCalculator.calculate(entityStates, entityStates.values(),
                    eventsJournal.removeEventsBetween(start, end), start, end);
            // Verify the results for this period
            Assert.assertEquals("period " + period, results[period].numStates, entityStates.size());
            if (results[period].numStates == 0) {
                continue;
            }
            Assert.assertTrue("period " + period, entityStates.containsKey(2116L));
            EntityState entityState = entityStates.get(2116L);
            Assert.assertEquals("period " + period, results[period].actions, entityState.getActionList());
            Assert.assertEquals("period " + period, results[period].deletePending, entityState.isDeletePending());
            Assert.assertEquals("period " + period, results[period].rs, entityState.getRealizedSavings());
            Assert.assertEquals("period " + period, results[period].ri, entityState.getRealizedInvestments());
        }
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
     * Define the expected results after different runs of the savings calculator.
     */
    class Result {
        public int numStates;
        public boolean deletePending;
        public List<Double> actions;
        public Double rs;
        public Double ri;

        Result(int numStates, boolean deletePending, List<Double> actions,
               Double rs, Double ri) {
            this.numStates = numStates;
            this.deletePending = deletePending;
            this.actions = actions;
            this.rs = rs;
            this.ri = ri;
        }
    }
}