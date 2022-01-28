package com.vmturbo.cost.component.savings;

import static com.vmturbo.cost.component.savings.SavingsUtil.EMPTY_PRICE_CHANGE;
import static org.mockito.Mockito.mock;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.math.DoubleMath;
import com.google.gson.Gson;
import com.google.gson.stream.JsonReader;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.ProviderChangeDetails;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventInfo;
import com.vmturbo.common.protobuf.topology.TopologyEventDTO.EntityEvents.TopologyEvent.TopologyEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.EventInjector.ScriptEvent;

/**
 * Tests to verify operation of the savings algorithm.
 */
public class SavingsCalculatorTest {
    private long actionId;

    /**
     * Test setup.
     *
     * @throws Exception if there's an error.
     */
    @Before
    public void setUp() throws Exception {
        actionId = 1000L;
    }

    /**
     * Ensure that Algorithm-2 is generating the correct savings entries.
     *
     *  @throws FileNotFoundException if the events file is missing.
     */
    @Test
    public void testAlgorithm2() throws FileNotFoundException {
        // Add events
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
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
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
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
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
        addTestEvents("src/test/resources/savings/action-aging-rolling.json", eventsJournal);

        // Run the algorithm.
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
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
        addTestEvents("src/test/resources/savings/alg2-test.json", eventsJournal);

        long entityUpdatedInLastPeriod = 5555555L;
        EntityState stateFromLastPeriod = new EntityState(entityUpdatedInLastPeriod, EMPTY_PRICE_CHANGE);
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
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal(mock(AuditLogWriter.class));
        addTestEvents("src/test/resources/savings/delete-volume.json", eventsJournal);

        // Run the algorithm.
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
                new Result(2, false, ImmutableList.of(), 0d, null)
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
     * Ensure that Algorithm-2 is expiring actions in the correct order after the configured
     * expiration duration is decreased.
     *
     *  @throws FileNotFoundException if the events file is missing.
     */
    @Test
    public void testAlgorithm2ExpirationDurationChange() throws FileNotFoundException {
        // Add events
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal(
                mock(AuditLogWriter.class));
        addTestEvents("src/test/resources/savings/expire-out-of-order.json", eventsJournal);

        // Run the algorithm.
        SavingsCalculator savingsCalculator = new SavingsCalculator();
        Map<Long, EntityState> entityStates = new HashMap<>();
        long baseTime = 1618819200000L;  // Mon Apr 19 04:00:00 2021

        Result[] results = {
                new Result(1, false, ImmutableList.of(-3d), 3d, null),
                new Result(1, false, ImmutableList.of(-3d), 3d, null),
                new Result(1, false, ImmutableList.of(-3d), 2.50d, 0.50d),
                new Result(1, false, ImmutableList.of(-3d), 3d, 0d),
                new Result(1, false, ImmutableList.of(-3d), 3d, 0d)
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
     * Ensure that we are handling recommendation removed events that occur before the associated
     * action executed event.
     *
     *  @throws FileNotFoundException if the events file is missing.
     */
    @Test
    public void testRemovedBeforeExecutionSuccess() throws FileNotFoundException {
        // Add events
        EntityEventsJournal eventsJournal = new InMemoryEntityEventsJournal(
                mock(AuditLogWriter.class));
        addTestEvents("src/test/resources/savings/recommendation-removed-before-execution.json",
                eventsJournal);

        // Run the algorithm.
        SavingsCalculator savingsCalculator = new SavingsCalculator();
        Map<Long, EntityState> entityStates = new HashMap<>();
        long baseTime = 1622174400000L;  // Fri May 28 00:00:00 2021

        Result[] results = {
                new Result(1, false, ImmutableList.of(), null, null),
                new Result(1, false, ImmutableList.of(2.0), null, 1.8333333333333333d),
                new Result(1, false, ImmutableList.of(2.0), null, 2.0d)
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
     * - Execute an action A->B, provider change with A->B: This is normal (discovery detects the
     *   entity on its new provider due to the executed action).  The provider change is dropped.
     */
    @Test
    public void testExecABProviderChangeAB() {
        // Create events for scenario.
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createActionEvent(0L, ActionEventType.RECOMMENDATION_ADDED, 3d, 5d),
                createActionEvent(1L, ActionEventType.SCALE_EXECUTION_SUCCESS, 3d, 5d),
                createProviderChange(2L, 3d, 5d));
        // Run the scenario.
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents);

        // Verify the results.
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertEquals(ImmutableList.of(2d), entityState.getActionList());
        Assert.assertFalse(entityState.getCurrentRecommendation().active());
    }

    /**
     * - Execute an action A->B, provider change with B->C: This is an external resize that does
     *   not attempt to revert the last executed action and should be dropped.
     */
    @Test
    public void testExecABProviderChangeBC() {
        // Create events for scenario.
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createActionEvent(0L, ActionEventType.RECOMMENDATION_ADDED, 3d, 5d),
                createActionEvent(1L, ActionEventType.SCALE_EXECUTION_SUCCESS, 3d, 5d),
                createProviderChange(2L, 5d, 4d));
        // Run the scenario.
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents);

        // Verify the results.
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertEquals(ImmutableList.of(2d), entityState.getActionList());
        Assert.assertFalse(entityState.getCurrentRecommendation().active());
    }

    /**
     * - Execute an action A->B, provider change with B->A: This is a revert. The previously
     *   executed action will be removed from the action list in the entity state.
     */
    @Test
    public void testExecABProviderChangeBA() {
        // Create events for scenario.
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createActionEvent(0L, ActionEventType.RECOMMENDATION_ADDED, 3d, 5d),
                createActionEvent(1L, ActionEventType.SCALE_EXECUTION_SUCCESS, 3d, 5d),
                createProviderChange(2L, 5d, 3d));
        // Run the scenario.
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents);

        // Verify the results.
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertTrue(entityState.getActionList().isEmpty());
        Assert.assertFalse(entityState.getCurrentRecommendation().active());
    }

    /**
     * - Execute an action A->B, new recommendation that is not B->A arrives, provider change from
     *   B->A: This is an attempt to revert an action after the market has generated a new
     *   recommendation for the entity.  Since the recommendation itself is not B->A, this is
     *   treated as a revert.  The previously executed action will be removed from the action list
     *   in the entity state and the current recommendation will remain untouched.
     */
    @Test
    public void testExecABUnrelatedRecommendationProviderChangeBA() {
        // Create events for scenario.
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createActionEvent(0L, ActionEventType.RECOMMENDATION_ADDED, 3d, 5d),
                createActionEvent(1L, ActionEventType.SCALE_EXECUTION_SUCCESS, 3d, 5d),
                createActionEvent(2L, ActionEventType.RECOMMENDATION_ADDED, 5d, 6d),
                createProviderChange(3L, 5d, 3d));
        // Run the scenario.
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents);

        // Verify the results.
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertTrue(entityState.getActionList().isEmpty());
        Assert.assertTrue(entityState.getCurrentRecommendation().active());
    }

    /**
     * - Execute an action A->B, new recommendation that is B->A arrives, provider change also from
     *   B->A: This is an attempt to revert an action after the market has generated a new
     *   recommendation for the entity that matches the user's attempt to revert.  Since the market
     *   recommended this action before the user reverted it, we do not treat this as a revert and
     *   instead take credit for the realized investment or savings.
     */
    @Ignore  // This is not currently supported for the revert-only case.  Keeping it here for the future.
    @Test
    public void testExecABRecommendationBAProviderChangeBA() {
        // Create events for scenario.
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createActionEvent(0L, ActionEventType.RECOMMENDATION_ADDED, 3d, 5d),
                createActionEvent(1L, ActionEventType.SCALE_EXECUTION_SUCCESS, 3d, 5d),
                createActionEvent(2L, ActionEventType.RECOMMENDATION_ADDED, 5d, 3d),
                createProviderChange(3L, 5d, 3d));
        // Run the scenario.
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents);

        // Verify the results.
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertEquals(ImmutableList.of(2d), entityState.getActionList());
        Assert.assertTrue(entityState.getCurrentRecommendation().active());
    }

    /**
     * Power state change topology events also generate provider change events where the new
     * destination provider becomes null. Ensure that these events are dropped.
     */
    @Test
    public void testNullDestProviderOid() {
        // Create events for scenario.
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createActionEvent(0L, ActionEventType.RECOMMENDATION_ADDED, 3d, 5d),
                createActionEvent(1L, ActionEventType.SCALE_EXECUTION_SUCCESS, 3d, 5d),
                createProviderChange(2L, 5d, null));
        // Run the scenario.
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents);

        // Verify the results.
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertEquals(ImmutableList.of(2d), entityState.getActionList());
        Assert.assertFalse(entityState.getCurrentRecommendation().active());
    }

    /**
     * Ensure that a valid provider change is ignored when the current provider is not known.
     */
    @Test
    public void testProviderChangeNoCurrentProvider() {
        // Create events for scenario.
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createProviderChange(2L, 5d, null));
        // Run the scenario. Pre-populate with an entity without a valid provider
        EntityState entityState = new EntityState(2116L, EMPTY_PRICE_CHANGE);
        entityState.setLastExecutedAction(Optional.of(
                new ActionEntry.Builder()
                        .eventType(ActionEventType.SCALE_EXECUTION_SUCCESS)
                        .sourceOid(0L)
                        .destinationOid(0L)
                        .build()));
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents,
                ImmutableMap.of(2116L, entityState));

        // Verify the results.
        Assert.assertEquals(1, entityStates.size());
        entityState = entityStates.values().iterator().next();
        Assert.assertTrue(entityState.getActionList().isEmpty());
        Assert.assertFalse(entityState.getCurrentRecommendation().active());
    }

    /**
     * Ensure that provider change events are handled correctly when the entity has no previously
     * executed action.
     */
    @Test
    public void testProviderChangeNoLastExecutedAction() {
        // Create events for scenario.
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createActionEvent(0L, ActionEventType.RECOMMENDATION_ADDED, 3d, 5d),
                createProviderChange(2L, 3d, 5d));
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents);

        // Verify the results.
        Assert.assertEquals(1, entityStates.size());
        EntityState entityState = entityStates.values().iterator().next();
        Assert.assertTrue(entityState.getActionList().isEmpty());
        Assert.assertTrue(entityState.getCurrentRecommendation().active());
    }

    /**
     * Tests to ensure that the provider change is fully formed.
     */
    @Test
    public void testProviderChangeValidityChecks() {
        // No algorithm state (invalid entity ID)
        List<SavingsEvent> savingsEvents = ImmutableList.of(
                createProviderChange(2L, 3d, 5d));
        Map<Long, EntityState> entityStates = runProviderChangeScenario(savingsEvents);

        // Verify the results.
        Assert.assertTrue(entityStates.isEmpty());

        // No provider change in topology event.
        SavingsEvent savingsEvent = createSavingsEvent(0L)
                .topologyEvent(TopologyEvent.newBuilder()
                        .setType(TopologyEventType.PROVIDER_CHANGE)
                        .setEventTimestamp(0L)
                        .setEventInfo(TopologyEventInfo.newBuilder())
                        .build())
                .build();
        entityStates = runProviderChangeScenario(ImmutableList.of(savingsEvent));

        // Verify the results.
        Assert.assertTrue(entityStates.isEmpty());
    }

    private Map<Long, EntityState> runProviderChangeScenario(List<SavingsEvent> savingsEvents,
            Map<Long, EntityState> entityStates) {
        // Run the algorithm. Run a single period of one hour
        SavingsCalculator savingsCalculator = new SavingsCalculator();
        savingsCalculator.calculate(entityStates, entityStates.values(),
                savingsEvents, 0, 3600000L);
        return entityStates;
    }

    private Map<Long, EntityState> runProviderChangeScenario(List<SavingsEvent> savingsEvents) {
        return runProviderChangeScenario(savingsEvents, new HashMap<>());
    }

    private SavingsEvent.Builder createSavingsEvent(long timestamp, Double sourceCost,
            Double destCost) {
        return new SavingsEvent.Builder()
                .entityId(2116L)
                .timestamp(timestamp)
                .expirationTime(timestamp + TimeUnit.DAYS.toMillis(365L))
                .entityPriceChange(new EntityPriceChange.Builder()
                        .sourceCost(sourceCost)
                        .destinationCost(destCost)
                        .sourceOid((long)Objects.hash(sourceCost))
                        .destinationOid((long)Objects.hash(destCost))
                        .build());
    }

    private SavingsEvent.Builder createSavingsEvent(long timestamp) {
        return new SavingsEvent.Builder()
                .entityId(2116L)
                .timestamp(timestamp)
                .expirationTime(timestamp + TimeUnit.DAYS.toMillis(365L));
    }

    private SavingsEvent createActionEvent(long timestamp, ActionEventType eventType,
            Double sourceCost, Double destCost) {
        return createSavingsEvent(timestamp, sourceCost, destCost)
                .actionEvent(new ActionEvent.Builder()
                        .eventType(eventType)
                        .entityType(10)
                        .actionId(actionId++)
                        .build())
                .build();
    }

    private SavingsEvent createProviderChange(long timestamp, Double sourceCost, Double destCost) {
        ProviderChangeDetails.Builder providerChange = ProviderChangeDetails.newBuilder()
                .setProviderType(10)
                .setSourceProviderOid(Objects.hash(sourceCost));
        if (destCost != null) {
            providerChange.setDestinationProviderOid(Objects.hash(destCost));
        }
        return createSavingsEvent(timestamp)
                .topologyEvent(TopologyEvent.newBuilder()
                        .setType(TopologyEventType.PROVIDER_CHANGE)
                        .setEventTimestamp(timestamp)
                        .setEventInfo(TopologyEventInfo.newBuilder()
                                .setProviderChange(providerChange))
                        .build())
                .build();
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
        AtomicBoolean purgePreviousTestState = new AtomicBoolean(false);
        Map<String, Long> oidMap = events.stream()
                .map(event -> event.uuid)
                .collect(Collectors.toSet()).stream()
                .collect(Collectors.toMap(Function.identity(), Long::valueOf));
        events.forEach(event -> EventInjector.addEvent(event, oidMap, entityEventsJournal,
                purgePreviousTestState));
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
