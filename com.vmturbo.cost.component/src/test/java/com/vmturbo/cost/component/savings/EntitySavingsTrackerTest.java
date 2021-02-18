package com.vmturbo.cost.component.savings;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.cost.Cost.EntitySavingsStatsType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent;
import com.vmturbo.cost.component.savings.EntityEventsJournal.ActionEvent.ActionEventType;
import com.vmturbo.cost.component.savings.EntityEventsJournal.SavingsEvent;
import com.vmturbo.cost.component.savings.EntityStateCache.EntityState;
import com.vmturbo.cost.component.savings.EntityStateCache.SavingsInvestments;

/**
 * Verify operation of the entity savings tracker.
 */
public class EntitySavingsTrackerTest {
    private static EntitySavingsStore entitySavingsStore;

    private static EntityEventsJournal entityEventsJournal;

    private static EntityStateCache entityStateCache;

    private EntitySavingsTracker tracker;

    private static final Calendar calendar = Calendar.getInstance();

    private static long time0800am = getTimestamp(8, 0);
    private static long time0830am = getTimestamp(8, 30);
    private static long time0900am = getTimestamp(9, 0);
    private static long time0915am = getTimestamp(9, 15);
    private static long time0930am = getTimestamp(9, 30);
    private static long time0945am = getTimestamp(9, 45);
    private static long time1000am = getTimestamp(10, 0);
    private static long time1030am = getTimestamp(10, 30);
    private static long time1100am = getTimestamp(11, 0);
    private static long time1115am = getTimestamp(11, 15);
    private static long time1130am = getTimestamp(11, 30);
    private static long time1200pm = getTimestamp(12, 0);

    private static final long vm1Id = 101L;
    private static final long vm2Id = 201L;
    private static final long vm3Id = 301L;

    private static final long action1Id = 1001L;
    private static final long action2Id = 1002L;
    private static final long action3Id = 1003L;

    // Maps the period start time to a list of events in the period that start at the start time and ends 1 hour later.
    private static final Map<Long, List<SavingsEvent>> eventsByPeriod = new HashMap<>();

    @Captor
    private ArgumentCaptor<Set<EntitySavingsStats>> statsCaptor;

    /**
     * Set up before each test case.
     */
    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        entityEventsJournal = mock(EntityEventsJournal.class);
        createEvents();
        when(entityEventsJournal.removeEventsBetween(time0900am, time1000am)).thenReturn(eventsByPeriod.get(time0900am));
        when(entityEventsJournal.removeEventsBetween(time1000am, time1100am)).thenReturn(eventsByPeriod.get(time1000am));
        when(entityEventsJournal.removeEventsBetween(time1100am, time1200pm)).thenReturn(eventsByPeriod.get(time1100am));
        entitySavingsStore = mock(EntitySavingsStore.class);
        entityStateCache = mock(InMemoryEntityStateCache.class);
        tracker = spy(new EntitySavingsTracker(entitySavingsStore, entityEventsJournal, entityStateCache));

        Set<EntityState> stateSet = ImmutableSet.of(
                createEntityState(vm1Id, 2, 0, 0, 0),
                createEntityState(vm2Id, 0, 0, 0, 3),
                createEntityState(vm3Id, 1, 2, 3, 4));
        Answer<Stream> stateStream = new Answer<Stream>() {
            public Stream answer(InvocationOnMock invocation) throws Throwable {
                return stateSet.stream();
            }
        };
        when(entityStateCache.getAll()).thenAnswer(stateStream);
    }

    private static void createEvents() {
        eventsByPeriod.put(time0900am, Arrays.asList(
                getActionEvent(vm1Id, time0915am, ActionEventType.EXECUTION_SUCCESS, action1Id),
                getActionEvent(vm2Id, time0945am, ActionEventType.EXECUTION_SUCCESS, action2Id)));
        eventsByPeriod.put(time1000am, new ArrayList<>());
        eventsByPeriod.put(time1100am, Arrays.asList(
                getActionEvent(vm1Id, time1130am, ActionEventType.EXECUTION_SUCCESS, action3Id)));
    }

    @Nonnull
    private static SavingsEvent getActionEvent(long vmId, long timestamp, ActionEventType actionType,
                                               long actionId) {
        final EntityPriceChange priceChange = new EntityPriceChange.Builder()
                .sourceOid(1001L)
                .sourceCost(10.518d)
                .destinationOid(2001L)
                .destinationCost(6.23d)
                .build();
        return new SavingsEvent.Builder()
                .actionEvent(new ActionEvent.Builder()
                        .actionId(actionId)
                        .eventType(actionType).build())
                .entityId(vmId)
                .timestamp(timestamp)
                .entityPriceChange(priceChange)
                .build();
    }

    /**
     * Scenario:
     * Max timestamp is entity_savings_stats_hourly table is 8:00.
     * (i.e. last period was 8am-9am. Next period should be 9am-10am.)
     * If current time is 8:30am. -> process should not proceed.
     * If current time is 9:30am. -> process should not proceed.
     *
     * @throws Exception exceptions
     */
    @Test
    public void noProcessingNeeded() throws Exception {
        // Current time: 8:30am.
        when(tracker.getCurrentTime()).thenReturn(time0830am);
        // Last stats in DB was at 8am. i.e. last period was 8am-9am.
        when(entitySavingsStore.getMaxStatsTime()).thenReturn(time0800am);
        tracker.processEvents();
        verify(entityEventsJournal, times(1)).removeEventsBetween(0, time0900am);
        verify(entityEventsJournal, never()).removeEventsBetween(time0900am, time1000am);

        // Current time: 9:30am.
        when(tracker.getCurrentTime()).thenReturn(time0930am);
        tracker.processEvents();
        verify(entityEventsJournal, never()).removeEventsBetween(time0900am, time1000am);
    }

    /**
     * Max timestamp is entity_savings_stats_hourly table is 8:00.
     * Current time is 10:30.
     * Process the period 9:00 - 10:00.
     *
     * @throws Exception exceptions
     */
    @Test
    public void processWithOnePeriod() throws Exception {
        // Current time: 10:30am.
        // This time the process will proceed with processing period (9am-10am).
        when(tracker.getCurrentTime()).thenReturn(time1030am);
        when(entitySavingsStore.getMaxStatsTime()).thenReturn(time0800am);
        tracker.processEvents();
        verify(entityEventsJournal).removeEventsBetween(time0900am, time1000am);
        verify(tracker).generateStats(time0900am);
    }

    /**
     * Max timestamp is entity_savings_stats_hourly table is 8:00.
     * Current time is 11:30.
     * Process the periods 9:00 - 10:00 and 10:00 - 11:00.
     *
     * @throws Exception exceptions
     */
    @Test
    public void processWithTwoPeriods() throws Exception {
        // Current time: 11:30.
        // Expect to process periods 9-10 and 10-11 periods.
        when(tracker.getCurrentTime()).thenReturn(time1130am);
        when(entitySavingsStore.getMaxStatsTime()).thenReturn(time0800am);

        tracker.processEvents();
        verify(entityEventsJournal).removeEventsBetween(time0900am, time1000am);
        verify(tracker).generateStats(time0900am);
        verify(entityEventsJournal).removeEventsBetween(time1000am, time1100am);
        verify(tracker).generateStats(time1000am);
        verify(tracker, times(2)).generateStats(anyLong());
    }

    /**
     * Test the generateStats method.
     * @throws Exception exceptions
     */
    @Test
    public void testGenerateStats() throws Exception {
        tracker.generateStats(time1000am);

        Set<EntitySavingsStats> stats = new HashSet<>();
        stats.add(new EntitySavingsStats(vm1Id, time1000am, EntitySavingsStatsType.REALIZED_SAVINGS, 2d));
        stats.add(new EntitySavingsStats(vm2Id, time1000am, EntitySavingsStatsType.MISSED_INVESTMENTS, 3d));
        stats.add(new EntitySavingsStats(vm3Id, time1000am, EntitySavingsStatsType.REALIZED_SAVINGS, 1d));
        stats.add(new EntitySavingsStats(vm3Id, time1000am, EntitySavingsStatsType.REALIZED_INVESTMENTS, 2d));
        stats.add(new EntitySavingsStats(vm3Id, time1000am, EntitySavingsStatsType.MISSED_SAVINGS, 3d));
        stats.add(new EntitySavingsStats(vm3Id, time1000am, EntitySavingsStatsType.MISSED_INVESTMENTS, 4d));

        verify(entitySavingsStore).addHourlyStats(statsCaptor.capture());
        Assert.assertEquals(12, statsCaptor.getValue().size());
        Assert.assertTrue(statsCaptor.getValue().containsAll(stats));
    }

    private EntityState createEntityState(long entityId, double realizedSavings, double realizedInvestments,
                                          double missedSavings, double missedInvestments) {
        EntityState state = new EntityState(entityId);
        if (realizedSavings != 0 || realizedInvestments != 0) {
            SavingsInvestments realized = new SavingsInvestments();
            realized.setSavings(realizedSavings);
            realized.setInvestments(realizedInvestments);
            state.setRealized(realized, 1L);
        }

        if (missedSavings != 0 || missedInvestments != 0) {
            SavingsInvestments missed = new SavingsInvestments();
            missed.setSavings(missedSavings);
            missed.setInvestments(missedInvestments);
            state.setMissed(missed, 1L);
        }
        return state;
    }

    private static long getTimestamp(int hour, int min) {
        calendar.set(Calendar.HOUR_OF_DAY, hour);
        calendar.set(Calendar.MINUTE, min);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }
}
