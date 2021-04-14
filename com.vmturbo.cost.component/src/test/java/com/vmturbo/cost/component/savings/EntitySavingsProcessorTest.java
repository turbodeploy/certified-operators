package com.vmturbo.cost.component.savings;

import java.time.Clock;
import java.time.LocalDateTime;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.cost.component.savings.EntitySavingsStore.LastRollupTimes;

/**
 * Test the entity savings processor.
 */
public class EntitySavingsProcessorTest {

    private TopologyEventsPoller topologyEventsPoller = Mockito.mock(TopologyEventsPoller.class);

    private EntitySavingsTracker entitySavingsTracker = Mockito.mock(EntitySavingsTracker.class);

    private RollupSavingsProcessor rollupSavingsProcessor = Mockito.mock(RollupSavingsProcessor.class);

    private EntitySavingsStore entitySavingsStore = Mockito.mock(EntitySavingsStore.class);

    private EntityEventsJournal entityEventsJournal = Mockito.mock(EntityEventsJournal.class);

    private final Clock clock = Clock.systemUTC();

    private EntitySavingsProcessor entitySavingsProcessor = Mockito.spy(new EntitySavingsProcessor(
            entitySavingsTracker, topologyEventsPoller, rollupSavingsProcessor, entitySavingsStore,
            entityEventsJournal, clock));

    /**
     * Start time is 9:00. End time is 10:00.
     * Verify start and end times are passed to the tracker.
     */
    @Test
    public void testExecute() {
        Mockito.when(topologyEventsPoller.isTopologyBroadcasted(Mockito.any(LocalDateTime.class))).thenReturn(true);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        Mockito.when(entitySavingsStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());
        LocalDateTime startTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.when(entitySavingsProcessor.getPeriodStartTime()).thenReturn(startTime);
        LocalDateTime endTime = LocalDateTime.of(2021, 3, 23, 10, 0);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime()).thenReturn(endTime);
        entitySavingsProcessor.execute();
        Mockito.verify(topologyEventsPoller).poll(startTime, endTime);
        Mockito.verify(entitySavingsTracker).processEvents(startTime, endTime);
    }

    /**
     * Start time is 9:00. End time is 9:00.
     * Verify poller and tracker are not called.
     */
    @Test
    public void testExecuteAbort() {
        Mockito.when(topologyEventsPoller.isTopologyBroadcasted(Mockito.any(LocalDateTime.class))).thenReturn(true);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        Mockito.when(entitySavingsStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());
        LocalDateTime startTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.when(entitySavingsProcessor.getPeriodStartTime()).thenReturn(startTime);
        LocalDateTime endTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime()).thenReturn(endTime);
        entitySavingsProcessor.execute();
        Mockito.verify(topologyEventsPoller, Mockito.never()).poll(startTime, endTime);
        Mockito.verify(entitySavingsTracker, Mockito.never()).processEvents(startTime, endTime);
    }

    /**
     * Start and end time are 9:00 and 10:00 respectively. There is no topology broadcasted after 10:00.
     * In this case, don't proceed with the processing.
     */
    @Test
    public void testAdjustEndTimeAbortProcess() {
        Mockito.when(topologyEventsPoller.isTopologyBroadcasted(Mockito.any(LocalDateTime.class))).thenReturn(false);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        Mockito.when(entitySavingsStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());
        LocalDateTime startTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.when(entitySavingsProcessor.getPeriodStartTime()).thenReturn(startTime);
        LocalDateTime endTime = LocalDateTime.of(2021, 3, 23, 10, 0);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime()).thenReturn(endTime);
        entitySavingsProcessor.execute();
        Mockito.verify(topologyEventsPoller, Mockito.never()).poll(Mockito.any(LocalDateTime.class), Mockito.any(LocalDateTime.class));
        Mockito.verify(entitySavingsTracker, Mockito.never()).processEvents(Mockito.any(LocalDateTime.class), Mockito.any(LocalDateTime.class));
    }

    /**
     * Start and end time are 8:00 and 10:00 respectively. There is no topology broadcasted after 10:00.
     * In this case, push end time back one hour to 9:00, and process 8:00 to 9:00.
     */
    @Test
    public void testAdjustEndTimeContinueProcess() {
        Mockito.when(topologyEventsPoller.isTopologyBroadcasted(Mockito.any(LocalDateTime.class))).thenReturn(false, true);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        Mockito.when(entitySavingsStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());
        LocalDateTime startTime = LocalDateTime.of(2021, 3, 23, 8, 0);
        Mockito.when(entitySavingsProcessor.getPeriodStartTime()).thenReturn(startTime);
        LocalDateTime endTime = LocalDateTime.of(2021, 3, 23, 10, 0);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime()).thenReturn(endTime);
        entitySavingsProcessor.execute();
        LocalDateTime updatedEndTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.verify(topologyEventsPoller).poll(startTime, updatedEndTime);
        Mockito.verify(entitySavingsTracker).processEvents(startTime, updatedEndTime);
    }

    /**
     * If there are no records in the entity_savings_by_day table and there are no events in the
     * event journal, we will process TEP events for the last hour.
     */
    @Test
    public void testGetPeriodStartTimeNoStatsAndEvents() {
        // Currently at 10:05am.
        LocalDateTime currentTime = LocalDateTime.of(2021, 3, 23, 10, 5);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime())
                .thenReturn(currentTime);
        // No record for entity_savings record in aggregation_meta_data table.
        // I.e. no savings stats.
        LastRollupTimes lastRollupTimes = new LastRollupTimes();
        Mockito.when(entitySavingsStore.getLastRollupTimes()).thenReturn(lastRollupTimes);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        LocalDateTime startTime = entitySavingsProcessor.getPeriodStartTime();
        LocalDateTime expectedStartTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Assert.assertEquals(expectedStartTime, startTime);
    }

    /**
     * There are no records in the entity_savings_by_day table. The oldest event in the event journal
     * is at 8:05am. Start time will be 8:00am.
     */
    @Test
    public void testGetPeriodStartTimeNoStatsEventsPresent() {
        // Currently at 10:05am.
        LocalDateTime currentTime = LocalDateTime.of(2021, 3, 23, 10, 5);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime())
                .thenReturn(currentTime);
        // No record for entity_savings record in aggregation_meta_data table.
        // I.e. no savings stats.
        LastRollupTimes lastRollupTimes = new LastRollupTimes();
        LocalDateTime oldestEventTime = LocalDateTime.of(2021, 3, 23, 8, 5);
        Mockito.when(entitySavingsStore.getLastRollupTimes()).thenReturn(lastRollupTimes);
        Mockito.when(entityEventsJournal.getOldestEventTime())
                .thenReturn(TimeUtil.localDateTimeToMilli(oldestEventTime, clock));
        LocalDateTime startTime = entitySavingsProcessor.getPeriodStartTime();
        LocalDateTime expectedStartTime = LocalDateTime.of(2021, 3, 23, 8, 0);
        Assert.assertEquals(expectedStartTime, startTime);
    }

    /**
     * There are records in the entity_savings_by_day table. Newest records is at 8:00.
     * Start time will be 9:00.
     */
    @Test
    public void testGetPeriodStartTimeStatsPresent() {
        // Currently at 10:05am.
        LocalDateTime currentTime = LocalDateTime.of(2021, 3, 23, 10, 5);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime())
                .thenReturn(currentTime);
        // No record for entity_savings record in aggregation_meta_data table.
        // I.e. no savings stats.
        LastRollupTimes lastRollupTimes = new LastRollupTimes();
        LocalDateTime maxStatsTime = LocalDateTime.of(2021, 3, 23, 8, 0);
        lastRollupTimes.setLastTimeByHour(TimeUtil.localDateTimeToMilli(maxStatsTime, clock));
        LocalDateTime oldestEventTime = LocalDateTime.of(2021, 3, 23, 8, 5);
        Mockito.when(entitySavingsStore.getLastRollupTimes()).thenReturn(lastRollupTimes);
        Mockito.when(entityEventsJournal.getOldestEventTime())
                .thenReturn(TimeUtil.localDateTimeToMilli(oldestEventTime, clock));
        LocalDateTime startTime = entitySavingsProcessor.getPeriodStartTime();
        LocalDateTime expectedStartTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Assert.assertEquals(expectedStartTime, startTime);
    }
}
