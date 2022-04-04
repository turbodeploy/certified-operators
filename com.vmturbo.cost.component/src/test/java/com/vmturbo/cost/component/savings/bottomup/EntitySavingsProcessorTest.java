package com.vmturbo.cost.component.savings.bottomup;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import com.vmturbo.components.api.TimeUtil;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.cost.component.notification.CostNotificationSender;
import com.vmturbo.cost.component.rollup.LastRollupTimes;
import com.vmturbo.cost.component.rollup.RollupTimesStore;
import com.vmturbo.cost.component.savings.DataRetentionProcessor;
import com.vmturbo.cost.component.savings.RollupSavingsProcessor;
import com.vmturbo.test.utils.FeatureFlagTestRule;

/**
 * Test the entity savings processor.
 */
@RunWith(Parameterized.class)
public class EntitySavingsProcessorTest {

    /**
     * Parameterized test data.
     *
     * @return whether to enable TEM.
     */
    @Parameters(name = "{index}: Test with enable TEM = {0}")
    public static Collection<Object[]> data() {
        Object[][] data = new Object[][] {{true}, {false}};
        return Arrays.asList(data);
    }

    /**
     * Test parameter.
     */
    @Parameter(0)
    public boolean enableTEM;

    private TopologyEventsPoller topologyEventsPoller = Mockito.mock(TopologyEventsPoller.class);

    private EntitySavingsTracker entitySavingsTracker = Mockito.mock(EntitySavingsTracker.class);

    private RollupSavingsProcessor rollupSavingsProcessor = Mockito.mock(RollupSavingsProcessor.class);

    private RollupTimesStore rollupTimesStore = Mockito.mock(RollupTimesStore.class);

    private EntitySavingsStore entitySavingsStore = Mockito.mock(EntitySavingsStore.class);

    private EntityEventsJournal entityEventsJournal = Mockito.mock(EntityEventsJournal.class);

    private final Clock clock = Clock.systemUTC();

    private EntitySavingsProcessor entitySavingsProcessor = Mockito.spy(new EntitySavingsProcessor(
            entitySavingsTracker, topologyEventsPoller, rollupSavingsProcessor, rollupTimesStore,
            entitySavingsStore, entityEventsJournal, clock, Mockito.mock(DataRetentionProcessor.class),
            Mockito.mock(CostNotificationSender.class)));

    /**
     * Rule to manage feature flag enablement.
     */
    @Rule
    public FeatureFlagTestRule featureFlagTestRule =
            new FeatureFlagTestRule(FeatureFlags.ENABLE_SAVINGS_TEM);

    /**
     * Test initialization.
     */
    @Before
    public void setup() {
        if (enableTEM) {
            featureFlagTestRule.enable(FeatureFlags.ENABLE_SAVINGS_TEM);
        } else {
            featureFlagTestRule.disable(FeatureFlags.ENABLE_SAVINGS_TEM);
        }
    }

    /**
     * Start time is 9:00. End time is 10:00.
     * Verify start and end times are passed to the tracker.
     */
    @Test
    public void testExecute() {
        Mockito.when(topologyEventsPoller.isTopologyBroadcasted(Mockito.any(LocalDateTime.class))).thenReturn(true);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        Mockito.when(rollupTimesStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());
        LocalDateTime startTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.when(entitySavingsProcessor.getPeriodStartTime()).thenReturn(startTime);
        LocalDateTime endTime = LocalDateTime.of(2021, 3, 23, 10, 0);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime()).thenReturn(endTime);
        entitySavingsProcessor.execute();
        if (!FeatureFlags.ENABLE_SAVINGS_TEM.isEnabled()) {
            // The poller is enabled only when TEM is disabled
            Mockito.verify(topologyEventsPoller).poll(startTime, endTime);
        }
        Mockito.verify(entitySavingsTracker).processEvents(startTime, endTime,
                Collections.emptySet());
    }

    /**
     * Start time is 9:00. End time is 9:00.
     * Verify poller and tracker are not called.
     */
    @Test
    public void testExecuteAbort() {
        Mockito.when(topologyEventsPoller.isTopologyBroadcasted(Mockito.any(LocalDateTime.class))).thenReturn(true);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        Mockito.when(rollupTimesStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());
        LocalDateTime startTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.when(entitySavingsProcessor.getPeriodStartTime()).thenReturn(startTime);
        LocalDateTime endTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime()).thenReturn(endTime);
        entitySavingsProcessor.execute();
        Mockito.verify(topologyEventsPoller, Mockito.never()).poll(startTime, endTime);
        Mockito.verify(entitySavingsTracker, Mockito.never()).processEvents(startTime, endTime, null);
    }

    /**
     * Start and end time are 9:00 and 10:00 respectively. There is no topology broadcasted after 10:00.
     * In this case, don't proceed with the processing.
     */
    @Test
    public void testAdjustEndTimeAbortProcess() {
        Mockito.when(topologyEventsPoller.isTopologyBroadcasted(Mockito.any(LocalDateTime.class))).thenReturn(false);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        Mockito.when(rollupTimesStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());
        LocalDateTime startTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Mockito.when(entitySavingsProcessor.getPeriodStartTime()).thenReturn(startTime);
        LocalDateTime endTime = LocalDateTime.of(2021, 3, 23, 10, 0);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime()).thenReturn(endTime);
        entitySavingsProcessor.execute();
        if (!FeatureFlags.ENABLE_SAVINGS_TEM.isEnabled()) {
            // We only run this test once (for the TEM disabled case) because verify fails when
            // the test itself runs twice.  We also should not invoke poll when TEM is enabled,
            // so skipping this when it is.
            Mockito.verify(topologyEventsPoller, Mockito.never()).poll(Mockito.any(LocalDateTime.class),
                    Mockito.any(LocalDateTime.class));
            Mockito.verify(entitySavingsTracker, Mockito.never()).processEvents(Mockito.any(LocalDateTime.class), Mockito.any(LocalDateTime.class),
                    Mockito.anySet());
        }
    }

    /**
     * Start and end time are 8:00 and 10:00 respectively. There is no topology broadcasted after 10:00.
     * In this case, push end time back one hour to 9:00, and process 8:00 to 9:00.
     */
    @Test
    public void testAdjustEndTimeContinueProcess() {
        Mockito.when(topologyEventsPoller.isTopologyBroadcasted(Mockito.any(LocalDateTime.class))).thenReturn(false, true);
        Mockito.when(entityEventsJournal.getOldestEventTime()).thenReturn(null);
        Mockito.when(rollupTimesStore.getLastRollupTimes()).thenReturn(new LastRollupTimes());
        LocalDateTime startTime = LocalDateTime.of(2021, 3, 23, 8, 0);
        Mockito.when(entitySavingsProcessor.getPeriodStartTime()).thenReturn(startTime);
        LocalDateTime endTime = LocalDateTime.of(2021, 3, 23, 10, 0);
        Mockito.when(entitySavingsProcessor.getCurrentDateTime()).thenReturn(endTime);
        entitySavingsProcessor.execute();
        LocalDateTime updatedEndTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        if (!FeatureFlags.ENABLE_SAVINGS_TEM.isEnabled()) {
            // The poller is enabled only when TEM is disabled
            Mockito.verify(topologyEventsPoller).poll(startTime, updatedEndTime);
        Mockito.verify(entitySavingsTracker).processEvents(startTime, updatedEndTime,
                Collections.emptySet());
        }
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
        Mockito.when(rollupTimesStore.getLastRollupTimes()).thenReturn(lastRollupTimes);
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
        Mockito.when(rollupTimesStore.getLastRollupTimes()).thenReturn(lastRollupTimes);
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
        Mockito.when(rollupTimesStore.getLastRollupTimes()).thenReturn(lastRollupTimes);
        Mockito.when(entityEventsJournal.getOldestEventTime())
                .thenReturn(TimeUtil.localDateTimeToMilli(oldestEventTime, clock));
        LocalDateTime startTime = entitySavingsProcessor.getPeriodStartTime();
        LocalDateTime expectedStartTime = LocalDateTime.of(2021, 3, 23, 9, 0);
        Assert.assertEquals(expectedStartTime, startTime);
    }
}
