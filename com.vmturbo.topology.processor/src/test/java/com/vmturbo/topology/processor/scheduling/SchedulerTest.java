package com.vmturbo.topology.processor.scheduling;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.OperationTestUtilities;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Schedule.ScheduleData;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule.TargetDiscoveryScheduleData;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.TopologyHandler;

/**
 * Tests for the {@link Scheduler} class.
 */
public class SchedulerTest {

    private final OperationManager operationManager = Mockito.mock(OperationManager.class);
    private final ScheduledExecutorService scheduledExecutorSpy = Mockito.spy(new DelegationExecutor());
    private final TargetStore targetStore = Mockito.mock(TargetStore.class);
    private final ProbeStore probeStore = Mockito.mock(ProbeStore.class);
    private final TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);
    private final KeyValueStore keyValueStore = Mockito.mock(KeyValueStore.class);
    private final StitchingJournalFactory journalFactory = StitchingJournalFactory.emptyStitchingJournalFactory();
    private Scheduler scheduler;

    public static final long TEST_SCHEDULE_MILLIS = 100;
    public static final long SCHEDULED_TIMEOUT_SECONDS = 10;
    public static final long INITIAL_BROADCAST_INTERVAL_MINUTES = 1;
    private final long targetId = 1234;
    private final long probeId = 1L;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static String scheduleKey(String key) {
        return Scheduler.SCHEDULE_KEY_OFFSET + key;
    }

    @Before
    public void setup() throws Exception {
        Target target = Mockito.mock(Target.class);
        when(target.getId()).thenReturn(targetId);
        when(target.getProbeId()).thenReturn(probeId);
        when(targetStore.getTarget(targetId)).thenReturn(Optional.of(target));
        when(probeStore.getProbe(Mockito.anyLong())).thenAnswer(answer -> Optional.of(ProbeInfo.getDefaultInstance()));
        when(keyValueStore.get(anyString())).thenReturn(Optional.empty());

        when(operationManager.getActionTimeoutMs()).thenReturn(2000L);
        when(operationManager.getDiscoveryTimeoutMs()).thenReturn(1000L);
        when(operationManager.getValidationTimeoutMs()).thenReturn(4000L);

        scheduler = new Scheduler(operationManager, targetStore, probeStore, topologyHandler,
            keyValueStore, journalFactory, scheduledExecutorSpy, INITIAL_BROADCAST_INTERVAL_MINUTES);
    }

    @Test
    public void testSetDiscoverySchedule() throws Exception {
        final CountDownLatch discoveryExecutedLatch = new CountDownLatch(1);

        Mockito.doAnswer(unused -> {
            discoveryExecutedLatch.countDown();
            return null;
        }).when(operationManager).addPendingDiscovery(targetId);
        scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        discoveryExecutedLatch.await(SCHEDULED_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        verify(operationManager).addPendingDiscovery(targetId);
        assertFalse(scheduler.getDiscoverySchedule(targetId).get().isSynchedToBroadcast());
    }

    @Test
    public void testSetDiscoverySchedulePersistsSchedule() throws Exception {
        scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        verify(keyValueStore).put(
            scheduleKey(Long.toString(targetId)),
            new Gson().toJson(new TargetDiscoveryScheduleData(TEST_SCHEDULE_MILLIS, false))
        );
    }

    // A discovery that is scheduled when there is no prior schedule should be executed immediately.
    @Test
    public void testSetDiscoveryScheduleWithoutExistingExecutedImmediately() throws Exception {
        TargetDiscoverySchedule discoverySchedule =
            scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);

        // Either the delay should be zero or lower, or the task should have already executed
        long executionDelay = discoverySchedule.getDelay(TimeUnit.MILLISECONDS);
        if (executionDelay > 0) {
            // The task has already executed and should have added a pending discovery
            verify(operationManager).addPendingDiscovery(targetId);
        } else {
            // The task has not executed but should be set up for immediate execution.
            assertThat(executionDelay, is(lessThanOrEqualTo(0L)));
        }
    }

    @Test
    public void testScheduledTargetNotFoundExceptionWhenSet() throws Exception {
        when(targetStore.getTarget(0)).thenReturn(Optional.empty());
        expectedException.expect(TargetNotFoundException.class);
        expectedException.expectMessage("Target with id 0 does not exist in the store.");

        scheduler.setDiscoverySchedule(0, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testScheduledTargetNotFoundExceptionWhenRun() throws Exception {
        when(operationManager.addPendingDiscovery(targetId)).thenThrow(new TargetNotFoundException(targetId));
        scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);

        // Triggering the TargetNotFoundException should cause the removal of the scheduled discovery
        OperationTestUtilities.waitForEvent(scheduler, scheduler -> !scheduler.getDiscoverySchedule(targetId).isPresent());
    }

    @Test
    public void testScheduledInterruptedException() throws Exception {
        when(operationManager.addPendingDiscovery(targetId)).thenThrow(new InterruptedException());
        scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);

        // Triggering the InterruptedException should cause the removal of the scheduled discovery
        OperationTestUtilities.waitForEvent(scheduler, scheduler -> !scheduler.getDiscoverySchedule(targetId).isPresent());
    }

    @Test
    public void testUpdateScheduledDiscoveryCancelsExistingSchedule() throws Exception {
        final long tenMinutesMillis = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
        final ScheduledFuture<?> mockFuture = Mockito.mock(ScheduledFuture.class);
        Mockito.doReturn(mockFuture).when(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), Mockito.anyLong(), eq(tenMinutesMillis), any()
        );

        scheduler.setDiscoverySchedule(targetId, tenMinutesMillis, TimeUnit.MILLISECONDS);
        scheduler.setDiscoverySchedule(targetId, 5, TimeUnit.MINUTES);

        verify(mockFuture).cancel(Mockito.anyBoolean());
    }

    @Test
    public void testIllegalDiscoveryIntervalZero() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal discovery interval: 0");

        scheduler.setDiscoverySchedule(targetId, 0, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testIllegalDiscoveryIntervalNegative() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal discovery interval: -100");

        scheduler.setDiscoverySchedule(targetId, -100, TimeUnit.MINUTES);
    }

    @Test
    public void testOnTargetAdded() throws Exception {
        assertFalse(scheduler.getDiscoverySchedule(targetId).isPresent());

        scheduler.onTargetAdded(targetStore.getTarget(targetId).get());

        assertTrue(scheduler.getDiscoverySchedule(targetId).isPresent());
    }

    /**
     * Test that when an existing schedule is updated that the time that elapsed against
     * the prior schedule is counted against the initial delay in the new schedule.
     *
     * @throws Exception When there is an exception.
     */
    @Test
    public void testUpdateScheduledDiscoveryCountsElapsedTime() throws Exception {
        final long tenMinutesMillis = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

        // Mock the first schedule to say that 7 minutes remain on the schedule..
        final ScheduledFuture<?> mockFuture = Mockito.mock(ScheduledFuture.class);
        when(mockFuture.getDelay(TimeUnit.MILLISECONDS))
            .thenReturn(TimeUnit.MILLISECONDS.convert(7, TimeUnit.MINUTES));
        Mockito.doReturn(mockFuture).when(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), eq(0L), eq(tenMinutesMillis), any()
        );

        // Set up a 10-minute schedule, getting the mocked future which will say that 3 minutes
        // have elapsed (10-7).
        scheduler.setDiscoverySchedule(targetId, tenMinutesMillis, TimeUnit.MILLISECONDS);
        // Override with a 5-minute schedule. The arguments differ from those passed to the mocked
        // spy above, so it will not get the mockFuture when called.
        final TargetDiscoverySchedule updatedSchedule =
            scheduler.setDiscoverySchedule(targetId, 5, TimeUnit.MINUTES);

        // Expect the overridden version to account for the mocked 3 minutes that elapsed
        // in the first schedule
        assertEquals(
            TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES),
            updatedSchedule.getScheduleInterval(TimeUnit.MILLISECONDS)
        );
        Assert.assertThat(
            updatedSchedule.getDelay(TimeUnit.MILLISECONDS),
            is(lessThanOrEqualTo(TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES)))
        );
    }

    /**
     * Test that when a schedule has more time elapsed than the interval
     * for the schedule replacing it that an initial delay of 0 is used
     * for the new schedule.
     *
     * @throws Exception When there is an exception.
     */
    @Test
    public void testUpdateScheduledDiscoveryTooMuchTimeElapsed() throws Exception {
        final long tenMinutesMillis = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

        // Mock the first schedule to say that 3 minutes remain.
        final ScheduledFuture<?> mockFuture = Mockito.mock(ScheduledFuture.class);
        when(mockFuture.getDelay(TimeUnit.MILLISECONDS))
            .thenReturn(TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES));
        Mockito.doReturn(mockFuture).when(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), eq(0L), eq(tenMinutesMillis), any()
        );

        // Set up a 10-minute schedule, getting the mocked future.
        scheduler.setDiscoverySchedule(targetId, tenMinutesMillis, TimeUnit.MILLISECONDS);
        // Override with a 5-minute schedule. The arguments differ from those passed to the mocked
        // spy above, so it will not get the mockFuture when called.
        scheduler.setDiscoverySchedule(targetId, 5, TimeUnit.MINUTES);

        verify(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), eq(0L), eq(tenMinutesMillis), any()
        );
        verify(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), eq(0L), eq(TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES)), any()
        );
    }

    @Test
    public void testGetDiscoverySchedule() throws Exception {
        assertFalse(scheduler.getDiscoverySchedule(targetId).isPresent());
        scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(scheduler.getDiscoverySchedule(targetId).isPresent());
    }

    @Test
    public void testCancelDiscoverySchedule() throws Exception {
        final TargetDiscoverySchedule task =
            scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(scheduler.getDiscoverySchedule(targetId).isPresent());
        assertFalse(task.isCancelled());

        final Optional<TargetDiscoverySchedule> cancelledTask = scheduler.cancelDiscoverySchedule(targetId);
        assertEquals(targetId, cancelledTask.get().getTargetId());
        assertTrue(cancelledTask.get().isCancelled());
        assertFalse(scheduler.getDiscoverySchedule(targetId).isPresent());
    }

    @Test
    public void testResetDiscoveryScheduleEmpty() throws Exception {
        assertFalse(scheduler.resetDiscoverySchedule(targetId).isPresent());
    }

    @Test
    public void testResetDiscoverySchedulePresent() throws Exception {
        final TargetDiscoverySchedule originalTask =
            scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        assertFalse(originalTask.isCancelled());

        final Optional<TargetDiscoverySchedule> resetTask = scheduler.resetDiscoverySchedule(targetId);
        assertTrue(originalTask.isCancelled());
        assertFalse(resetTask.get().isCancelled());
    }

    @Test
    public void testSetBroadcastSynchedDiscoverySchedule() throws Exception {
        scheduler.setBroadcastSynchedDiscoverySchedule(targetId);

        TargetDiscoverySchedule schedule = scheduler.getDiscoverySchedule(targetId).get();
        assertTrue(schedule.isSynchedToBroadcast());
        assertEquals(
            scheduler.getBroadcastSchedule().get().getScheduleInterval(TimeUnit.MINUTES),
            schedule.getScheduleInterval(TimeUnit.MINUTES)
        );
    }

    @Test
    public void testSetBroadcastSynchedDiscoverySchedulePersistsSchedule() throws Exception {
        long broadcastIntervalMillis =
            scheduler.getBroadcastSchedule().get().getScheduleInterval(TimeUnit.MILLISECONDS);
        scheduler.setBroadcastSynchedDiscoverySchedule(targetId);

        verify(keyValueStore).put(
            scheduleKey(Long.toString(targetId)),
            new Gson().toJson(new TargetDiscoveryScheduleData(broadcastIntervalMillis, true))
        );
    }

    @Test
    public void testSetBroadcastSchedule() throws Exception {
        final CountDownLatch broadcastLatch = new CountDownLatch(1);

        Mockito.doAnswer(unused -> {
            broadcastLatch.countDown();
            return null;
        }).when(topologyHandler).broadcastLatestTopology(any(StitchingJournalFactory.class));
        scheduler.setBroadcastSchedule(TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        broadcastLatch.await(SCHEDULED_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        final Optional<TopologyBroadcastSchedule> task = scheduler.getBroadcastSchedule();
        assertTrue(task.isPresent());
        assertEquals(TEST_SCHEDULE_MILLIS, task.get().getScheduleInterval(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testSetBroadcastScheduleSavesSchedule() throws Exception {
        long broadcastIntervalMillis =
            scheduler.getBroadcastSchedule().get().getScheduleInterval(TimeUnit.MILLISECONDS);

        // SetBroadcastSchedule should be called in the constructor
        verify(keyValueStore).put(
            scheduleKey(Scheduler.BROADCAST_SCHEDULE_KEY),
            new Gson().toJson(new ScheduleData(broadcastIntervalMillis))
        );
    }

    @Test
    public void testIllegalBroadcastIntervalZero() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal broadcast interval: 0");

        scheduler.setBroadcastSchedule(0, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testIllegalBroadcastIntervalNegative() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal broadcast interval: -1234");

        scheduler.setBroadcastSchedule(-1234, TimeUnit.MILLISECONDS);
    }

    @Test
    public void testGetBroadcastScheduleWhenNoneSet() throws Exception {
        scheduler.cancelBroadcastSchedule();
        final Optional<TopologyBroadcastSchedule> task = scheduler.getBroadcastSchedule();
        assertFalse(task.isPresent());
    }

    @Test
    public void testInitialBroadcastInterval() throws Exception {
        assertEquals(
            INITIAL_BROADCAST_INTERVAL_MINUTES,
            scheduler.getBroadcastSchedule().get().getScheduleInterval(TimeUnit.MINUTES)
        );
    }

    @Test
    public void testIllegalInitialBroadcastInterval() throws Exception {
        final Scheduler schedulerWithIllegalInitialInterval = new Scheduler(operationManager,
            targetStore, probeStore, topologyHandler, keyValueStore, journalFactory,
                scheduledExecutorSpy, -1);

        assertEquals(
            Scheduler.FAILOVER_INITIAL_BROADCAST_INTERVAL_MINUTES,
            schedulerWithIllegalInitialInterval.getBroadcastSchedule().get().getScheduleInterval(TimeUnit.MINUTES)
        );
    }

    @Test
    public void testInitialBroadcastScheduleWhenLoaded() throws Exception {
        when(keyValueStore.get(scheduleKey(Scheduler.BROADCAST_SCHEDULE_KEY)))
            .thenReturn(Optional.of(new Gson().toJson(new ScheduleData(TEST_SCHEDULE_MILLIS))));

        Scheduler scheduler = new Scheduler(operationManager, targetStore, probeStore,
                topologyHandler, keyValueStore, journalFactory, scheduledExecutorSpy,
                INITIAL_BROADCAST_INTERVAL_MINUTES);

        TopologyBroadcastSchedule schedule = scheduler.getBroadcastSchedule().get();
        assertEquals(TEST_SCHEDULE_MILLIS, schedule.getScheduleInterval(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testUpdateBroadcastScheduleUpdatesSynchedDiscoveries() throws Exception {
        long synchedTarget = 9999;
        when(targetStore.getTarget(synchedTarget)).thenReturn(Optional.of(Mockito.mock(Target.class)));

        TargetDiscoverySchedule nonSynchedSchedule = scheduler.setDiscoverySchedule(targetId, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        TargetDiscoverySchedule synchedSchedule = scheduler.setBroadcastSynchedDiscoverySchedule(synchedTarget);

        assertNotEquals(7, nonSynchedSchedule.getScheduleInterval(TimeUnit.MINUTES));
        assertNotEquals(7, synchedSchedule.getScheduleInterval(TimeUnit.MINUTES));
        assertTrue(synchedSchedule.isSynchedToBroadcast());

        scheduler.setBroadcastSchedule(7, TimeUnit.MINUTES);
        nonSynchedSchedule = scheduler.getDiscoverySchedule(targetId).get();
        synchedSchedule = scheduler.getDiscoverySchedule(synchedTarget).get();

        assertNotEquals(7, nonSynchedSchedule.getScheduleInterval(TimeUnit.MINUTES));
        assertEquals(7, synchedSchedule.getScheduleInterval(TimeUnit.MINUTES));
        assertTrue(synchedSchedule.isSynchedToBroadcast());
    }

    /**
     * Test that when an existing schedule is updated that the time that elapsed against
     * the prior schedule is counted against the initial delay in the new schedule.
     *
     * @throws Exception When there is an exception.
     */
    @Test
    public void testUpdateTopologyBroadcastCountsElapsedTime() throws Exception {
        final long tenMinutesMillis = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

        // Mock the first schedule to say that 7 minutes remain on the schedule..
        final ScheduledFuture<?> mockFuture = Mockito.mock(ScheduledFuture.class);
        when(mockFuture.getDelay(TimeUnit.MILLISECONDS))
            .thenReturn(TimeUnit.MILLISECONDS.convert(7, TimeUnit.MINUTES));
        Mockito.doReturn(mockFuture).when(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), Mockito.anyLong(), eq(tenMinutesMillis), any()
        );

        // Set up a 10-minute schedule, getting the mocked future which will say that 3 minutes
        // have elapsed (10-7).
        scheduler.setBroadcastSchedule(tenMinutesMillis, TimeUnit.MILLISECONDS);
        // Override with a 5-minute schedule. The arguments differ from those passed to the mocked
        // spy above, so it will not get the mockFuture when called.
        final TopologyBroadcastSchedule updatedSchedule =
            scheduler.setBroadcastSchedule(5, TimeUnit.MINUTES);

        // Expect the overridden version to account for the mocked 3 minutes that elapsed
        // in the first schedule
        assertEquals(
            TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES),
            updatedSchedule.getScheduleInterval(TimeUnit.MILLISECONDS)
        );
        Assert.assertThat(
            updatedSchedule.getDelay(TimeUnit.MILLISECONDS),
            is(lessThanOrEqualTo(TimeUnit.MILLISECONDS.convert(2, TimeUnit.MINUTES)))
        );
    }

    /**
     * Test that when a schedule has more time elapsed than the interval
     * for the schedule replacing it that an initial delay of 0 is used
     * for the new schedule.
     *
     * @throws Exception When there is an exception.
     */
    @Test
    public void testUpdateScheduledBroadcastTooMuchTimeElapsed() throws Exception {
        final long tenMinutesMillis = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);

        // Mock the first schedule to say that 3 minutes remain.
        final ScheduledFuture<?> mockFuture = Mockito.mock(ScheduledFuture.class);
        when(mockFuture.getDelay(TimeUnit.MILLISECONDS))
            .thenReturn(TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES));
        Mockito.doReturn(mockFuture).when(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), Mockito.anyLong(), eq(tenMinutesMillis), any()
        );

        // Set up a 10-minute schedule, getting the mocked future.
        scheduler.setBroadcastSchedule(tenMinutesMillis, TimeUnit.MILLISECONDS);
        // Override with a 5-minute schedule. The arguments differ from those passed to the mocked
        // spy above, so it will not get the mockFuture when called.
        scheduler.setBroadcastSchedule(5, TimeUnit.MINUTES);

        verify(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), Mockito.anyLong(), eq(tenMinutesMillis), any()
        );
        verify(scheduledExecutorSpy).scheduleAtFixedRate(
            any(), eq(0L), eq(TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES)), any()
        );
    }

    @Test
    public void testCancelBroadcastSchedule() throws Exception {
        final TopologyBroadcastSchedule task =
            scheduler.setBroadcastSchedule(TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(scheduler.getBroadcastSchedule().isPresent());
        assertFalse(task.isCancelled());

        final Optional<TopologyBroadcastSchedule> cancelledTask = scheduler.cancelBroadcastSchedule();
        assertTrue(cancelledTask.get().isCancelled());
        assertFalse(scheduler.getDiscoverySchedule(targetId).isPresent());
    }

    @Test
    public void testCancelBroadcastScheduleDeletesPersistedSchedule() throws Exception {
        scheduler.cancelBroadcastSchedule();
        verify(keyValueStore).remove(scheduleKey(Scheduler.BROADCAST_SCHEDULE_KEY));
    }

    @Test
    public void testResetBroadcastScheduleEmpty() throws Exception {
        scheduler.cancelBroadcastSchedule();
        assertFalse(scheduler.resetBroadcastSchedule().isPresent());
    }

    @Test
    public void testResetBroadcastSchedulePresent() throws Exception {
        final TopologyBroadcastSchedule originalTask =
            scheduler.setBroadcastSchedule(TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        assertFalse(originalTask.isCancelled());

        final Optional<TopologyBroadcastSchedule> resetTask = scheduler.resetBroadcastSchedule();
        assertTrue(originalTask.isCancelled());
        assertFalse(resetTask.get().isCancelled());
    }

    @Test
    public void testTargetDiscoverySchedulesAreDefaultInitializedWhenNotLoaded() throws Exception {
        Target target = targetStore.getTarget(targetId).get();
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target));

        Scheduler scheduler = new Scheduler(operationManager, targetStore, probeStore,
                topologyHandler, keyValueStore, journalFactory, scheduledExecutorSpy,
                INITIAL_BROADCAST_INTERVAL_MINUTES);

        TargetDiscoverySchedule schedule = scheduler.getDiscoverySchedule(targetId).get();
        assertEquals(INITIAL_BROADCAST_INTERVAL_MINUTES, schedule.getScheduleInterval(TimeUnit.MINUTES));
        assertTrue(schedule.isSynchedToBroadcast());
    }

    @Test
    public void testTargetDiscoverySchedulesAreLoaded() throws Exception {
        Target target = targetStore.getTarget(targetId).get();
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target));
        when(keyValueStore.get(scheduleKey(Long.toString(targetId))))
            .thenReturn(Optional.of(new Gson().toJson(new TargetDiscoveryScheduleData(TEST_SCHEDULE_MILLIS, false))));

        Scheduler scheduler = new Scheduler(operationManager, targetStore, probeStore,
                topologyHandler, keyValueStore, journalFactory, scheduledExecutorSpy,
                INITIAL_BROADCAST_INTERVAL_MINUTES);

        TargetDiscoverySchedule schedule = scheduler.getDiscoverySchedule(targetId).get();
        assertEquals(TEST_SCHEDULE_MILLIS, schedule.getScheduleInterval(TimeUnit.MILLISECONDS));
        assertFalse(schedule.isSynchedToBroadcast());
    }

    @Test
    public void testScheduleToCheckForTimeouts() throws Exception {
        when(operationManager.getActionTimeoutMs()).thenReturn(10L);
        when(operationManager.getDiscoveryTimeoutMs()).thenReturn(20L);
        when(operationManager.getValidationTimeoutMs()).thenReturn(30L);

        scheduler = new Scheduler(operationManager, targetStore, probeStore, topologyHandler,
            keyValueStore, journalFactory,
            scheduledExecutorSpy, INITIAL_BROADCAST_INTERVAL_MINUTES);

        // A schedule should be added that checks for timeouts based on the shortest timeout among
        // action, discovery, and validation operations.
        Mockito.verify(scheduledExecutorSpy).scheduleAtFixedRate(any(), eq(10L), eq(10L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testProbeDiscoveryInterval() {
        ProbeInfo standardProbeInfo = ProbeInfo.newBuilder()
                .setProbeType("TestProbe")
                .setProbeCategory("Test")
                .build();
        Assert.assertEquals(60000, scheduler.getProbeDiscoveryInterval(standardProbeInfo));

        // test with longer discovery interval
        ProbeInfo slowDiscoveryProbeInfo = ProbeInfo.newBuilder(standardProbeInfo)
                .setFullRediscoveryIntervalSeconds(999999).build();
        Assert.assertEquals(999999000, scheduler.getProbeDiscoveryInterval(slowDiscoveryProbeInfo));

        // test with longer discovery interval and performance discovery interval
        ProbeInfo slowFullFastPerformanceProbeInfo = ProbeInfo.newBuilder(standardProbeInfo)
                .setFullRediscoveryIntervalSeconds(999999)
                .setPerformanceRediscoveryIntervalSeconds(99).build();
        Assert.assertEquals(99000, scheduler.getProbeDiscoveryInterval(slowFullFastPerformanceProbeInfo));

        // test with performance discovery interval slower than full discovery interval
        ProbeInfo fastFullSlowPerformanceProbeInfo = ProbeInfo.newBuilder(standardProbeInfo)
                .setFullRediscoveryIntervalSeconds(99)
                .setPerformanceRediscoveryIntervalSeconds(9999).build();
        Assert.assertEquals(99000, scheduler.getProbeDiscoveryInterval(fastFullSlowPerformanceProbeInfo));

    }

    /**
     * Because actual executors are declared final and cannot be mocked or spyed by Mockito,
     * we implement the ScheduledExecutorService interface in a simple class that simply delegates all calls
     * to a contained single thread scheduled executor.
     */
    public class DelegationExecutor implements ScheduledExecutorService {
        private final ScheduledExecutorService innerExecutor = Executors.newSingleThreadScheduledExecutor(
                new BasicThreadFactory.Builder().namingPattern("DelegationExecutor-%d").build()
        );

        @Override
        public void shutdown() {
            innerExecutor.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return innerExecutor.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return innerExecutor.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return innerExecutor.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return innerExecutor.awaitTermination(timeout, unit);
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            return innerExecutor.submit(task);
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            return innerExecutor.submit(task, result);
        }

        @Override
        public Future<?> submit(Runnable task) {
            return innerExecutor.submit(task);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
                throws InterruptedException {
            return innerExecutor.invokeAll(tasks);
        }

        @Override
        public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
            return innerExecutor.invokeAll(tasks, timeout, unit);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
            return innerExecutor.invokeAny(tasks);
        }

        @Override
        public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return innerExecutor.invokeAny(tasks, timeout, unit);
        }

        @Override
        public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
            return innerExecutor.schedule(callable, delay, unit);
        }

        @Override
        public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
            return innerExecutor.schedule(command, delay, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay,
                                                      long period, TimeUnit unit) {
            return innerExecutor.scheduleAtFixedRate(command, initialDelay, period, unit);
        }

        @Override
        public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay,
                                                         long delay, TimeUnit unit) {
            return innerExecutor.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        }

        @Override
        public void execute(Runnable command) {
            innerExecutor.execute(command);
        }
    }
}