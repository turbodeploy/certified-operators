package com.vmturbo.topology.processor.scheduling;

import static junit.framework.TestCase.assertFalse;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Collections;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import com.vmturbo.kvstore.KeyValueStore;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.topology.processor.operation.OperationManager;
import com.vmturbo.topology.processor.operation.OperationTestUtilities;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.probes.ProbeStore;
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

    @Mock
    private OperationManager operationManager;

    @Mock
    private TargetStore targetStore;

    @Mock
    private ProbeStore probeStore = mock(ProbeStore.class);

    @Mock
    private TopologyHandler topologyHandler = mock(TopologyHandler.class);

    @Mock
    private KeyValueStore keyValueStore = mock(KeyValueStore.class);

    @Mock
    private StitchingJournalFactory journalFactory =
            StitchingJournalFactory.emptyStitchingJournalFactory();

    @Spy
    private final ScheduledExecutorService fullDiscoveryExecutorSpy1 =
            Mockito.spy(new DelegationExecutor());
    @Spy
    private final ScheduledExecutorService fullDiscoveryExecutorSpy2 =
            Mockito.spy(new DelegationExecutor());

    @Spy
    private final ScheduledExecutorService fullDiscoveryExecutorSpy4 =
            Mockito.spy(new DelegationExecutor());

    @Spy
    private final ScheduledExecutorService incrementalDiscoveryExecutorSpy1 =
            Mockito.spy(new DelegationExecutor());

    @Spy
    private final ScheduledExecutorService incrementalDiscoveryExecutorSpy2 =
            Mockito.spy(new DelegationExecutor());

    @Spy
    private final ScheduledExecutorService incrementalDiscoveryExecutorSpy4 =
            Mockito.spy(new DelegationExecutor());

    @Spy
    private final ScheduledExecutorService broadcastExecutorSpy =
            Mockito.spy(new DelegationExecutor());

    @Spy
    private final ScheduledExecutorService expirationExecutorSpy =
            Mockito.spy(new DelegationExecutor());

    private Scheduler scheduler;

    private static final long TEST_SCHEDULE_MILLIS = 100;
    private static final long SCHEDULED_TIMEOUT_SECONDS = 10;
    private static final long INITIAL_BROADCAST_INTERVAL_MINUTES = 1;
    private static final int  NUM_DISCOVERIES_SKIPPED_BEFORE_LOGGING = 10;
    private static final int PROBE_4_REDISCOVERY_INTERVAL = 120;
    private static final int PROBE_4_INCREMENTAL_REDISCOVERY_INTERVAL = 5;
    private static final long PROBE_ID_1 = 1L;
    private static final long PROBE_ID_2 = 2L;
    private static final long PROBE_ID_3 = 3L;
    private static final long PROBE_ID_4 = 4L;
    private static final String PROBE_TYPE_1 = "type 1";
    private static final ProbeInfo PROBE_TYPE_1_INFO = ProbeInfo.newBuilder()
            .setProbeType(PROBE_TYPE_1)
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setFullRediscoveryIntervalSeconds(10)
            .build();
    private static final String PROBE_TYPE_2 = "type 2";
    private static final ProbeInfo PROBE_TYPE_2_INFO = ProbeInfo.newBuilder()
            .setProbeType(PROBE_TYPE_2)
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setFullRediscoveryIntervalSeconds(10)
            .build();
    private static final String PROBE_TYPE_3 = "type 3";
    private static final ProbeInfo PROBE_TYPE_3_INFO = ProbeInfo.newBuilder()
            .setProbeType(PROBE_TYPE_3)
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .build();
    private static final String PROBE_TYPE_4 = "type 4";
    private static final ProbeInfo PROBE_TYPE_4_INFO = ProbeInfo.newBuilder()
            .setProbeType(PROBE_TYPE_4)
            .setProbeCategory(ProbeCategory.HYPERVISOR.getCategory())
            .setFullRediscoveryIntervalSeconds(PROBE_4_REDISCOVERY_INTERVAL)
            .setIncrementalRediscoveryIntervalSeconds(PROBE_4_INCREMENTAL_REDISCOVERY_INTERVAL)
            .build();

    private final long targetId = 1234;
    private final long targetId2 = 1235;
    private final long targetId4 = 1237;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static String scheduleKey(String key) {
        return Scheduler.SCHEDULE_KEY_OFFSET + key;
    }

    private ScheduledExecutorService getFullDiscoveryExecutorService(String name) {
        if (name.startsWith(PROBE_TYPE_1)) {
            return fullDiscoveryExecutorSpy1;
        } else if (name.startsWith(PROBE_TYPE_2)) {
            return fullDiscoveryExecutorSpy2;
        }
        else if (name.startsWith(PROBE_TYPE_4)) {
            return fullDiscoveryExecutorSpy4;
        }
        else {
            return null;
        }
    }

    private ScheduledExecutorService getIncrementalDiscoveryExecutorService(String name) {
        if (name.startsWith(PROBE_TYPE_1)) {
            return incrementalDiscoveryExecutorSpy1;
        } else if (name.startsWith(PROBE_TYPE_2)) {
            return incrementalDiscoveryExecutorSpy2;
        } else if (name.startsWith(PROBE_TYPE_4)) {
            return incrementalDiscoveryExecutorSpy4;
        } else {
            return null;
        }
    }

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(this);

        final Target target = mock(Target.class);
        when(target.getId()).thenReturn(targetId);
        when(target.getProbeId()).thenReturn(PROBE_ID_1);
        when(target.getProbeInfo()).thenReturn(PROBE_TYPE_1_INFO);
        final Target target2 = mock(Target.class);
        when(target2.getId()).thenReturn(targetId2);
        when(target2.getProbeId()).thenReturn(PROBE_ID_3);
        when(target2.getProbeInfo()).thenReturn(PROBE_TYPE_3_INFO);
        final Target target4 = mock(Target.class);
        when(target4.getId()).thenReturn(targetId4);
        when(target4.getProbeId()).thenReturn(PROBE_ID_4);
        when(target4.getProbeInfo()).thenReturn(PROBE_TYPE_4_INFO);

        when(targetStore.getTarget(targetId)).thenReturn(Optional.of(target));
        when(targetStore.getTarget(targetId2)).thenReturn(Optional.of(target2));
        when(targetStore.getTarget(targetId4)).thenReturn(Optional.of(target4));
        when(targetStore.getProbeTargets(PROBE_ID_4)).thenReturn(ImmutableList.of(target4));

        when(probeStore.getProbe(PROBE_ID_1)).thenAnswer(answer -> Optional.of(PROBE_TYPE_1_INFO));
        when(probeStore.getProbe(PROBE_ID_2)).thenAnswer(answer -> Optional.of(PROBE_TYPE_2_INFO));
        when(probeStore.getProbe(PROBE_ID_3)).thenReturn(Optional.of(PROBE_TYPE_3_INFO));
        when(probeStore.getProbe(PROBE_ID_4)).thenReturn(Optional.of(PROBE_TYPE_4_INFO));

        when(keyValueStore.get(anyString())).thenReturn(Optional.empty());

        when(operationManager.getActionTimeoutMs()).thenReturn(2000L);
        when(operationManager.getDiscoveryTimeoutMs()).thenReturn(1000L);
        when(operationManager.getValidationTimeoutMs()).thenReturn(4000L);
        when(operationManager.getLastDiscoveryForTarget(anyLong(), any()))
                .thenReturn(Optional.empty());

        scheduler = new Scheduler(operationManager, targetStore, probeStore, topologyHandler,
            keyValueStore, journalFactory, (name) -> getFullDiscoveryExecutorService(name),
            (name) -> getIncrementalDiscoveryExecutorService(name), broadcastExecutorSpy,
            expirationExecutorSpy, INITIAL_BROADCAST_INTERVAL_MINUTES,
            NUM_DISCOVERIES_SKIPPED_BEFORE_LOGGING);

        scheduler.initialize();
    }

    @After
    public void cleanup() {
        fullDiscoveryExecutorSpy1.shutdownNow();
        fullDiscoveryExecutorSpy2.shutdownNow();
        fullDiscoveryExecutorSpy4.shutdownNow();
        incrementalDiscoveryExecutorSpy1.shutdownNow();
        incrementalDiscoveryExecutorSpy2.shutdownNow();
        incrementalDiscoveryExecutorSpy4.shutdownNow();
        broadcastExecutorSpy.shutdownNow();
        expirationExecutorSpy.shutdownNow();
    }

    @Test
    public void testSetDiscoverySchedule() throws Exception {
        final CountDownLatch discoveryExecutedLatch = new CountDownLatch(1);

        Mockito.doAnswer(unused -> {
            discoveryExecutedLatch.countDown();
            return null;
        }).when(operationManager).addPendingDiscovery(targetId, DiscoveryType.FULL);
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
            false);
        discoveryExecutedLatch.await(SCHEDULED_TIMEOUT_SECONDS, TimeUnit.SECONDS);

        verify(operationManager).addPendingDiscovery(targetId, DiscoveryType.FULL);
        assertFalse(scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).get().isSynchedToBroadcast());
    }

    @Test
    public void testSetDiscoverySchedulePersistsSchedule() throws Exception {
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
            false);
        verify(keyValueStore).put(
            scheduleKey(Long.toString(targetId)),
            new Gson().toJson(new TargetDiscoveryScheduleData(TEST_SCHEDULE_MILLIS, false))
        );
        // no additional consul call to get it since it's already in memory
        verify(keyValueStore, times(0)).getByPrefix(Scheduler.SCHEDULE_KEY_OFFSET + targetId);
    }

    // A discovery that is scheduled when there is no prior schedule should be executed immediately.
    @Test
    public void testSetDiscoveryScheduleWithoutExistingExecutedImmediately() throws Exception {
        TargetDiscoverySchedule discoverySchedule =
            scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
                false);

        // Either the delay should be zero or lower, or the task should have already executed
        long executionDelay = discoverySchedule.getDelay(TimeUnit.MILLISECONDS);
        if (executionDelay > 0) {
            // The task has already executed and should have added a pending discovery
            verify(operationManager).addPendingDiscovery(targetId, DiscoveryType.FULL);
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

        scheduler.setDiscoverySchedule(0, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
            false);
    }

    @Test
    public void testScheduledTargetNotFoundExceptionWhenRun() throws Exception {
        when(operationManager.addPendingDiscovery(targetId, DiscoveryType.FULL)).thenThrow(new TargetNotFoundException(targetId));
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
            false);

        // Triggering the TargetNotFoundException should cause the removal of the scheduled discovery
        OperationTestUtilities.waitForEvent(() ->
            !scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
    }

    @Test
    public void testScheduledInterruptedException() throws Exception {
        when(operationManager.addPendingDiscovery(targetId, DiscoveryType.FULL)).thenThrow(new InterruptedException());
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
            false);

        // Triggering the InterruptedException should cause the removal of the scheduled discovery
        OperationTestUtilities.waitForEvent(() ->
            !scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
    }

    @Test
    public void testUpdateScheduledDiscoveryCancelsExistingSchedule() throws Exception {
        final long tenMinutesMillis = TimeUnit.MILLISECONDS.convert(10, TimeUnit.MINUTES);
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        Mockito.doReturn(mockFuture).when(fullDiscoveryExecutorSpy1).scheduleAtFixedRate(
            any(), Mockito.anyLong(), eq(tenMinutesMillis), any()
        );

        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, tenMinutesMillis, TimeUnit.MILLISECONDS,
            false);
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, 5, TimeUnit.MINUTES, false);

        verify(mockFuture).cancel(Mockito.anyBoolean());
    }

    @Test
    public void testIllegalDiscoveryIntervalZero() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal discovery interval: 0");

        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, 0, TimeUnit.MILLISECONDS,
            false);
    }

    @Test
    public void testIllegalDiscoveryIntervalNegative() throws Exception {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Illegal discovery interval: -100");

        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, -100, TimeUnit.MINUTES, false);
    }

    @Test
    public void testOnTargetAdded() throws Exception {
        assertFalse(scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());

        scheduler.onTargetAdded(targetStore.getTarget(targetId).get());

        assertTrue(scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
    }

    /**
     * Tests that discovery is not scheduled for a target, that is not able to perform discoveries.
     */
    @Test
    public void testOnNonDiscoveryTargetAdded() {
        Assert.assertFalse(scheduler.getDiscoverySchedule(targetId2, DiscoveryType.FULL).isPresent());
        scheduler.onTargetAdded(targetStore.getTarget(targetId2).get());
        Assert.assertFalse(scheduler.getDiscoverySchedule(targetId2, DiscoveryType.FULL).isPresent());
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
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        when(mockFuture.getDelay(TimeUnit.MILLISECONDS))
            .thenReturn(TimeUnit.MILLISECONDS.convert(7, TimeUnit.MINUTES));
        Mockito.doReturn(mockFuture).when(fullDiscoveryExecutorSpy1).scheduleAtFixedRate(
            any(), eq(0L), eq(tenMinutesMillis), any()
        );

        // Set up a 10-minute schedule, getting the mocked future which will say that 3 minutes
        // have elapsed (10-7).
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, tenMinutesMillis,
            TimeUnit.MILLISECONDS, false);
        // Override with a 5-minute schedule. The arguments differ from those passed to the mocked
        // spy above, so it will not get the mockFuture when called.
        final TargetDiscoverySchedule updatedSchedule =
            scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, 5, TimeUnit.MINUTES, false);

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
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        when(mockFuture.getDelay(TimeUnit.MILLISECONDS))
            .thenReturn(TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES));
        Mockito.doReturn(mockFuture).when(fullDiscoveryExecutorSpy1).scheduleAtFixedRate(
            any(), eq(0L), eq(tenMinutesMillis), any()
        );

        // Set up a 10-minute schedule, getting the mocked future.
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, tenMinutesMillis, TimeUnit.MILLISECONDS,
            false);
        // Override with a 5-minute schedule. The arguments differ from those passed to the mocked
        // spy above, so it will not get the mockFuture when called.
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, 5, TimeUnit.MINUTES, false);

        verify(fullDiscoveryExecutorSpy1).scheduleAtFixedRate(
            any(), eq(0L), eq(tenMinutesMillis), any()
        );
        verify(fullDiscoveryExecutorSpy1).scheduleAtFixedRate(
            any(), eq(0L), eq(TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES)), any()
        );
    }

    @Test
    public void testGetDiscoverySchedule() throws Exception {
        assertFalse(scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
            false);
        assertTrue(scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
    }

    @Test
    public void testCancelDiscoverySchedule() throws Exception {
        final TargetDiscoverySchedule task =
            scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
                false);
        assertTrue(scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
        assertFalse(task.isCancelled());

        final Optional<TargetDiscoverySchedule> cancelledTask =
            scheduler.disableDiscoverySchedule(targetId, DiscoveryType.FULL);
        assertEquals(targetId, cancelledTask.get().getTargetId());
        assertTrue(cancelledTask.get().isCancelled());
        assertFalse(scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
    }

    @Test
    public void testResetDiscoveryScheduleEmpty() throws Exception {
        assertFalse(scheduler.resetDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
    }

    @Test
    public void testResetDiscoverySchedulePresent() throws Exception {
        final TargetDiscoverySchedule originalTask =
            scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS,
                false);
        assertFalse(originalTask.isCancelled());

        final Optional<TargetDiscoverySchedule> resetTask = scheduler.resetDiscoverySchedule(targetId, DiscoveryType.FULL);
        assertTrue(originalTask.isCancelled());
        assertFalse(resetTask.get().isCancelled());
    }

    @Test
    public void testSetBroadcastSynchedDiscoverySchedule() throws Exception {
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, true);

        TargetDiscoverySchedule schedule = scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).get();
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
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, true);

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
                (name) -> fullDiscoveryExecutorSpy1, (name) -> incrementalDiscoveryExecutorSpy1,
                broadcastExecutorSpy, expirationExecutorSpy, -1,
                NUM_DISCOVERIES_SKIPPED_BEFORE_LOGGING);
        schedulerWithIllegalInitialInterval.initialize();
        assertEquals(
            Scheduler.FAILOVER_INITIAL_BROADCAST_INTERVAL_MINUTES,
            schedulerWithIllegalInitialInterval.getBroadcastSchedule().get().getScheduleInterval(TimeUnit.MINUTES)
        );
    }

    @Test
    public void testUpdateBroadcastScheduleUpdatesSynchedDiscoveries() throws Exception {
        long synchedTarget = 9999;
        Target target = mock(Target.class);
        when(target.getProbeInfo()).thenReturn(PROBE_TYPE_1_INFO);
        when(targetStore.getTarget(synchedTarget)).thenReturn(Optional.of(target));
        when(target.getProbeId()).thenReturn(PROBE_ID_1);

        TargetDiscoverySchedule nonSynchedSchedule = scheduler.setDiscoverySchedule(targetId,
            DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS, false);
        TargetDiscoverySchedule synchedSchedule = scheduler.setDiscoverySchedule(synchedTarget,
            DiscoveryType.FULL, TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS, true);

        assertNotEquals(7, nonSynchedSchedule.getScheduleInterval(TimeUnit.MINUTES));
        assertNotEquals(7, synchedSchedule.getScheduleInterval(TimeUnit.MINUTES));
        assertTrue(synchedSchedule.isSynchedToBroadcast());

        scheduler.setBroadcastSchedule(7, TimeUnit.MINUTES);
        nonSynchedSchedule = scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).get();
        synchedSchedule = scheduler.getDiscoverySchedule(synchedTarget, DiscoveryType.FULL).get();

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
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        when(mockFuture.getDelay(TimeUnit.MILLISECONDS))
            .thenReturn(TimeUnit.MILLISECONDS.convert(7, TimeUnit.MINUTES));
        Mockito.doReturn(mockFuture).when(broadcastExecutorSpy).scheduleAtFixedRate(
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
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        when(mockFuture.getDelay(TimeUnit.MILLISECONDS))
            .thenReturn(TimeUnit.MILLISECONDS.convert(3, TimeUnit.MINUTES));
        Mockito.doReturn(mockFuture).when(broadcastExecutorSpy).scheduleAtFixedRate(
            any(), Mockito.anyLong(), eq(tenMinutesMillis), any()
        );

        // Set up a 10-minute schedule, getting the mocked future.
        scheduler.setBroadcastSchedule(tenMinutesMillis, TimeUnit.MILLISECONDS);
        // Override with a 5-minute schedule. The arguments differ from those passed to the mocked
        // spy above, so it will not get the mockFuture when called.
        scheduler.setBroadcastSchedule(5, TimeUnit.MINUTES);

        verify(broadcastExecutorSpy).scheduleAtFixedRate(
            any(), Mockito.anyLong(), eq(tenMinutesMillis), any()
        );
        verify(broadcastExecutorSpy).scheduleAtFixedRate(
            any(), eq(0L), eq(TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES)), any()
        );
    }

    @Test
    public void testCancelBroadcastSchedule() throws Exception {
        final TopologyBroadcastSchedule task =
            scheduler.setBroadcastSchedule(TEST_SCHEDULE_MILLIS, TimeUnit.MILLISECONDS);
        assertTrue(scheduler.getBroadcastSchedule().isPresent());
        assertFalse(task.isCancelled());

        assertFalse(scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).isPresent());
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
            topologyHandler, keyValueStore, journalFactory, (name) -> fullDiscoveryExecutorSpy1,
            (name) -> incrementalDiscoveryExecutorSpy1, broadcastExecutorSpy, expirationExecutorSpy,
            INITIAL_BROADCAST_INTERVAL_MINUTES, NUM_DISCOVERIES_SKIPPED_BEFORE_LOGGING);
        scheduler.initialize();
        TargetDiscoverySchedule schedule = scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).get();
        assertEquals(INITIAL_BROADCAST_INTERVAL_MINUTES, schedule.getScheduleInterval(TimeUnit.MINUTES));
        assertTrue(schedule.isSynchedToBroadcast());
    }

    @Test
    public void testTargetDiscoverySchedulesAreLoaded() throws Exception {
        Target target = targetStore.getTarget(targetId).get();
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target));
        when(keyValueStore.getByPrefix(Scheduler.SCHEDULE_KEY_OFFSET)).thenReturn(
                Collections.singletonMap(Scheduler.SCHEDULE_KEY_OFFSET + targetId,
                        new Gson().toJson(new TargetDiscoveryScheduleData(TEST_SCHEDULE_MILLIS, false))));

        Scheduler scheduler = new Scheduler(operationManager, targetStore, probeStore,
            topologyHandler, keyValueStore, journalFactory, (name) -> fullDiscoveryExecutorSpy1,
            (name) -> incrementalDiscoveryExecutorSpy1, broadcastExecutorSpy, expirationExecutorSpy,
            INITIAL_BROADCAST_INTERVAL_MINUTES, NUM_DISCOVERIES_SKIPPED_BEFORE_LOGGING);
        scheduler.initialize();
        TargetDiscoverySchedule schedule = scheduler.getDiscoverySchedule(targetId, DiscoveryType.FULL).get();
        scheduler.initialize();
        assertEquals(TEST_SCHEDULE_MILLIS, schedule.getScheduleInterval(TimeUnit.MILLISECONDS));
        assertFalse(schedule.isSynchedToBroadcast());
    }

    @Test
    public void testScheduleToCheckForTimeouts() throws Exception {
        when(operationManager.getActionTimeoutMs()).thenReturn(10L);
        when(operationManager.getDiscoveryTimeoutMs()).thenReturn(20L);
        when(operationManager.getValidationTimeoutMs()).thenReturn(30L);

        scheduler = new Scheduler(operationManager, targetStore, probeStore, topologyHandler,
            keyValueStore, journalFactory, (name) -> fullDiscoveryExecutorSpy1,
            (name) -> incrementalDiscoveryExecutorSpy1, broadcastExecutorSpy, expirationExecutorSpy,
            INITIAL_BROADCAST_INTERVAL_MINUTES, NUM_DISCOVERIES_SKIPPED_BEFORE_LOGGING);
        scheduler.initialize();
        // A schedule should be added that checks for timeouts based on the shortest timeout among
        // action, discovery, and validation operations.
        Mockito.verify(expirationExecutorSpy).scheduleAtFixedRate(any(), eq(10L), eq(10L), eq(TimeUnit.MILLISECONDS));
    }

    @Test
    public void testProbeDiscoveryInterval() {
        ProbeInfo standardProbeInfo = ProbeInfo.newBuilder()
                .setProbeType("TestProbe")
                .setProbeCategory("Test")
                .setUiProbeCategory("Test")
                .build();
        Assert.assertEquals(60000, scheduler.getFullDiscoveryInterval(standardProbeInfo, true));

        // test with longer discovery interval
        ProbeInfo slowDiscoveryProbeInfo = ProbeInfo.newBuilder(standardProbeInfo)
                .setFullRediscoveryIntervalSeconds(999999).build();
        Assert.assertEquals(999999000, scheduler.getFullDiscoveryInterval(slowDiscoveryProbeInfo, true));

        /*
         * Test with discovery interval and performance discovery interval
         * and check that performance discovery interval is ignored.
         */
        ProbeInfo slowFullFastPerformanceProbeInfo = ProbeInfo.newBuilder(standardProbeInfo)
                .setFullRediscoveryIntervalSeconds(999999)
                .setPerformanceRediscoveryIntervalSeconds(99).build();
        Assert.assertEquals(999999000, scheduler.getFullDiscoveryInterval(slowFullFastPerformanceProbeInfo, true));

        /*
         * Test with both full/performance discovery interval are lower than broadcast interval
         * and sync with broadcast schedule.
         */
        ProbeInfo lowerThanBroadcastIntervalProbeInfo = ProbeInfo.newBuilder(standardProbeInfo)
            .setFullRediscoveryIntervalSeconds(50)
            .setPerformanceRediscoveryIntervalSeconds(30).build();
        Assert.assertEquals(60000, scheduler.getFullDiscoveryInterval(lowerThanBroadcastIntervalProbeInfo, true));

        /*
         * Test with both full/performance discovery interval are lower than broadcast interval
         * and do not sync with broadcast schedule
         */
        Assert.assertEquals(50000, scheduler.getFullDiscoveryInterval(lowerThanBroadcastIntervalProbeInfo, false));
    }

    @Test
    public void testPendingDiscoveryShouldntBlockBroadcast() throws Exception {
        // verify that pending discoveries won't block the realtime broadcast.
        long testTargetId = targetId;
        CountDownLatch discoveryStartedLatch = new CountDownLatch(1);
        CountDownLatch discoveryCompleteLatch = new CountDownLatch(1);
        CountDownLatch broadcastLatch = new CountDownLatch(1);
        Mockito.doAnswer(unused -> {
            discoveryStartedLatch.countDown();
            // discovery will block until the broadcast completes, or we timeout while waiting.
            broadcastLatch.await();
            discoveryCompleteLatch.countDown();
            return null;
        }).when(operationManager).addPendingDiscovery(testTargetId, DiscoveryType.FULL);

        Mockito.doAnswer(unused -> {
            // broadcast will release the broadcast latch, unblocking discovery
            broadcastLatch.countDown();
            return null;
        }).when(topologyHandler).broadcastLatestTopology(any());

        // set the discovery schedule -- this will trigger an immediate discovery.
        scheduler.setDiscoverySchedule(testTargetId, DiscoveryType.FULL, 10, TimeUnit.MINUTES,
            false);
        // ... but we'll wait for it to start anyways.
        discoveryStartedLatch.await();
        // now set a fast broadcast schedule. With the old threading model, this would lead to a
        // deadlock since a pending discovery would block the broadcast schedule.
        scheduler.setBroadcastSchedule(5, TimeUnit.MILLISECONDS);
        broadcastLatch.await(5, TimeUnit.SECONDS);
        discoveryCompleteLatch.await(5, TimeUnit.SECONDS);
        // verify that both discovery and broadcast have completed.
        assertEquals(0, broadcastLatch.getCount());
        assertEquals(0, discoveryCompleteLatch.getCount());
    }

    /**
     * Test that two targets with different probe types have their discoveries executed on different
     * threads.
     *
     * @throws Exception when Scheduler throws one.
     */
    @Test
    public void testDifferentProbeTypesGetDifferentExecutors() throws Exception {
        long target2Id = 9999;
        Target target2 = mock(Target.class);
        when(target2.getProbeInfo()).thenReturn(PROBE_TYPE_2_INFO);
        when(targetStore.getTarget(target2Id)).thenReturn(Optional.of(target2));
        when(target2.getProbeId()).thenReturn(PROBE_ID_2);
        CountDownLatch discovery1StartedLatch = new CountDownLatch(1);
        CountDownLatch discovery2StartedLatch = new CountDownLatch(1);
        CountDownLatch discoveryCompleteLatch = new CountDownLatch(1);
        CountDownLatch releaseDiscoveryLatch = new CountDownLatch(1);

        Mockito.doAnswer(unused -> {
            discovery1StartedLatch.countDown();
            // discovery will block until we release releaseDiscoveryLatch.
            releaseDiscoveryLatch.await();
            discoveryCompleteLatch.countDown();
            return null;
        }).when(operationManager).addPendingDiscovery(targetId, DiscoveryType.FULL);

        Mockito.doAnswer(unused -> {
            discovery2StartedLatch.countDown();
            return null;
        }).when(operationManager).addPendingDiscovery(target2Id, DiscoveryType.FULL);

        // set the discovery schedule -- this will trigger an immediate discovery.
        scheduler.setDiscoverySchedule(targetId, DiscoveryType.FULL, 10,
                TimeUnit.MINUTES, false);
        // ... but we'll wait for it to start anyways.
        discovery1StartedLatch.await(SCHEDULED_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        scheduler.setDiscoverySchedule(target2Id, DiscoveryType.FULL, 10, TimeUnit.MINUTES,
                false);
        // wait until discovery of target 2 starts
        discovery2StartedLatch.await(SCHEDULED_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        verify(operationManager).addPendingDiscovery(target2Id, DiscoveryType.FULL);
        // confirm other thread is still waiting
        assertEquals(1, discoveryCompleteLatch.getCount());
        // release other thread and wait for it to finish.
        releaseDiscoveryLatch.countDown();
        discoveryCompleteLatch.await();
        assertEquals(0, discoveryCompleteLatch.getCount());
        assertEquals(0, discovery2StartedLatch.getCount());
    }

    /**
     * Test that logging code does not throw an exception when a discovery has a null completion
     * time.
     */
    @Test
    public void testLoggingWithNullCompletionTime() {
        when(probeStore.getProbes()).thenReturn(ImmutableMap.of(PROBE_ID_1, PROBE_TYPE_1_INFO,
                PROBE_ID_2, PROBE_TYPE_2_INFO));
        final Target target = targetStore.getTarget(targetId).get();
        when(targetStore.getProbeTargets(PROBE_ID_1)).thenReturn(Collections.singletonList(target));
        when(targetStore.getProbeTargets(PROBE_ID_2)).thenReturn(Collections.emptyList());
        final Discovery discovery = mock(Discovery.class);
        when(operationManager.getLastDiscoveryForTarget(targetId, DiscoveryType.FULL))
                .thenReturn(Optional.of(discovery));
        when(discovery.getCompletionTime()).thenReturn(null);
        scheduler.logLaggingDiscoveries();
        // no assertion needed, we just want to make sure no exception was thrown
    }

    /**
     * Test that when a probe registers, if we don't already have a schedule for a target of that
     * probe type, we create both full and incremental discovery intervals,
     * if the ProbeInfo has them.
     *
     * @throws Exception if something goes wrong
     */
    @Test
    public void testProbeRegistrationCreatesFullAndIncrementalDiscovery() throws Exception {
        final long fullMillis = TimeUnit.MILLISECONDS
                .convert(PROBE_4_REDISCOVERY_INTERVAL, TimeUnit.SECONDS);
        final ScheduledFuture<?> mockFuture = mock(ScheduledFuture.class);
        Mockito.doReturn(mockFuture).when(fullDiscoveryExecutorSpy4).scheduleAtFixedRate(
                any(), Mockito.anyLong(), eq(fullMillis), any());

        final long incrementalMillis = TimeUnit.MILLISECONDS
                .convert(PROBE_4_INCREMENTAL_REDISCOVERY_INTERVAL, TimeUnit.SECONDS);
        final ScheduledFuture<?> mockFuture2 = mock(ScheduledFuture.class);
        Mockito.doReturn(mockFuture2).when(incrementalDiscoveryExecutorSpy4).scheduleAtFixedRate(
                any(), Mockito.anyLong(), eq(incrementalMillis), any());

        scheduler.onProbeRegistered(PROBE_ID_4, PROBE_TYPE_4_INFO);
        verify(keyValueStore).put(
                scheduleKey(Long.toString(targetId4)),
                new Gson().toJson(new TargetDiscoveryScheduleData(fullMillis,
                        true))
        );
        verify(keyValueStore).put(
                scheduleKey(Long.toString(targetId4)),
                new Gson().toJson(new TargetDiscoveryScheduleData(fullMillis,
                        incrementalMillis, true))
        );
    }

    /**
     * Test that if a full discovery schedule already exists when a probe registers and adds
     * an incremental discovery interval, the full discovery interval is preserved and not
     * overwritten by the new full discovery interval.
     *
     * @throws Exception if something goes wrong.
     */
    @Test
    public void testProbeRegistersFullScheduleExists() throws Exception {
        Target target = targetStore.getTarget(targetId4).get();
        when(targetStore.getAll()).thenReturn(ImmutableList.of(target));
        when(keyValueStore.getByPrefix(Scheduler.SCHEDULE_KEY_OFFSET)).thenReturn(
                Collections.singletonMap(Scheduler.SCHEDULE_KEY_OFFSET + targetId4,
                        new Gson().toJson(new TargetDiscoveryScheduleData(TEST_SCHEDULE_MILLIS,
                                false))));
        final long incrementalMillis = TimeUnit.MILLISECONDS
                .convert(PROBE_4_INCREMENTAL_REDISCOVERY_INTERVAL, TimeUnit.SECONDS);
        final ScheduledFuture<?> mockFuture2 = mock(ScheduledFuture.class);
        Mockito.doReturn(mockFuture2).when(incrementalDiscoveryExecutorSpy4).scheduleAtFixedRate(
                any(), Mockito.anyLong(), eq(incrementalMillis), any());

        Scheduler scheduler = new Scheduler(operationManager, targetStore, probeStore,
                topologyHandler, keyValueStore, journalFactory, (name) -> fullDiscoveryExecutorSpy1,
                (name) -> incrementalDiscoveryExecutorSpy1, broadcastExecutorSpy, expirationExecutorSpy,
                INITIAL_BROADCAST_INTERVAL_MINUTES, NUM_DISCOVERIES_SKIPPED_BEFORE_LOGGING);
        scheduler.initialize();

        scheduler.onProbeRegistered(PROBE_ID_4, PROBE_TYPE_4_INFO);
        verify(keyValueStore).put(
                scheduleKey(Long.toString(targetId4)),
                new Gson().toJson(new TargetDiscoveryScheduleData(TEST_SCHEDULE_MILLIS,
                        incrementalMillis, false))
        );

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
