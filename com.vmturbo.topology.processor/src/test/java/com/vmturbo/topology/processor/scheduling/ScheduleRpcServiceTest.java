package com.vmturbo.topology.processor.scheduling;

import static org.mockito.Matchers.eq;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.topology.Scheduler;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;

/**
 * Tests for the {@link ScheduleRpcService} class.
 */
public class ScheduleRpcServiceTest {

    private com.vmturbo.topology.processor.scheduling.Scheduler scheduler =
            Mockito.mock(com.vmturbo.topology.processor.scheduling.Scheduler.class);

    private ScheduleRpcService scheduleRpcServiceBackend = new ScheduleRpcService(scheduler);

    private ScheduleServiceGrpc.ScheduleServiceBlockingStub scheduleRpcServiceClient;

    private static final long FULL_INTERVAL_MINUTES = 1;
    private static final long INCREMENTAL_INTERVAL_SECONDS = 5;
    private static final long INITIAL_BROADCAST_INTERVAL_MINUTES = 10;
    private static final long DELAY_MS = 100;

    private static final long TARGET_ID = 7;

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(scheduleRpcServiceBackend);

    @Before
    public void startup() throws Exception {
        scheduleRpcServiceClient = ScheduleServiceGrpc.newBlockingStub(server.getChannel());
    }

    @Test
    public void testSetDiscoverySchedule() throws Exception {
        final boolean synchedToBroadcast = false;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS,
            FULL_INTERVAL_MINUTES, TimeUnit.MINUTES, synchedToBroadcast);
        Mockito.when(scheduler.setDiscoverySchedule(eq(TARGET_ID), eq(DiscoveryType.FULL),
            eq(FULL_INTERVAL_MINUTES), eq(TimeUnit.MINUTES), eq(synchedToBroadcast)))
            .thenReturn(targetDiscoverySchedule);

        Scheduler.SetDiscoveryScheduleRequest request = Scheduler.SetDiscoveryScheduleRequest
                .newBuilder()
                .setTargetId(TARGET_ID)
                .setFullIntervalMinutes(FULL_INTERVAL_MINUTES)
                .build();

        final Scheduler.DiscoveryScheduleResponse response = scheduleRpcServiceClient
                .setDiscoverySchedule(request);

        Assert.assertEquals(FULL_INTERVAL_MINUTES, response.getFullSchedule().getIntervalMinutes());
        Assert.assertEquals(DELAY_MS, response.getFullSchedule().getTimeToNextMillis());
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testSetIncrementalDiscoverySchedule() throws Exception {
        final boolean synchedToBroadcast = false;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS,
            INCREMENTAL_INTERVAL_SECONDS, TimeUnit.SECONDS, synchedToBroadcast);
        Mockito.when(scheduler.setDiscoverySchedule(eq(TARGET_ID), eq(DiscoveryType.INCREMENTAL),
            eq(INCREMENTAL_INTERVAL_SECONDS), eq(TimeUnit.SECONDS), eq(synchedToBroadcast)))
            .thenReturn(targetDiscoverySchedule);

        Scheduler.SetDiscoveryScheduleRequest request = Scheduler.SetDiscoveryScheduleRequest
            .newBuilder()
            .setTargetId(TARGET_ID)
            .setIncrementalIntervalSeconds(INCREMENTAL_INTERVAL_SECONDS)
            .build();

        final Scheduler.DiscoveryScheduleResponse response = scheduleRpcServiceClient
            .setDiscoverySchedule(request);

        Assert.assertEquals(INCREMENTAL_INTERVAL_SECONDS, response.getIncrementalSchedule().getIntervalSeconds());
        Assert.assertEquals(DELAY_MS, response.getIncrementalSchedule().getTimeToNextMillis());
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testDisableIncrementalDiscoverySchedule() throws Exception {
        final boolean synchedToBroadcast = false;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS,
            INCREMENTAL_INTERVAL_SECONDS, TimeUnit.SECONDS, synchedToBroadcast);
        Mockito.when(scheduler.disableDiscoverySchedule(eq(TARGET_ID), eq(DiscoveryType.INCREMENTAL)))
            .thenReturn(Optional.of(targetDiscoverySchedule));

        Scheduler.SetDiscoveryScheduleRequest request = Scheduler.SetDiscoveryScheduleRequest
            .newBuilder()
            .setTargetId(TARGET_ID)
            .setIncrementalIntervalSeconds(-1)
            .build();

        final Scheduler.DiscoveryScheduleResponse response = scheduleRpcServiceClient
            .setDiscoverySchedule(request);

        Assert.assertEquals(-1, response.getIncrementalSchedule().getIntervalSeconds());
        Assert.assertEquals(-1, response.getIncrementalSchedule().getTimeToNextMillis());
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testSetDiscoveryScheduleSyncedToBroadcast() throws Exception {
        final long initialBroadcastIntervalSeconds = INITIAL_BROADCAST_INTERVAL_MINUTES;
        final boolean synchedToBroadcast = true;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS,
                initialBroadcastIntervalSeconds, TimeUnit.MINUTES, synchedToBroadcast);
        Mockito.when(scheduler.setDiscoverySchedules(eq(TARGET_ID), eq(true)))
            .thenReturn(ImmutableMap.of(DiscoveryType.FULL, targetDiscoverySchedule));

        Scheduler.SetDiscoveryScheduleRequest request = Scheduler.SetDiscoveryScheduleRequest
                .newBuilder()
                .setTargetId(TARGET_ID)
                .build();

        final Scheduler.DiscoveryScheduleResponse response = scheduleRpcServiceClient
                .setDiscoverySchedule(request);

        Assert.assertEquals(initialBroadcastIntervalSeconds,
                response.getFullSchedule().getIntervalMinutes());
        Assert.assertEquals(DELAY_MS, response.getFullSchedule().getTimeToNextMillis());
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testGetDiscoverySchedule() throws Exception {
        final boolean synchedToBroadcast = false;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS,
            FULL_INTERVAL_MINUTES, TimeUnit.MINUTES, synchedToBroadcast);

        Mockito.when(scheduler.getDiscoverySchedule(eq(TARGET_ID)))
                .thenReturn(ImmutableMap.of(DiscoveryType.FULL, targetDiscoverySchedule));
        final Scheduler.DiscoveryScheduleResponse response =
                scheduleRpcServiceClient.getDiscoverySchedule(
                Scheduler.GetDiscoveryScheduleRequest.newBuilder()
                        .setTargetId(TARGET_ID)
                        .build());

        Assert.assertEquals(FULL_INTERVAL_MINUTES, response.getFullSchedule().getIntervalMinutes());
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testSetBroadcastSchedule() throws Exception {
        final long intervalMin = FULL_INTERVAL_MINUTES;
        TopologyBroadcastSchedule topologyBroadcastSchedule = mockBroadcastSchedule(intervalMin);
        Mockito.when(scheduler.setBroadcastSchedule(eq(intervalMin), eq(TimeUnit.MINUTES)))
                .thenReturn(topologyBroadcastSchedule);

        Scheduler.BroadcastScheduleResponse response = scheduleRpcServiceClient.setBroadcastSchedule(
            Scheduler.SetBroadcastScheduleRequest.newBuilder()
                .setIntervalMinutes(FULL_INTERVAL_MINUTES)
                .build());

        Assert.assertEquals(TimeUnit.MINUTES.convert(FULL_INTERVAL_MINUTES, TimeUnit.SECONDS),
            response.getInfo().getIntervalSeconds());
    }

    @Test
    public void testSetBroadcastSynchedDiscoverySchedule() throws Exception {
        final long intervalMin = FULL_INTERVAL_MINUTES;
        final boolean synchedToBroadcast = false;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS, intervalMin,
            TimeUnit.MINUTES, synchedToBroadcast);
        Mockito.when(scheduler.setDiscoverySchedule(eq(TARGET_ID),
                eq(DiscoveryType.FULL),
                eq(intervalMin),
                eq(TimeUnit.MINUTES),
                eq(synchedToBroadcast)))
                .thenReturn(targetDiscoverySchedule);

        Scheduler.SetDiscoveryScheduleRequest request = Scheduler.SetDiscoveryScheduleRequest
                .newBuilder()
                .setTargetId(TARGET_ID)
                .setFullIntervalMinutes(FULL_INTERVAL_MINUTES)
                .build();

        final Scheduler.DiscoveryScheduleResponse response = scheduleRpcServiceClient
                .setDiscoverySchedule(request);
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testGetBroadcastSchedule() throws Exception {
        TopologyBroadcastSchedule topologyBroadcastSchedule = mockBroadcastSchedule(
            FULL_INTERVAL_MINUTES);

        Mockito.when(scheduler.getBroadcastSchedule()).thenReturn(Optional.of(topologyBroadcastSchedule));
        Scheduler.BroadcastScheduleResponse response = scheduleRpcServiceClient.getBroadcastSchedule(
                Scheduler.GetBroadcastScheduleRequest.newBuilder()
                        .build());

        Assert.assertEquals(FULL_INTERVAL_MINUTES, response.getInfo().getIntervalMinutes());
    }

    @Test
    public void testGetEmptyBroadcastSchedule() throws Exception {
        Mockito.when(scheduler.getBroadcastSchedule()).thenReturn(Optional.empty());
        Scheduler.BroadcastScheduleResponse response = scheduleRpcServiceClient.getBroadcastSchedule(
                Scheduler.GetBroadcastScheduleRequest.newBuilder()
                        .build());

        Assert.assertFalse(response.hasInfo());
    }

    private TargetDiscoverySchedule mockSchedule(long delayMs, long interval, TimeUnit timeUnit,
            boolean synchedToBroadcast) {
        TargetDiscoverySchedule ret = Mockito.mock(TargetDiscoverySchedule.class);
        Mockito.when(ret.getDelay(eq(TimeUnit.MILLISECONDS))).thenReturn(delayMs);
        Mockito.when(ret.getScheduleInterval(eq(timeUnit))).thenReturn(interval);
        Mockito.when(ret.isSynchedToBroadcast()).thenReturn(synchedToBroadcast);
        return ret;
    }

    private TopologyBroadcastSchedule mockBroadcastSchedule(long intervalMin) {
        TopologyBroadcastSchedule ret = Mockito.mock(TopologyBroadcastSchedule.class);
        Mockito.when(ret.getScheduleInterval(eq(TimeUnit.MINUTES))).thenReturn(intervalMin);
        return ret;
    }
}
