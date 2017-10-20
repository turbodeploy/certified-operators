package com.vmturbo.topology.processor.scheduling;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import com.vmturbo.common.protobuf.topology.ScheduleServiceGrpc;
import com.vmturbo.common.protobuf.topology.Scheduler;
import com.vmturbo.components.api.test.GrpcTestServer;

/**
 * Tests for the {@link ScheduleRpcService} class.
 */
public class ScheduleRpcServiceTest {

    private com.vmturbo.topology.processor.scheduling.Scheduler scheduler =
            Mockito.mock(com.vmturbo.topology.processor.scheduling.Scheduler.class);

    private ScheduleRpcService scheduleRpcServiceBackend = new ScheduleRpcService(scheduler);

    private ScheduleServiceGrpc.ScheduleServiceBlockingStub scheduleRpcServiceClient;

    private static final long INTERVAL_MINUTES = 1;
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

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS, INTERVAL_MINUTES,
                synchedToBroadcast);
        Mockito.when(scheduler.setDiscoverySchedule(Mockito.eq(TARGET_ID), Mockito.eq(INTERVAL_MINUTES),
                Mockito.eq(TimeUnit.MINUTES)))
                .thenReturn(targetDiscoverySchedule);

        Scheduler.SetDiscoveryScheduleRequest request = Scheduler.SetDiscoveryScheduleRequest
                .newBuilder()
                .setTargetId(TARGET_ID)
                .setIntervalMinutes(INTERVAL_MINUTES)
                .build();

        final Scheduler.DiscoveryScheduleResponse response = scheduleRpcServiceClient
                .setDiscoverySchedule(request);

        Assert.assertEquals(INTERVAL_MINUTES, response.getInfo().getIntervalMinutes());
        Assert.assertEquals(DELAY_MS, response.getInfo().getTimeToNextMillis());
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testSetDiscoveryScheduleSyncedToBroadcast() throws Exception {
        final long initialBroadcastIntervalMinutes = INITIAL_BROADCAST_INTERVAL_MINUTES;
        final boolean synchedToBroadcast = false;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS,
                initialBroadcastIntervalMinutes, synchedToBroadcast);
        Mockito.when(scheduler.setBroadcastSynchedDiscoverySchedule(Mockito.eq(TARGET_ID)))
                .thenReturn(targetDiscoverySchedule);

        Scheduler.SetDiscoveryScheduleRequest request = Scheduler.SetDiscoveryScheduleRequest
                .newBuilder()
                .setTargetId(TARGET_ID)
                .build();

        final Scheduler.DiscoveryScheduleResponse response = scheduleRpcServiceClient
                .setDiscoverySchedule(request);

        Assert.assertEquals(initialBroadcastIntervalMinutes,
                response.getInfo().getIntervalMinutes());
        Assert.assertEquals(DELAY_MS, response.getInfo().getTimeToNextMillis());
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testGetDiscoverySchedule() throws Exception {
        final boolean synchedToBroadcast = false;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS,
                INTERVAL_MINUTES, synchedToBroadcast);

        Mockito.when(scheduler.getDiscoverySchedule(Mockito.eq(TARGET_ID)))
                .thenReturn(Optional.of(targetDiscoverySchedule));
        final Scheduler.DiscoveryScheduleResponse response =
                scheduleRpcServiceClient.getDiscoverySchedule(
                Scheduler.GetDiscoveryScheduleRequest.newBuilder()
                        .setTargetId(TARGET_ID)
                        .build());

        Assert.assertEquals(INTERVAL_MINUTES, (long)response.getInfo().getIntervalMinutes());
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testSetBroadcastSchedule() throws Exception {
        final long intervalMin = INTERVAL_MINUTES;
        TopologyBroadcastSchedule topologyBroadcastSchedule = mockBroadcastSchedule(intervalMin);
        Mockito.when(scheduler.setBroadcastSchedule(Mockito.eq(intervalMin), Mockito.eq(TimeUnit.MINUTES)))
                .thenReturn(topologyBroadcastSchedule);

        Scheduler.BroadcastScheduleResponse response = scheduleRpcServiceClient.setBroadcastSchedule(
                Scheduler.SetBroadcastScheduleRequest.newBuilder()
                .setIntervalMinutes(INTERVAL_MINUTES)
                .build());

        Assert.assertEquals(INTERVAL_MINUTES, (long)response.getInfo().getIntervalMinutes());
    }

    @Test
    public void testSetBroadcastSynchedDiscoverySchedule() throws Exception {
        final long intervalMin = INTERVAL_MINUTES;
        final boolean synchedToBroadcast = false;

        TargetDiscoverySchedule targetDiscoverySchedule = mockSchedule(DELAY_MS, intervalMin,
                synchedToBroadcast);
        Mockito.when(scheduler.setDiscoverySchedule(Mockito.eq(TARGET_ID),
                Mockito.eq(intervalMin),
                Mockito.eq(TimeUnit.MINUTES)))
                .thenReturn(targetDiscoverySchedule);

        Scheduler.SetDiscoveryScheduleRequest request = Scheduler.SetDiscoveryScheduleRequest
                .newBuilder()
                .setTargetId(TARGET_ID)
                .setIntervalMinutes(INTERVAL_MINUTES)
                .build();

        final Scheduler.DiscoveryScheduleResponse response = scheduleRpcServiceClient
                .setDiscoverySchedule(request);
        Assert.assertEquals(synchedToBroadcast, response.getSynchedToBroadcastSchedule());
    }

    @Test
    public void testGetBroadcastSchedule() throws Exception {
        TopologyBroadcastSchedule topologyBroadcastSchedule = mockBroadcastSchedule(INTERVAL_MINUTES);

        Mockito.when(scheduler.getBroadcastSchedule()).thenReturn(Optional.of(topologyBroadcastSchedule));
        Scheduler.BroadcastScheduleResponse response = scheduleRpcServiceClient.getBroadcastSchedule(
                Scheduler.GetBroadcastScheduleRequest.newBuilder()
                        .build());

        Assert.assertEquals(INTERVAL_MINUTES, response.getInfo().getIntervalMinutes());
    }

    @Test
    public void testGetEmptyBroadcastSchedule() throws Exception {
        Mockito.when(scheduler.getBroadcastSchedule()).thenReturn(Optional.empty());
        Scheduler.BroadcastScheduleResponse response = scheduleRpcServiceClient.getBroadcastSchedule(
                Scheduler.GetBroadcastScheduleRequest.newBuilder()
                        .build());

        Assert.assertFalse(response.hasInfo());
    }

    private TargetDiscoverySchedule mockSchedule(long delayMs, long intervalMin, boolean synchedToBroadcast) {
        TargetDiscoverySchedule ret = Mockito.mock(TargetDiscoverySchedule.class);
        Mockito.when(ret.getDelay(Mockito.eq(TimeUnit.MILLISECONDS))).thenReturn(delayMs);
        Mockito.when(ret.getScheduleInterval(Mockito.eq(TimeUnit.MINUTES))).thenReturn(intervalMin);
        Mockito.when(ret.isSynchedToBroadcast()).thenReturn(synchedToBroadcast);
        return ret;
    }

    private TopologyBroadcastSchedule mockBroadcastSchedule(long intervalMin) {
        TopologyBroadcastSchedule ret = Mockito.mock(TopologyBroadcastSchedule.class);
        Mockito.when(ret.getScheduleInterval(Mockito.eq(TimeUnit.MINUTES))).thenReturn(intervalMin);
        return ret;
    }
}
