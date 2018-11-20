package com.vmturbo.api.component.communication;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.topology.TopologyDTOMoles.TopologyServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.history.component.api.HistoryComponentNotifications.StatsAvailable;
import com.vmturbo.topology.processor.api.DiscoveryStatus;

public class ApiComponentTargetListenerTest {

    private final UINotificationChannel uiNotificationChannel = mock(UINotificationChannel.class);
    private TopologyServiceBlockingStub topologyServiceBlockingStub;
    private ApiComponentTargetListener targetListener;
    private TopologyServiceMole topologyServiceSpy = spy(new TopologyServiceMole());

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(topologyServiceSpy);

    @Before
    public void setUp() throws IOException {
        topologyServiceBlockingStub =
                TopologyServiceGrpc.newBlockingStub(testServer.getChannel());
        targetListener = new ApiComponentTargetListener(topologyServiceBlockingStub, uiNotificationChannel);
    }

    @Before
    public final void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testOnTargetDiscovered() throws Exception {
        final DiscoveryStatus result = mock(DiscoveryStatus.class);
        when(result.isSuccessful()).thenReturn(true);
        targetListener.triggerBroadcastAfterNextDiscovery();
        targetListener.onTargetDiscovered(result);
        verify(topologyServiceSpy).requestTopologyBroadcast(any());
    }

    @Test
    public void testOnTargetDiscoverFailed() throws Exception {
        final DiscoveryStatus result = mock(DiscoveryStatus.class);
        when(result.isSuccessful()).thenReturn(false);
        targetListener.triggerBroadcastAfterNextDiscovery();
        targetListener.onTargetDiscovered(result);
        verifyZeroInteractions(topologyServiceSpy);
    }

    @Test
    public void testOnTargetDiscoverOnNotFirstTarget() throws Exception {
        final DiscoveryStatus result = mock(DiscoveryStatus.class);
        when(result.isSuccessful()).thenReturn(true);
        targetListener.onTargetDiscovered(result);
        verifyZeroInteractions(topologyServiceSpy);
    }

    @Test
    public void testOnHistoryStatsAvailableButNotSourceTopology() throws Exception {
        final DiscoveryStatus result = mock(DiscoveryStatus.class);
        when(result.isSuccessful()).thenReturn(true);
        targetListener.triggerBroadcastAfterNextDiscovery();
        targetListener.onTargetDiscovered(result);
        targetListener.onStatsAvailable(StatsAvailable.getDefaultInstance());
        verifyZeroInteractions(uiNotificationChannel);
    }


    @Test
    public void testSendNotification() throws Exception {
        final DiscoveryStatus result = mock(DiscoveryStatus.class);
        when(result.isSuccessful()).thenReturn(true);
        targetListener.triggerBroadcastAfterNextDiscovery();
        targetListener.onTargetDiscovered(result);
        targetListener.onSourceTopologyAvailable(1l, 1l);
        targetListener.onStatsAvailable(StatsAvailable.getDefaultInstance());
        verify(uiNotificationChannel).broadcastTargetsNotification(any());
    }


    @Test
    public void testOnSourceTopologyAvailableButNotHistoryStat() throws Exception {
        final DiscoveryStatus result = mock(DiscoveryStatus.class);
        when(result.isSuccessful()).thenReturn(true);
        targetListener.triggerBroadcastAfterNextDiscovery();
        targetListener.onTargetDiscovered(result);
        targetListener.onSourceTopologyAvailable(1l, 1l);
        verifyZeroInteractions(uiNotificationChannel);
    }

    @Test
    public void testOnStatsAvailableOnDiscoverFailed() throws Exception {
        final DiscoveryStatus result = mock(DiscoveryStatus.class);
        when(result.isSuccessful()).thenReturn(false);
        targetListener.triggerBroadcastAfterNextDiscovery();
        targetListener.onTargetDiscovered(result);
        targetListener.onStatsAvailable(StatsAvailable.getDefaultInstance());
        verifyZeroInteractions(uiNotificationChannel);
    }

    @Test
    public void testOnStatsAvailableOnNotFirstTarget() throws Exception {
        final DiscoveryStatus result = mock(DiscoveryStatus.class);
        when(result.isSuccessful()).thenReturn(true);
        targetListener.triggerBroadcastAfterNextDiscovery();
        targetListener.onTargetDiscovered(result);
        targetListener.onStatsAvailable(StatsAvailable.getDefaultInstance());
        verifyZeroInteractions(uiNotificationChannel);
    }
}