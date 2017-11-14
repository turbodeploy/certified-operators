package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.topology.processor.targets.TargetStore;

public class TopologyRpcServiceTest {

    private TopologyServiceGrpc.TopologyServiceBlockingStub topologyRpcClient;

    private TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);

    private TargetStore targetStore = Mockito.mock(TargetStore.class);

    private TopologyRpcService topologyRpcServiceBackend = new TopologyRpcService(topologyHandler);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(topologyRpcServiceBackend);

    @Before
    public void startup() throws Exception {
        topologyRpcClient = TopologyServiceGrpc.newBlockingStub(server.getChannel());
    }

    @Test
    public void testRequestTopologyBroadcast() throws Exception {
        topologyRpcClient.requestTopologyBroadcast(
            TopologyBroadcastRequest.newBuilder()
                .build());
        verify(topologyHandler).broadcastLatestTopology();
    }

    @Test
    public void testRequestTopologyError() throws Exception {
        when(topologyHandler.broadcastLatestTopology()).thenThrow(new RuntimeException("foo"));

        try {
            topologyRpcClient.requestTopologyBroadcast(
                TopologyBroadcastRequest.newBuilder()
                    .build());
            fail("Should have thrown exception in request.");
        } catch (StatusRuntimeException grpcException) {
            assertEquals(Status.INTERNAL.getClass(), grpcException.getStatus().getClass());
            assertEquals("foo", grpcException.getStatus().getDescription());
        }
    }
}