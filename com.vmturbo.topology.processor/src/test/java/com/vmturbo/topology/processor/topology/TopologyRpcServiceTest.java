package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;

public class TopologyRpcServiceTest {

    private TopologyServiceGrpc.TopologyServiceBlockingStub topologyRpcClient;

    private GrpcTestServer server;
    private TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);

    @Before
    public void startup() throws Exception {
        TopologyRpcService topologyRpcServiceBackend = new TopologyRpcService(topologyHandler);
        server = GrpcTestServer.withServices(topologyRpcServiceBackend);
        topologyRpcClient = TopologyServiceGrpc.newBlockingStub(server.getChannel());
    }

    @After
    public void teardown() throws Exception {
        server.close();
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