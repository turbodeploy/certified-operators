package com.vmturbo.repository.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcExceptionMatcher;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufHandler;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/*
 *  Test Repository RPC functions
 */
public class RepositoryRpcServiceTest {

    private RepositoryClient repoClient;
    private final long topologyContextId = 1111;
    private final long topologyId = 2222;
    private RepositoryServiceBlockingStub repositoryService;

    private TopologyProtobufsManager topologyProtobufsManager = mock(TopologyProtobufsManager.class);

    private TopologyProtobufReader topologyProtobufReader = mock(TopologyProtobufReader.class);

    private TopologyProtobufHandler topologyProtobufHandler = mock(TopologyProtobufHandler.class);

    private TopologyLifecycleManager topologyLifecycleManager = mock(TopologyLifecycleManager.class);

    private RepositoryRpcService repoRpcService = new RepositoryRpcService(
            topologyLifecycleManager, topologyProtobufsManager);

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(repoRpcService);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        repoClient = new RepositoryClient(grpcServer.getChannel());
        repositoryService = RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel());

    }

    @Test
    public void testDeleteTopology() throws TopologyDeletionException {

        when(topologyProtobufsManager.createTopologyProtobufReader(eq(topologyId), any()))
            .thenReturn(topologyProtobufReader);

        RepositoryOperationResponse repoResponse =
            repoClient.deleteTopology(topologyId,
                    topologyContextId);

        verify(topologyLifecycleManager).deleteTopology(eq(new TopologyID(topologyContextId,
                topologyId,
                TopologyType.PROJECTED)));
        Assert.assertEquals(repoResponse.getResponseCode(),
                RepositoryOperationResponseCode.OK);
    }

    @Test
    public void testDeleteTopologyMissingParameter() {

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INVALID_ARGUMENT)
            .descriptionContains("Topology Context ID missing"));

        RepositoryOperationResponse response = repositoryService.deleteTopology(
                createDeleteTopologyRequest(topologyId));

    }

    @Test
    public void testDeleteTopologyException() throws Exception {
        Mockito.doThrow(TopologyDeletionException.class)
            .when(topologyLifecycleManager).deleteTopology(new TopologyID(topologyContextId,
                        topologyId,
                        TopologyType.PROJECTED));

        final StreamObserver<RepositoryOperationResponse> responseObserver =
                (StreamObserver<RepositoryOperationResponse>)mock(StreamObserver.class);

        repoRpcService.deleteTopology(createDeleteTopologyRequest(topologyId, topologyContextId),
                responseObserver);

        final ArgumentCaptor<StatusException> errCaptor =
                ArgumentCaptor.forClass(StatusException.class);
        verify(responseObserver).onError(errCaptor.capture());
        assertThat(errCaptor.getValue(), GrpcExceptionMatcher.hasCode(Code.INTERNAL).anyDescription());
    }

    private static final TopologyEntityDTO ENTITY = TopologyEntityDTO.newBuilder()
            .setEntityType(10)
            .setOid(1L)
            .build();

    @Test
    public void testRetrieveTopology() throws Exception {
        final StreamObserver<RetrieveTopologyResponse> responseObserver =
                (StreamObserver<RetrieveTopologyResponse>)mock(StreamObserver.class);

        when(topologyProtobufsManager.createTopologyProtobufReader(topologyId, Optional.empty()))
                .thenReturn(topologyProtobufReader);
        when(topologyProtobufReader.hasNext()).thenReturn(true, false);
        when(topologyProtobufReader.nextChunk()).thenReturn(Collections.singletonList(ENTITY));

        repoRpcService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                .setTopologyId(topologyId)
                .build(), responseObserver);

        ArgumentCaptor<RetrieveTopologyResponse> respCaptor =
                ArgumentCaptor.forClass(RetrieveTopologyResponse.class);

        verify(responseObserver).onNext(respCaptor.capture());
        verify(responseObserver).onCompleted();

        assertThat(respCaptor.getValue().getEntitiesList(), containsInAnyOrder(ENTITY));

    }

    @Test
    public void testRetrieveTopologyWithFilter() throws Exception {
        final TopologyEntityFilter topologyEntityFilter = TopologyEntityFilter.newBuilder()
                .setUnplacedOnly(true)
                .build();
        final StreamObserver<RetrieveTopologyResponse> responseObserver =
                (StreamObserver<RetrieveTopologyResponse>)mock(StreamObserver.class);
        when(topologyProtobufsManager.createTopologyProtobufReader(topologyId,
                Optional.of(topologyEntityFilter))).thenReturn(topologyProtobufReader);
        when(topologyProtobufReader.hasNext()).thenReturn(false);

        repoRpcService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                .setTopologyId(topologyId)
                .setEntityFilter(topologyEntityFilter)
                .build(), responseObserver);

        verify(responseObserver).onCompleted();
    }

    private DeleteTopologyRequest createDeleteTopologyRequest(long topologyId) {
        return DeleteTopologyRequest.newBuilder()
            .setTopologyId(topologyId)
            .build();
    }

    private DeleteTopologyRequest createDeleteTopologyRequest(
            long topologyId,
            long topologyContextId) {

        return DeleteTopologyRequest.newBuilder()
            .setTopologyId(topologyId)
            .setTopologyContextId(topologyContextId)
            .build();
    }
}
