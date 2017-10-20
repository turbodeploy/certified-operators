package com.vmturbo.repository.service;

import static org.mockito.Mockito.mock;

import java.util.NoSuchElementException;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import com.arangodb.ArangoDBException;
import io.grpc.Status.Code;

import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.topology.TopologyEventHandler;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyID;
import com.vmturbo.repository.topology.TopologyIDManager.TopologyType;
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

    private TopologyEventHandler topologyEventHandler = mock(TopologyEventHandler.class);

    private RepositoryRpcService repoRpcService = new RepositoryRpcService(
            topologyProtobufsManager,
            topologyEventHandler);

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
    public void testDeleteTopology() {

        Mockito.when(topologyProtobufsManager.createTopologyProtobufReader(
                    topologyId)).thenReturn(topologyProtobufReader);

        Mockito.when(topologyEventHandler.dropDatabase(
                    new TopologyID(topologyContextId,
                        topologyId,
                        TopologyType.PROJECTED)))
                .thenReturn(true);

        RepositoryOperationResponse repoResponse =
            repoClient.deleteTopology(topologyId,
                    topologyContextId);

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
    public void testDeleteTopologyProtoBufNotFoundException() throws Exception {

        Mockito.when(topologyProtobufsManager.createTopologyProtobufReader(
                    topologyId)).thenReturn(topologyProtobufReader);

        Mockito.doThrow(new NoSuchElementException("Error deleting topology"))
            .when(topologyProtobufReader).delete();

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains("Cannot find rawTopology with topologyId: " +
                topologyId + " and topologyContextId: " + topologyContextId));

        RepositoryOperationResponse response = repositoryService.deleteTopology(
                createDeleteTopologyRequest(topologyId, topologyContextId));
    }

    @Test
    public void testDeleteTopologyDBNotFoundException() throws Exception {

        Mockito.when(topologyProtobufsManager.createTopologyProtobufReader(
                    topologyId)).thenReturn(topologyProtobufReader);

        ArangoDBException arangoException = mock(ArangoDBException.class);
        Mockito.doThrow(arangoException)
                .when(topologyEventHandler).dropDatabase(
                    new TopologyID(topologyContextId,
                        topologyId,
                        TopologyType.PROJECTED));

        Mockito.when(arangoException.getErrorNum())
            .thenReturn(RepositoryRpcService.ERROR_ARANGO_DATABASE_NOT_FOUND);

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.NOT_FOUND)
            .descriptionContains("Cannot find topologyGraph with topologyId: " +
                topologyId + " and topologyContextId: " + topologyContextId));

        RepositoryOperationResponse response = repositoryService.deleteTopology(
                createDeleteTopologyRequest(topologyId, topologyContextId));
    }

    @Test
    public void testDeleteTopologyUnknownException() throws Exception {

        Mockito.when(topologyProtobufsManager.createTopologyProtobufReader(
                    topologyId)).thenReturn(topologyProtobufReader);

        Mockito.doThrow(new RuntimeException("Error deleting topology"))
            .when(topologyEventHandler).dropDatabase(
                    new TopologyID(topologyContextId,
                        topologyId,
                        TopologyType.PROJECTED));

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.UNKNOWN)
                .anyDescription());

        RepositoryOperationResponse response = repositoryService.deleteTopology(
                createDeleteTopologyRequest(topologyId, topologyContextId));
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
