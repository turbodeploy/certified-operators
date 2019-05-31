package com.vmturbo.repository.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityBatch;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.service.ArangoRepositoryRpcService.PlanEntityStatsExtractor;
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
public class ArangoRepositoryRpcServiceTest {

    private RepositoryClient repoClient;
    private final long topologyContextId = 1111;
    private final long topologyId = 2222;
    private RepositoryServiceBlockingStub repositoryService;

    private TopologyProtobufsManager topologyProtobufsManager = mock(TopologyProtobufsManager.class);

    private TopologyProtobufReader topologyProtobufReader = mock(TopologyProtobufReader.class);

    private TopologyProtobufHandler topologyProtobufHandler = mock(TopologyProtobufHandler.class);

    private TopologyLifecycleManager topologyLifecycleManager = mock(TopologyLifecycleManager.class);

    private GraphDBService graphDBService = mock(GraphDBService.class);

    private EntityStatsPaginationParamsFactory paginationParamsFactory =
            mock(EntityStatsPaginationParamsFactory.class);

    private EntityStatsPaginator entityStatsPaginator = mock(EntityStatsPaginator.class);

    private PlanEntityStatsExtractor planEntityStatsExtractor = mock(PlanEntityStatsExtractor.class);

    private LiveTopologyStore liveTopologyStore = mock(LiveTopologyStore.class);

    private ArangoRepositoryRpcService repoRpcService = new ArangoRepositoryRpcService(
        topologyLifecycleManager, topologyProtobufsManager, graphDBService,
        paginationParamsFactory, entityStatsPaginator, planEntityStatsExtractor, 10);

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
        assertEquals(repoResponse.getResponseCode(),
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

        expectedException.expect(GrpcRuntimeExceptionMatcher.hasCode(Code.INTERNAL).anyDescription());
        repositoryService.deleteTopology(createDeleteTopologyRequest(topologyId, topologyContextId));
    }


    @Test
    public void testRetrieveTopology() throws Exception {
        final ProjectedTopologyEntity entity = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                    .setEntityType(10)
                    .setOid(1L))
            .build();
        when(topologyProtobufsManager.createTopologyProtobufReader(topologyId, Optional.empty()))
                .thenReturn(topologyProtobufReader);
        when(topologyProtobufReader.hasNext()).thenReturn(true, false);
        when(topologyProtobufReader.nextChunk()).thenReturn(Collections.singletonList(entity));

        final List<RetrieveTopologyResponse> responseList = new ArrayList<>();
        repositoryService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
            .setTopologyId(topologyId)
            .build()).forEachRemaining(responseList::add);

        assertThat(responseList.size(), is(1));
        assertThat(responseList.get(0).getEntitiesList(), containsInAnyOrder(entity.getEntity()));

    }

    @Test
    public void testRetrieveTopologyWithFilter() throws Exception {
        final TopologyEntityFilter topologyEntityFilter = TopologyEntityFilter.newBuilder()
                .setUnplacedOnly(true)
                .build();
        when(topologyProtobufsManager.createTopologyProtobufReader(topologyId,
                Optional.of(topologyEntityFilter))).thenReturn(topologyProtobufReader);
        when(topologyProtobufReader.hasNext()).thenReturn(false);

        repositoryService.retrieveTopology(RetrieveTopologyRequest.newBuilder()
                .setTopologyId(topologyId)
                .setEntityFilter(topologyEntityFilter)
                .build());
    }

    @Test
    public void testRetrieveTopologyEntities() {
        final TopologyID topologyID = mock(TopologyID.class);
        when(topologyLifecycleManager.getTopologyId(topologyContextId, TopologyType.PROJECTED))
            .thenReturn(Optional.of(topologyID));
        when(graphDBService.retrieveTopologyEntities(eq(topologyID),
                Mockito.anySet()))
            .thenReturn(Either.right(Collections.emptyList()));
        repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setTopologyId(topologyId)
                .addAllEntityOids(Lists.newArrayList(1L))
                .setTopologyType(RetrieveTopologyEntitiesRequest.TopologyType.PROJECTED)
                .build());
    }

    @Test
    public void testRetrieveTopologyEntitiesByType() {
        final TopologyID topologyID = mock(TopologyID.class);
        when(topologyLifecycleManager.getTopologyId(topologyContextId, TopologyType.PROJECTED))
            .thenReturn(Optional.of(topologyID));
        when(graphDBService.retrieveTopologyEntities(eq(topologyID),
            Mockito.anySet())).thenReturn(Either.right(Collections.emptyList()));
        repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                                                   .setTopologyContextId(topologyContextId)
                                                   .setTopologyId(topologyId)
                                                   .addAllEntityType(Lists.newArrayList(EntityType.VIRTUAL_MACHINE_VALUE))
                                                   .setTopologyType(RetrieveTopologyEntitiesRequest.TopologyType.PROJECTED)
                                                   .build());
    }

    @Test
    public void testRetrieveRealTimeTopologyEntities() {
        when(graphDBService.retrieveRealTimeTopologyEntities(Mockito.anySet()))
                .thenReturn(Either.right(Collections.emptyList()));
        repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityOids(Lists.newArrayList(1L))
                .setTopologyType(RetrieveTopologyEntitiesRequest.TopologyType.SOURCE)
                .build());
    }

    @Test
    public void testRetrieveTopologyEntitiesStreaming() {
        // test that a response that should get chunked.
        Collection<TopologyEntityDTO> manyEntities = new ArrayList<>();
        // we configured the service for a batch size of 10, so let's send 11 entities.
        int numEntities = 11;
        for (int x = 0 ; x < numEntities ; x++) {
            TopologyEntityDTO newEntity = TopologyEntityDTO.newBuilder()
                    .setOid(x)
                    .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .setDisplayName(String.valueOf(x))
                    .build();
            manyEntities.add(newEntity);
        }
        final TopologyID topologyID = mock(TopologyID.class);
        when(topologyLifecycleManager.getTopologyId(topologyContextId, TopologyType.SOURCE))
            .thenReturn(Optional.of(topologyID));
        when(graphDBService.retrieveTopologyEntities(eq(topologyID),
            Mockito.anySet()))
                .thenReturn(Either.right(manyEntities));
        RetrieveTopologyEntitiesRequest request = RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .setTopologyId(topologyId)
                .setTopologyType(RetrieveTopologyEntitiesRequest.TopologyType.SOURCE)
                .build();
        // call the service directly so we can monitor the response
        StreamObserver<EntityBatch> responseStreamObserver = Mockito.spy(StreamObserver.class);
        repoRpcService.retrieveTopologyEntities(request, responseStreamObserver);
        verify(responseStreamObserver, times(2)).onNext(any());

        // verify we get all entities in the final response too
        Iterator<EntityBatch> response = repositoryService.retrieveTopologyEntities(request);
        int totalEntities = 0;
        while(response.hasNext()) {
            totalEntities += response.next().getEntitiesCount();
        }
        assertEquals(numEntities, totalEntities);

    }

    @Test
    public void testRetrievePlanProjectedStats() {
        // arrange
        final ProjectedTopologyEntity topologyEntityDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                    .setOid(1L)
                    .setEntityType(10)
                    .setDisplayName("x"))
            .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
                .setCursor("foo")
                .build();
        final PlanTopologyStatsRequest request = PlanTopologyStatsRequest.newBuilder()
                .setTopologyId(topologyId)
                .setPaginationParams(paginationParameters)
                .setFilter(StatsFilter.newBuilder()
                        .setStartDate(Instant.now().toEpochMilli() + 100000))
                .setEntityFilter(RepositoryDTO.EntityFilter.newBuilder()
                        .addEntityIds(topologyEntityDTO.getEntity().getOid()))
                .build();

        final TopologyProtobufReader protobufReader = mock(TopologyProtobufReader.class);
        when(protobufReader.hasNext()).thenReturn(true).thenReturn(false);
        when(protobufReader.nextChunk()).thenReturn(Collections.singletonList(topologyEntityDTO));

        when(topologyProtobufsManager.createTopologyProtobufReader(topologyId, Optional.empty()))
                .thenReturn(protobufReader);

        final EntityStats.Builder statsBuilder = EntityStats.newBuilder()
                .setOid(topologyEntityDTO.getEntity().getOid());
        when(planEntityStatsExtractor.extractStats(topologyEntityDTO, request)).thenReturn(statsBuilder);

        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn("foo");
        when(paginationParamsFactory.newPaginationParams(paginationParameters)).thenReturn(paginationParams);

        final PaginatedStats paginatedStats = mock(PaginatedStats.class);
        when(paginatedStats.getNextPageIds()).thenReturn(Collections.singletonList(topologyEntityDTO.getEntity().getOid()));

        final PaginationResponse paginationResponse = PaginationResponse.newBuilder()
                .setNextCursor("bar")
                .build();
        when(paginatedStats.getPaginationResponse()).thenReturn(paginationResponse);

        when(entityStatsPaginator.paginate(eq(Collections.singleton(topologyEntityDTO.getEntity().getOid())), any(), eq(paginationParams)))
                .thenReturn(paginatedStats);

        // act
        final PlanTopologyStatsResponse response = repositoryService.getPlanTopologyStats(request);

        // assert
        verify(topologyProtobufsManager).createTopologyProtobufReader(topologyId, Optional.empty());
        verify(planEntityStatsExtractor).extractStats(topologyEntityDTO, request);
        verify(paginationParamsFactory).newPaginationParams(paginationParameters);
        verify(entityStatsPaginator).paginate(eq(Collections.singleton(topologyEntityDTO.getEntity().getOid())), any(), eq(paginationParams));

        assertThat(response.getPaginationResponse(), is(paginationResponse));
        assertThat(response.getEntityStatsList(), is(Collections.singletonList(PlanEntityStats.newBuilder()
                .setPlanEntity(topologyEntityDTO.getEntity())
                .setPlanEntityStats(statsBuilder)
                .build())));
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
