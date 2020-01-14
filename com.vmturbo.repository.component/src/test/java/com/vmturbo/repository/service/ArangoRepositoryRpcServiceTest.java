package com.vmturbo.repository.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nullable;

import org.assertj.core.util.Lists;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import io.grpc.Status.Code;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityAndCombinedStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityAndCombinedStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse.TypeCase;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RequestDetails;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyEntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.test.GrpcRuntimeExceptionMatcher;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyIDFactory;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 *  Test Repository RPC functions.
 */
@RunWith(MockitoJUnitRunner.class)
public class ArangoRepositoryRpcServiceTest {

    private RepositoryClient repoClient;
    private final long topologyContextId = 1111;
    private final long topologyId = 2222;
    private RepositoryServiceBlockingStub repositoryService;

    private TopologyProtobufsManager topologyProtobufsManager = mock(TopologyProtobufsManager.class);

    private TopologyProtobufReader topologyProtobufReader = mock(TopologyProtobufReader.class);

    private TopologyLifecycleManager topologyLifecycleManager = mock(TopologyLifecycleManager.class);

    private GraphDBService graphDBService = mock(GraphDBService.class);

    private PlanStatsService planStatsService = mock(PlanStatsService.class);

    private PartialEntityConverter partialEntityConverter = new PartialEntityConverter();

    private final TopologyIDFactory topologyIDFactory = new TopologyIDFactory("turbonomic-");

    private ArangoRepositoryRpcService repoRpcService = new ArangoRepositoryRpcService(
        topologyLifecycleManager, topologyProtobufsManager, graphDBService,
        planStatsService, partialEntityConverter, 10, topologyIDFactory);

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
        final RepositoryDTO.TopologyType topologyType = RepositoryDTO.TopologyType.PROJECTED;
        when(topologyProtobufsManager.createTopologyProtobufReader(eq(topologyId), any()))
            .thenReturn(topologyProtobufReader);

        RepositoryOperationResponse repoResponse =
            repoClient.deleteTopology(topologyId, topologyContextId, topologyType);

        final TopologyID expectedTopologyId = topologyIDFactory.createTopologyID(topologyContextId,
            topologyId,
            TopologyType.PROJECTED
        );
        verify(topologyLifecycleManager).deleteTopology(eq(expectedTopologyId));
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
            .when(topologyLifecycleManager).deleteTopology(topologyIDFactory.createTopologyID(topologyContextId,
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
        assertEquals(responseList.get(0).getEntitiesList().get(0).getFullEntity(), entity.getEntity());

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
                .setTopologyType(RepositoryDTO.TopologyType.PROJECTED)
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
                                                   .setTopologyType(RepositoryDTO.TopologyType.PROJECTED)
                                                   .build());
    }

    @Test
    public void testRetrieveRealTimeTopologyEntities() {
        when(graphDBService.retrieveRealTimeTopologyEntities(Mockito.anySet()))
            .thenReturn(Either.right(Collections.emptyList()));
        repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                .addAllEntityOids(Lists.newArrayList(1L))
                .setTopologyType(RepositoryDTO.TopologyType.SOURCE)
                .build());
    }

    @Test
    public void testRetrieveTopologyEntitiesStreaming() {
        // test that a response that should get chunked.
        Collection<TopologyEntityDTO> manyEntities = new ArrayList<>();
        // we configured the service for a batch size of 10, so let's send 11 entities.
        int numEntities = 11;
        for (int x = 0; x < numEntities; x++) {
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
                .setTopologyType(RepositoryDTO.TopologyType.SOURCE)
                .build();
        // call the service directly so we can monitor the response
        StreamObserver<PartialEntityBatch> responseStreamObserver = Mockito.spy(StreamObserver.class);
        repoRpcService.retrieveTopologyEntities(request, responseStreamObserver);
        verify(responseStreamObserver, times(2)).onNext(any());

        // verify we get all entities in the final response too
        Iterator<PartialEntityBatch> response = repositoryService.retrieveTopologyEntities(request);
        int totalEntities = 0;
        while (response.hasNext()) {
            totalEntities += response.next().getEntitiesCount();
        }
        assertEquals(numEntities, totalEntities);

    }

    /**
     * Test retrieving projected statistics.
     */
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
        final long startDate = Instant.now().toEpochMilli() + 100000;
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .setStartDate(startDate)
            .build();
        final Type returnType = Type.MINIMAL;
        final PlanTopologyStatsRequest request = PlanTopologyStatsRequest.newBuilder()
            .setTopologyId(topologyId)
            .setRequestDetails(RequestDetails.newBuilder()
                .setPaginationParams(paginationParameters)
                .setFilter(statsFilter)
                .setEntityFilter(EntityFilter.newBuilder()
                    .addEntityIds(topologyEntityDTO.getEntity().getOid()))
                .setReturnType(returnType))
            .build();

        final TopologyProtobufReader protobufReader = mock(TopologyProtobufReader.class);
        when(protobufReader.hasNext()).thenReturn(true).thenReturn(false);
        when(protobufReader.nextChunk()).thenReturn(Collections.singletonList(topologyEntityDTO));

        when(topologyProtobufsManager.createTopologyProtobufReader(topologyId, Optional.empty()))
                .thenReturn(protobufReader);

        TopologyID topologyID = topologyIDFactory.createTopologyID(0, topologyId, TopologyType.PROJECTED);
        when(topologyLifecycleManager.getTopologyId(topologyId)).thenReturn(Optional.of(topologyID));

        final EntityStats stats = EntityStats.newBuilder()
            .setOid(topologyEntityDTO.getEntity().getOid())
            .addStatSnapshots(StatSnapshot.newBuilder()
                .setStatEpoch(StatEpoch.PLAN_PROJECTED)
                .setSnapshotDate(startDate))
            .build();

        final PaginationResponse paginationResponse = PaginationResponse.newBuilder()
                .setNextCursor("bar")
                .build();

        // Create the stats response
        final PlanEntityStats planEntityStats = PlanEntityStats.newBuilder()
            .setPlanEntity(partialEntityConverter
                .createPartialEntity(topologyEntityDTO.getEntity(), returnType))
            .setPlanEntityStats(stats)
            .build();

        // Mock the response
        doAnswer((Answer<Void>)invocation -> {
            StreamObserver observer = invocation.getArgumentAt(6, StreamObserver.class);
            observer.onNext(PlanTopologyStatsResponse.newBuilder()
                .setEntityStatsWrapper(PlanEntityStatsChunk.newBuilder()
                .addEntityStats(planEntityStats))
                .build());
            observer.onNext(PlanTopologyStatsResponse.newBuilder()
                .setPaginationResponse(paginationResponse)
                .build());
            observer.onCompleted();
            return null;
        }).when(planStatsService)
            .getPlanTopologyStats(eq(protobufReader), any(), eq(statsFilter),
                any(), eq(paginationParameters), eq(returnType), any());

        // act
        final Iterator<PlanTopologyStatsResponse> response =
            repositoryService.getPlanTopologyStats(request);

        List<PlanEntityStats> returnedPlanEntityStats = new ArrayList<>();
        PaginationResponse returnedPaginationResponse = null;
        while(response.hasNext()){
            PlanTopologyStatsResponse chunk = response.next();
            if (chunk.getTypeCase() == TypeCase.PAGINATION_RESPONSE) {
                returnedPaginationResponse = chunk.getPaginationResponse();
            } else {
                returnedPlanEntityStats.addAll(chunk.getEntityStatsWrapper().getEntityStatsList());
            }
        }
        // assert
        verify(topologyProtobufsManager).createTopologyProtobufReader(topologyId, Optional.empty());
        verify(planStatsService)
            .getPlanTopologyStats(eq(protobufReader), any(), eq(statsFilter),
                any(), eq(paginationParameters), eq(returnType), any());

        assertThat(returnedPaginationResponse, is(paginationResponse));
        assertThat(returnedPlanEntityStats, is(Collections.singletonList(planEntityStats)));
    }

    /**
     * Test an invalid request for retrieving plan combined (source and projected) statistics.
     */
    @Test
    public void testInvalidRetrievePlanCombinedStats() {
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
        final PlanCombinedStatsRequest request = PlanCombinedStatsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setRequestDetails(RequestDetails.newBuilder()
                .setPaginationParams(paginationParameters)
                .setFilter(StatsFilter.newBuilder()
                    .setStartDate(Instant.now().toEpochMilli() + 100000))
                .setEntityFilter(RepositoryDTO.EntityFilter.newBuilder()
                    .addEntityIds(topologyEntityDTO.getEntity().getOid())))
            .build();

        // act
        final Iterator<PlanCombinedStatsResponse> response =
            repositoryService.getPlanCombinedStats(request);

        // verify
        expectedException.expect(GrpcRuntimeExceptionMatcher
            .hasCode(Code.INVALID_ARGUMENT)
            .anyDescription());
        // checking the response should cause a StatusRuntimeException to be thrown, because our
        // request did not include the required field 'topologyToSortOn'
        response.hasNext();
    }

    /**
     * Test retrieving plan combined (source and projected) statistics.
     */
    @Test
    public void testRetrievePlanCombinedStats() {
        // arrange
        final long sourceEntityId = 1L;
        final long projectedEntityId = 2L;
        final long commonEntityId = 3L;
        // Create an entity that exists only in the source topology
        final ProjectedTopologyEntity sourceTopologyEntityDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setOid(sourceEntityId)
                .setEntityType(10)
                .setDisplayName("x"))
            .build();
        // Create an entity that exists only in the projected topology
        final ProjectedTopologyEntity projectedTopologyEntityDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setOid(projectedEntityId)
                .setEntityType(10)
                .setDisplayName("y"))
            .build();
        // Create an entity that exists in both the source and projected topologies
        final ProjectedTopologyEntity commonTopologyEntityDTO = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setOid(commonEntityId)
                .setEntityType(10)
                .setDisplayName("z"))
            .build();
        final String sortCommodity = "Mem";
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setLimit(10)
            .setOrderBy(OrderBy.newBuilder()
                .setEntityStats(EntityStatsOrderBy.newBuilder()
                    .setStatName(sortCommodity)))
            .build();
        final Type returnType = Type.MINIMAL;
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .setStartDate(Instant.now().toEpochMilli() + 100000)
            .build();
        final PlanCombinedStatsRequest request = PlanCombinedStatsRequest.newBuilder()
            .setTopologyContextId(topologyContextId)
            .setTopologyToSortOn(RepositoryDTO.TopologyType.PROJECTED)
            .setRequestDetails(RequestDetails.newBuilder()
                .setPaginationParams(paginationParameters)
                .setFilter(statsFilter)
                .setReturnType(returnType))
            .build();

        // Create two mock protobuf readers, one for each source and projected topologies
        final TopologyProtobufReader sourceProtobufReader = mock(TopologyProtobufReader.class);
        when(sourceProtobufReader.hasNext()).thenReturn(true).thenReturn(false);
        when(sourceProtobufReader.nextChunk())
            .thenReturn(Lists.newArrayList(sourceTopologyEntityDTO, commonTopologyEntityDTO));

        final TopologyProtobufReader projectedProtobufReader = mock(TopologyProtobufReader.class);
        when(projectedProtobufReader.hasNext()).thenReturn(true).thenReturn(false);
        when(projectedProtobufReader.nextChunk())
            .thenReturn(Lists.newArrayList(commonTopologyEntityDTO, projectedTopologyEntityDTO));

        final long sourceTopologyId = 4567;
        final long projectedTopologyId = 6789;
        final TopologyID sourceTid =
            topologyIDFactory.createTopologyID(topologyContextId, sourceTopologyId, TopologyType.SOURCE);
        final TopologyID projectedTid =
            topologyIDFactory.createTopologyID(topologyContextId, projectedTopologyId, TopologyType.PROJECTED);

        when(topologyLifecycleManager.getTopologyId(topologyContextId, TopologyType.SOURCE))
            .thenReturn(Optional.of(sourceTid));
        when(topologyLifecycleManager.getTopologyId(topologyContextId, TopologyType.PROJECTED))
            .thenReturn(Optional.of(projectedTid));
        when(topologyProtobufsManager
            .createTopologyProtobufReader(sourceTopologyId, Optional.empty()))
            .thenReturn(sourceProtobufReader);
        when(topologyProtobufsManager
            .createTopologyProtobufReader(projectedTopologyId, Optional.empty()))
            .thenReturn(projectedProtobufReader);

        final PaginationResponse paginationResponse = PaginationResponse.newBuilder()
            .build();

        // Create the stats response
        final PlanEntityAndCombinedStats sourceEntityAndStats =
            buildPlanEntityAndCombinedStats(sourceTopologyEntityDTO, null);
        final PlanEntityAndCombinedStats projectedEntityAndStats =
            buildPlanEntityAndCombinedStats(null, projectedTopologyEntityDTO);
        final PlanEntityAndCombinedStats commonEntityAndStats =
            buildPlanEntityAndCombinedStats(commonTopologyEntityDTO, commonTopologyEntityDTO);

        // Mock the response
        doAnswer((Answer<Void>)invocation -> {
            StreamObserver observer = invocation.getArgumentAt(7, StreamObserver.class);
            observer.onNext(PlanCombinedStatsResponse.newBuilder()
                .setEntityCombinedStatsWrapper(PlanEntityAndCombinedStatsChunk.newBuilder()
                    .addEntityAndCombinedStats(sourceEntityAndStats)
                    .addEntityAndCombinedStats(projectedEntityAndStats)
                    .addEntityAndCombinedStats(commonEntityAndStats))
                .build());
            observer.onNext(PlanCombinedStatsResponse.newBuilder()
                .setPaginationResponse(paginationResponse)
                .build());
            observer.onCompleted();
            return null;
        }).when(planStatsService)
            .getPlanCombinedStats(eq(sourceProtobufReader), eq(projectedProtobufReader),
                eq(statsFilter), any(), any(), eq(paginationParameters), eq(returnType), any());

        // act
        final Iterator<PlanCombinedStatsResponse> response =
            repositoryService.getPlanCombinedStats(request);

        List<PlanEntityAndCombinedStats> returnedPlanEntityCombinedStats = new ArrayList<>();
        PaginationResponse returnedPaginationResponse = null;
        while (response.hasNext()) {
            PlanCombinedStatsResponse chunk = response.next();
            if (chunk.getTypeCase() == PlanCombinedStatsResponse.TypeCase.PAGINATION_RESPONSE) {
                returnedPaginationResponse = chunk.getPaginationResponse();
            } else {
                returnedPlanEntityCombinedStats.addAll(
                    chunk.getEntityCombinedStatsWrapper().getEntityAndCombinedStatsList());
            }
        }
        // assert
        // both source and projected topologies must be read
        verify(topologyProtobufsManager)
            .createTopologyProtobufReader(sourceTopologyId, Optional.empty());
        verify(topologyProtobufsManager)
            .createTopologyProtobufReader(projectedTopologyId, Optional.empty());
        // only one pagination should occur (on the combined entities/stats)
        verify(planStatsService)
            .getPlanCombinedStats(eq(sourceProtobufReader), eq(projectedProtobufReader),
                eq(statsFilter), any(), any(), eq(paginationParameters), eq(returnType), any());

        // verify the returned data
        assertThat(returnedPaginationResponse, is(paginationResponse));
        assertEquals(3, returnedPlanEntityCombinedStats.size());
        assertEquals(2, returnedPlanEntityCombinedStats.stream()
            .filter(PlanEntityAndCombinedStats::hasPlanSourceEntity)
            .filter(PlanEntityAndCombinedStats::hasPlanCombinedStats)
            .count());
        // Check that there are two results with source entity and source stats
        assertEquals(2, returnedPlanEntityCombinedStats.stream()
            .filter(PlanEntityAndCombinedStats::hasPlanSourceEntity)
            .filter(PlanEntityAndCombinedStats::hasPlanCombinedStats)
            .map(PlanEntityAndCombinedStats::getPlanCombinedStats)
            .filter(entityStats -> entityStats.getStatSnapshotsList().stream()
                .filter(StatSnapshot::hasStatEpoch)
                .map(StatSnapshot::getStatEpoch)
                .anyMatch(statEpoch -> StatEpoch.PLAN_SOURCE == statEpoch))
            .count());
        // Check that there are two results with projected entity and projected stats
        assertEquals(2, returnedPlanEntityCombinedStats.stream()
            .filter(PlanEntityAndCombinedStats::hasPlanProjectedEntity)
            .filter(PlanEntityAndCombinedStats::hasPlanCombinedStats)
            .map(PlanEntityAndCombinedStats::getPlanCombinedStats)
            .filter(entityStats -> entityStats.getStatSnapshotsList().stream()
                .filter(StatSnapshot::hasStatEpoch)
                .map(StatSnapshot::getStatEpoch)
                .anyMatch(statEpoch -> StatEpoch.PLAN_PROJECTED == statEpoch))
            .count());
        Set<Long> allReturnedEntityIds = new HashSet<>();
        for (PlanEntityAndCombinedStats planEntityAndCombinedStats : returnedPlanEntityCombinedStats) {
            long entityId = planEntityAndCombinedStats.hasPlanSourceEntity()
                ? planEntityAndCombinedStats.getPlanSourceEntity().getMinimal().getOid()
                : planEntityAndCombinedStats.getPlanProjectedEntity().getMinimal().getOid();
            allReturnedEntityIds.add(entityId);
        }
        assertThat(allReturnedEntityIds,
            containsInAnyOrder(sourceEntityId,
                projectedEntityId,
                commonEntityId));
    }

    private DeleteTopologyRequest createDeleteTopologyRequest(long topologyId) {
        return DeleteTopologyRequest.newBuilder()
            .setTopologyId(topologyId)
            .setTopologyType(RepositoryDTO.TopologyType.PROJECTED)
            .build();
    }

    private DeleteTopologyRequest createDeleteTopologyRequest(
            long topologyId,
            long topologyContextId) {

        return DeleteTopologyRequest.newBuilder()
            .setTopologyId(topologyId)
            .setTopologyContextId(topologyContextId)
            .setTopologyType(RepositoryDTO.TopologyType.PROJECTED)
            .build();
    }

    private PlanEntityAndCombinedStats buildPlanEntityAndCombinedStats(
            @Nullable final ProjectedTopologyEntity sourceEntity,
            @Nullable final ProjectedTopologyEntity projectedEntity) {
        final PlanEntityAndCombinedStats.Builder builder =  PlanEntityAndCombinedStats.newBuilder();
        final EntityStats.Builder stats = EntityStats.newBuilder();
        if (sourceEntity != null) {
            builder.setPlanSourceEntity(partialEntityConverter
                    .createPartialEntity(sourceEntity.getEntity(), Type.MINIMAL));
            stats.setOid(sourceEntity.getEntity().getOid())
                .addStatSnapshots(StatSnapshot.newBuilder()
                    .setStatEpoch(StatEpoch.PLAN_SOURCE)
                    .setSnapshotDate(Instant.now().toEpochMilli())
                    .build());
        }

        if (projectedEntity != null) {
            builder.setPlanProjectedEntity(partialEntityConverter
                .createPartialEntity(projectedEntity.getEntity(), Type.MINIMAL));
            stats.setOid(projectedEntity.getEntity().getOid())
                .addStatSnapshots(StatSnapshot.newBuilder()
                    .setStatEpoch(StatEpoch.PLAN_PROJECTED)
                    .setSnapshotDate(Instant.now().toEpochMilli() + 1000)
                    .build());
        }
        builder.setPlanCombinedStats(stats);
        return builder.build();
    }
}
