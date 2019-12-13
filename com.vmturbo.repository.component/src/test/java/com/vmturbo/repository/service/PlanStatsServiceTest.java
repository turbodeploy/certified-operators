package com.vmturbo.repository.service;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

import org.assertj.core.util.Lists;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.common.Pagination.OrderBy;
import com.vmturbo.common.protobuf.common.Pagination.OrderBy.EntityStatsOrderBy;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.common.Pagination.PaginationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityAndCombinedStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse.TypeCase;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.PlanScenarioOrigin;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.PaginatedStats;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;

/**
 *  Test plan entity stats retrieval.
 */
@RunWith(MockitoJUnitRunner.class)
public class PlanStatsServiceTest {

    /**
     * Mock, a factory for creating {@link EntityStatsPaginationParams}.
     */
    private EntityStatsPaginationParamsFactory paginationParamsFactory =
        mock(EntityStatsPaginationParamsFactory.class);

    /**
     * Mock, to do in-memory pagination of entities.
     */
    private EntityStatsPaginator entityStatsPaginator = mock(EntityStatsPaginator.class);

    /**
     * Converts entities to partial entities with the appropriate detail levels.
     */
    private PartialEntityConverter partialEntityConverter = new PartialEntityConverter();

    /**
     * The class under test.
     */
    private PlanStatsService planStatsService =
        new PlanStatsService(paginationParamsFactory, entityStatsPaginator, partialEntityConverter, 10);

    /**
     * Allow certain unit tests to declare expected exceptions.
     */
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    /**
     * Test retrieving projected statistics.
     */
    @Test
    public void testRetrievePlanSourceStats() {
        // arrange
        final ProjectedTopologyEntity originalEntity = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setOid(1L)
                .setEntityType(10)
                .setDisplayName("x"))
            .build();
        // This entity was added as part of the plan configuration, and should be filtered out of
        // the plan source topology stats results.
        final ProjectedTopologyEntity entityAddedByScenario = ProjectedTopologyEntity.newBuilder()
            .setEntity(TopologyEntityDTO.newBuilder()
                .setOid(2L)
                .setEntityType(10)
                .setDisplayName("y")
                .setOrigin(Origin.newBuilder()
                    .setPlanScenarioOrigin(PlanScenarioOrigin.newBuilder()
                        .setPlanId(123L)
                    )))
            .build();
        final PaginationParameters paginationParameters = PaginationParameters.newBuilder()
            .setCursor("foo")
            .build();
        final long startDate = Instant.now().toEpochMilli() + 100000;
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .setStartDate(startDate)
            .build();

        final TopologyProtobufReader protobufReader = mock(TopologyProtobufReader.class);
        when(protobufReader.hasNext()).thenReturn(true).thenReturn(false);
        when(protobufReader.nextChunk()).thenReturn(Lists.newArrayList(originalEntity, entityAddedByScenario));

        final StatEpoch statEpoch = StatEpoch.PLAN_SOURCE;
        final EntityStats.Builder statsBuilder = EntityStats.newBuilder()
            .setOid(originalEntity.getEntity().getOid())
            .addStatSnapshots(StatSnapshot.newBuilder()
                .setStatEpoch(statEpoch)
                .setSnapshotDate(startDate)
                .build());

        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn("foo");
        when(paginationParamsFactory.newPaginationParams(paginationParameters)).thenReturn(paginationParams);

        final PaginatedStats paginatedStats = mock(PaginatedStats.class);
        when(paginatedStats.getNextPageIds()).thenReturn(Collections.singletonList(originalEntity.getEntity().getOid()));

        final PaginationResponse paginationResponse = PaginationResponse.newBuilder()
            .setNextCursor("bar")
            .build();
        when(paginatedStats.getPaginationResponse()).thenReturn(paginationResponse);

        when(entityStatsPaginator.paginate(eq(Collections.singleton(originalEntity.getEntity().getOid())), any(), eq(paginationParams)))
            .thenReturn(paginatedStats);

        // Some final parameters
        final Predicate<TopologyEntityDTO> noFilterPredicate = (entity) -> true;
        final Type entityReturnType = Type.MINIMAL;

        // for checking the results (the StreamObserver will deposit the response into results list)
        final List<PlanTopologyStatsResponse> results = Lists.newArrayList();
        final StreamObserver<PlanTopologyStatsResponse> responseObserver = getResponseObserver(results);

        // act
        planStatsService.getPlanTopologyStats(protobufReader,
            statEpoch,
            statsFilter,
            noFilterPredicate,
            paginationParameters,
            entityReturnType,
            responseObserver);

        // Extract the results
        List<PlanEntityStats> returnedPlanEntityStats = new ArrayList<>();
        PaginationResponse returnedPaginationResponse = null;
        for (PlanTopologyStatsResponse chunk : results) {
            if (chunk.getTypeCase() == TypeCase.PAGINATION_RESPONSE) {
                returnedPaginationResponse = chunk.getPaginationResponse();
            } else {
                returnedPlanEntityStats.addAll(chunk.getEntityStatsWrapper().getEntityStatsList());
            }
        }
        // assert
        verify(paginationParamsFactory).newPaginationParams(paginationParameters);
        verify(entityStatsPaginator).paginate(eq(Collections.singleton(
            originalEntity.getEntity().getOid())), any(), eq(paginationParams));

        assertThat(returnedPaginationResponse, is(paginationResponse));
        assertThat(returnedPlanEntityStats, is(Collections.singletonList(PlanEntityStats.newBuilder()
            .setPlanEntity(partialEntityConverter
                .createPartialEntity(originalEntity.getEntity(), entityReturnType))
            .setPlanEntityStats(statsBuilder)
            .build())));
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

        final TopologyProtobufReader protobufReader = mock(TopologyProtobufReader.class);
        when(protobufReader.hasNext()).thenReturn(true).thenReturn(false);
        when(protobufReader.nextChunk()).thenReturn(Collections.singletonList(topologyEntityDTO));

        final StatEpoch statEpoch = StatEpoch.PLAN_PROJECTED;
        final EntityStats.Builder statsBuilder = EntityStats.newBuilder()
            .setOid(topologyEntityDTO.getEntity().getOid())
            .addStatSnapshots(StatSnapshot.newBuilder()
                .setStatEpoch(statEpoch)
                .setSnapshotDate(startDate)
                .build());

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

        // Some final parameters
        final Predicate<TopologyEntityDTO> noFilterPredicate = (entity) -> true;
        final Type entityReturnType = Type.MINIMAL;

        // for checking the results (the StreamObserver will deposit the response into results list)
        final List<PlanTopologyStatsResponse> results = Lists.newArrayList();
        final StreamObserver<PlanTopologyStatsResponse> responseObserver = getResponseObserver(results);

        // act
        planStatsService.getPlanTopologyStats(protobufReader,
            statEpoch,
            statsFilter,
            noFilterPredicate,
            paginationParameters,
            entityReturnType,
            responseObserver);

        // Extract the results
        List<PlanEntityStats> returnedPlanEntityStats = new ArrayList<>();
        PaginationResponse returnedPaginationResponse = null;
        for (PlanTopologyStatsResponse chunk : results) {
            if (chunk.getTypeCase() == TypeCase.PAGINATION_RESPONSE) {
                returnedPaginationResponse = chunk.getPaginationResponse();
            } else {
                returnedPlanEntityStats.addAll(chunk.getEntityStatsWrapper().getEntityStatsList());
            }
        }
        // assert
        verify(paginationParamsFactory).newPaginationParams(paginationParameters);
        verify(entityStatsPaginator).paginate(eq(Collections.singleton(
            topologyEntityDTO.getEntity().getOid())), any(), eq(paginationParams));

        assertThat(returnedPaginationResponse, is(paginationResponse));
        assertThat(returnedPlanEntityStats, is(Collections.singletonList(PlanEntityStats.newBuilder()
            .setPlanEntity(partialEntityConverter
                .createPartialEntity(topologyEntityDTO.getEntity(), entityReturnType))
            .setPlanEntityStats(statsBuilder)
            .build())));
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

        final long startDate = Instant.now().toEpochMilli();
        final long endDate = Instant.now().toEpochMilli() + 100000;
        final StatsFilter statsFilter = StatsFilter.newBuilder()
            .setStartDate(startDate)
            .setEndDate(endDate)
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

        final EntityStatsPaginationParams paginationParams = mock(EntityStatsPaginationParams.class);
        when(paginationParams.getSortCommodity()).thenReturn(sortCommodity);
        when(paginationParamsFactory.newPaginationParams(paginationParameters)).thenReturn(paginationParams);

        List<Long> combinedEntityIds = Lists.newArrayList(
            sourceTopologyEntityDTO.getEntity().getOid(),
            commonTopologyEntityDTO.getEntity().getOid(),
            projectedTopologyEntityDTO.getEntity().getOid());

        // Pagination happens on all plan entities, simultaneously
        final PaginatedStats paginatedCombinedStats = mock(PaginatedStats.class);
        when(paginatedCombinedStats.getNextPageIds())
            .thenReturn(combinedEntityIds);

        final PaginationResponse paginationResponse = PaginationResponse.newBuilder()
            .build();
        when(paginatedCombinedStats.getPaginationResponse()).thenReturn(paginationResponse);

        when(entityStatsPaginator.paginate(anyCollection(), any(), any()))
            .thenReturn(paginatedCombinedStats);

        // Some final parameters
        final TopologyType topologyTypeToSortOn = TopologyType.PROJECTED;
        final Predicate<TopologyEntityDTO> noFilterPredicate = (entity) -> true;
        final Type entityReturnType = Type.MINIMAL;

        // for checking the results (the StreamObserver will deposit the response into results list)
        final List<PlanCombinedStatsResponse> results = Lists.newArrayList();
        final StreamObserver<PlanCombinedStatsResponse> responseObserver = getResponseObserver(results);

        // act
        planStatsService.getPlanCombinedStats(sourceProtobufReader,
            projectedProtobufReader,
            statsFilter,
            noFilterPredicate,
            topologyTypeToSortOn,
            paginationParameters,
            entityReturnType,
            responseObserver);

        // Extract the results
        List<PlanEntityAndCombinedStats> returnedPlanEntityCombinedStats = new ArrayList<>();
        PaginationResponse returnedPaginationResponse = null;
        for (PlanCombinedStatsResponse chunk : results) {
            if (chunk.getTypeCase() == PlanCombinedStatsResponse.TypeCase.PAGINATION_RESPONSE) {
                returnedPaginationResponse = chunk.getPaginationResponse();
            } else {
                returnedPlanEntityCombinedStats.addAll(
                    chunk.getEntityCombinedStatsWrapper().getEntityAndCombinedStatsList());
            }
        }

        // assert

        // only one pagination should occur (on the combined entities/stats)
        verify(entityStatsPaginator).paginate(anyCollection(), any(), eq(paginationParams));

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

    /**
     * Builds a StreamObserver, backed by the provided list.
     *
     * @param results a list to store the results in
     * @param <T> the type of entries to be returned by the stream
     * @return a StreamObserver, backed by the provided list
     */
    private <T> StreamObserver<T> getResponseObserver(final List<T> results) {
        return new StreamObserver<T>() {
            @Override
            public void onNext(final T value) {
                results.add(value);
            }

            @Override
            public void onError(final Throwable t) {
                throw new RuntimeException(t);
            }

            @Override
            public void onCompleted() {
                // no-op
            }
        };
    }

}
