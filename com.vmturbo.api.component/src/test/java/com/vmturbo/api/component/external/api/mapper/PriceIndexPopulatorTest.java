package com.vmturbo.api.component.external.api.mapper;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.vmturbo.api.dto.entity.ServiceEntityApiDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStats;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanEntityStatsChunk;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.Stats.EntityStats;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityStatsResponse;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord.StatValue;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Test for {@link PriceIndexPopulator}.
 */
public class PriceIndexPopulatorTest {

    private final ServiceEntityApiDTO vm1 = new ServiceEntityApiDTO();
    private final ServiceEntityApiDTO vm2 = new ServiceEntityApiDTO();
    private final ServiceEntityApiDTO network1 = new ServiceEntityApiDTO();

    private StatsHistoryServiceMole statsHistoryServiceMole = spy(new StatsHistoryServiceMole());

    private RepositoryServiceMole repositoryServiceMole = spy(new RepositoryServiceMole());

    @Rule
    public GrpcTestServer grpcServer = GrpcTestServer.newServer(statsHistoryServiceMole,
            repositoryServiceMole);

    private PriceIndexPopulator priceIndexPopulator;

    /**
     * Init for each test.
     */
    @Before
    public void setup() {
        StatsHistoryServiceBlockingStub statsHistoryServiceBlockingStub =
                StatsHistoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        RepositoryServiceBlockingStub repositoryServiceBlockingStub =
                RepositoryServiceGrpc.newBlockingStub(grpcServer.getChannel());
        priceIndexPopulator = new PriceIndexPopulator(statsHistoryServiceBlockingStub,
                repositoryServiceBlockingStub);

        vm1.setUuid("1");
        vm1.setClassName(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        vm2.setUuid("2");
        vm2.setClassName(ApiEntityType.VIRTUAL_MACHINE.apiStr());
        network1.setUuid("3");
        network1.setClassName(ApiEntityType.NETWORK.apiStr());
    }

    /**
     * Test that entities with priceIndex are populated correctly and those without priceIndex are
     * not populated.
     */
    @Test
    public void testPopulatePriceIndexForRealTimeEntities() {
        List<ServiceEntityApiDTO> entities = Arrays.asList(vm1, vm2, network1);
        when(statsHistoryServiceMole.getEntityStats(any())).thenReturn(
                createEntityStatsResponse(ImmutableMap.of(1L, 1000F, 2L, 5000F)));

        priceIndexPopulator.populateRealTimeEntities(entities);

        assertThat(entities.get(0).getPriceIndex(), is(1000F));
        assertThat(entities.get(1).getPriceIndex(), is(5000F));
        assertNull(entities.get(2).getPriceIndex());
    }

    /**
     * Test that priceIndex is populated correctly for plan entities.
     */
    @Test
    public void testPopulatePriceIndexForPlanEntities() {
        List<ServiceEntityApiDTO> entities = Arrays.asList(vm1, vm2);

        when(repositoryServiceMole.getPlanTopologyStats(any())).thenReturn(Lists.newArrayList(
                createPlanTopologyStatsResponse(ImmutableMap.of(1L, 8000F, 2L, 3000F))));

        priceIndexPopulator.populatePlanEntities(888888, entities);

        assertThat(entities.get(0).getPriceIndex(), is(8000F));
        assertThat(entities.get(1).getPriceIndex(), is(3000F));
    }

    /**
     * Create real time {@link GetEntityStatsResponse} containing the given price indexes for
     * different entities.
     *
     * @param priceIndexByEntity mapping from entity oid to priceIndex
     * @return {@link GetEntityStatsResponse}
     */
    private GetEntityStatsResponse createEntityStatsResponse(
            @Nonnull Map<Long, Float> priceIndexByEntity) {
        final List<EntityStats> stats = priceIndexByEntity.entrySet().stream()
                .map(entry -> EntityStats.newBuilder()
                        .setOid(entry.getKey())
                        .addStatSnapshots(StatSnapshot.newBuilder()
                                .addStatRecords(StatRecord.newBuilder()
                                        .setUsed(StatValue.newBuilder()
                                                .setMax(entry.getValue()))))
                        .build())
                .collect(Collectors.toList());

        return GetEntityStatsResponse.newBuilder()
                .addAllEntityStats(stats)
                .build();
    }

    /**
     * Create {@link PlanTopologyStatsResponse} containing the given price indexes for different
     * entities.
     *
     * @param priceIndexByEntity mapping from entity oid to priceIndex
     * @return {@link PlanTopologyStatsResponse}
     */
    private PlanTopologyStatsResponse createPlanTopologyStatsResponse(
            @Nonnull Map<Long, Float> priceIndexByEntity) {
        final List<PlanEntityStats> stats = priceIndexByEntity.entrySet().stream()
                .map(entry -> PlanEntityStats.newBuilder()
                        .setPlanEntity(PartialEntity.newBuilder()
                            .setMinimal(MinimalEntity.newBuilder()
                                .setOid(entry.getKey())
                                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)))
                        .setPlanEntityStats(EntityStats.newBuilder()
                                .addStatSnapshots(StatSnapshot.newBuilder()
                                        .addStatRecords(StatRecord.newBuilder()
                                                .setUsed(StatValue.newBuilder()
                                                        .setMax(entry.getValue())))))
                        .build())
                .collect(Collectors.toList());

        return PlanTopologyStatsResponse.newBuilder()
                .setEntityStatsWrapper(PlanEntityStatsChunk.newBuilder()
                        .addAllEntityStats(stats))
                .build();
    }
}
