package com.vmturbo.market.reserved.instance.analysis;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.commitment.CloudCommitmentUtils;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.ComputeTierInfo;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysis.BuyCommitmentImpactResult;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory.DefaultBuyRIImpactAnalysisFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * Unit tests for {@link BuyRIImpactAnalysis}.
 */
public class BuyRIImpactAnalysisTest {

    private final CoverageAllocatorFactory allocatorFactory = mock(CoverageAllocatorFactory.class);

    private final CoverageTopologyFactory coverageTopologyFactory =
            mock(CoverageTopologyFactory.class);

    private final CloudCommitmentCoverageAllocator coverageAllocator =
            mock(CloudCommitmentCoverageAllocator.class);

    private final CloudCommitmentAggregator cloudCommitmentAggregator = mock(CloudCommitmentAggregator.class);

    private final CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory =
            mock(CloudCommitmentAggregatorFactory.class);

    private final CoverageTopology coverageTopology = mock(CoverageTopology.class);

    /**
     * JUnit setup.
     */
    @Before
    public void setup() {
        when(allocatorFactory.createAllocator(any())).thenReturn(coverageAllocator);
        when(cloudCommitmentAggregatorFactory.newIdentityAggregator(any()))
                .thenReturn(cloudCommitmentAggregator);
    }

    /**
     * Test creation of the coverage records from supplemental allocation results.
     */
    @Test
    public void testCreateCoverageRecordsFromSupplementalAllocation() {
        IdentityGenerator.initPrefix(0);
        // setup RI coverage input
        final Map<Long, EntityReservedInstanceCoverage> entityRICoverageInput = ImmutableMap.of(
                1L, EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(1L)
                        .putCouponsCoveredByRi(4L, 2.0)
                        .build(),
                2L, EntityReservedInstanceCoverage.newBuilder()
                        .setEntityId(2L)
                        .putCouponsCoveredByRi(5L, 4.0)
                        .build());
        // setup RI allocator output
        final CloudCommitmentCoverageAllocation coverageAllocation = CloudCommitmentCoverageAllocation.from(
                // total coverage
                ImmutableTable.<Long, Long, CloudCommitmentAmount>builder()
                        .put(1L, 4L, CloudCommitmentAmount.newBuilder().setCoupons(2.0).build())
                        .put(1L, 5L, CloudCommitmentAmount.newBuilder().setCoupons(2.0).build())
                        .put(2L, 5L, CloudCommitmentAmount.newBuilder().setCoupons(4.0).build())
                        .put(3L, 6L, CloudCommitmentAmount.newBuilder().setCoupons(4.0).build())
                        .build(),
                // supplemental allocations
                ImmutableTable.<Long, Long, CloudCommitmentAmount>builder()
                        .put(1L, 5L, CloudCommitmentAmount.newBuilder().setCoupons(2.0).build())
                        .put(3L, 6L, CloudCommitmentAmount.newBuilder().setCoupons(4.0).build())
                        .build());
        when(coverageAllocator.allocateCoverage()).thenReturn(coverageAllocation);

        // setup coverage topology
        // Entity ID 3 will require resolution of the coverage capacity
        when(coverageTopology.getCoverageCapacityForEntity(3L, CloudCommitmentUtils.COUPON_COVERAGE_TYPE_INFO))
                .thenReturn(8.0);

        /*
        Setup RI data
         */

        final ReservedInstanceBought ri1 = ReservedInstanceBought.newBuilder()
                        .setId(1)
                        .build();
        final ReservedInstanceBought ri2 = ReservedInstanceBought.newBuilder()
                .setId(2)
                .build();

        final ReservedInstanceSpec riSpec1 = ReservedInstanceSpec.newBuilder()
                .setId(3)
                .build();
        final ReservedInstanceSpec riSpec2 = ReservedInstanceSpec.newBuilder()
                .setId(3)
                .build();

        /*
        Setup cloud topology
         */
        final CloudTopology cloudTopology = mock(CloudTopology.class);
        TopologyEntityDTO targetEntityA = TopologyEntityDTO.newBuilder().setOid(1L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        TopologyEntityDTO computeTierA = TopologyEntityDTO.newBuilder().setOid(20L)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(
                        ComputeTierInfo.newBuilder().setFamily("Family1"))).build();
        TopologyEntityDTO targetEntityB = TopologyEntityDTO.newBuilder().setOid(3L)
                .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .build();
        TopologyEntityDTO computeTierB = TopologyEntityDTO.newBuilder().setOid(30L)
                .setEntityType(EntityType.COMPUTE_TIER_VALUE)
                .setEnvironmentType(EnvironmentType.CLOUD)
                .setTypeSpecificInfo(TypeSpecificInfo.newBuilder().setComputeTier(
                        ComputeTierInfo.newBuilder().setFamily("Family2"))).build();

        when(cloudTopology.getEntity(targetEntityA.getOid())).thenReturn(Optional.of(targetEntityA));
        when(cloudTopology.getComputeTier(targetEntityA.getOid())).thenReturn(Optional.of(computeTierA));
        when(cloudTopology.getEntity(targetEntityB.getOid())).thenReturn(Optional.of(targetEntityB));
        when(cloudTopology.getComputeTier(targetEntityB.getOid())).thenReturn(Optional.of(computeTierB));

        /*
        Setup mocks for factory
         */
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setTopologyContextId(1L)
                .setTopologyId(2L)
                .build();

        final CloudCostData cloudCostData = mock(CloudCostData.class);
        when(cloudCostData.getAllBuyRIs()).thenReturn(
                ImmutableList.of(
                        new ReservedInstanceData(
                                ri1,
                                riSpec1),
                        new ReservedInstanceData(
                                ri2,
                                riSpec2)));
        when(coverageTopologyFactory.createCoverageTopology(
                eq(cloudTopology),
                any())).thenReturn(coverageTopology);

        /*
        Setup SUT
         */
        final BuyRIImpactAnalysisFactory factory =
                new DefaultBuyRIImpactAnalysisFactory(
                        allocatorFactory,
                        coverageTopologyFactory,
                        cloudCommitmentAggregatorFactory,
                        true,
                        false) {
                };

        /*
        Invoke SUT
         */
        final BuyRIImpactAnalysis coverageAnalysis = factory.createAnalysis(
                topologyInfo,
                cloudTopology,
                cloudCostData,
                entityRICoverageInput);
        final BuyCommitmentImpactResult impactResult =
                coverageAnalysis.allocateCoverageFromBuyRIImpactAnalysis();


        /*
        Expected results
         */
        final Table<Long, Long, Double> expectedEntityRICoverage = ImmutableTable.<Long, Long, Double>builder()
                .put(1L, 5L, 2.0)
                .put(3L, 6L, 4.0)
                .build();

        /*
        Assertions
         */
        final Table<Long, Long, Double> actualAllocatedCoverage = impactResult.buyCommitmentCoverage();
        assertThat(actualAllocatedCoverage.size(), equalTo(2));
        assertThat(actualAllocatedCoverage, equalTo(expectedEntityRICoverage));


        /*
        Assert generated allocate actions
         */
        final List<Action.Builder> allocateActions = impactResult.buyAllocationActions();
        assertThat(allocateActions.size(), equalTo(2));

        final Action.Builder allocatActionA = allocateActions.get(0);
        Assert.assertEquals(targetEntityA.getOid(),
                allocatActionA.getInfoBuilder().getAllocate().getTarget().getId());
        Assert.assertEquals(computeTierA.getOid(), allocatActionA.getInfoBuilder().getAllocate().getWorkloadTier().getId());
        Assert.assertEquals("Family1", allocatActionA.getExplanation().getAllocate().getInstanceSizeFamily());

        final Action.Builder allocatActionB = allocateActions.get(1);
        Assert.assertEquals(targetEntityB.getOid(),
                allocatActionB.getInfoBuilder().getAllocate().getTarget().getId());
        Assert.assertEquals(computeTierB.getOid(), allocatActionB.getInfoBuilder().getAllocate().getWorkloadTier().getId());
        Assert.assertEquals("Family2", allocatActionB.getExplanation().getAllocate().getInstanceSizeFamily());
    }
}
