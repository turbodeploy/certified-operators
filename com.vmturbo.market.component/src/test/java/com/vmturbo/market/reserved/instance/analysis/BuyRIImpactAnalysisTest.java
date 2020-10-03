package com.vmturbo.market.reserved.instance.analysis;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.CloudCostData;
import com.vmturbo.cost.calculation.integration.CloudCostDataProvider.ReservedInstanceData;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.market.reserved.instance.analysis.BuyRIImpactAnalysisFactory.DefaultBuyRIImpactAnalysisFactory;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * Unit tests for {@link BuyRIImpactAnalysis}.
 */
public class BuyRIImpactAnalysisTest {

    private final CoverageAllocatorFactory allocatorFactory = mock(CoverageAllocatorFactory.class);

    private final CoverageTopologyFactory coverageTopologyFactory =
            mock(CoverageTopologyFactory.class);

    private final ReservedInstanceCoverageAllocator riCoverageAllocator =
            mock(ReservedInstanceCoverageAllocator.class);

    private final CloudCommitmentAggregator cloudCommitmentAggregator = mock(CloudCommitmentAggregator.class);

    private final CloudCommitmentAggregatorFactory cloudCommitmentAggregatorFactory =
            mock(CloudCommitmentAggregatorFactory.class);

    private final CoverageTopology coverageTopology = mock(CoverageTopology.class);

    /**
     * JUnit setup.
     */
    @Before
    public void setup() {
        when(allocatorFactory.createAllocator(any())).thenReturn(riCoverageAllocator);
        when(cloudCommitmentAggregatorFactory.newIdentityAggregator(any()))
                .thenReturn(cloudCommitmentAggregator);
    }

    /**
     * Test creation of the coverage records from supplemental allocation results.
     */
    @Test
    public void testCreateCoverageRecordsFromSupplementalAllocation() {

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
        final ReservedInstanceCoverageAllocation coverageAllocation = ReservedInstanceCoverageAllocation.from(
                // total coverage
                ImmutableTable.<Long, Long, Double>builder()
                        .put(1L, 4L, 2.0)
                        .put(1L, 5L, 2.0)
                        .put(2L, 5L, 4.0)
                        .put(3L, 6L, 4.0)
                        .build(),
                // supplemental allocations
                ImmutableTable.<Long, Long, Double>builder()
                        .put(1L, 5L, 2.0)
                        .put(3L, 6L, 4.0)
                        .build());
        when(riCoverageAllocator.allocateCoverage()).thenReturn(coverageAllocation);

        // setup coverage topology
        // Entity ID 3 will require resolution of the coverage capacity
        when(coverageTopology.getCoverageCapacityForEntity(eq(3L))).thenReturn(8.0);


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
        final CloudTopology cloudTopology = mock(CloudTopology.class);

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
        final Table<Long, Long, Double> entityRICoverageOutput =
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
        assertThat(entityRICoverageOutput.size(), equalTo(2));
        assertThat(entityRICoverageOutput, equalTo(expectedEntityRICoverage));

    }
}
