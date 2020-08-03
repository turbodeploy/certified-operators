package com.vmturbo.cost.component.reserved.instance.coverage.analysis;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableTable;

import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage.RICoverageSource;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceSpecStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.reserved.instance.coverage.allocator.RICoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

public class SupplementalRICoverageAnalysisTest {

    private final RICoverageAllocatorFactory allocatorFactory =
            mock(RICoverageAllocatorFactory.class);

    private final CoverageTopologyFactory coverageTopologyFactory =
            mock(CoverageTopologyFactory.class);

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore =
            mock(ReservedInstanceBoughtStore.class);

    private final ReservedInstanceSpecStore reservedInstanceSpecStore =
            mock(ReservedInstanceSpecStore.class);

    private final ReservedInstanceCoverageAllocator riCoverageAllocator =
            mock(ReservedInstanceCoverageAllocator.class);
    private final CoverageTopology coverageTopology = mock(CoverageTopology.class);

    @Before
    public void setup() {
        when(allocatorFactory.createAllocator(any())).thenReturn(riCoverageAllocator);
    }

    @Test
    public void testCreateCoverageRecordsFromSupplementalAllocation() {
        // setup billing coverage records
        final List<EntityRICoverageUpload> entityRICoverageUploads = ImmutableList.of(
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(1)
                        .setTotalCouponsRequired(4)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(4)
                                        .setCoveredCoupons(2)
                                        .setRiCoverageSource(RICoverageSource.BILLING)
                                        .build())
                        .build(),
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(2)
                        .setTotalCouponsRequired(4)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(5)
                                        .setCoveredCoupons(4)
                                        .setRiCoverageSource(RICoverageSource.BILLING)
                                        .build())
                        .build());
        // setup RI allocator output
        final ReservedInstanceCoverageAllocation coverageAllocation = ReservedInstanceCoverageAllocation.from(
                // total coverage
                ImmutableTable.<Long, Long, Double>builder()
                        .put(1L, 4L, 4.0)
                        .put(2L, 5L, 4.0)
                        .put(3L, 6L, 4.0)
                        .build(),
                // supplemental allocations
                ImmutableTable.<Long, Long, Double>builder()
                        .put(1L, 4L, 2.0)
                        .put(3L, 6L, 4.0)
                        .build());
        when(riCoverageAllocator.allocateCoverage()).thenReturn(coverageAllocation);

        // setup coverage topology
        // Entity ID 3 will require resolution of the coverage capacity
        when(coverageTopology.getRICoverageCapacityForEntity(eq(3L))).thenReturn(8L);


        /*
        Setup Factory
         */

        final List<ReservedInstanceBought> reservedInstances = ImmutableList.of(
                ReservedInstanceBought.newBuilder()
                        .setId(1)
                        .build(),
                ReservedInstanceBought.newBuilder()
                        .setId(2)
                        .build());

        final List<ReservedInstanceSpec> riSpecs = ImmutableList.of(
                ReservedInstanceSpec.newBuilder()
                        .setId(3)
                        .build(),
                ReservedInstanceSpec.newBuilder()
                        .setId(4)
                        .build());
        final CloudTopology cloudTopology = mock(CloudTopology.class);

        /*
        Setup mocks for factory
         */
        when(reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(
                eq(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER)))
                .thenReturn(reservedInstances);
        when(reservedInstanceSpecStore.getReservedInstanceSpecByIds(any()))
                .thenReturn(riSpecs);
        when(coverageTopologyFactory.createCoverageTopology(
                eq(cloudTopology),
                eq(riSpecs),
                eq(reservedInstances))).thenReturn(coverageTopology);

        /*
        Setup SUT
         */
        final SupplementalRICoverageAnalysisFactory factory =
                new SupplementalRICoverageAnalysisFactory(
                        allocatorFactory,
                        coverageTopologyFactory,
                        reservedInstanceBoughtStore,
                        reservedInstanceSpecStore,
                        true,
                        false);

        /*
        Invoke SUT
         */
        final SupplementalRICoverageAnalysis coverageAnalysis = factory.createCoverageAnalysis(
                cloudTopology,
                entityRICoverageUploads);
        final List<EntityRICoverageUpload> aggregateRICoverages =
                coverageAnalysis.createCoverageRecordsFromSupplementalAllocation();


        /*
        Expected results
         */
        final List<EntityRICoverageUpload> expectedAggregateRICoverages = ImmutableList.of(
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(1)
                        .setTotalCouponsRequired(4)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(4)
                                        .setCoveredCoupons(2)
                                        .setRiCoverageSource(RICoverageSource.BILLING)
                                        .build())
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(4)
                                        .setCoveredCoupons(2)
                                        .setRiCoverageSource(RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION)
                                        .build())
                        .build(),
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(2)
                        .setTotalCouponsRequired(4)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(5)
                                        .setCoveredCoupons(4)
                                        .setRiCoverageSource(RICoverageSource.BILLING)
                                        .build())
                        .build(),
                EntityRICoverageUpload.newBuilder()
                        .setEntityId(3)
                        .setTotalCouponsRequired(8L)
                        .addCoverage(
                                Coverage.newBuilder()
                                        .setReservedInstanceId(6)
                                        .setCoveredCoupons(4)
                                        .setRiCoverageSource(RICoverageSource.SUPPLEMENTAL_COVERAGE_ALLOCATION)
                                        .build())
                        .build());

        /*
        Assertions
         */
        assertThat(aggregateRICoverages, iterableWithSize(3));
        assertThat(aggregateRICoverages, containsInAnyOrder(expectedAggregateRICoverages.toArray()));

    }
}
