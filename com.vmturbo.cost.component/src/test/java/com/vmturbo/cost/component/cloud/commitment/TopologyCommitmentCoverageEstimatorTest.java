package com.vmturbo.cost.component.cloud.commitment;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Table;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.vmturbo.cloud.common.commitment.CloudCommitmentTopology.CloudCommitmentTopologyFactory;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.topology.CloudTopology;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentDTO.CloudCommitmentAmount;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.cloud.commitment.TopologyCommitmentCoverageEstimator.CommitmentCoverageEstimatorFactory;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.CloudCommitmentCoverageAllocator;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocationConfig;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopology;
import com.vmturbo.reserved.instance.coverage.allocator.topology.CoverageTopologyFactory;

/**
 * Test class for {@link TopologyCommitmentCoverageEstimator}.
 */
@RunWith(MockitoJUnitRunner.class)
public class TopologyCommitmentCoverageEstimatorTest {

    @Mock
    private EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    @Mock
    private TopologyCommitmentCoverageWriter.Factory coverageWriterFactory;

    @Mock
    private TopologyCommitmentCoverageWriter coverageWriter;

    @Mock
    private CoverageAllocatorFactory coverageAllocatorFactory;

    @Mock
    private CloudCommitmentCoverageAllocator coverageAllocator;

    @Mock
    private CoverageTopologyFactory coverageTopologyFactory;

    @Mock
    private CoverageTopology coverageTopology;

    @Mock
    private CloudCommitmentAggregatorFactory commitmentAggregatorFactory;

    @Mock
    private CloudCommitmentAggregator cloudCommitmentAggregator;

    @Mock
    private CloudCommitmentTopologyFactory<TopologyEntityDTO> commitmentTopologyFactory;

    @Mock
    private CloudTopology<TopologyEntityDTO> cloudTopology;

    @Before
    public void setup() {
        // set up factories
        when(coverageWriterFactory.newWriter(any())).thenReturn(coverageWriter);
        when(coverageAllocatorFactory.createAllocator(any())).thenReturn(coverageAllocator);
        when(coverageTopologyFactory.createCoverageTopology(any(), any())).thenReturn(coverageTopology);
        when(commitmentAggregatorFactory.newIdentityAggregator(any())).thenReturn(cloudCommitmentAggregator);
    }

    @Test
    public void testEstimatorEnabled() {

        final TopologyCommitmentCoverageEstimator.Factory coverageEstimatorFactory = new CommitmentCoverageEstimatorFactory(
                entityReservedInstanceMappingStore,
                coverageWriterFactory,
                coverageAllocatorFactory,
                coverageTopologyFactory,
                commitmentAggregatorFactory,
                commitmentTopologyFactory,
                true);


        // set up commitment aggregate mock
        when(cloudCommitmentAggregator.getAggregates()).thenReturn(Collections.emptySet());

        // set up entity RI store mock
        final Map<Long, EntityReservedInstanceCoverage> entityRIMappings = ImmutableMap.of(1L, EntityReservedInstanceCoverage.newBuilder()
                .setEntityId(1)
                .putCouponsCoveredByRi(2, 3.0)
                .build());
        when(entityReservedInstanceMappingStore.getEntityRiCoverage()).thenReturn(entityRIMappings);

        // set up coverage allocator mock
        final CloudCommitmentCoverageAllocation coverageAllocation = CloudCommitmentCoverageAllocation.from(
                ImmutableTable.of(),
                ImmutableTable.of(3L, 4L, CloudCommitmentAmount.newBuilder().setCoupons(5.0).build()),
                Collections.emptyList());
        when(coverageAllocator.allocateCoverage()).thenReturn(coverageAllocation);

        // invoke the estimator
        final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
                .setCreationTime(123)
                .setTopologyId(456)
                .build();
        final TopologyCommitmentCoverageEstimator commitmentCoverageEstimator = coverageEstimatorFactory.newEstimator(
                topologyInfo, cloudTopology);
        commitmentCoverageEstimator.estimateAndPersistCoverage();

        // Verify invocations
        final ArgumentCaptor<CoverageAllocationConfig> coverageAllocationConfigCaptor = ArgumentCaptor.forClass(CoverageAllocationConfig.class);
        verify(coverageAllocatorFactory).createAllocator(coverageAllocationConfigCaptor.capture());

        // Invocation of coverage writer
        final ArgumentCaptor<TopologyInfo> topologyInfoCaptor = ArgumentCaptor.forClass(TopologyInfo.class);
        final ArgumentCaptor<Table> allocationTableCaptor = ArgumentCaptor.forClass(Table.class);
        verify(coverageWriter).persistCommitmentAllocations(topologyInfoCaptor.capture(), allocationTableCaptor.capture());

        // assertions
        final Table<Long, Long, CloudCommitmentAmount> expectedSourceCoverage = ImmutableTable.of(
                1L, 2L, CloudCommitmentAmount.newBuilder().setCoupons(3.0).build());
        final CoverageAllocationConfig actualAllocationConfig = coverageAllocationConfigCaptor.getValue();
        assertThat(actualAllocationConfig.sourceCoverage(), equalTo(expectedSourceCoverage));

        // assertions on persisted data
        final TopologyInfo actualTopologyInfo = topologyInfoCaptor.getValue();
        final Table<Long, Long, CloudCommitmentAmount> actualPersistedAllocations = allocationTableCaptor.getValue();
        assertThat(actualTopologyInfo, equalTo(topologyInfo));
        assertThat(actualPersistedAllocations, equalTo(coverageAllocation.allocatorCoverageTable()));
    }
}
