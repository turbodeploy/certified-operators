package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;

import org.junit.Before;

import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopologySegment;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.AggregateDemandPreference.AggregateDemandPreferenceFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.AnalysisCoverageTopology.AnalysisCoverageTopologyFactory;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationTask.CoverageCalculationTaskFactory;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator;
import com.vmturbo.cloud.common.commitment.aggregator.CloudCommitmentAggregator.CloudCommitmentAggregatorFactory;
import com.vmturbo.cloud.common.data.TimeInterval;
import com.vmturbo.cloud.common.topology.MinimalCloudTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.reserved.instance.coverage.allocator.CoverageAllocatorFactory;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocation;
import com.vmturbo.reserved.instance.coverage.allocator.ReservedInstanceCoverageAllocator;

public class CoverageCalculationTaskTest {

    private final CloudCommitmentAggregatorFactory aggregatorFactory = mock(CloudCommitmentAggregatorFactory.class);

    private final CloudCommitmentAggregator commitmentAggregator = mock(CloudCommitmentAggregator.class);

    private final AnalysisCoverageTopologyFactory coverageTopologyFactory = mock(AnalysisCoverageTopologyFactory.class);
    private final AnalysisCoverageTopology coverageTopology = mock(AnalysisCoverageTopology.class);

    private final CoverageAllocatorFactory coverageAllocatorFactory = mock(CoverageAllocatorFactory.class);
    private final ReservedInstanceCoverageAllocator coverageAllocator =
            mock(ReservedInstanceCoverageAllocator.class);

    private final AggregateDemandPreferenceFactory preferenceFactory =
            mock(AggregateDemandPreferenceFactory.class);
    private final AggregateDemandPreference demandPreference = mock(AggregateDemandPreference.class);

    private final AnalysisTopologySegment analysisSegment = AnalysisTopologySegment.builder()
            .timeInterval(TimeInterval.builder()
                    .startTime(Instant.now().minusSeconds(10))
                    .endTime(Instant.now())
                    .build())
            .build();

    private final CloudTopology<TopologyEntityDTO> cloudTierTopology = mock(CloudTopology.class);

    private final MinimalCloudTopology<MinimalEntity> cloudTopology = mock(MinimalCloudTopology.class);

    private final CoverageCalculationTaskFactory calculationTaskFactory =
            new CoverageCalculationTaskFactory(
                    aggregatorFactory,
                    coverageTopologyFactory,
                    coverageAllocatorFactory,
                    preferenceFactory);


    @Before
    public void setup() {
        when(aggregatorFactory.newAggregator(any())).thenReturn(commitmentAggregator);
        when(coverageTopologyFactory.newTopology(any(), any(), any(), any(), any())).thenReturn(coverageTopology);
        when(coverageAllocatorFactory.createAllocator(any())).thenReturn(coverageAllocator);
        when(preferenceFactory.newPreference(any())).thenReturn(demandPreference);
        when(coverageAllocator.allocateCoverage()).thenReturn(ReservedInstanceCoverageAllocation.EMPTY_ALLOCATION);
    }
}
