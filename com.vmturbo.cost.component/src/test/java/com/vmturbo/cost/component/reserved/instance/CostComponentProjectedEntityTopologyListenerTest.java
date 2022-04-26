package com.vmturbo.cost.component.reserved.instance;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.concurrent.TimeoutException;

import com.google.common.collect.Sets;

import io.opentracing.SpanContext;

import org.junit.Test;

import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopology;
import com.vmturbo.cloud.common.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.component.cloud.commitment.ProjectedCommitmentMappingProcessor;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class CostComponentProjectedEntityTopologyListenerTest {

    private final long topologyId = 111L;
    private final TopologyInfo topologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(topologyId)
            .setTopologyType(TopologyType.PLAN)
            .build();
    private final RemoteIterator<ProjectedTopologyEntity> remoteIterator = mock(RemoteIterator.class);
    private final long realtimeTopopologyContextId = 111L;
    private final ComputeTierDemandStatsWriter computeTierDemandStatsWriter = mock(ComputeTierDemandStatsWriter.class);
    private final TopologyEntityCloudTopologyFactory topologyEntityCloudTopologyFactory = mock(TopologyEntityCloudTopologyFactory.class);
    private final ProjectedCommitmentMappingProcessor projectedCommitmentMappingProcessor = mock(ProjectedCommitmentMappingProcessor.class);
    final ProjectedTopology.Metadata metadata = ProjectedTopology.Metadata.newBuilder()
            .setSourceTopologyInfo(topologyInfo)
            .build();

    /**
     * Test when projected topology received, projectedAvailable is being called.
     */
    @Test
    public void testProjectedAvailableBeingCalled()
            throws CommunicationException, InterruptedException, TimeoutException {

        when(remoteIterator.hasNext()).thenReturn(true, false);
        ProjectedTopologyEntity projectedTopologyEntity1 = ProjectedTopologyEntity.newBuilder()
                .setEntity(TopologyEntityDTO.newBuilder().setOid(1L).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setEnvironmentType(EnvironmentType.CLOUD).build())
                .build();
        ProjectedTopologyEntity projectedTopologyEntity2 = ProjectedTopologyEntity.newBuilder()
                .setEntity(TopologyEntityDTO.newBuilder().setOid(2L).setEntityType(EntityType.VIRTUAL_MACHINE_VALUE).setEnvironmentType(EnvironmentType.CLOUD).build())
                .build();
        when(remoteIterator.nextChunk())
                .thenReturn(Sets.newHashSet(projectedTopologyEntity1, projectedTopologyEntity2));

        final SpanContext tracingContext = mock(SpanContext.class);
        final TopologyEntityCloudTopology cloudTopology = mock(TopologyEntityCloudTopology.class);
        when(topologyEntityCloudTopologyFactory.newCloudTopology(any())).thenReturn(cloudTopology);

        CostComponentProjectedEntityTopologyListener costComponentProjectedEntityTopologyListener =
                new CostComponentProjectedEntityTopologyListener(realtimeTopopologyContextId,
                        computeTierDemandStatsWriter,
                        topologyEntityCloudTopologyFactory,
                        projectedCommitmentMappingProcessor);
        costComponentProjectedEntityTopologyListener.onProjectedTopologyReceived(metadata, remoteIterator, tracingContext);

        verify(projectedCommitmentMappingProcessor).projectedAvailable(anyLong(), eq(realtimeTopopologyContextId), any());
    }
}