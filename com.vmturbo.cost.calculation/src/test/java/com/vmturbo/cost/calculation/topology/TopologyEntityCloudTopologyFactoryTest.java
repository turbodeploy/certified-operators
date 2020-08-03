package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.Test;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.group.api.GroupMemberRetriever;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

public class TopologyEntityCloudTopologyFactoryTest {

    private static final long CLOUD_TARGET_ID = 11;
    private static final long NON_CLOUD_TARGET_ID = 22;

    private static final TopologyEntityDTO CLOUD_ENTITY_E = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setEnvironmentType(EnvironmentType.CLOUD)
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .putDiscoveredTargetData(CLOUD_TARGET_ID, PerTargetEntityInformation.getDefaultInstance())))
            .build();

    private static final TopologyDTO.Topology.DataSegment
        CLOUD_ENTITY = TopologyDTO.Topology.DataSegment.newBuilder()
                                                       .setEntity(CLOUD_ENTITY_E)
                                                       .build();

    private static final TopologyEntityDTO NON_CLOUD_ENTITY_E = TopologyEntityDTO.newBuilder()
            .setOid(2L)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setEnvironmentType(EnvironmentType.ON_PREM)
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .putDiscoveredTargetData(NON_CLOUD_TARGET_ID, PerTargetEntityInformation.getDefaultInstance())))
            .build();

    private static final TopologyDTO.Topology.DataSegment
        NON_CLOUD_ENTITY = TopologyDTO.Topology.DataSegment.newBuilder()
                                                       .setEntity(NON_CLOUD_ENTITY_E)
                                                       .build();
    @Test
    public void testStream() {
        final TopologyEntityCloudTopologyFactory factory =
                new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class));
        final TopologyEntityCloudTopology topology = factory.newCloudTopology(Stream.of(CLOUD_ENTITY_E, NON_CLOUD_ENTITY_E));
        assertThat(topology.getEntities().values(), contains(CLOUD_ENTITY_E));
    }

    @Test
    public void testRemoteIterator() throws InterruptedException, TimeoutException, CommunicationException {
        final RemoteIterator<TopologyDTO.Topology.DataSegment> remoteIterator = mock(RemoteIterator.class);
        when(remoteIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(remoteIterator.nextChunk())
                .thenReturn(Collections.singleton(NON_CLOUD_ENTITY))
                .thenReturn(Collections.singleton(CLOUD_ENTITY));
        final TopologyEntityCloudTopologyFactory factory =
                new DefaultTopologyEntityCloudTopologyFactory(mock(GroupMemberRetriever.class));
        final TopologyEntityCloudTopology topology = factory.newCloudTopology(1, remoteIterator);
        assertThat(topology.getEntities().values(), contains(CLOUD_ENTITY_E));
    }
}
