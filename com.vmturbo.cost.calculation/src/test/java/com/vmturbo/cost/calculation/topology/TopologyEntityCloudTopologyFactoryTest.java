package com.vmturbo.cost.calculation.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Origin;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.RemoteIterator;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory.DefaultTopologyEntityCloudTopologyFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.api.ProbeInfo;
import com.vmturbo.topology.processor.api.TargetInfo;
import com.vmturbo.topology.processor.api.TopologyProcessor;

public class TopologyEntityCloudTopologyFactoryTest {

    private TopologyProcessor topologyProcessor = mock(TopologyProcessor.class);

    private static final long CLOUD_TARGET_ID = 11;
    private static final long CLOUD_PROBE_ID = 111;
    private static final long NON_CLOUD_TARGET_ID = 22;
    private static final long NON_CLOUD_PROBE_ID = 222;

    private static final TopologyEntityDTO CLOUD_ENTITY = TopologyEntityDTO.newBuilder()
            .setOid(1L)
            .setEntityType(EntityType.COMPUTE_TIER_VALUE)
            .setOrigin(Origin.newBuilder()
                .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                        .addDiscoveringTargetIds(CLOUD_TARGET_ID)))
            .build();

    private static final TopologyEntityDTO NON_CLOUD_ENTITY = TopologyEntityDTO.newBuilder()
            .setOid(2L)
            .setEntityType(EntityType.PHYSICAL_MACHINE_VALUE)
            .setOrigin(Origin.newBuilder()
                    .setDiscoveryOrigin(DiscoveryOrigin.newBuilder()
                            .addDiscoveringTargetIds(NON_CLOUD_TARGET_ID)))
            .build();

    private TargetInfo CLOUD_TARGET_INFO;

    private ProbeInfo CLOUD_PROBE_INFO;

    private TargetInfo NON_CLOUD_TARGET_INFO;

    private ProbeInfo NON_CLOUD_PROBE_INFO;

    @Before
    public void setup() throws CommunicationException {
        CLOUD_TARGET_INFO = mock(TargetInfo.class);
        when(CLOUD_TARGET_INFO.getId()).thenReturn(CLOUD_TARGET_ID);
        when(CLOUD_TARGET_INFO.getProbeId()).thenReturn(CLOUD_PROBE_ID);

        CLOUD_PROBE_INFO = mock(ProbeInfo.class);
        when(CLOUD_PROBE_INFO.getId()).thenReturn(CLOUD_PROBE_ID);
        when(CLOUD_PROBE_INFO.getType()).thenReturn(SDKProbeType.AWS.getProbeType());

        NON_CLOUD_TARGET_INFO = mock(TargetInfo.class);
        when(NON_CLOUD_TARGET_INFO.getId()).thenReturn(NON_CLOUD_TARGET_ID);
        when(NON_CLOUD_TARGET_INFO.getProbeId()).thenReturn(NON_CLOUD_PROBE_ID);

        NON_CLOUD_PROBE_INFO = mock(ProbeInfo.class);
        when(NON_CLOUD_PROBE_INFO.getId()).thenReturn(NON_CLOUD_PROBE_ID);
        when(NON_CLOUD_PROBE_INFO.getType()).thenReturn(SDKProbeType.VCENTER.getProbeType());

        when(topologyProcessor.getAllTargets()).thenReturn(Sets.newHashSet(CLOUD_TARGET_INFO, NON_CLOUD_TARGET_INFO));
        when(topologyProcessor.getAllProbes()).thenReturn(Sets.newHashSet(CLOUD_PROBE_INFO, NON_CLOUD_PROBE_INFO));

    }

    @Test
    public void testStream() {
        final TopologyEntityCloudTopologyFactory factory =
                new DefaultTopologyEntityCloudTopologyFactory(topologyProcessor);
        final TopologyEntityCloudTopology topology = factory.newCloudTopology(Stream.of(CLOUD_ENTITY, NON_CLOUD_ENTITY));
        assertThat(topology.getEntities().values(), contains(CLOUD_ENTITY));
    }

    @Test
    public void testRemoteIterator() throws InterruptedException, TimeoutException, CommunicationException {
        final RemoteIterator<TopologyEntityDTO> remoteIterator = mock(RemoteIterator.class);
        when(remoteIterator.hasNext()).thenReturn(true).thenReturn(true).thenReturn(false);
        when(remoteIterator.nextChunk())
                .thenReturn(Collections.singleton(NON_CLOUD_ENTITY))
                .thenReturn(Collections.singleton(CLOUD_ENTITY));
        final TopologyEntityCloudTopologyFactory factory =
                new DefaultTopologyEntityCloudTopologyFactory(topologyProcessor);
        final TopologyEntityCloudTopology topology = factory.newCloudTopology(1, remoteIterator);
        assertThat(topology.getEntities().values(), contains(CLOUD_ENTITY));
    }
}
