package com.vmturbo.topology.processor.topology;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineFactory;

/**
 * Unit test for {@link TopologyHandler}.
 */
public class TopologyHandlerTest {

    private final IdentityProvider identityProvider = mock(IdentityProvider.class);

    private TopologyHandler topologyHandler;

    private final long topologyId = 0L;

    private final long realtimeTopologyContextId = 7000;

    private final long clockTime = 77L;

    private final TopologyPipelineFactory pipelineFactory = mock(TopologyPipelineFactory.class);

    private final Clock clock = mock(Clock.class);

    private final EntityStore entityStore = mock(EntityStore.class);

    private final TopologyInfo realtimeTopologyInfo = TopologyInfo.newBuilder()
            .setTopologyContextId(realtimeTopologyContextId)
            .setTopologyId(topologyId)
            .setCreationTime(clockTime)
            .setTopologyType(TopologyType.REALTIME)
            .build();

    @Before
    public void init() {
        topologyHandler = new TopologyHandler(realtimeTopologyContextId, pipelineFactory,
                identityProvider, entityStore, clock);
        when(identityProvider.generateTopologyId()).thenReturn(topologyId);
        when(clock.millis()).thenReturn(clockTime);
    }

    @Test
    public void testBroadcastTopology() throws Exception {
        TopologyPipeline<EntityStore, TopologyBroadcastInfo> pipeline =
                (TopologyPipeline<EntityStore, TopologyBroadcastInfo>)mock(TopologyPipeline.class);
        TopologyBroadcastInfo broadcastInfo = mock(TopologyBroadcastInfo.class);
        when(pipeline.run(eq(entityStore))).thenReturn(broadcastInfo);
        when(pipelineFactory.liveTopology(eq(realtimeTopologyInfo), eq(Collections.emptyList()))).thenReturn(pipeline);

        assertThat(topologyHandler.broadcastLatestTopology(), is(broadcastInfo));
    }
}
