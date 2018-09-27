package com.vmturbo.topology.processor.topology;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.Stages.BroadcastStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineFactory;

public class TopologyRpcServiceTest {

    private TopologyServiceGrpc.TopologyServiceBlockingStub topologyRpcClient;

    private TopologyHandler topologyHandler = mock(TopologyHandler.class);

    private final TopologyPipelineFactory topologyPipelineFactory = mock(TopologyPipelineFactory.class);
    private final IdentityProvider identityProvider = mock(IdentityProvider.class);
    private final EntityStore entityStore = mock(EntityStore.class);
    private final long realtimeTopologyContextId = 1234567L;
    private final Clock clock = mock(Clock.class);
    private final Scheduler scheduler = mock(Scheduler.class);

    private TopologyRpcService topologyRpcServiceBackend = new TopologyRpcService(topologyHandler,
        topologyPipelineFactory, identityProvider, entityStore, scheduler,
        StitchingJournalFactory.emptyStitchingJournalFactory(), realtimeTopologyContextId, clock);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(topologyRpcServiceBackend);

    @Before
    public void startup() throws Exception {
        topologyRpcClient = TopologyServiceGrpc.newBlockingStub(server.getChannel());
    }

    @Test
    public void testRequestTopologyBroadcast() throws Exception {
        topologyRpcClient.requestTopologyBroadcast(
            TopologyBroadcastRequest.newBuilder()
                .build());
        verify(topologyHandler).broadcastLatestTopology(any(StitchingJournalFactory.class));
        verify(scheduler).resetBroadcastSchedule();
    }

    @Test
    public void testRequestTopologyError() throws Exception {
        when(topologyHandler.broadcastLatestTopology(any(StitchingJournalFactory.class)))
            .thenThrow(new RuntimeException("foo"));

        try {
            topologyRpcClient.requestTopologyBroadcast(
                TopologyBroadcastRequest.newBuilder()
                    .build());
            fail("Should have thrown exception in request.");
        } catch (StatusRuntimeException grpcException) {
            assertEquals(Status.INTERNAL.getClass(), grpcException.getStatus().getClass());
            assertEquals("foo", grpcException.getStatus().getDescription());
        }
    }

    @Test
    public void testBroadcastAndReturnTopology() throws Exception {
        final GroupResolver groupResolver = mock(GroupResolver.class);
        // We will send this DTO in the broadcast.
        final TopologyEntityDTO.Builder entityDto = TopologyEntityDTO.newBuilder()
            .setOid(12345L)
            .setDisplayName("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);

        // Set up an answer that builds a fake pipeline whose sole purpose is to inject
        // the DTO above into a broadcast stage that pushes the topology through the
        // grpc broadcast manager we provide.
        Answer<TopologyPipeline<EntityStore, TopologyBroadcastInfo>> answer = invocation -> {
            final TopologyInfo info = invocation.getArgumentAt(0, TopologyInfo.class);
            final List<TopoBroadcastManager> broadcastManager =
                (List<TopoBroadcastManager>)invocation.getArgumentAt(1, List.class);
            final TopologyPipelineContext context =
                new TopologyPipelineContext(groupResolver, info);

            return TopologyPipeline.<EntityStore, TopologyBroadcastInfo>newBuilder(context)
                .addStage(new MockGraphStage(entityDto))
                .addStage(new BroadcastStage(Collections.singletonList(broadcastManager.get(0))))
                .build();
        };

        when(topologyPipelineFactory.liveTopology(any(TopologyInfo.class), any(),
                any(StitchingJournalFactory.class))).thenAnswer(answer);

        Iterable<Topology> topologyIter =
            () -> topologyRpcClient.broadcastAndReturnTopology(TopologyBroadcastRequest.newBuilder().build());
        final List<Topology> topologyList =
            StreamSupport.stream(topologyIter.spliterator(), false).collect(Collectors.toList());

        verify(scheduler).resetBroadcastSchedule();
        assertEquals(3, topologyList.size());
        assertTrue(topologyList.get(0).hasStart());
        assertTrue(topologyList.get(1).hasData());
        assertTrue(topologyList.get(2).hasEnd());

        assertEquals(entityDto.build(), topologyList.get(1).getData().getEntities(0));
        assertEquals(1, topologyList.get(2).getEnd().getTotalCount());
    }

    @Test
    public void testBroadcastAndReturnTopologyException() throws Exception {
        when(topologyPipelineFactory.liveTopology(any(TopologyInfo.class), any(),
                any(StitchingJournalFactory.class))).thenThrow(new RuntimeException("foo"));

        try {
            Iterable<Topology> topologyIter =
                () -> topologyRpcClient.broadcastAndReturnTopology(TopologyBroadcastRequest.newBuilder().build());
            StreamSupport.stream(topologyIter.spliterator(), false).collect(Collectors.toList());
            fail("Should have thrown exception in request.");
        } catch (StatusRuntimeException grpcException) {
            assertEquals(Status.INTERNAL.getClass(), grpcException.getStatus().getClass());
            assertEquals("foo", grpcException.getStatus().getDescription());
        }
    }

    /**
     * A pipeline stage that ignores the entity store and outputs a graph whose contents
     * contain only the entity injected via the constructor.
     */
    private static class MockGraphStage extends Stage<EntityStore, TopologyGraph> {

        private final TopologyEntityDTO.Builder entityDTO;

        public MockGraphStage(@Nonnull final TopologyEntityDTO.Builder entityDTO) {
            this.entityDTO = Objects.requireNonNull(entityDTO);
        }

        @Nonnull
        @Override
        public TopologyGraph execute(@Nonnull EntityStore entityStore)
            throws PipelineStageException, InterruptedException {
            final TopologyGraph mockGraph = mock(TopologyGraph.class);
            when(mockGraph.entities()).thenReturn(
                Stream.of(TopologyEntity.newBuilder(entityDTO).build()));

            return mockGraph;
        }
    }
}