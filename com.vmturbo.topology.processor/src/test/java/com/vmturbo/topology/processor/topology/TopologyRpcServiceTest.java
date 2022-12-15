package com.vmturbo.topology.processor.topology;

import static com.vmturbo.matrix.component.external.MatrixInterface.Component.CONSUMER_2_PROVIDER;
import static com.vmturbo.matrix.component.external.MatrixInterface.Component.OVERLAY;
import static com.vmturbo.matrix.component.external.MatrixInterface.Component.UNDERLAY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.TopologyRpcService.GrpcBroadcastManager;
import com.vmturbo.topology.processor.topology.pipeline.Stages.BroadcastStage;
import com.vmturbo.topology.processor.topology.pipeline.Stages.TopSortStage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.Stage;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.TopologyPipelineRequest;

public class TopologyRpcServiceTest {

    private static final long TIMEOUT_MS = 5;

    private TopologyServiceGrpc.TopologyServiceBlockingStub topologyRpcClient;

    private TopologyHandler topologyHandler = mock(TopologyHandler.class);

    private final TopologyPipelineExecutorService pipelineExecutorService = mock(TopologyPipelineExecutorService.class);
    private final IdentityProvider identityProvider = mock(IdentityProvider.class);
    private final long realtimeTopologyContextId = 1234567L;
    private final Clock clock = mock(Clock.class);
    private final Scheduler scheduler = mock(Scheduler.class);

    private TopologyRpcService topologyRpcServiceBackend = new TopologyRpcService(topologyHandler,
        pipelineExecutorService, identityProvider, scheduler,
        StitchingJournalFactory.emptyStitchingJournalFactory(), realtimeTopologyContextId, clock,
        TIMEOUT_MS, TimeUnit.MILLISECONDS);

    @Rule
    public GrpcTestServer server = GrpcTestServer.newServer(topologyRpcServiceBackend);

    /**
     * Public method to return a GrpcBroadcastManager constructor outside the package
     * (used in TopologyPipelineFactoryFromDiagsTest).
     * @param responseObserver to use in the constructor
     * @return a new instance of GrpcBroadcastManager
     */
    public static GrpcBroadcastManager createInstance(StreamObserver<Topology> responseObserver) {
        return new GrpcBroadcastManager(responseObserver);
    }

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

    private static @Nonnull CommonDTO.EntityIdentityData endpoint(final @Nonnull String ipAddress,
                                                                  final int port) {
        return CommonDTO.EntityIdentityData.newBuilder().setIpAddress(ipAddress).setPort(port).build();
    }

    private static @Nonnull CommonDTO.FlowDTO edge(final double flow,
                                                   final int latency,
                                                   final CommonDTO.FlowDTO.Protocol protocol,
                                                   final CommonDTO.EntityIdentityData source,
                                                   final CommonDTO.EntityIdentityData sink) {
        return edge(flow, latency, (long)(flow / 2), (long)(flow / 2), protocol, source, sink);
    }

    private static @Nonnull CommonDTO.FlowDTO edge(final double flow,
                                                   final int latency,
                                                   final long tx,
                                                   final long rx,
                                                   final CommonDTO.FlowDTO.Protocol protocol,
                                                   final CommonDTO.EntityIdentityData source,
                                                   final CommonDTO.EntityIdentityData sink) {
        return CommonDTO.FlowDTO.newBuilder().setFlowAmount((long)flow).setLatency(latency)
                                .setTransmittedAmount(tx).setReceivedAmount(rx)
                                .setProtocol(protocol).setSourceEntityIdentityData(source)
                                .setDestEntityIdentityData(sink).build();
    }

    private static @Nonnull String ipFromIndex(final @Nonnull String prefix, final int index) {
        int x = index;
        int z = x % 256;
        x -= z;
        x /= 255;
        int y = x % 256;
        int x1 = x - y;
        x1 /= 255;
        x1 %= 256;
        return prefix + "." + x1 + "." + y + "." + z;
    }

    /**
     * Constructs a list of edges.
     *
     * @param count         The count. Must be less than 255 * 255
     * @param sourceIPStart The starting source IP index (when constructing IP address, we use
     *                      that as starting number which will get increased). If {@code -1},
     *                      then use static address.
     * @param sinkPort      The sink port.
     * @param sinkIPStart   The starting sink IP index (same as source in use with exception to
     *                      the {@code -1} case).
     * @param latency       The latency.
     * @param flow          The flow.
     * @param protocol      The protocol.
     * @return The list of edges.
     */
    private static List<CommonDTO.FlowDTO> constructEdges(final int count,
                                                          final int sourceIPStart,
                                                          final int sinkPort,
                                                          final int sinkIPStart,
                                                          final int latency,
                                                          final double flow,
                                                          final CommonDTO.FlowDTO.Protocol protocol) {
        List<CommonDTO.FlowDTO> edges = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            String sourceIPAddr = "1.1.1.1";
            if (sourceIPStart > 0) {
                sourceIPAddr = ipFromIndex("1", sourceIPStart + i);
            }
            CommonDTO.EntityIdentityData source = endpoint(sourceIPAddr, 0);
            CommonDTO.EntityIdentityData sink = endpoint(ipFromIndex("2", sinkIPStart + i),
                                                         sinkPort);
            edges.add(edge(flow, latency, protocol, source, sink));
        }
        return edges;
    }

    /**
     * Tests broadcast and return of the topology.
     *
     * @throws Exception If there is any error.
     */
    @SuppressWarnings("unchecked")
    @Test
    public void testBroadcastAndReturnTopology() throws Exception {
        final GroupResolver groupResolver = mock(GroupResolver.class);
        // We will send this DTO in the broadcast.
        final TopologyEntityImpl entityDto = new TopologyEntityImpl()
            .setOid(12345L)
            .setDisplayName("foo")
            .setEntityType(EntityType.VIRTUAL_MACHINE_VALUE);
        // Matrix
        final int count = 100000;
        List<CommonDTO.FlowDTO> edges = constructEdges(count, -1, 8080, 1, 1, 10,
                                                       CommonDTO.FlowDTO.Protocol.TCP);
        final MatrixInterface matrix = TheMatrix.instance();
        matrix.update(edges);
        long oid = 101;
        // See if we have an accurate reading. The default is SITE.
        final long underlayStart = 1000000L;
        for (int i = 0; i < count; i++) {
            matrix.populateUnderlay(underlayStart + i + 1, 999L);
        }
        for (int i = 0; i < count; i++) {
            String ip = ipFromIndex("2", 1 + i);
            matrix.setEndpointOID(i + 1, ip);
        }
        for (int i = 0; i < count; i++) {
            matrix.place(i + 1, underlayStart + i + 1);
        }
        matrix.setEndpointOID(Long.MAX_VALUE, "1.1.1.1");
        matrix.place(Long.MAX_VALUE, 999L);
        // Set up an answer that builds a fake pipeline whose sole purpose is to inject
        // the DTO above into a broadcast stage that pushes the topology through the
        // grpc broadcast manager we provide.
        Answer<TopologyPipelineRequest> answer = invocation -> {
            final TopologyInfo info = invocation.getArgumentAt(0, TopologyInfo.class);
            final List<TopoBroadcastManager> broadcastManager =
                (List<TopoBroadcastManager>)invocation.getArgumentAt(1, List.class);
            final TopologyPipelineContext context =
                new TopologyPipelineContext(info);

            TopologyPipeline<EntityStore, TopologyBroadcastInfo> pipeline =
                new TopologyPipeline<>(PipelineDefinition.<EntityStore, TopologyBroadcastInfo, TopologyPipelineContext>newBuilder(context)
                        .addStage(new MockGraphStage(entityDto))
                        .addStage(new TopSortStage())
                        .finalStage(new BroadcastStage(Collections.singletonList(broadcastManager.get(0)), matrix)));
            TopologyBroadcastInfo broadcastInfo = pipeline.run(mock(EntityStore.class));
            TopologyPipelineRequest req = mock(TopologyPipelineRequest.class);
            when(req.waitForBroadcast(TIMEOUT_MS, TimeUnit.MILLISECONDS)).thenReturn(broadcastInfo);
            return req;
        };

        when(pipelineExecutorService.queueLivePipeline(any(TopologyInfo.class), any(),
                any(StitchingJournalFactory.class))).thenAnswer(answer);

        Iterable<Topology> topologyIter =
            () -> topologyRpcClient.broadcastAndReturnTopology(TopologyBroadcastRequest.newBuilder().build());
        final List<Topology> topologyList =
            StreamSupport.stream(topologyIter.spliterator(), false).collect(Collectors.toList());

        verify(scheduler).resetBroadcastSchedule();
        final int last = topologyList.size() - 1;
        assertTrue(topologyList.get(0).hasStart());
        for (int i = 1; i < last; i++) {
            assertTrue(topologyList.get(i).hasData());
        }
        assertTrue(topologyList.get(last).hasEnd());
        assertEquals(entityDto.toProto(), topologyList.get(1).getData().getEntities(0).getEntity());
        // Test the matrix transfer.
        final MatrixInterface matrixRestored = TheMatrix.newInstance();
        final MatrixInterface.Codec importer = matrixRestored.getMatrixImporter();
        MatrixInterface.Component current = null;
        for (int i = 1; i < last; i++) {
            for (Topology.DataSegment e : topologyList.get(i).getData().getEntitiesList()) {
                if (e.hasEntity()) {
                    continue;
                }
                final TopologyDTO.TopologyExtension.Matrix topoMatrix =
                    e.getExtension().getMatrix();
                if (topoMatrix.getEdgesCount() > 0) {
                    current = switchMatrixComponent(current, OVERLAY, importer);
                    topoMatrix.getEdgesList().forEach(importer::next);
                } else if (topoMatrix.getUnderlayCount() > 0) {
                    current = switchMatrixComponent(current, UNDERLAY, importer);
                    topoMatrix.getUnderlayList().forEach(importer::next);
                } else if (topoMatrix.getConsumerToProviderCount() > 0) {
                    current = switchMatrixComponent(current, CONSUMER_2_PROVIDER, importer);
                    topoMatrix.getConsumerToProviderList().forEach(importer::next);
                } else {
                    throw new IllegalStateException("Unrecognized Matrix component type.");
                }
            }
        }
        importer.finish();
        // Now test the matrix has been transmitted correctly.
        assertEquals(matrix.getVpods().size(), matrixRestored.getVpods().size());
        assertEquals(new ArrayList<>(matrix.getVpods()).get(0),
                     new ArrayList<>(matrixRestored.getVpods()).get(0));
    }

    /**
     * Switches the importer to a desired component if needed.
     * The components are:
     * <ul>
     *     <li>{@link MatrixInterface.Component#OVERLAY}
     *     <li>{@link MatrixInterface.Component#UNDERLAY}
     *     <li>{@link MatrixInterface.Component#CONSUMER_2_PROVIDER}
     * </ul>
     *
     * @param current  The current component.
     * @param desired  The desired component.
     * @param importer The importer.
     * @return The desired component.
     */
    private MatrixInterface.Component switchMatrixComponent(
        final @Nullable MatrixInterface.Component current,
        final @Nonnull MatrixInterface.Component desired,
        final @Nonnull MatrixInterface.Codec importer) {
        if (current != desired) {
            // Finish previous section if needed.
            if (current != null) {
                importer.finish();
            }
            importer.start(desired);
        }
        return desired;
    }

    @Test
    public void testBroadcastAndReturnTopologyException() throws Exception {
        TopologyPipelineRequest req = mock(TopologyPipelineRequest.class);
        when(req.waitForBroadcast(TIMEOUT_MS, TimeUnit.MILLISECONDS)).thenThrow(new RuntimeException("foo"));
        when(pipelineExecutorService.queueLivePipeline(any(TopologyInfo.class), any(),
                any(StitchingJournalFactory.class))).thenReturn(req);

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
    private static class MockGraphStage extends Stage<EntityStore, TopologyGraph<TopologyEntity>> {

        private final TopologyEntityImpl entityDTO;

        public MockGraphStage(@Nonnull final TopologyEntityImpl entityDTO) {
            this.entityDTO = Objects.requireNonNull(entityDTO);
        }

        @NotNull
        @Nonnull
        @Override
        public StageResult<TopologyGraph<TopologyEntity>> executeStage(@NotNull @Nonnull EntityStore entityStore)
            throws PipelineStageException, InterruptedException {
            final TopologyGraph<TopologyEntity> mockGraph = mock(TopologyGraph.class);
            final Stream<TopologyEntity> entityStream = Stream.of(
                    TopologyEntity.newBuilder(entityDTO).build());
            when(mockGraph.entities()).thenReturn(entityStream);
            when(mockGraph.topSort()).thenReturn(entityStream);
            when(mockGraph.topSort(any())).thenReturn(entityStream);

            return StageResult.withResult(mockGraph).andStatus(TopologyPipeline.Status.success());
        }
    }
}
