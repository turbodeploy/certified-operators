package com.vmturbo.topology.processor.topology;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.Data;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.End;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology.Start;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastRequest;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyBroadcastResponse;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceImplBase;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.chunking.MessageChunker;
import com.vmturbo.topology.processor.api.server.TopoBroadcastManager;
import com.vmturbo.topology.processor.api.server.TopologyBroadcast;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;

/**
 * Implementation of the TopologyService defined in topology/TopologyDTO.proto.
 */
public class TopologyRpcService extends TopologyServiceImplBase {
    private static final Logger logger = LogManager.getLogger();

    private final TopologyHandler topologyHandler;
    private final TopologyPipelineExecutorService pipelineExecutorService;
    private final StitchingJournalFactory journalFactory;
    private final IdentityProvider identityProvider;
    private final long realtimeTopologyContextId;
    private final Clock clock;
    private final Scheduler scheduler;
    private final long waitForBroadcastTimeout;
    private final TimeUnit waitForBroadcastTimeUnit;

    public TopologyRpcService(@Nonnull final TopologyHandler topologyHandler,
                              @Nonnull final TopologyPipelineExecutorService pipelineExecutorService,
                              @Nonnull final IdentityProvider identityProvider,
                              @Nonnull final Scheduler scheduler,
                              @Nonnull final StitchingJournalFactory journalFactory,
                              final long realtimeTopologyContextId,
                              @Nonnull final Clock clock,
                              final long waitForBroadcastTimeout,
                              @Nonnull final TimeUnit waitForBroadcastTimeUnit) {
        this.topologyHandler = Objects.requireNonNull(topologyHandler);
        this.pipelineExecutorService = Objects.requireNonNull(pipelineExecutorService);
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.journalFactory = Objects.requireNonNull(journalFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.clock = Objects.requireNonNull(clock);
        this.waitForBroadcastTimeout = waitForBroadcastTimeout;
        this.waitForBroadcastTimeUnit = waitForBroadcastTimeUnit;
    }

    @Override
    public void requestTopologyBroadcast(TopologyBroadcastRequest request,
                                         StreamObserver<TopologyBroadcastResponse> responseObserver) {
        try {
            scheduler.resetBroadcastSchedule();
            topologyHandler.broadcastLatestTopology(journalFactory);
            responseObserver.onNext(TopologyBroadcastResponse.newBuilder()
                                                             .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Unable to broadcast topology due to error: ", e);
            responseObserver.onError(Status.INTERNAL
                                         .withDescription(e.getMessage())
                                         .withCause(e)
                                         .asException()
                                    );
        }
    }

    @Override
    public void broadcastAndReturnTopology(TopologyBroadcastRequest request,
                                           StreamObserver<Topology> responseObserver) {
        try {
            final TopologyInfo.Builder topologyInfo =
                    TopologyInfo.newBuilder()
                            .setTopologyType(TopologyType.REALTIME)
                            .setTopologyId(identityProvider.generateTopologyId())
                            .setTopologyContextId(realtimeTopologyContextId)
                            .setCreationTime(clock.millis())
                            .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
                            .addAnalysisType(AnalysisType.BUY_RI_IMPACT_ANALYSIS);

            if (topologyHandler.includesWastedFiles()) {
                topologyInfo.addAnalysisType(AnalysisType.WASTED_FILES);
            }

            // Because this RPC triggers a broadcast, be sure to reset the broadcast schedule
            // so that we don't send too many in close succession.
            scheduler.resetBroadcastSchedule();
            pipelineExecutorService.queueLivePipeline(topologyInfo.build(),
                              Collections.singletonList(new GrpcBroadcastManager(responseObserver)),
                              journalFactory)
                .waitForBroadcast(waitForBroadcastTimeout, waitForBroadcastTimeUnit);
        } catch (Exception e) {
            logger.error("Unable to get broadcast topology due to error: ", e);
            responseObserver.onError(Status.INTERNAL
                                         .withDescription(e.getMessage())
                                         .withCause(e)
                                         .asException()
                                    );
        }
    }

    /**
     * A simple implementation of the {@link TopoBroadcastManager} interface for sending
     * topologies in chunks over gRPC.
     */
    static class GrpcBroadcastManager implements TopoBroadcastManager {

        private final StreamObserver<Topology> responseObserver;

        GrpcBroadcastManager(@Nonnull final StreamObserver<Topology> responseObserver) {
            this.responseObserver = Objects.requireNonNull(responseObserver);
        }

        @Nonnull
        @Override
        public TopologyBroadcast broadcastLiveTopology(@Nonnull TopologyInfo topologyInfo) {
            return new GrpcBroadcastImpl(responseObserver, topologyInfo);
        }

        @Nonnull
        @Override
        public TopologyBroadcast broadcastUserPlanTopology(@Nonnull TopologyInfo topologyInfo) {
            return new GrpcBroadcastImpl(responseObserver, topologyInfo);
        }

        @Nonnull
        @Override
        public TopologyBroadcast broadcastScheduledPlanTopology(
            @Nonnull TopologyInfo topologyInfo) {
            return new GrpcBroadcastImpl(responseObserver, topologyInfo);
        }
    }

    /**
     * A simple implementation of the {@link TopologyBroadcast} interface for sending topologies
     * over
     * gRPC.
     */
    @NotThreadSafe
    static class GrpcBroadcastImpl implements TopologyBroadcast {

        private final TopologyInfo topologyInfo;
        private final StreamObserver<Topology> responseObserver;
        private final Collection<TopologyEntityDTO> chunk;
        private final Collection<TopologyDTO.TopologyExtension> extensionChunk;

        private long entitiesSent;

        GrpcBroadcastImpl(@Nonnull final StreamObserver<Topology> responseObserver,
                                 @Nonnull final TopologyInfo topologyInfo) {
            this.responseObserver = Objects.requireNonNull(responseObserver);
            this.topologyInfo = Objects.requireNonNull(topologyInfo);
            this.chunk = new ArrayList<>(MessageChunker.CHUNK_SIZE);
            this.extensionChunk = new ArrayList<>(MessageChunker.CHUNK_SIZE);
            entitiesSent = 0;

            // Send the first message for the start of the topology.
            final Topology initialMessage = Topology.newBuilder()
                                                    .setTopologyId(getTopologyId())
                                                    .setStart(Start.newBuilder()
                                                                   .setTopologyInfo(topologyInfo))
                                                    .build();
            responseObserver.onNext(initialMessage);
        }

        @Override
        public void append(@Nonnull TopologyEntityDTO entity)
            throws CommunicationException, InterruptedException {
            chunk.add(entity);
            if (chunk.size() >= MessageChunker.CHUNK_SIZE) {
                sendChunk();
            }
        }

        /**
         * Appends the next topology extension entity to the notification.
         * This call may block until the next chunk is sent.
         *
         * @param extension to add to broadcast.
         * @throws InterruptedException   if thread has been interrupted
         * @throws NullPointerException   if {@code entity} is {@code null}
         * @throws IllegalStateException  if {@link #finish()} has been already called
         * @throws CommunicationException persistent communication exception
         */
        @Override
        public void appendExtension(@Nonnull TopologyDTO.TopologyExtension extension)
            throws CommunicationException, InterruptedException {
            // Send the entities first.
            if (chunk.size() > 0) {
                sendChunk();
            }
            extensionChunk.add(extension);
            if (extensionChunk.size() >= MessageChunker.CHUNK_SIZE) {
                sendExtensionChunk();
            }
        }

        @Override
        public long finish() throws CommunicationException, InterruptedException {
            sendChunk();
            sendExtensionChunk();

            final Topology endMessage = Topology.newBuilder()
                                                .setTopologyId(getTopologyId())
                                                .setEnd(
                                                    End.newBuilder().setTotalCount(entitiesSent))
                                                .build();
            responseObserver.onNext(endMessage);
            responseObserver.onCompleted();

            return entitiesSent;
        }

        @Override
        public long getTopologyId() {
            return topologyInfo.getTopologyId();
        }

        @Override
        public long getTopologyContextId() {
            return topologyInfo.getTopologyContextId();
        }

        @Override
        public TopologyType getTopologyType() {
            return topologyInfo.getTopologyType();
        }

        @Override
        public long getCreationTime() {
            return topologyInfo.getCreationTime();
        }

        private void sendChunk() throws CommunicationException, InterruptedException {
            if (chunk.size() <= 0) {
                return;
            }
            Collection<Topology.DataSegment> segments = chunk.stream().map(dto -> {
                return Topology.DataSegment.newBuilder().setEntity(dto).build();
            }).collect(Collectors.toList());
            final Topology message = Topology.newBuilder()
                                             .setData(Data.newBuilder().addAllEntities(segments))
                                             .setTopologyId(getTopologyId())
                                             .build();
            responseObserver.onNext(message);
            entitiesSent += chunk.size();
            chunk.clear();
        }

        private void sendExtensionChunk() throws CommunicationException, InterruptedException {
            if (extensionChunk.size() <= 0) {
                return;
            }
            Collection<Topology.DataSegment> segments = extensionChunk.stream().map(ext -> {
                return Topology.DataSegment.newBuilder().setExtension(ext).build();
            }).collect(Collectors.toList());
            final Topology message = Topology.newBuilder()
                                             .setData(Data.newBuilder().addAllEntities(segments))
                                             .setTopologyId(getTopologyId())
                                             .build();
            responseObserver.onNext(message);
            entitiesSent += extensionChunk.size();
            chunk.clear();
        }
    }
}
