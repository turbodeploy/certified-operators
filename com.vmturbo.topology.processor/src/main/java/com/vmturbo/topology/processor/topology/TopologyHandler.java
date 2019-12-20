package com.vmturbo.topology.processor.topology;

import java.time.Clock;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineExecutorService.QueueCapacityExceededException;

/**
 * Stores topology snapshots per-target and broadcasts the results to listening components.
 */
@ThreadSafe
public class TopologyHandler {
    private final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;

    private final IdentityProvider identityProvider;

    private final Clock clock;

    private final ProbeStore probeStore;

    private final TargetStore targetStore;

    private final TopologyPipelineExecutorService pipelineExecutorService;

    private final long waitForBroadcastTimeout;

    private final TimeUnit waitForBroadcastTimeUnit;

    private final Set<String> wastedFilesProbeTypes =
        ImmutableSet.of(SDKProbeType.AZURE_STORAGE_BROWSE.getProbeType(),
            SDKProbeType.AWS.getProbeType(),
            SDKProbeType.VC_STORAGE_BROWSE.getProbeType());

    public TopologyHandler(final long realtimeTopologyContextId,
                           @Nonnull final TopologyPipelineExecutorService pipelineExecutorService,
                           @Nonnull final IdentityProvider identityProvider,
                           @Nonnull final ProbeStore probeStore,
                           @Nonnull final TargetStore targetStore,
                           @Nonnull final Clock clock,
                           final long waitForBroadcastTimeout,
                           @Nonnull final TimeUnit waitForBroadcastTimeUnit) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.pipelineExecutorService = Objects.requireNonNull(pipelineExecutorService);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.clock = clock;
        this.waitForBroadcastTimeout = waitForBroadcastTimeout;
        this.waitForBroadcastTimeUnit = waitForBroadcastTimeUnit;
    }

    /**
     * Broadcast the current topology to other services.
     *
     * @param journalFactory The journal factory to be used to create a journal to track changes made
     *                       during stitching.
     * @return The count of the total number of entities broadcast.
     * @throws TopologyPipelineException If there is an error broadcasting the topology.
     * @throws InterruptedException If the broadcast is interrupted.
     */
    public TopologyBroadcastInfo broadcastLatestTopology(
        @Nonnull final StitchingJournalFactory journalFactory)
            throws TopologyPipelineException, InterruptedException {

        final TopologyInfo.Builder tinfo = TopologyInfo.newBuilder()
            .setTopologyType(TopologyType.REALTIME)
            .setTopologyId(identityProvider.generateTopologyId())
            .setTopologyContextId(realtimeTopologyContextId)
            .setCreationTime(clock.millis())
            .addAnalysisType(AnalysisType.MARKET_ANALYSIS)
            .addAnalysisType(AnalysisType.BUY_RI_IMPACT_ANALYSIS);
        if (includesWastedFiles()) {
            tinfo.addAnalysisType(AnalysisType.WASTED_FILES);
        }

        try {
            return pipelineExecutorService.queueLivePipeline(tinfo.build(), Collections.emptyList(), journalFactory)
                .waitForBroadcast(waitForBroadcastTimeout, waitForBroadcastTimeUnit);
        } catch (TimeoutException e) {
            logger.error("Live topology pipeline timed out after {} {}",
                waitForBroadcastTimeout, waitForBroadcastTimeUnit);
            throw new TopologyPipelineException("Timed out waiting for pipeline to finish", e);
        } catch (QueueCapacityExceededException e) {
            // This shouldn't happen in a live topology: we collapse requests with the same
            // context, so the realtime pipeline queue should never grow beyond one.
            logger.error("Failed to queue live pipeline due to error.", e);
            throw new TopologyPipelineException("Failed to enqueue pipeline", e);
        }
    }

    /**
     * If any targets are from probe types that contains wasted file information, return true.
     *
     * @return true if any targets may contain wasted file information, false otherwise.
     */
    public boolean includesWastedFiles() {
        return targetStore.getAll().stream()
            .map(Target::getProbeId)
            .map(id -> probeStore.getProbe(id))
            .filter(Optional::isPresent)
            .map(Optional::get)
            .map(ProbeInfo::getProbeType)
            .anyMatch(str -> wastedFilesProbeTypes.contains(str));
    }
}
