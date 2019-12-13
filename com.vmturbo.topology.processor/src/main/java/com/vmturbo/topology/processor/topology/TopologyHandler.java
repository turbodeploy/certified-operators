package com.vmturbo.topology.processor.topology;

import java.time.Clock;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.common.protobuf.topology.TopologyDTO.AnalysisType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;
import com.vmturbo.topology.processor.entity.EntityStore;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.topology.pipeline.LivePipelineFactory;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.TopologyPipelineException;

/**
 * Stores topology snapshots per-target and broadcasts the results to listening components.
 */
@ThreadSafe
public class TopologyHandler {
    private final Logger logger = LogManager.getLogger();

    private final long realtimeTopologyContextId;

    private final IdentityProvider identityProvider;

    private final Clock clock;

    private final EntityStore entityStore;

    private final ProbeStore probeStore;

    private final TargetStore targetStore;

    private LivePipelineFactory livePipelineFactory;

    private final Set<String> wastedFilesProbeTypes =
        ImmutableSet.of(SDKProbeType.AZURE_STORAGE_BROWSE.getProbeType(),
            SDKProbeType.AWS.getProbeType(),
            SDKProbeType.VC_STORAGE_BROWSE.getProbeType());

    public TopologyHandler(final long realtimeTopologyContextId,
                           @Nonnull final LivePipelineFactory livePipelineFactory,
                           @Nonnull final IdentityProvider identityProvider,
                           @Nonnull final EntityStore entityStore,
                           @Nonnull final ProbeStore probeStore,
                           @Nonnull final TargetStore targetStore,
                           @Nonnull final Clock clock) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.identityProvider = Objects.requireNonNull(identityProvider);
        this.livePipelineFactory = Objects.requireNonNull(livePipelineFactory);
        this.entityStore = Objects.requireNonNull(entityStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.targetStore = Objects.requireNonNull(targetStore);
        this.clock = clock;
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
    public synchronized TopologyBroadcastInfo broadcastLatestTopology(
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

        return livePipelineFactory.liveTopology(tinfo.build(), Collections.emptyList(), journalFactory)
                .run(entityStore);
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
