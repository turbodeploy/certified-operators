package com.vmturbo.market.runner;

import java.time.Clock;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.runner.Analysis.AnalysisFactory;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.proactivesupport.DataMetricHistogram;

/**
 * Orchestrate running of real-time (i.e. live market) and non-real-time analysis runs.
 *
 */
public class MarketRunner {

    private final Logger logger = LogManager.getLogger();
    private final ExecutorService runnerThreadPool;
    private final MarketNotificationSender serverApi;
    private final AnalysisFactory analysisFactory;

    private final Map<Long, Analysis> analysisMap = Maps.newConcurrentMap();

    private static final DataMetricHistogram INPUT_TOPOLOGY = DataMetricHistogram.builder()
            .withName("mkt_input_topology")
            .withHelp("Size of the topology to analyze.")
            .withBuckets(1000, 10000, 30000, 50000, 75000, 100000, 150000, 200000)
            .build()
            .register();

    public MarketRunner(
            @Nonnull final ExecutorService runnerThreadPool,
            @Nonnull final MarketNotificationSender serverApi,
            @Nonnull final AnalysisFactory analysisFactory) {
        this.runnerThreadPool = runnerThreadPool;
        this.serverApi = serverApi;
        this.analysisFactory = analysisFactory;
    }

    /**
     * Schedule a call to the Analysis methods on the given topology scoped to the given SE OIDs.
     *
     * @param topologyInfo describes this topology, including contextId, id, etc
     * @param topologyDTOs the TopologyEntityDTOs in this topology
     * @param includeVDC should VDC's be included in the analysis
     * @param settingServiceClient Client for getting the Settings from Setting Service
     * @param maxPlacementsOverride If present, overrides the default number of placement rounds performed
     *                              by the market during analysis.
     * @return the resulting Analysis object capturing the results
     */
    @Nonnull
    public Analysis scheduleAnalysis(@Nonnull final TopologyDTO.TopologyInfo topologyInfo,
                                     @Nonnull final Set<TopologyEntityDTO> topologyDTOs,
                                     final boolean includeVDC,
                                     @Nonnull SettingServiceBlockingStub settingServiceClient,
                                     @Nonnull final Optional<Integer> maxPlacementsOverride) {

        INPUT_TOPOLOGY.observe((double)topologyDTOs.size());
        final Analysis analysis;
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();

        synchronized (analysisMap) {
            if (analysisMap.containsKey(topologyContextId)) {
                logger.info("Analysis {} is already running with topology {}. Discarding.",
                        topologyContextId, topologyId);
                return analysisMap.get(topologyContextId);
            }
            logger.info("Received analysis {}: topology {}" +
                    " with {} topology DTOs from TopologyProcessor",
                    topologyContextId, topologyId, topologyDTOs.size());
            analysis = analysisFactory.newAnalysisBuilder()
                    .setTopologyInfo(topologyInfo)
                    .setTopologyDTOs(topologyDTOs)
                    .setIncludeVDC(includeVDC)
                    .setSettingsServiceClient(settingServiceClient)
                    .setClock(Clock.systemUTC())
                    .setMaxPlacementsOverride(maxPlacementsOverride)
                    .build();
            analysisMap.put(topologyContextId, analysis);
        }
        if (logger.isTraceEnabled()) {
            logger.trace("{} Topology DTOs", topologyDTOs.size());
            topologyDTOs.stream().map(dto -> "Topology DTO: " + dto).forEach(logger::trace);
        }
        logger.info("Queueing analysis {} for execution", topologyContextId);
        analysis.queued();
        runnerThreadPool.execute(() -> runAnalysis(analysis));
        return analysis;
    }

    /**
     * Run the analysis, when done - remove it from the map of runs and notify listeners.
     * @param analysis the object on which to run the analysis.
     */
    private void runAnalysis(@Nonnull final Analysis analysis) {
        analysis.execute();
        analysisMap.remove(analysis.getContextId());
        if (analysis.getState() == AnalysisState.SUCCEEDED) {
            try {
                // if this was a plan topology, broadcast the plan analysis topology
                if (analysis.getTopologyInfo().hasPlanInfo()) {
                    Set<TopologyEntityDTO> planAnalysisTopology = analysis.getScopedTopology().isEmpty()
                            ? analysis.getTopology()
                            : analysis.getScopedTopology();
                    serverApi.notifyPlanAnalysisTopology(analysis.getTopologyInfo(), planAnalysisTopology);
                }
                // Send projected topology before recommended actions, because some recommended
                // actions will have OIDs that are only present in the projected topology, and we
                // want to minimize the risk of the projected topology being unavailable.
                serverApi.notifyProjectedTopology(analysis.getTopologyInfo(),
                        analysis.getProjectedTopologyId().get(),
                        analysis.getProjectedTopology().get());
                serverApi.notifyActionsRecommended(analysis.getActionPlan().get());
                final PriceIndexMessage pim =
                    PriceIndexMessage.newBuilder(analysis.getPriceIndexMessage().get())
                        .setTopologyContextId(analysis.getContextId())
                        .build();
                serverApi.sendPriceIndex(analysis.getTopologyInfo(), pim);
            } catch (CommunicationException | InterruptedException e) {
                // TODO we need to decide, whether to commit the incoming topology here or not.
                logger.error("Could not send market notifications", e);
            }
        }
    }

    /**
     * Get all the analysis runs.
     * @return a collection of the analysis runs
     */
    public  Collection<Analysis> getRuns() {
        return analysisMap.values();
    }
}
