package com.vmturbo.market.runner;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.market.component.api.MarketNotificationSender;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.priceindex.api.PriceIndexNotificationSender;
import com.vmturbo.proactivesupport.DataMetricHistogram;

/**
 * Orchestrate running of real-time (i.e. live market) and non-real-time analysis runs.
 *
 */
public class MarketRunner {

    private final Logger logger = LogManager.getLogger();
    private final ExecutorService runnerThreadPool;
    private final MarketNotificationSender serverApi;
    private final PriceIndexNotificationSender priceIndexApi;

    private final Map<Long, Analysis> analysisMap = Maps.newConcurrentMap();

    private static final DataMetricHistogram INPUT_TOPOLOGY = DataMetricHistogram.builder()
            .withName("mkt_input_topology")
            .withHelp("Size of the topology to analyze.")
            .withBuckets(1000, 10000, 30000, 50000, 75000, 100000, 150000, 200000)
            .build()
            .register();

    public MarketRunner(
            ExecutorService runnerThreadPool,
            MarketNotificationSender serverApi,
            PriceIndexNotificationSender priceIndexApi) {
        this.runnerThreadPool = runnerThreadPool;
        this.serverApi = serverApi;
        this.priceIndexApi = priceIndexApi;
    }

    @Nonnull
    public Analysis scheduleAnalysis(@Nonnull final TopologyDTO.TopologyInfo topologyInfo,
                                     @Nonnull final Set<TopologyEntityDTO> topologyDTOs,
                                     final boolean includeVDC) {
        INPUT_TOPOLOGY.observe((double)topologyDTOs.size());
        final Analysis analysis;
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();
        synchronized (analysisMap) {
            if (analysisMap.containsKey(topologyContextId)) {
                logger.info("Analysis " + topologyContextId + " is already running with topology " + topologyId
                        + ". Discarding.");
                return analysisMap.get(topologyContextId);
            }

            logger.info("Received analysis " + topologyContextId + ": topology " + topologyId + " with " + topologyDTOs.size()
                + " topology DTOs from TopologyProcessor");
            analysis = new Analysis(topologyInfo, topologyDTOs, includeVDC);
            analysisMap.put(topologyContextId, analysis);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(topologyDTOs.size() + " Topology DTOs");
            topologyDTOs.stream().map(dto -> "Topology DTO: " + dto).forEach(logger::debug);
        }
        logger.info("Queueing analysis " + topologyContextId + " for execution");
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
            serverApi.notifyActionsRecommended(analysis.getActionPlan().get());
            serverApi.notifyProjectedTopology(analysis.getTopologyInfo(),
                    analysis.getProjectedTopologyId().get(),
                    analysis.getProjectedTopology().get());
            final PriceIndexMessage pim =
                PriceIndexMessage.newBuilder(analysis.getPriceIndexMessage().get())
                    .setTopologyContextId(analysis.getContextId())
                    .build();
            priceIndexApi.sendPriceIndex(analysis.getTopologyInfo(), pim);
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