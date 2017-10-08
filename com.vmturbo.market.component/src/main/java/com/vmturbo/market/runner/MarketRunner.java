package com.vmturbo.market.runner;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.market.PriceIndexNotificationSender;
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

    public Analysis scheduleAnalysis(long topologyContextId, long topologyId, long creationTime,
                    Set<TopologyEntityDTO> topologyDTOs, TopologyType topologyType,
                    boolean includeVDC) {
        INPUT_TOPOLOGY.observe((double)topologyDTOs.size());
        Analysis analysis;
        synchronized (analysisMap) {
            if (analysisMap.containsKey(topologyContextId)) {
                logger.info("Analysis " + topologyContextId + " is already running with topology " + topologyId
                        + ". Discarding.");
                return analysisMap.get(topologyContextId);
            }

            logger.info("Received analysis " + topologyContextId + ": topology " + topologyId + " with " + topologyDTOs.size()
                + " topology DTOs from TopologyProcessor");
            analysis = new Analysis(topologyContextId, topologyId, topologyDTOs, topologyType, includeVDC);
            analysisMap.put(topologyContextId, analysis);
        }
        if (logger.isDebugEnabled()) {
            logger.debug(topologyDTOs.size() + " Topology DTOs");
            topologyDTOs.stream().map(dto -> "Topology DTO: " + dto).forEach(logger::debug);
        }
        logger.info("Queueing analysis " + topologyContextId + " for execution");
        analysis.queued();
        runnerThreadPool.execute(() -> runAnalysis(analysis, creationTime));
        return analysis;
    }

    /**
     * Run the analysis, when done - remove it from the map of runs and notify listeners.
     * @param analysis the object on which to run the analysis.
     * @param creationTime the time of topology created, it will be part of price index message object.
     */
    private void runAnalysis(Analysis analysis, long creationTime) {
        analysis.execute();
        analysisMap.remove(analysis.getContextId());
        if (analysis.getState() == AnalysisState.SUCCEEDED) {
            serverApi.notifyActionsRecommended(analysis.getActionPlan().get());
            serverApi.notifyProjectedTopology(analysis.getTopologyId(), IdentityGenerator.next(),
                    analysis.getContextId(), analysis.getTopologyType(), creationTime,
                    analysis.getProjectedTopology().get());
            PriceIndexMessage pim = PriceIndexMessage.newBuilder(analysis.getPriceIndexMessage().get())
                            .setTopologyContextId(analysis.getContextId())
                            .build();
            priceIndexApi.sendPriceIndex(analysis.getTopologyId(), creationTime, pim);
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