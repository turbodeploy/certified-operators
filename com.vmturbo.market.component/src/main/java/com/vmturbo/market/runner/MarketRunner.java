package com.vmturbo.market.runner;

import java.time.Clock;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Maps;

import io.grpc.StatusRuntimeException;

import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.commons.analysis.AnalysisUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.rpc.MarketDebugRpcService;
import com.vmturbo.market.runner.Analysis.AnalysisFactory;
import com.vmturbo.market.runner.Analysis.AnalysisState;
import com.vmturbo.proactivesupport.DataMetricHistogram;

/**
 * Orchestrate running of real-time (i.e. live market) and non-real-time analysis runs.
 *
 */
public class MarketRunner {

    private final Logger logger = LogManager.getLogger();
    private final ExecutorService runnerThreadPool;
    private final MarketNotificationSender serverApi;
    private final SettingServiceBlockingStub settingServiceClient;
    private final AnalysisFactory analysisFactory;
    private float alleviatePressureQuoteFactor;

    private final Optional<MarketDebugRpcService> marketDebugRpcService;

    private final Map<Long, Analysis> analysisMap = Maps.newConcurrentMap();

    private static final DataMetricHistogram INPUT_TOPOLOGY = DataMetricHistogram.builder()
            .withName("mkt_input_topology")
            .withHelp("Size of the topology to analyze.")
            .withBuckets(1000, 10000, 30000, 50000, 75000, 100000, 150000, 200000)
            .build()
            .register();

    public MarketRunner(@Nonnull final ExecutorService runnerThreadPool,
            @Nonnull final MarketNotificationSender serverApi,
            @Nonnull final AnalysisFactory analysisFactory,
            @Nonnull final SettingServiceBlockingStub settingServiceClient,
            @Nonnull final Optional<MarketDebugRpcService> marketDebugRpcService,
            float alleviatePressureQuoteFactor) {
        this.runnerThreadPool = runnerThreadPool;
        this.serverApi = serverApi;
        this.settingServiceClient = settingServiceClient;
        this.marketDebugRpcService = marketDebugRpcService;
        this.analysisFactory = analysisFactory;
        this.alleviatePressureQuoteFactor = alleviatePressureQuoteFactor;
    }

    /**
     * Schedule a call to the Analysis methods on the given topology scoped to the given SE OIDs.
     *
     * @param topologyInfo describes this topology, including contextId, id, etc
     * @param topologyDTOs the TopologyEntityDTOs in this topology
     * @param includeVDC should VDC's be included in the analysis
     * @param maxPlacementsOverride If present, overrides the default number of placement rounds performed
     *                              by the market during analysis.
     * @param rightsizeLowerWatermark the minimum utilization threshold, if entity utilization is below
     *                                it, Market could generate resize down actions.
     * @param rightsizeUpperWatermark the maximum utilization threshold, if entity utilization is above
     *                                it, Market could generate resize up actions.
     * @return the resulting Analysis object capturing the results
     */
    @Nonnull
    public Analysis scheduleAnalysis(@Nonnull final TopologyDTO.TopologyInfo topologyInfo,
                                     @Nonnull final Set<TopologyEntityDTO> topologyDTOs,
                                     final boolean includeVDC,
                                     @Nonnull final Optional<Integer> maxPlacementsOverride,
                                     final float rightsizeLowerWatermark,
                                     final float rightsizeUpperWatermark) {

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
                    .setSettingsMap(retrieveSettings())
                    .setClock(Clock.systemUTC())
                    .setMaxPlacementsOverride(maxPlacementsOverride)
                    .setRightsizeLowerWatermark(rightsizeLowerWatermark)
                    .setRightsizeUpperWatermark(rightsizeUpperWatermark)
                    .setQuoteFactor(getQuoteFactor(topologyInfo))
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
        if (analysis.isDone()) {
            marketDebugRpcService.ifPresent(debugService -> {
                debugService.recordCompletedAnalysis(analysis);
            });

            if (analysis.getState() == AnalysisState.SUCCEEDED) {
                try {
                    // if this was a plan topology, broadcast the plan analysis topology
                    if (analysis.getTopologyInfo().hasPlanInfo()) {
                        serverApi.notifyPlanAnalysisTopology(analysis.getTopologyInfo(),
                                analysis.getTopology().values());
                    }
                    // Send projected topology before recommended actions, because some recommended
                    // actions will have OIDs that are only present in the projected topology, and we
                    // want to minimize the risk of the projected topology being unavailable.
                    serverApi.notifyProjectedTopology(analysis.getTopologyInfo(),
                            analysis.getProjectedTopologyId().get(),
                            analysis.getSkippedEntities(),
                            analysis.getProjectedTopology().get());
                    serverApi.notifyActionsRecommended(analysis.getActionPlan().get());
                } catch (CommunicationException | InterruptedException e) {
                    // TODO we need to decide, whether to commit the incoming topology here or not.
                    logger.error("Could not send market notifications", e);
                }
            }
        }
    }

    /**
     * Get all the analysis runs.
     * @return a collection of the analysis runs
     */
    public Collection<Analysis> getRuns() {
        return analysisMap.values();
    }

    private float getQuoteFactor(TopologyInfo topologyInfo) {
        if (topologyInfo.hasPlanInfo()) {
            if (topologyInfo.getPlanInfo().getPlanType().equals("ALLEVIATE_PRESSURE")) {
                return alleviatePressureQuoteFactor;
            }
        }
        return AnalysisUtil.QUOTE_FACTOR;
    }

    /**
     * Retrieve global settings used for analysis configuration.
     *
     * @return The map of setting values, arranged by name.
     */
    private Map<String, Setting> retrieveSettings() {

        final Map<String, Setting> settingsMap = new HashMap<>();

        // for now only interested in one global settings: RateOfResize
        final GetSingleGlobalSettingRequest settingRequest =
                GetSingleGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(GlobalSettingSpecs.RateOfResize.getSettingName())
                        .build();

        try {
            final GetGlobalSettingResponse response =
                    settingServiceClient.getGlobalSetting(settingRequest);
            if (response.hasSetting()) {
                settingsMap.put(response.getSetting().getSettingSpecName(), response.getSetting());
            }
        } catch (StatusRuntimeException e) {
            logger.error("Failed to get global settings from group component. Will run analysis " +
                    " without global settings.", e);
        }
        return settingsMap;
    }
}
