package com.vmturbo.market.runner;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.MarketActionPlanInfo;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.rpc.MarketDebugRpcService;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.proactivesupport.DataMetricHistogram;
import com.vmturbo.topology.processor.api.util.TopologyProcessingGate;
import com.vmturbo.topology.processor.api.util.TopologyProcessingGate.Ticket;

/**
 * Orchestrate running of real-time (i.e. live market) and non-real-time analysis runs.
 *
 */
public class MarketRunner {

    private final Logger logger = LogManager.getLogger();
    private final ExecutorService runnerThreadPool;
    private final MarketNotificationSender serverApi;
    private final AnalysisFactory analysisFactory;
    // This variable keeps track of actions to replay from last round of analysis.
    // For real time analysis, this is passed to Analysis object created in this class.
    private @Nonnull ReplayActions realtimeReplayActions = new ReplayActions();

    private final Optional<MarketDebugRpcService> marketDebugRpcService;

    private final Map<Long, Analysis> analysisMap = Maps.newConcurrentMap();

    private final TopologyProcessingGate topologyProcessingGate;

    private final Set<Long> analysisStoppageSet = Sets.newConcurrentHashSet();

    private static final DataMetricHistogram INPUT_TOPOLOGY = DataMetricHistogram.builder()
            .withName("mkt_input_topology")
            .withHelp("Size of the topology to analyze.")
            .withBuckets(1000, 10000, 30000, 50000, 75000, 100000, 150000, 200000)
            .build()
            .register();

    public MarketRunner(@Nonnull final ExecutorService runnerThreadPool,
                        @Nonnull final MarketNotificationSender serverApi,
                        @Nonnull final AnalysisFactory analysisFactory,
                        @Nonnull final Optional<MarketDebugRpcService> marketDebugRpcService,
                        final TopologyProcessingGate topologyProcessingGate) {
        this.runnerThreadPool = Objects.requireNonNull(runnerThreadPool);
        this.serverApi = Objects.requireNonNull(serverApi);
        this.marketDebugRpcService = Objects.requireNonNull(marketDebugRpcService);
        this.analysisFactory = Objects.requireNonNull(analysisFactory);
        this.topologyProcessingGate = Objects.requireNonNull(topologyProcessingGate);
    }

    /**
     * Schedule a call to the Analysis methods on the given topology scoped to the given SE OIDs.
     *
     * @param topologyInfo describes this topology, including contextId, id, etc
     * @param topologyDTOs the TopologyEntityDTOs in this topology
     * @param includeVDC should VDC's be included in the analysis
     * @param maxPlacementsOverride If present, overrides the default number of placement rounds performed
     *                              by the market during analysis.
     * @param useQuoteCacheDuringSNM Whether quotes should be cached for reuse during SNM-enabled
     *                               placement analysis. Setting to true can improve performance in
     *                               some cases. Usually those cases involve a high number of
     *                               biclique overlaps and volumes per VM.
     * @param replayProvisionsForRealTime Whether provision and activate actions from one real-time
     *                                    analysis cycle should be replayed during the next.
     * @param rightsizeLowerWatermark the minimum utilization threshold, if entity utilization is below
     *                                it, Market could generate resize down actions.
     * @param rightsizeUpperWatermark the maximum utilization threshold, if entity utilization is above
     *                                it, Market could generate resize up actions.
     * @param discountedComputeCostFactor The maximum ratio of the on-demand cost of new template
     *                                    to current template that is allowed for analysis engine
     *                                    to recommend resize up to utilize a RI.
     * @return the resulting Analysis object capturing the results
     */
    @Nonnull
    public Analysis scheduleAnalysis(@Nonnull final TopologyDTO.TopologyInfo topologyInfo,
                                     @Nonnull final Set<TopologyEntityDTO> topologyDTOs,
                                     final boolean includeVDC,
                                     @Nonnull final Optional<Integer> maxPlacementsOverride,
                                     final boolean useQuoteCacheDuringSNM,
                                     final boolean replayProvisionsForRealTime,
                                     final float rightsizeLowerWatermark,
                                     final float rightsizeUpperWatermark,
                                     final float discountedComputeCostFactor) {

        INPUT_TOPOLOGY.observe((double)topologyDTOs.size());
        final Analysis analysis;
        final long topologyContextId = topologyInfo.getTopologyContextId();
        final long topologyId = topologyInfo.getTopologyId();

        synchronized (analysisMap) {
            if (analysisMap.containsKey(topologyContextId)) {
                analysis = analysisMap.get(topologyContextId);
                logger.info("Analysis {} is already running with topology {}. Discarding {}.",
                        topologyContextId, analysis.getTopologyId(), topologyId);
                TheMatrix.clearInstance(topologyId);
                return analysis;
            }
            logger.info("Received analysis {}: topology {}" +
                    " with {} topology DTOs from TopologyProcessor",
                    topologyContextId, topologyId, topologyDTOs.size());
            analysis = analysisFactory.newAnalysis(topologyInfo,
                    topologyDTOs,
                    configBuilder -> configBuilder
                        .setIncludeVDC(includeVDC)
                        .setMaxPlacementsOverride(maxPlacementsOverride)
                        .setUseQuoteCacheDuringSNM(useQuoteCacheDuringSNM)
                        .setReplayProvisionsForRealTime(replayProvisionsForRealTime)
                        .setRightsizeLowerWatermark(rightsizeLowerWatermark)
                        .setRightsizeUpperWatermark(rightsizeUpperWatermark)
                        .setDiscountedComputeCostFactor(discountedComputeCostFactor));

            if (!analysis.getTopologyInfo().hasPlanInfo()) {
                Optional<Setting> disbaleAllActionsSetting = analysis.getConfig()
                                .getGlobalSetting(GlobalSettingSpecs.DisableAllActions);
                if (disbaleAllActionsSetting.isPresent()
                        && disbaleAllActionsSetting.get().getBooleanSettingValue().getValue()) {
                    logger.info("Disabling analysis execution because all actions are disabled from settings.");
                    try {
                        // Notify to clear current actions.
                        serverApi.notifyActionsRecommended(getEmptyActionPlan(topologyInfo));
                    } catch (CommunicationException | InterruptedException e) {
                        logger.error("Could not send market notifications", e);
                    } finally {
                        TheMatrix.clearInstance(analysis.getTopologyId());
                    }
                    return analysis;
                }
            }

            analysisMap.put(topologyContextId, analysis);
            // stop analysis for topology if the entry is in analysisStoppageMap for this topoId
            // clean analysisStoppageMap since we have prevented analysis
            if (analysisStoppageSet.remove(topologyContextId)) {
                logger.info("Analysis {} has been stopped for topology {} by user. Discarding.",
                        topologyContextId, topologyId);
                TheMatrix.clearInstance(analysis.getTopologyId());
                analysisMap.remove(topologyContextId);
                return analysis;
            }
        }
        if (logger.isTraceEnabled()) {
            logger.trace("{} Topology DTOs", topologyDTOs.size());
            topologyDTOs.stream().map(dto -> "Topology DTO: " + dto).forEach(logger::trace);
        }
        logger.info("Queueing analysis {} for execution", topologyContextId);
        analysis.queued();
        runnerThreadPool.execute(() -> {
            try (Ticket ticket = topologyProcessingGate.enter(topologyInfo, topologyDTOs)) {
                runAnalysis(analysis);
            } catch (InterruptedException e) {
                logger.error("Analysis of topology {} interrupted while waiting to start.",
                    topologyInfo);
                // Send notification of Analysis FAILURE
                sendAnalysisStatus(analysis, AnalysisState.FAILED.ordinal());
            } catch (TimeoutException e) {
                logger.error("Analysis of topology timed out waiting to start.", e);
                // Send notification of Analysis FAILURE
                sendAnalysisStatus(analysis, AnalysisState.FAILED.ordinal());
            }
        });
        return analysis;
    }

    private ActionPlan getEmptyActionPlan(TopologyDTO.TopologyInfo topologyInfo) {
        return ActionPlan.newBuilder()
            .setId(IdentityGenerator.next())
            .setInfo(ActionPlanInfo.newBuilder()
                .setMarket(MarketActionPlanInfo.newBuilder()
                    .setSourceTopologyInfo(topologyInfo)))
            .setDeprecatedTopologyId(topologyInfo.getTopologyId())
            .setDeprecatedTopologyContextId(topologyInfo.getTopologyContextId())
            .setAnalysisStartTimestamp(System.currentTimeMillis())
            .setAnalysisCompleteTimestamp(System.currentTimeMillis())
            .build();
    }

    /**
     * Schedule a call to stop Analysis of a plan with a specific configuration.
     *
     * @param planId The id of the plan to stop.
     */
    public void stopAnalysis(final long planId) {
        synchronized (analysisMap) {
            if (analysisMap.containsKey(planId)) {
                // call stopAnalysis that sets the forceStop boolean to true
                logger.info("Performing forceStop on plan with id:" + planId);
                analysisMap.get(planId).cancelAnalysis();
            } else {
                // topology is not being processed so mark topologyId to be stopped when we receive topology
                logger.info("Caching plan stop for plan with id:" + planId);
                analysisStoppageSet.add(planId);
            }
        }
    }

    /**
     * Run the analysis, when done - remove it from the map of runs and notify listeners.
     *
     * <P>AnalysisStatusNotification is for RunTime exceptions. Actions/Projected entity/topology creation,
     * are updated in the absence of such exceptions.  The Plan Orchestrator state itself goes to succeeded
     * once all notifications have been received from components it's listening to.
     * @param analysis the object on which to run the analysis.
     */
    private void runAnalysis(@Nonnull final Analysis analysis) {
        boolean isPlan = analysis.getTopologyInfo().hasPlanInfo();
        if (!isPlan) {
            // Set replay actions from last succeeded market cycle.
            analysis.setReplayActions(realtimeReplayActions);
        }
        try {
            analysis.execute();
        } finally {
            TheMatrix.clearInstance(analysis.getTopologyInfo().getTopologyId());
            analysisMap.remove(analysis.getContextId());
        }
        try {
            if (analysis.isDone()) {
                marketDebugRpcService.ifPresent(debugService -> {
                    debugService.recordCompletedAnalysis(analysis);
                });

                if (analysis.getState() == AnalysisState.SUCCEEDED) {
                    // if this was a plan topology, broadcast the plan analysis topology
                    if (isPlan) {
                        serverApi.notifyPlanAnalysisTopology(analysis.getTopologyInfo(),
                                                             analysis.getTopology().values());
                    } else {
                        // Update replay actions to contain actions from most recent analysis.
                        realtimeReplayActions = analysis.getReplayActions();
                    }
                    // Send notification of Analysis SUCCESS
                    sendAnalysisStatus(analysis, AnalysisState.SUCCEEDED.ordinal());

                    // Send projected topology before recommended actions, because some recommended
                    // actions will have OIDs that are only present in the projected topology, and we
                    // want to minimize the risk of the projected topology being unavailable.
                    serverApi.notifyProjectedTopology(analysis.getTopologyInfo(),
                                                      analysis.getProjectedTopologyId().get(),
                                                      analysis.getProjectedTopology().get(),
                                                      analysis.getActionPlan().get().getId());
                    serverApi.notifyActionsRecommended(analysis.getActionPlan().get());

                    // A flag that shows if there is any cloud entity in the topology or not.
                    final boolean hasAnyCloudEntity = !CollectionUtils.isEmpty(analysis.getOriginalCloudTopology().getEntities());

                    // Send projected entity costs. We only send them for the real time topology.
                    final Map<Long, CostJournal<TopologyEntityDTO>> projectedCosts =
                                                               analysis.getProjectedCosts().get();

                    if (hasAnyCloudEntity) {
                        serverApi.notifyProjectedEntityCosts(analysis.getTopologyInfo(),
                                analysis.getProjectedTopologyId().get(),
                                Collections2.transform(projectedCosts
                                                .values(),
                                        CostJournal::toEntityCostProto));
                    }

                    // EntityRICoverage is already a protobuf object so
                    // we can directly send values of map in the message.
                    final Map<Long, EntityReservedInstanceCoverage> projectedCoverage =
                                                          analysis.getProjectedEntityRiCoverage()
                                                                          .get();
                    if (hasAnyCloudEntity) {
                        serverApi.notifyProjectedEntityRiCoverage(analysis.getTopologyInfo(),
                                analysis.getProjectedTopologyId()
                                        .get(),
                                projectedCoverage.values());
                    }
                }
            } else if (analysis.getState() == AnalysisState.FAILED) {
                // Send notification of Analysis FAILURE
                sendAnalysisStatus(analysis, AnalysisState.FAILED.ordinal());
            }
        } catch (CommunicationException | InterruptedException e) {
            // TODO we need to decide, whether to commit the incoming topology here or not.
            logger.error("Could not send market notifications", e);
        } catch (Exception e) {
            logger.error("Exception during market execution for topologyId: "
                         + analysis.getTopologyInfo().getTopologyId(), e);
            // Send notification of Analysis FAILURE
            sendAnalysisStatus(analysis, AnalysisState.FAILED.ordinal());
        }
    }

    /**
     * Send the status of the analysis run to listeners such as PlanProgresslistener.
     *
     * @param analysis the object on which to run the analysis.
     * @param status The status of a market analysis run.
     */
    private void sendAnalysisStatus(@Nonnull final Analysis analysis, final int status) {
        boolean isPlan = analysis.getTopologyInfo().hasPlanInfo();
        // TODO:  If analysis status is ever needed in real-time (for e.g to know if
        // actions are stale) send AnalysisState.FAILED.ordinal()

        if (!isPlan) {
            return;
        }
        if (status == AnalysisState.FAILED.ordinal()) {
            logger.error("Sending analysis failure notification..");
        } else if (status == AnalysisState.SUCCEEDED.ordinal()) {
            logger.info("Sending analysis success notification..");
        }
        serverApi.sendAnalysisStatus(analysis.getTopologyInfo(), status);
    }

    /**
     * Check if a round of analysis is already running for the realtime-topology
     * @return a collection of the analysis runs
     */
    public boolean isAnalysisRunningForRtTopology(TopologyDTO.TopologyInfo topologyInfo) {
        synchronized (analysisMap) {
            long topologyContextId = topologyInfo.getTopologyContextId();
            long topologyId = topologyInfo.getTopologyId();
            if (topologyInfo.getTopologyType() == TopologyDTO.TopologyType.REALTIME &&
                    analysisMap.containsKey(topologyContextId)) {
                Analysis analysis = analysisMap.get(topologyContextId);
                logger.info("Analysis {} is already running with topology {}. Discarding new {}.",
                        topologyContextId, analysis.getTopologyId(), topologyId);
                return true;
            }
        }
        return false;
    }

    /**
     * Check if the topology received is for a Buy RI Optimize Cloud Plan
     *
     * @param topologyInfo Topology Info
     * @return true if the received topology is for Buy RI Optimize Cloud Plan
     */
    public boolean isBuyRIOnlyOptimizeCloudPlan(@Nonnull TopologyDTO.TopologyInfo topologyInfo) {
        long topologyContextId = topologyInfo.getTopologyContextId();
        long topologyId = topologyInfo.getTopologyId();
        if (TopologyDTO.TopologyType.PLAN.equals(topologyInfo.getTopologyType()) &&
            StringConstants.OPTIMIZE_CLOUD_PLAN.equals(topologyInfo.getPlanInfo().getPlanType()) &&
            StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_ONLY.equals(topologyInfo.getPlanInfo().getPlanSubType())) {
            return true;
        }
        return false;
    }

    /**
     * Get all the analysis runs.
     * @return a collection of the analysis runs
     */
    public Collection<Analysis> getRuns() {
        return analysisMap.values();
    }

}
