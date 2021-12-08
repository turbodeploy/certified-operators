package com.vmturbo.market.runner;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.opentracing.SpanContext;

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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.UnplacementReason.PlacementProblem;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.api.tracing.Tracing.TracingScope;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.cost.calculation.journal.CostJournal;
import com.vmturbo.market.MarketNotificationSender;
import com.vmturbo.market.reservations.InitialPlacementFinder;
import com.vmturbo.market.rpc.MarketDebugRpcService;
import com.vmturbo.market.topology.conversions.CommodityConverter;
import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.platform.analysis.ede.ReplayActions;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
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

    private final long rtAnalysisTimeoutSecs;

    private static final DataMetricHistogram INPUT_TOPOLOGY = DataMetricHistogram.builder()
            .withName("mkt_input_topology")
            .withHelp("Size of the topology to analyze.")
            .withBuckets(1000, 10000, 30000, 50000, 75000, 100000, 150000, 200000)
            .build()
            .register();

    private final InitialPlacementFinder initialPlacementFinder;

    public MarketRunner(@Nonnull final ExecutorService runnerThreadPool,
                        @Nonnull final MarketNotificationSender serverApi,
                        @Nonnull final AnalysisFactory analysisFactory,
                        @Nonnull final Optional<MarketDebugRpcService> marketDebugRpcService,
                        final TopologyProcessingGate topologyProcessingGate,
                        @Nonnull final InitialPlacementFinder initialPlacementFinder,
                        final long rtAnalysisTimeoutSecs) {
        this.runnerThreadPool = Objects.requireNonNull(runnerThreadPool);
        this.serverApi = Objects.requireNonNull(serverApi);
        this.marketDebugRpcService = Objects.requireNonNull(marketDebugRpcService);
        this.analysisFactory = Objects.requireNonNull(analysisFactory);
        this.topologyProcessingGate = Objects.requireNonNull(topologyProcessingGate);
        this.initialPlacementFinder = Objects.requireNonNull(initialPlacementFinder);
        this.rtAnalysisTimeoutSecs = rtAnalysisTimeoutSecs;
    }

    /**
     * Schedule a call to the Analysis methods on the given topology scoped to the given SE OIDs.
     *
     * @param topologyInfo describes this topology, including contextId, id, etc
     * @param topologyDTOs the TopologyEntityDTOs in this topology
     * @param tracingContext The distributed tracing context for the analysis.
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
                                     @Nonnull final Collection<TopologyEntityDTO> topologyDTOs,
                                     @Nullable final SpanContext tracingContext,
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
            analysis = analysisFactory.newAnalysis(
                    topologyInfo,
                    topologyDTOs,
                    configBuilder -> configBuilder
                        .setIncludeVDC(includeVDC)
                        .setMaxPlacementsOverride(maxPlacementsOverride)
                        .setUseQuoteCacheDuringSNM(useQuoteCacheDuringSNM)
                        .setReplayProvisionsForRealTime(replayProvisionsForRealTime)
                        .setRightsizeLowerWatermark(rightsizeLowerWatermark)
                        .setRightsizeUpperWatermark(rightsizeUpperWatermark)
                        .setDiscountedComputeCostFactor(discountedComputeCostFactor),
                        initialPlacementFinder);

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
                try (TracingScope scope = Tracing.trace("analysis", tracingContext)
                    .tag("context_id", topologyInfo.getTopologyContextId())
                    .tag("topology_id", topologyId)
                    .tag("topology_size", topologyDTOs.size())) {
                    runAnalysis(analysis);
                }
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
     * Schedule a call to stop Analysis of a plan with a specific configuration.
     *
     * @param contextID The context id of RT topologies.
     * @param topologyId The id of the RT topology to stop.
     */
    private void stopRTAnalysis(final long contextID, long topologyId) throws InterruptedException {
        synchronized (analysisMap) {
            if (analysisMap.containsKey(contextID)) {
                Analysis analysis = analysisMap.get(contextID);
                if (analysis.getTopologyId() == topologyId) {
                    // call stopAnalysis that sets the forceStop boolean to true
                    logger.info("Performing forceStop on real time analysis with id {}.", topologyId);
                    analysis.cancelAnalysis();
                } else {
                    logger.info("Real time with id {} already completed. "
                            + "Returning without triggering forcestop.", topologyId);
                }
            } else {
                logger.info("Real time with context {} already completed. "
                        + "Returning without triggering forcestop.", contextID);
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
        long startTime = 0;
        if (!isPlan) {
            if (FeatureFlags.ENABLE_ANALYSIS_TIMEOUT.isEnabled()) {
                ScheduledExecutorService timeoutScheduler = Executors.newSingleThreadScheduledExecutor();
                logger.info("Scheduling forcestop for analysis {}", analysis.getTopologyId());
                timeoutScheduler.schedule(() -> {
                    try {
                        stopRTAnalysis(analysis.getContextId(), analysis.getTopologyId());
                    } catch (InterruptedException e) {
                        logger.error("Could not send market notifications due to a potential timeout.", e);
                    }
                }, rtAnalysisTimeoutSecs, TimeUnit.SECONDS);
            }
            // Set replay actions from last succeeded market cycle.
            analysis.setReplayActions(realtimeReplayActions);
        } else {
            startTime = System.currentTimeMillis();
            logger.info("{} Starting plan market analysis.",
                    TopologyDTOUtil.formatPlanLogPrefix(analysis.getTopologyInfo()
                            .getTopologyContextId()));
        }
        try {
            analysis.execute();
        } finally {
            TheMatrix.clearInstance(analysis.getTopologyInfo().getTopologyId());
        }
        try {
            if (analysis.isDone()) {
                marketDebugRpcService.ifPresent(debugService -> {
                    debugService.recordCompletedAnalysis(analysis);
                });

                // create ActionPlan for successful plan, successful RT and also for stopped RT.
                if (analysis.getState() == AnalysisState.SUCCEEDED ||
                    // create ActionPlan for stopped RT analysis
                    (analysis.getTopologyInfo().getTopologyType() == TopologyType.REALTIME
                            && analysis.isStopAnalysis())) {
                    // if this was a plan topology, broadcast the plan analysis topology
                    if (!isPlan) {
                        // Update replay actions to contain actions from most recent analysis.
                        realtimeReplayActions = analysis.getReplayActions();
                    } else {
                        logger.info("{} Completed plan market analysis. Took {} secs.",
                                TopologyDTOUtil.formatPlanLogPrefix(analysis.getTopologyInfo()
                                        .getTopologyContextId()),
                                (System.currentTimeMillis() - startTime)/1000);
                    }
                    // Send notification of Analysis SUCCESS
                    sendAnalysisStatus(analysis, AnalysisState.SUCCEEDED.ordinal());

                    // log the projected entity with infinite quote reasons
                    logger.info("==== Start logging projected entity DTO with infinite quote ====");
                    analysis.getProjectedTopology().get().forEach((oid, p) -> {
                       if (!p.getEntity().getUnplacedReasonList().isEmpty()) {
                           logger.info("Entity oid {}, displayName {} has infinite quote due to : {}",
                                   oid, p.getEntity().getDisplayName(),
                                   printReason(p.getEntity().getUnplacedReasonList()));
                           logger.info("Protobuf in failure is {}", p.getEntity().getUnplacedReasonList());
                       }
                    });
                    logger.info("==== End logging projected entity DTO with infinite quote ====");
                    // Send projected topology before recommended actions, because some recommended
                    // actions will have OIDs that are only present in the projected topology, and we
                    // want to minimize the risk of the projected topology being unavailable.
                    serverApi.notifyProjectedTopology(analysis.getTopologyInfo(),
                                                      analysis.getProjectedTopologyId().get(),
                                                      analysis.getProjectedTopology().get().values(),
                                                      analysis.getActionPlan().get());
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
                    if (isPlan) {
                        logger.info("{} Sent notifications on plan completion.",
                                TopologyDTOUtil.formatPlanLogPrefix(analysis.getTopologyInfo()
                                        .getTopologyContextId()));
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
        } finally {
            analysisMap.remove(analysis.getContextId());
        }
    }

    /**
     * Print {@link UnplacementReason}.
     *
     * @param unplacedReasonList A list of {@link UnplacementReason}.
     * @return A string constructed based on {@link UnplacementReason}.
     */
    private String printReason(@Nonnull List<UnplacementReason> unplacedReasonList) {
        StringBuilder sb = new StringBuilder();
        for (UnplacementReason reason : unplacedReasonList) {
            sb.append(" {");
            if (!reason.getFailedResourcesList().isEmpty()) {
                reason.getFailedResourcesList().forEach(f -> {
                    sb.append(" commodity ").append(CommodityConverter.commodityDebugInfo(f.getCommType())).append(
                            " which has a requested amount of ").append(f.getRequestedAmount());
                    if (f.hasMaxAvailable()) {
                        sb.append(" but the max available is ").append(f.getMaxAvailable());
                    }
                });
            } else if (reason.getPlacementProblem() == PlacementProblem.COSTS_NOT_FOUND) {
                sb.append(" Cost is not found.");
            } else if (reason.getPlacementProblem() == PlacementProblem.NOT_CONTROLLABLE) {
                sb.append(" VM is not controllable.");
            }
            if (reason.hasClosestSeller()) {
                sb.append(" from closest entity ").append(reason.getClosestSeller());
            }
            if (reason.hasProviderType()) {
                sb.append(" when looking for supplier type ").append(EntityType.forNumber(
                        reason.getProviderType()));
            }
            sb.append(" }");
        }
        return sb.toString();
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
