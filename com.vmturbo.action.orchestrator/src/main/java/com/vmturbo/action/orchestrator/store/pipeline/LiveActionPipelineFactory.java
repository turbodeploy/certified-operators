package com.vmturbo.action.orchestrator.store.pipeline;

import static com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineContextMembers.ATOMIC;
import static com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineContextMembers.MARKET;

import java.time.Clock;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.AuditedActionsManager;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.LiveActionStore.ActionSource;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionIdentityStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionPlanSummaryStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionProcessingInfoStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionStatisticsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ActionStoreSummaryStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.AddRemoveAndClearAtomicActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.AddRemoveAndClearMarketActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.AuditAndCacheBookkeepingStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.CompilePreviousActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.CreateAtomicActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.CreateLastExecutedRecommendationsTrackerStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.GetEntitiesAndSettingsSnapshotStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.GetInvolvedEntityIdsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.GetOrCreateLiveActionStoreStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.MarketActionsSegment;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.MarketReRecommendedActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.PopulateLiveActionsSegment;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.PrepareAggregatedActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ProcessLiveBuyRIActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.ProcessStartSuspendActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.RefreshProbeCapabilitiesStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.SupportLevelAndPrerequisitesStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.TranslateActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateAutomationStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateReRecommendedActionsStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineStages.UpdateSeverityCacheStage;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineDefinition;
import com.vmturbo.components.common.pipeline.SegmentStage.SegmentDefinition;
import com.vmturbo.identity.IdentityService;

/**
 * A factory class for properly configured {@link ActionPipeline} objects for live topologies.
 *
 * <p>Users should not instantiate live {@link ActionPipeline}s themselves. Instead, they should
 * use the appropriately configured pipelines provided by this factory - e.g.
 * {@link LiveActionPipelineFactory#marketActionsPipeline(ActionPlan)}.
 */
public class LiveActionPipelineFactory {

    private static final Logger logger = LogManager.getLogger();

    private static final String ACTION_COUNTS_TITLE = "ActionPlan Summary: (difference from previous action plan in parentheses)";
    private ActionCounts previousActionPlanCounts = new ActionCounts(ACTION_COUNTS_TITLE, Stream.empty());

    private final ActionStorehouse actionStorehouse;
    private final ActionAutomationManager automationManager;
    private final AtomicActionFactory atomicActionFactory;
    private final EntitiesAndSettingsSnapshotFactory entitiesAndSettingsSnapshotFactory;
    private final ReentrantLock sharedLiveActionsLock = new ReentrantLock(true);
    private final long liveActionsLockWaitTimeMinutes;
    private final ProbeCapabilityCache probeCapabilityCache;

    private long marketActionPlanCount = 0;
    private long buyRiActionPlanCount = 0;
    private long startSuspendActionPlanCount = 0;
    private final ActionHistoryDao actionHistoryDao;
    private final IActionFactory actionFactory;
    private final Clock clock;
    private final int queryTimeWindowForLastExecutedActionsMins;
    private final IdentityService<ActionInfo> actionIdentityService;
    private final ActionTargetSelector actionTargetSelector;
    private final ActionTranslator actionTranslator;
    private final LiveActionsStatistician actionsStatistician;
    private final ActionAuditSender actionAuditSender;
    private final AuditedActionsManager auditedActionsManager;

    private final ActionTopologyStore actionTopologyStore;

    private final long realtimeContextId;

    private final int telemetryChunkSize;

    /**
     * Create a new {@link LiveActionPipelineFactory}.
     *
     * @param actionStorehouse The {@link ActionStorehouse}.
     * @param automationManager The {@link ActionAutomationManager}.
     * @param atomicActionFactory The {@link AtomicActionFactory}.
     * @param entitiesAndSettingsSnapshotFactory The {@link EntitiesAndSettingsSnapshotFactory}.
     * @param liveActionsLockMaxWaitTimeMinutes Max time to wait in minutes for the shared live
     *                                          actions lock during live actions pipeline execution
     *                                          before timing out. Must be > 0.
     * @param probeCapabilityCache The {@link ProbeCapabilityCache}.
     * @param actionHistoryDao DAO layer working with executed actions.
     * @param actionFactory The {@link IActionFactory} for creating actions.
     * @param clock the {@link Clock}.
     * @param queryTimeWindowForLastExecutedActionsMins time window within which actions will not
     *                                                  be populated if they are already executed (SUCEEDED).
     * @param actionIdentityService The {@link IdentityService} for actions.
     * @param actionTargetSelector The {@link ActionTargetSelector}.
     * @param actionTranslator the {@link ActionTranslator}.
     * @param actionsStatistician the {@link LiveActionsStatistician}.
     * @param actionAuditSender the {@link ActionAuditSender}.
     * @param auditedActionsManager the {@link AuditedActionsManager}.
     * @param actionTopologyStore the {@link ActionTopologyStore}.
     * @param realtimeContextId realtime context ID.
     * @param telemetryChunkSize the number of entities to process per chunk.
     */
    public LiveActionPipelineFactory(@Nonnull final ActionStorehouse actionStorehouse,
                                     @Nonnull final ActionAutomationManager automationManager,
                                     @Nonnull final AtomicActionFactory atomicActionFactory,
                                     @Nonnull final EntitiesAndSettingsSnapshotFactory entitiesAndSettingsSnapshotFactory,
                                     final long liveActionsLockMaxWaitTimeMinutes,
                                     @Nonnull final ProbeCapabilityCache probeCapabilityCache,
                                     @Nonnull final ActionHistoryDao actionHistoryDao,
                                     @Nonnull final IActionFactory actionFactory,
                                     @Nonnull final Clock clock,
                                     final int queryTimeWindowForLastExecutedActionsMins,
                                     @Nonnull final IdentityService<ActionInfo> actionIdentityService,
                                     @Nonnull final ActionTargetSelector actionTargetSelector,
                                     @Nonnull final ActionTranslator actionTranslator,
                                     @Nonnull final LiveActionsStatistician actionsStatistician,
                                     @Nonnull final ActionAuditSender actionAuditSender,
                                     @Nonnull final AuditedActionsManager auditedActionsManager,
                                     @Nonnull final ActionTopologyStore actionTopologyStore,
                                     final long realtimeContextId,
                                     final int telemetryChunkSize) {
        Preconditions.checkArgument(liveActionsLockMaxWaitTimeMinutes > 0,
            "Illegal value %s for liveActionsLockMaxWaitTimeMinutes", liveActionsLockMaxWaitTimeMinutes);
        Preconditions.checkArgument(queryTimeWindowForLastExecutedActionsMins > 0,
            "Illegal value %s for liveActionsLockMaxWaitTimeMinutes", queryTimeWindowForLastExecutedActionsMins);

        this.actionStorehouse = Objects.requireNonNull(actionStorehouse);
        this.automationManager = Objects.requireNonNull(automationManager);
        this.atomicActionFactory = Objects.requireNonNull(atomicActionFactory);
        this.entitiesAndSettingsSnapshotFactory = Objects.requireNonNull(entitiesAndSettingsSnapshotFactory);
        this.liveActionsLockWaitTimeMinutes = liveActionsLockMaxWaitTimeMinutes;
        this.probeCapabilityCache = Objects.requireNonNull(probeCapabilityCache);
        this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.clock = Objects.requireNonNull(clock);
        this.queryTimeWindowForLastExecutedActionsMins = queryTimeWindowForLastExecutedActionsMins;
        this.actionIdentityService = Objects.requireNonNull(actionIdentityService);
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.actionsStatistician = Objects.requireNonNull(actionsStatistician);
        this.actionAuditSender = Objects.requireNonNull(actionAuditSender);
        this.auditedActionsManager = Objects.requireNonNull(auditedActionsManager);
        this.actionTopologyStore = actionTopologyStore;
        this.realtimeContextId = realtimeContextId;
        this.telemetryChunkSize = telemetryChunkSize;
    }

    /**
     * Create a new {@link ActionPipeline} for a given action plan. The type of the pipeline
     * created depends on the type of the action plan (ie A MarketActionPipeline will be
     * created for a MARKET actionPlan, BuyRIActionPipeline will be created for a
     * BUY_RI actionPlan and startSuspendActionsPipeline will be created for START_SUSPEND
     * actionPlan.
     *
     * @param actionPlan The actionplan to be processed for which we need a pipeline to process.
     * @return An {@link ActionPlan} appropriate for processing the action plan.
     */
    public ActionPipeline<ActionPlan, ActionProcessingInfo> actionPipeline(@Nonnull final ActionPlan actionPlan) {
        switch (actionPlan.getInfo().getTypeInfoCase()) {
            case MARKET:
                return marketActionsPipeline(actionPlan);
            case BUY_RI:
                return buyRiActionsPipeline(actionPlan);
            case START_SUSPEND:
                return startSuspendActionsPipeline(actionPlan);
            default:
                throw new IllegalArgumentException("Unknown ActionPlan type: "
                    + actionPlan.getInfo().getTypeInfoCase());
        }
    }

    /**
     * Create a pipeline capable of processing a live market {@link ActionPlan}.
     *
     * @param actionPlan The action plan to process.
     * @return The {@link ActionPipeline}. This pipeline will accept an {@link ActionPlan} containing
     *         market actions and return the {@link ActionProcessingInfo} for the processing done by
     *         the pipeline. For an action pipeline accepting BuyRI action plans {@see #buyRiActionsPipeline}.
     */
    private ActionPipeline<ActionPlan, ActionProcessingInfo> marketActionsPipeline(@Nonnull final ActionPlan actionPlan) {
        final ActionPipeline<ActionPlan, ActionProcessingInfo> processingPipeline =
            buildLiveMarketActionsPipeline(actionPlan);

        if (marketActionPlanCount == 1) {
            logger.info("\n" + processingPipeline.tabularDescription("Live Market Action Pipeline"));
        }
        return processingPipeline;
    }

    /**
     * Create a pipeline capable of processing a live BuyRI {@link ActionPlan}.
     *
     * @param actionPlan The action plan to process.
     * @return The {@link ActionPipeline}. This pipeline will accept an {@link ActionPlan} containing
     *         buyRI actions and return the {@link ActionProcessingInfo} for the processing done by
     *         the pipeline. For an action pipeline accepting market action plans {@see #marketActionsPipeline}.
     */
    private ActionPipeline<ActionPlan, ActionProcessingInfo> buyRiActionsPipeline(@Nonnull final ActionPlan actionPlan) {
        final ActionPipeline<ActionPlan, ActionProcessingInfo> processingPipeline = buildBuyRiActionsPipeline(actionPlan);
        if (buyRiActionPlanCount == 1) {
            logger.info("\n" + processingPipeline.tabularDescription("Live BuyRI Action Pipeline"));
        }
        return processingPipeline;
    }

    /**
     * Create a pipeline capable of processing a live Start/Suspend {@link ActionPlan}.
     *
     * @param actionPlan The action plan to process.
     * @return The {@link ActionPipeline}. This pipeline will accept an {@link ActionPlan}
     *         containing
     *         Start/Suspend actions and return the {@link ActionProcessingInfo} for the processing
     *         done by the pipeline.
     *         For an action pipeline accepting Start/Suspend action plans.
     */
    private ActionPipeline<ActionPlan, ActionProcessingInfo> startSuspendActionsPipeline(
            @Nonnull final ActionPlan actionPlan) {
        final ActionPipeline<ActionPlan, ActionProcessingInfo> processingPipeline =
                buildStartSuspendActionsPipeline(actionPlan);

        if (startSuspendActionPlanCount == 1) {
            logger.info("\n" + processingPipeline.tabularDescription(
                    "Live Start/Suspend Action Pipeline"));
        }
        return processingPipeline;
    }

    private ActionPipeline<ActionPlan, ActionProcessingInfo> buildLiveMarketActionsPipeline(@Nonnull final ActionPlan actionPlan) {
        // Increment the number of action plans observed.
        marketActionPlanCount++;

        final ActionCounts newActionCounts = new ActionCounts(ACTION_COUNTS_TITLE, actionPlan.getActionList().stream());
        final ActionCounts oldCounts = previousActionPlanCounts;
        final Supplier<ActionCounts> newCountsSupplier = () -> newActionCounts;
        final Supplier<ActionCounts> previousCountsSupplier = () -> oldCounts;
        previousActionPlanCounts = newActionCounts;

        final ActionPipelineContext pipelineContext = new ActionPipelineContext(
            actionPlan.getId(),
            TopologyType.REALTIME,
            actionPlan.getInfo());

        return new ActionPipeline<>(PipelineDefinition.<ActionPlan, ActionProcessingInfo, ActionPipelineContext>newBuilder(pipelineContext)
            .initialContextMember(ActionPipelineContextMembers.CURRENT_ACTION_PLAN_COUNTS, newCountsSupplier)
            .initialContextMember(ActionPipelineContextMembers.PREVIOUS_ACTION_PLAN_COUNTS, previousCountsSupplier)
            .addStage(new GetInvolvedEntityIdsStage(auditedActionsManager, true))
            .addStage(new PrepareAggregatedActionsStage(atomicActionFactory))
            .addStage(new GetOrCreateLiveActionStoreStage(actionStorehouse))
            .addStage(new GetEntitiesAndSettingsSnapshotStage(entitiesAndSettingsSnapshotFactory))
            // The PopulateLiveActionsSegment holds a shared lock during the lifetime of its execution
            // so only one of these can be executed at a time.
            .addStage(new PopulateLiveActionsSegment(sharedLiveActionsLock, liveActionsLockWaitTimeMinutes,
                    SegmentDefinition.addStage(new ActionPlanSummaryStage())
                .addStage(new RefreshProbeCapabilitiesStage(probeCapabilityCache))
                .addStage(new CreateLastExecutedRecommendationsTrackerStage(actionHistoryDao, actionFactory, clock, queryTimeWindowForLastExecutedActionsMins))
                // MarketActionsSegment for processing market actions
                .addStage(new MarketActionsSegment(SegmentDefinition
                    .addStage(new ActionIdentityStage(actionIdentityService, MARKET.getInputActionCount()))
                    .addStage(new SupportLevelAndPrerequisitesStage(actionTargetSelector))
                    .addStage(new CompilePreviousActionsStage(MARKET.getActionDifference(), ActionSource.MARKET))
                    .addStage(new MarketReRecommendedActionsStage(actionFactory))
                    .addStage(new TranslateActionsStage(actionTranslator, MARKET.getActionDifference()))
                    .finalStage(new AddRemoveAndClearMarketActionsStage(MARKET.getActionDifference(),
                        MARKET.getInputActionCount(),
                        MARKET.getNewActionCount(),
                        MARKET.getActionsToAdd()))))
                // AtomicActionsSegment for creating and processing atomic actions
                .addStage(SegmentDefinition.addStage(new CreateAtomicActionsStage(atomicActionFactory))
                    .addStage(new ActionIdentityStage(actionIdentityService, ATOMIC.getInputActionCount()))
                    .addStage(new SupportLevelAndPrerequisitesStage(actionTargetSelector))
                    .addStage(new CompilePreviousActionsStage(ATOMIC.getActionDifference(), ActionSource.ATOMIC))
                    .addStage(new UpdateReRecommendedActionsStage(actionFactory, ATOMIC.getActionDifference(), ATOMIC.getNewActionCount()))
                    .addStage(new TranslateActionsStage(actionTranslator, ATOMIC.getActionDifference()))
                    .finalStage(new AddRemoveAndClearAtomicActionsStage(ATOMIC.getActionDifference(),
                        ATOMIC.getInputActionCount(),
                        ATOMIC.getNewActionCount(),
                        ATOMIC.getActionsToAdd()))
                    .asStage("AtomicActionsSegment"))
                .addStage(new ActionPipelineStages.CreateRelatedActionsStage())
                .addStage(new AuditAndCacheBookkeepingStage(actionAuditSender))
                .finalStage(new ActionStoreSummaryStage())
            ))
            .addStage(new UpdateAutomationStage(automationManager))
            .addStage(new UpdateSeverityCacheStage())
            .addStage(new ActionStatisticsStage(actionsStatistician, actionTopologyStore,
                    entitiesAndSettingsSnapshotFactory, realtimeContextId, telemetryChunkSize))
            .finalStage(new ActionProcessingInfoStage()), clock
        );
    }

    private ActionPipeline<ActionPlan, ActionProcessingInfo> buildBuyRiActionsPipeline(@Nonnull final ActionPlan actionPlan) {
        // Increment the number of action plans observed.
        buyRiActionPlanCount++;

        final ActionPipelineContext pipelineContext = new ActionPipelineContext(
            actionPlan.getId(),
            TopologyType.REALTIME,
            actionPlan.getInfo());

        return new ActionPipeline<>(PipelineDefinition.<ActionPlan, ActionProcessingInfo, ActionPipelineContext>newBuilder(pipelineContext)
            .addStage(new GetOrCreateLiveActionStoreStage(actionStorehouse))
            .addStage(new PopulateLiveActionsSegment(sharedLiveActionsLock, liveActionsLockWaitTimeMinutes,
                SegmentDefinition.finalStage(new ProcessLiveBuyRIActionsStage())
            ))
            .addStage(new UpdateAutomationStage(automationManager))
            .addStage(new UpdateSeverityCacheStage())
            .finalStage(new ActionProcessingInfoStage()), clock
        );
    }

    private ActionPipeline<ActionPlan, ActionProcessingInfo> buildStartSuspendActionsPipeline(@Nonnull final ActionPlan actionPlan) {
        // Increment the number of action plans observed.
        startSuspendActionPlanCount++;

        final ActionPipelineContext pipelineContext = new ActionPipelineContext(
                actionPlan.getId(),
                TopologyType.REALTIME,
                actionPlan.getInfo());

        return new ActionPipeline<>(PipelineDefinition.<ActionPlan, ActionProcessingInfo, ActionPipelineContext>newBuilder(pipelineContext)
                .addStage(new GetOrCreateLiveActionStoreStage(actionStorehouse))
                .addStage(new PopulateLiveActionsSegment(sharedLiveActionsLock, liveActionsLockWaitTimeMinutes,
                        SegmentDefinition.finalStage(new ProcessStartSuspendActionsStage())
                ))
                .addStage(new UpdateAutomationStage(automationManager))
                .finalStage(new ActionProcessingInfoStage()), clock
        );
    }
}
