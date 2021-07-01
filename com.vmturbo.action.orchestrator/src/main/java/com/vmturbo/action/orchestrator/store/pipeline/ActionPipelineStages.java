package com.vmturbo.action.orchestrator.store.pipeline;

import static com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT;
import static com.vmturbo.action.orchestrator.store.pipeline.ActionPipelineContextMembers.LAST_EXECUTED_RECOMMENDATIONS_TRACKER;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Streams;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.audit.ActionAuditSender;
import com.vmturbo.action.orchestrator.exception.ExecutionInitiationException;
import com.vmturbo.action.orchestrator.execution.ActionAutomationManager;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.store.ActionStore;
import com.vmturbo.action.orchestrator.store.ActionStorehouse;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.IActionFactory;
import com.vmturbo.action.orchestrator.store.LiveActionStore;
import com.vmturbo.action.orchestrator.store.LiveActionStore.ActionSource;
import com.vmturbo.action.orchestrator.store.LiveActionStore.RecommendationTracker;
import com.vmturbo.action.orchestrator.store.atomic.AggregatedAction;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory.AtomicActionResult;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipeline.RequiredPassthroughStage;
import com.vmturbo.action.orchestrator.store.pipeline.ActionPipeline.Stage;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.Prerequisite;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionEnvironmentType;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.pipeline.ExclusiveLockedSegmentStage;
import com.vmturbo.components.common.pipeline.Pipeline.PipelineStageException;
import com.vmturbo.components.common.pipeline.Pipeline.StageResult;
import com.vmturbo.components.common.pipeline.Pipeline.Status;
import com.vmturbo.components.common.pipeline.PipelineContext.PipelineContextMemberDefinition;
import com.vmturbo.components.common.pipeline.SegmentStage;
import com.vmturbo.identity.IdentityService;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * A wrapper class for the various {@link ActionPipeline.Stage} and {@link ActionPipeline.PassthroughStage}
 * implementations. Since the stages are pretty small, it makes some sense to keep them in one place for now.
 */
public class ActionPipelineStages {
    /**
     * Stores an action plan in a store created in the ActionStorehouse. Returns the
     * {@link ActionStore} where the actions in the {@link ActionPlan} were stored.
     */
    public static class PopulateActionStoreStage extends Stage<ActionPlan, ActionStore> {
        private final ActionStorehouse storehouse;

        /**
         * Create a new {@link PopulateActionStoreStage}.
         *
         * @param storehouse The {@link ActionStorehouse}.
         */
        public PopulateActionStoreStage(@Nonnull final ActionStorehouse storehouse) {
            this.storehouse = Objects.requireNonNull(storehouse);
        }

        @Nonnull
        @Override
        public StageResult<ActionStore> executeStage(@Nonnull final ActionPlan actionPlan) throws InterruptedException {
            final ActionStore actionStore = storehouse.storeActions(actionPlan);

            return StageResult.withResult(actionStore)
                .andStatus(Status.success());
        }
    }

    /**
     * Get or create an appropriate {@link ActionStore} for an {@link ActionPlan} from the ActionStorehouse.
     */
    public static class GetOrCreateLiveActionStoreStage extends Stage<ActionPlan, ActionPlanAndStore> {
        private final ActionStorehouse storehouse;

        /**
         * Create a new {@link PopulateActionStoreStage}.
         *
         * @param storehouse The {@link ActionStorehouse}.
         */
        public GetOrCreateLiveActionStoreStage(@Nonnull final ActionStorehouse storehouse) {
            this.storehouse = Objects.requireNonNull(storehouse);
        }

        @Nonnull
        @Override
        public StageResult<ActionPlanAndStore> executeStage(@Nonnull final ActionPlan actionPlan)
            throws PipelineStageException {
            final ActionStore actionStore = storehouse.measurePlanAndGetOrCreateStore(actionPlan);
            if (actionStore instanceof LiveActionStore) {
                return StageResult
                    .withResult(new ActionPlanAndStore(actionPlan, (LiveActionStore)actionStore))
                    .andStatus(Status.success());
            } else {
                // TODO: Figure out the right interface on the storehouse to allow direct fetching
                // of the LiveActionStore. Or how to inject the store directly as a LiveActionStore.
                // In practice we can never hit this case though because we check earlier that
                // the only pipeline that uses this stage is the live actions pipeline.
                throw new PipelineStageException("Unsupported action store type "
                    + actionStore.getClass().getSimpleName() + " for live action pipeline.");
            }
        }
    }

    /**
     * Get the IDs of the entities involved in the actions in an {@link ActionPlan}.
     */
    public static class GetInvolvedEntityIdsStage extends RequiredPassthroughStage<ActionPlanAndStore> {
        // The set of involved entity ids. This set gets replaced with a new set computed
        // during the execution of the stage.
        private Set<Long> involvedEntityIds = Collections.emptySet();

        /**
         * Create a new {@link GetInvolvedEntityIdsStage}.
         */
        public GetInvolvedEntityIdsStage() {
            providesToContext(ActionPipelineContextMembers.INVOLVED_ENTITY_IDS,
                (Supplier<Set<Long>>)this::getInvolvedEntityIds);
        }

        @Nonnull
        @Override
        public Status passthrough(ActionPlanAndStore input) {
            involvedEntityIds = ActionDTOUtil.getInvolvedEntityIds(input.actionPlan.getActionList());
            return Status.success();
        }

        @Nonnull
        private Set<Long> getInvolvedEntityIds() {
            return involvedEntityIds;
        }
    }

    /**
     * Prepare aggregated action data for use later in the action processing pipeline.
     */
    public static class PrepareAggregatedActionsStage extends RequiredPassthroughStage<ActionPlanAndStore> {
        private final AtomicActionFactory atomicActionFactory;

        private final FromContext<Set<Long>> involvedEntityIds =
            requiresFromContext(ActionPipelineContextMembers.INVOLVED_ENTITY_IDS);
        private Map<Long, AggregatedAction> aggregatedActions = Collections.emptyMap();
        private final Map<Long, AggregatedAction> actionIdToAggregateAction;

        /**
         * Create a new {@link PrepareAggregatedActionsStage}.
         *
         * @param atomicActionFactory The {@link AtomicActionFactory} to use in preparing aggregated
         *                            action data.
         */
        public PrepareAggregatedActionsStage(@Nonnull final AtomicActionFactory atomicActionFactory) {
            this.atomicActionFactory = Objects.requireNonNull(atomicActionFactory);
            actionIdToAggregateAction = providesToContext(
                ActionPipelineContextMembers.ACTION_ID_TO_AGGREGATE_ACTION, new HashMap<>());
            // We have to provide through a supplier because the reference to the object aggregatedActions
            // may change during the execution of the stage.
            providesToContext(ActionPipelineContextMembers.AGGREGATED_ACTIONS,
                (Supplier<Map<Long, AggregatedAction>>)this::getAggregatedActions);
        }

        @Nonnull
        @Override
        public Status passthrough(ActionPlanAndStore input) {
            // First the market actions are processed to create AggregatedAction.
            // Check if the atomic action factory contains specs to create atomic actions
            // for the actions received, create atomic actions if the specs are received
            // from the topology processor
            if (atomicActionFactory.canMerge()) {
                final ActionPlan actionPlan = input.actionPlan;

                // First aggregate the market actions that should be de-duplicated and merged
                aggregatedActions = atomicActionFactory.aggregate(actionPlan.getActionList());

                // The original market actions that were merged
                for (AggregatedAction aggregatedAction : aggregatedActions.values()) {
                    aggregatedAction.getAllActions().forEach(action ->
                        actionIdToAggregateAction.put(action.getId(), aggregatedAction));
                    involvedEntityIds.get()
                        .addAll(aggregatedAction.getActionEntities());
                }
            }

            return Status.success(String.format("Created %d aggregated actions by merging %d actions",
                aggregatedActions.size(), actionIdToAggregateAction.size()));
        }

        private Map<Long, AggregatedAction> getAggregatedActions() {
            return aggregatedActions;
        }
    }

    /**
     * The the {@link EntitiesAndSettingsSnapshot} for the topology associated with the {@link ActionPlan}
     * being processed.
     */
    public static class GetEntitiesAndSettingsSnapshotStage extends RequiredPassthroughStage<ActionPlanAndStore> {

        private EntitiesAndSettingsSnapshotFactory snapshotFactory;

        private final FromContext<Set<Long>> involvedEntityIds =
            requiresFromContext(ActionPipelineContextMembers.INVOLVED_ENTITY_IDS);

        private EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot;

        /**
         * Create a new {@link EntitiesAndSettingsSnapshot}.
         *
         * @param snapshotFactory The snapshot factory to use to create the {@link EntitiesAndSettingsSnapshot}.
         */
        public GetEntitiesAndSettingsSnapshotStage(@Nonnull final EntitiesAndSettingsSnapshotFactory snapshotFactory) {

            this.snapshotFactory = snapshotFactory;

            providesToContext(ENTITIES_AND_SETTINGS_SNAPSHOT,
                (Supplier<EntitiesAndSettingsSnapshot>)this::getEntitiesAndSettingsSnapshot);
        }

        @Nonnull
        @Override
        public Status passthrough(ActionPlanAndStore input) throws PipelineStageException {
            final long topologyContextId = getContext().getTopologyContextId();
            entitiesAndSettingsSnapshot = snapshotFactory.newSnapshot(
                involvedEntityIds.get(), topologyContextId);
            return Status.success();
        }

        private EntitiesAndSettingsSnapshot getEntitiesAndSettingsSnapshot() {
            return entitiesAndSettingsSnapshot;
        }
    }

    /**
     * Log a simple sumary of the actions in the {@link ActionPlan} being processed.
     */
    public static class ActionPlanSummaryStage extends RequiredPassthroughStage<ActionPlanAndStore> {

        private ActionCounts actionStoreStartingCounts;
        private final FromContext<ActionCounts> previousPlanCounts =
            requiresFromContext(ActionPipelineContextMembers.PREVIOUS_ACTION_PLAN_COUNTS);
        private final FromContext<ActionCounts> currentPlanCounts =
            requiresFromContext(ActionPipelineContextMembers.CURRENT_ACTION_PLAN_COUNTS);

        /**
         * Create a new {@link ActionPlanSummaryStage}.
         */
        public ActionPlanSummaryStage() {
            providesToContext(ActionPipelineContextMembers.ACTION_STORE_STARTING_COUNTS,
                (Supplier<ActionCounts>)this::getActionStoreStartingCounts);
        }

        @Nonnull
        @Override
        public Status passthrough(ActionPlanAndStore input) {
            actionStoreStartingCounts = input.actionStore.getActionCounts();
            return Status.success(currentPlanCounts.get().difference(previousPlanCounts.get()));
        }

        public ActionCounts getActionStoreStartingCounts() {
            return actionStoreStartingCounts;
        }
    }

    /**
     * Log a simple summary breaking down the actions in the {@link ActionStore} after processing
     * the {@link ActionPlan}. Also includes a breakdown of the difference in counts in comparison
     * to the actions in the store prior to processing the new plan.
     */
    public static class ActionStoreSummaryStage extends RequiredPassthroughStage<LiveActionStore> {

        private final FromContext<ActionCounts> actionStoreStartingCounts =
            requiresFromContext(ActionPipelineContextMembers.ACTION_STORE_STARTING_COUNTS);

        @Nonnull
        @Override
        public Status passthrough(LiveActionStore input) {
            final ActionCounts endingCounts = input.getActionCounts();
            endingCounts.setTitle("Live Action Store Counts (difference from before action plan processing in parentheses): ");
            return Status.success(endingCounts.difference(actionStoreStartingCounts.get()));
        }
    }

    /**
     * This step sets the support level for actions by querying the probe capability cache and is
     * used by both market and atomic actions.
     * <p/>
     * Attempt to fully refresh the cache - this gets the most up-to-date target and
     * probe information from the topology processor.
     * <p/>
     * Note - we don't REALLY need to do this, because the cache tries to stay up to date by
     * listening for probe registrations and target additions/removals. But fully refreshing
     * it is cheap, so we do it to be safe.
     */
    public static class RefreshProbeCapabilitiesStage extends RequiredPassthroughStage<ActionPlanAndStore> {

        private final ProbeCapabilityCache probeCapabilityCache;

        /**
         * Create a new {@link RefreshProbeCapabilitiesStage}.
         *
         * @param probeCapabilityCache The {@link ProbeCapabilityCache}.
         */
        public RefreshProbeCapabilitiesStage(@Nonnull final ProbeCapabilityCache probeCapabilityCache) {
            this.probeCapabilityCache = Objects.requireNonNull(probeCapabilityCache);
        }

        @Nonnull
        @Override
        public Status passthrough(ActionPlanAndStore input) {
            probeCapabilityCache.fullRefresh();
            return Status.success();
        }
    }

    /**
     * RecommendationTracker created to remove actions which have been already executed recently
     * but re-recommended by the market. We do this by querying historical, executed actions
     * from the database. This information is later used during the {@link SupportLevelAndPrerequisitesStage}
     * to skip recently executed actions since there may be a small lag with regards to action execution
     * between the current state of the world and what the market knew when it generated actions.
     */
    public static class CreateLastExecutedRecommendationsTrackerStage
        extends RequiredPassthroughStage<ActionPlanAndStore> {

        private static final Logger logger = LogManager.getLogger();
        private final Clock clock;
        private final ActionHistoryDao actionHistoryDao;
        private final IActionFactory actionFactory;
        private final int queryTimeWindowForLastExecutedActionsMins;

        private final RecommendationTracker lastExecutedRecommendationsTracker;

        /**
         * Create the stage.
         *
         * @param actionHistoryDao DAO layer working with executed actions.
         * @param actionFactory The {@link IActionFactory} for creating actions.
         * @param clock the {@link Clock}.
         * @param queryTimeWindowForLastExecutedActionsMins time window within which actions will not
         *                                                  be populated if they are already executed (SUCEEDED).
         */
        public CreateLastExecutedRecommendationsTrackerStage(@Nonnull final ActionHistoryDao actionHistoryDao,
                                                             @Nonnull final IActionFactory actionFactory,
                                                             @Nonnull final Clock clock,
                                                             final int queryTimeWindowForLastExecutedActionsMins) {
            Preconditions.checkArgument(queryTimeWindowForLastExecutedActionsMins > 0,
                "Illegal value %s for queryTimeWindowForLastExecutedActionsMins",
                queryTimeWindowForLastExecutedActionsMins);

            this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
            this.actionFactory = Objects.requireNonNull(actionFactory);
            this.clock = Objects.requireNonNull(clock);
            this.queryTimeWindowForLastExecutedActionsMins = queryTimeWindowForLastExecutedActionsMins;

            lastExecutedRecommendationsTracker = providesToContext(
                LAST_EXECUTED_RECOMMENDATIONS_TRACKER, new RecommendationTracker());
        }

        @Nonnull
        @Override
        public Status passthrough(ActionPlanAndStore input) {
            // Remove actions which have been already executed recently. Executed actions could be re-recommended by market,
            // if the discovery has not happened and TP broadcasts the old topology.
            // NOTE: A corner case which is not handled: If the discovery is delayed for a long time, then just looking at the last n minutes
            // in the action_history DB may not be enough. We need to know the "freshness" of the discovered results. But this facility is
            // not currently available. So we don't handle this case.
            final LocalDateTime startDate = LocalDateTime.now(clock).minusMinutes(queryTimeWindowForLastExecutedActionsMins);
            final LocalDateTime endDate = LocalDateTime.now(clock);
            String warningMessage = null;

            List<ActionView> lastSuccessfullyExecutedActions = new ArrayList<>();
            try {
                lastSuccessfullyExecutedActions = actionHistoryDao.getActionHistoryByDate(startDate, endDate);
            } catch (DataAccessException dae) {
                // We continue on DB exception as we don't want to block actions.
                logger.warn("Error while fetching last executed actions from action history", dae);
                warningMessage = dae.getMessage();
            }

            lastSuccessfullyExecutedActions.stream()
                .filter(actionView -> actionView.getState().equals(ActionState.SUCCEEDED))
                .filter(LiveActionStore::isNonRepeatableAction)
                .forEach(actionView ->
                    lastExecutedRecommendationsTracker.add(actionFactory.newAction(
                        actionView.getRecommendation(), actionView.getActionPlanId(),
                        actionView.getRecommendationOid())));

            return warningMessage == null ? Status.success() : Status.withWarnings(warningMessage);
        }
    }

    /**
     * The {@link ActionIdentityStage} assigns durable, persistent IDs to action model objects.
     * Runs as part of both the market and atomic actions segment.
     */
    public static class ActionIdentityStage extends Stage<ActionDTOsAndStore, IdentifiedActionsAndStore> {

        private final IdentityService<ActionInfo> actionIdentityService;
        private int inputActionCount;

        /**
         * Create a new {@link ActionIdentityStage}.
         *
         * @param actionIdentityService The identity service for assigning action OIDs.
         * @param inputActionCount The context member this stage will provide with regards to the
         *                         number of actions being processed.
         */
        public ActionIdentityStage(@Nonnull final IdentityService<ActionInfo> actionIdentityService,
                                   @Nonnull final PipelineContextMemberDefinition<Integer> inputActionCount) {
            this.actionIdentityService = actionIdentityService;
            providesToContext(inputActionCount, (Supplier<Integer>)this::getInputActionCount);
        }

        @Nonnull
        @Override
        protected StageResult<IdentifiedActionsAndStore> executeStage(@Nonnull ActionDTOsAndStore input)
            throws PipelineStageException {
            inputActionCount = input.actionDTOs.size();
            try {
                final List<Long> oids = actionIdentityService.getOidsForObjects(input.actionDTOs.stream()
                    .map(ActionDTO.Action::getInfo)
                    .collect(Collectors.toList()));
                final List<IdentifiedActionDTO> identifiedActions = Streams
                    .zip(input.actionDTOs.stream(), oids.stream(), IdentifiedActionDTO::new)
                    .collect(Collectors.toList());

                return StageResult.withResult(new IdentifiedActionsAndStore(identifiedActions, input.actionStore))
                    .andStatus(Status.success("Assigned " + oids.size() + " action identities"));
            } catch (IdentityServiceException e) {
                throw new PipelineStageException("Unable to assign Action OIDs", e);
            }
        }

        private int getInputActionCount() {
            return inputActionCount;
        }
    }

    /**
     * The {@link SupportLevelAndPrerequisitesStage} assigns support level and prerequsiite data
     * to actions. It will also skip over any recently executed actions that the market did not
     * know were executed when it generated the action plan being processed.
     */
    public static class SupportLevelAndPrerequisitesStage
        extends Stage<IdentifiedActionsAndStore, IdentifiedActionsAndStore> {

        private static final Logger logger = LogManager.getLogger();
        private static final DataMetricGauge SUPPORT_LEVELS = DataMetricGauge.builder()
            .withName("ao_live_action_support_level_gauge")
            .withHelp("Current number of actions of various support levels in the live action store.")
            .withLabelNames("support_level")
            .build();

        private final ActionTargetSelector actionTargetSelector;

        private final FromContext<RecommendationTracker> lastExecutedRecommendationsTracker = requiresFromContext(
            ActionPipelineContextMembers.LAST_EXECUTED_RECOMMENDATIONS_TRACKER);
        private final FromContext<EntitiesAndSettingsSnapshot> entitiesAndSettingsSnapshot =
            requiresFromContext(ENTITIES_AND_SETTINGS_SNAPSHOT);

        /**
         * Create the {@link SupportLevelAndPrerequisitesStage}.
         *
         * @param actionTargetSelector The {@link ActionTargetSelector} for selecting the execution
         *                             targets for the actions being processed.
         */
        public SupportLevelAndPrerequisitesStage(@Nonnull final ActionTargetSelector actionTargetSelector) {
            this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
        }

        @Nonnull
        @Override
        protected StageResult<IdentifiedActionsAndStore> executeStage(@Nonnull IdentifiedActionsAndStore input) {
            final ArrayList<IdentifiedActionDTO> retainedActions = new ArrayList<>(input.actions.size());
            final RecommendationTracker previouslyExecutedTracker = lastExecutedRecommendationsTracker.get();
            for (IdentifiedActionDTO action : input.actions) {
                if (previouslyExecutedTracker.take(action.oid).isPresent()) {
                    logger.debug("Skipping action: {} as it has already been executed", action.action);
                } else {
                    retainedActions.add(action);
                }
            }

            final Map<Long, ActionTargetInfo> actionAndTargetInfo =
                actionTargetSelector.getTargetsForActions(retainedActions.stream().map(a -> a.action),
                    entitiesAndSettingsSnapshot.get(), Collections.emptyMap());

            // Increment the relevant counters.
            final Map<SupportLevel, Long> actionsBySupportLevel =
                actionAndTargetInfo.values().stream()
                    .collect(Collectors.groupingBy(ActionTargetInfo::supportingLevel, Collectors.counting()));
            logger.info("Action support counts: {}", actionsBySupportLevel);

            // Zero out values and then set gauge to the corresponding action counts
            SUPPORT_LEVELS.getLabeledMetrics().values()
                .forEach(gaugeData -> gaugeData.setData(0.0));
            actionsBySupportLevel.forEach((supportLevel, numActions) -> {
                SUPPORT_LEVELS.labels(supportLevel.name()).setData((double)numActions);
            });

            final List<IdentifiedActionDTO> withPrerequisitesAndSupportLevel = retainedActions.stream()
                .map(action -> addPrerequisitesAndSupportLevel(action, actionAndTargetInfo))
                .collect(Collectors.toList());

            final int numDiscardedActions = input.actions.size() - withPrerequisitesAndSupportLevel.size();
            final StringBuilder statusMessage = buildStatusMessage(
                numDiscardedActions, actionsBySupportLevel);

            return StageResult.withResult(
                new IdentifiedActionsAndStore(withPrerequisitesAndSupportLevel, input.actionStore))
                .andStatus(Status.success(statusMessage.toString()));
        }

        @Nonnull
        private StringBuilder buildStatusMessage(final int numDiscardedActions,
                                                 @Nonnull final  Map<SupportLevel, Long> actionsBySupportLevel) {
            final StringBuilder statusMessage = new StringBuilder();

            statusMessage.append("Discarded ").append(numDiscardedActions)
                .append(" recently executed actions that were recommended again.\n")
                .append("Actions by support level:\n\t")
                .append(actionsBySupportLevel.entrySet().stream()
                    .map(e -> e.getKey().toString() + ": " + String.format("%,d", e.getValue()))
                    .collect(Collectors.joining("\n\t")));
            return statusMessage;
        }

        private IdentifiedActionDTO addPrerequisitesAndSupportLevel(@Nonnull final IdentifiedActionDTO identifiedAction,
                                                                    @Nonnull final Map<Long, ActionTargetInfo> actionAndTargetInfo) {
            final ActionDTO.Action action = identifiedAction.action;
            final ActionTargetInfo targetInfo = actionAndTargetInfo.get(action.getId());
            final SupportLevel supportLevel = Optional.ofNullable(targetInfo)
                .map(ActionTargetInfo::supportingLevel)
                .orElse(SupportLevel.UNSUPPORTED);
            final Set<Prerequisite> prerequisites = Optional.ofNullable(targetInfo)
                .map(ActionTargetInfo::prerequisites)
                .orElse(Collections.emptySet());
            final Boolean disruptiveNew = Optional.ofNullable(targetInfo)
                .map(ActionTargetInfo::disruptive)
                .orElse(null);
            final Boolean reversibleNew = Optional.ofNullable(targetInfo)
                .map(ActionTargetInfo::reversible)
                .orElse(null);

            final Boolean disruptiveOld = action.hasDisruptive() ? action.getDisruptive() : null;
            final Boolean reversibleOld = action.hasReversible() ? action.getReversible() : null;

            // If there are any updates to the action, update and rebuild it.
            if (action.getSupportingLevel() != supportLevel || !prerequisites.isEmpty()
                || !Objects.equals(disruptiveNew, disruptiveOld)
                || !Objects.equals(reversibleNew, reversibleOld)) {
                final ActionDTO.Action.Builder actionBuilder = action.toBuilder()
                    .setSupportingLevel(supportLevel)
                    .addAllPrerequisite(prerequisites);
                if (disruptiveNew != null) {
                    actionBuilder.setDisruptive(disruptiveNew);
                } else {
                    actionBuilder.clearDisruptive();
                }
                if (reversibleNew != null) {
                    actionBuilder.setReversible(reversibleNew);
                } else {
                    actionBuilder.clearReversible();
                }
                return new IdentifiedActionDTO(actionBuilder.build(), identifiedAction.oid);
            } else {
                return identifiedAction;
            }
        }
    }

    /**
     * The {@link CreateAtomicActionsStage} takes information from the {@link PrepareAggregatedActionsStage}
     * as well as the {@link MarketActionsSegment} in order to merge actions together to create
     * atomic actions. Atomic actions are created to combine the actions the market generated for
     * individual service entity replicas that are controlled or managed by a single, shared configuration
     * (ie individual Container replicas which are controlled via WorkloadControllers).
     */
    public static class CreateAtomicActionsStage extends Stage<LiveActionStore, ActionDTOsAndStore> {
        private static final Logger logger = LogManager.getLogger();

        private final FromContext<Map<Long, AggregatedAction>> aggregatedActions =
            requiresFromContext(ActionPipelineContextMembers.AGGREGATED_ACTIONS);

        private final AtomicActionFactory atomicActionFactory;

        /**
         * Create the {@link CreateAtomicActionsStage}.
         *
         * @param atomicActionFactory The {@link AtomicActionFactory} for creating atomic actions.
         */
        public CreateAtomicActionsStage(@Nonnull final AtomicActionFactory atomicActionFactory) {
            this.atomicActionFactory = Objects.requireNonNull(atomicActionFactory);
        }

        @Nonnull
        @Override
        protected StageResult<ActionDTOsAndStore> executeStage(@Nonnull LiveActionStore input) {
            // First create the action DTOs for the atomic actions
            final List<AtomicActionResult> atomicActionResults =
                atomicActionFactory.atomicActions(aggregatedActions.get());

            // List of all the Action DTOs for the atomic actions that will be created
            // The aggregated atomic actions that will be executed by the aggregation target
            final List<ActionDTO.Action> atomicActions = atomicActionResults.stream()
                .filter(atomicActionResult -> atomicActionResult.atomicAction().isPresent())
                .map(atomicActionResult -> atomicActionResult.atomicAction().get())
                .collect(Collectors.toList());
            final int executableAtomicActionsCount = atomicActions.size();

            // The de-duplicated atomic actions that were merged inside the aggregated atomic actions above
            // These actions are non-executable
            final List<ActionDTO.Action> deDupedAtomicActions = atomicActionResults.stream()
                .flatMap(atomicActionResult -> atomicActionResult.deDuplicatedActions().keySet().stream())
                .collect(Collectors.toList());

            atomicActions.addAll(deDupedAtomicActions);
            logger.info("Created {} atomic actions, including {} executable atomic actions and {} "
                    + "non-executable deDuplicated actions",
                atomicActions.size(), executableAtomicActionsCount, deDupedAtomicActions.size());
            final StringBuilder stringBuilder = new StringBuilder();

            return StageResult.withResult(new ActionDTOsAndStore(atomicActions, input))
                .andStatus(Status.success(stringBuilder.append("Atomic Actions: ")
                    .append(atomicActions.size())
                    .append("\nExecutable Atomic Actions: ")
                    .append(executableAtomicActionsCount)
                    .append("\nNon-executable DeDuplicated Actions: ")
                    .append(deDupedAtomicActions.size())
                    .toString()));
        }
    }

    /**
     * The {@link CompilePreviousActionsStage} compiles actions previously recommended by the market and
     * currently in the {@link ActionStore} into a {@link RecommendationTracker} for use in computing
     * what actions are new, which are re-recommended, and which are no longer recommended and should be
     * cleared.
     */
    public static class CompilePreviousActionsStage extends RequiredPassthroughStage<IdentifiedActionsAndStore> {

        private final ActionDifference actionDifference = new ActionDifference();
        private final FromContext<List<ActionView>> completedSinceLastPopulate =
            requiresFromContext(ActionPipelineContextMembers.COMPLETED_SINCE_LAST_POPULATE);
        private final ActionSource actionSource;

        /**
         * Create a new {@link CompilePreviousActionsStage}.
         *
         * @param actionDifferenceMember The context member for the {@link ActionDifference} to be used
         *                               in the pipeline segment (either market or atomic) where this
         *                               stage is to be run.
         * @param actionSource The {@link ActionSource} of the actions (either market or atomic).
         */
        public CompilePreviousActionsStage(
            @Nonnull final PipelineContextMemberDefinition<ActionDifference> actionDifferenceMember,
            @Nonnull final ActionSource actionSource) {
            providesToContext(actionDifferenceMember, actionDifference);
            this.actionSource = Objects.requireNonNull(actionSource);
        }

        @Nonnull
        @Override
        public Status passthrough(IdentifiedActionsAndStore input) {
            input.actionStore.compilePreviousActions(actionDifference.previousRecommendations,
                    completedSinceLastPopulate.get(), actionDifference.actionsToRemove, actionSource);

            return Status.success();
        }
    }

    /**
     * Process actions that have been re-recommended by the market. When executing this stage
     * for market actions (as opposed to atomic actions) we also have to update
     * {@link AggregatedAction} action views in addition to the normal logic of the
     * {@link UpdateReRecommendedActionsStage}.
     */
    public static class MarketReRecommendedActionsStage extends UpdateReRecommendedActionsStage {

        private final FromContext<Map<Long, AggregatedAction>> actionIdToAggregateAction =
            requiresFromContext(ActionPipelineContextMembers.ACTION_ID_TO_AGGREGATE_ACTION);

        /**
         * Market actions that were merged to create the atomic actions. These actions should be removed
         * from the action store because we retain the atomic actions in favor of the original market
         * actions.
         */
        private final List<Action> mergedActions = providesToContext(
            ActionPipelineContextMembers.MERGED_MARKET_ACTIONS_FOR_ATOMIC, new ArrayList<>());

        /**
         * Create a new {@link UpdateReRecommendedActionsStage}.
         *
         * @param actionFactory A factory for creating action model objectcs.
         */
        public MarketReRecommendedActionsStage(@Nonnull final IActionFactory actionFactory) {
            super(actionFactory, ActionPipelineContextMembers.MARKET.getActionDifference(),
                ActionPipelineContextMembers.MARKET.getNewActionCount());
        }

        @Override
        protected void processAction(@Nonnull final IdentifiedActionDTO identifiedAction,
                                     @Nonnull final Action action) {
            final Map<Long, AggregatedAction> actionsToAggregateActions = actionIdToAggregateAction.get();

            // while iterating over action views for action dTOs, save the action views
            // for the market actions that will be merged in the atomic actions
            if (actionsToAggregateActions.containsKey(identifiedAction.action.getId())) {
                mergedActions.add(action);
                final AggregatedAction aa = actionsToAggregateActions.get(identifiedAction.action.getId());
                aa.updateActionView(identifiedAction.action.getId(), action);
            }
        }
    }

    /**
     * Update actions that have been re-recommended by the market. This includes things like
     * minor updates to a prior recommendation from the market including to the action category,
     * creating action model objects for market action DTO recommendations, and categorizing
     * each action to decide if it needs a translation or not.
     */
    public static class UpdateReRecommendedActionsStage extends Stage<IdentifiedActionsAndStore, ActionsToTranslateAndStore> {
        private static final Logger logger = LogManager.getLogger();

        /**
         * The number of newly recommended actions from the market that we will attempt to store.
         */
        private int newActionCount = 0;

        private final IActionFactory actionFactory;

        private final FromContext<ActionDifference> actionDifference;

        /**
         * Create a new {@link UpdateReRecommendedActionsStage}.
         *
         * @param actionFactory A factory for creating action model objects.
         * @param actionDifferenceMember The context member for the {@link ActionDifference} used in
         *                               this pipeline segment (either market or atomic).
         * @param newActionCount The context member for the count of new actions to be provided by this stage.
         */
        public UpdateReRecommendedActionsStage(@Nonnull final IActionFactory actionFactory,
                                               @Nonnull final PipelineContextMemberDefinition<ActionDifference> actionDifferenceMember,
                                               @Nonnull final PipelineContextMemberDefinition<Integer> newActionCount) {
            this.actionFactory = Objects.requireNonNull(actionFactory);
            actionDifference = requiresFromContext(actionDifferenceMember);
            providesToContext(newActionCount, (Supplier<Integer>)this::getNewActionCount);
        }

        @Nonnull
        @Override
        protected StageResult<ActionsToTranslateAndStore> executeStage(@Nonnull IdentifiedActionsAndStore input) {
            final List<Action> actionsToTranslate = new ArrayList<>(input.actions.size());
            final RecommendationTracker recommendations = actionDifference.get().previousRecommendations;
            final List<Action> actionsToAdd = actionDifference.get().actionsToAdd;

            final int totalActions = input.actions.size();
            for (IdentifiedActionDTO identifiedAction : input.actions) {
                final Action action = processRecommendation(recommendations, identifiedAction);
                processAction(identifiedAction, action);

                categorizeAction(actionsToTranslate, actionsToAdd, action);
            }

            final StringBuilder builder = new StringBuilder()
                .append("Of ").append(String.format("%,d", totalActions))
                .append(" actions, ").append(String.format("%,d", totalActions - newActionCount))
                .append(" were re-recommended and ").append(String.format("%,d", newActionCount))
                .append(" were new.");

            return StageResult.withResult(new ActionsToTranslateAndStore(actionsToTranslate, input.actionStore))
                .andStatus(Status.success(builder.toString()));
        }

        protected void processAction(@Nonnull final IdentifiedActionDTO identifiedAction,
                                     @Nonnull final Action action) {
            // Nothing to do in the base class, subclasses may override.
        }

        private void categorizeAction(@Nonnull final  List<Action> actionsToTranslate,
                                      @Nonnull final  List<Action> actionsToAdd,
                                      @Nonnull final  Action action) {
            final ActionState actionState = action.getState();
            switch (actionState) {
                case READY:             // fall through
                case ACCEPTED:          // fall through
                case REJECTED:
                    actionsToTranslate.add(action);
                    break;
                case QUEUED:            // fall through
                case PRE_IN_PROGRESS:   // fall through
                case IN_PROGRESS:       // fall through
                case POST_IN_PROGRESS:  // fall through
                    // retain the action which is in progress of executing, but without updating translation for it
                    actionsToAdd.add(action);
                    break;
                case FAILED:            // fall through
                case SUCCEEDED:         // fall through
                case CLEARED:           // fall through
                case FAILING:           // fall through
                    // do nothing
                    break;
                default:
                    throw new IllegalArgumentException("Unknown action state " + action.getState()
                        + " on action with OID " + action.getId());
            }
        }

        @Nonnull
        private Action processRecommendation(RecommendationTracker recommendations, IdentifiedActionDTO identifiedAction) {
            final long recommendationOid = identifiedAction.oid;
            final Optional<Action> existingActionOpt = recommendations.take(recommendationOid);
            final Action action;
            if (existingActionOpt.isPresent()) {
                action = existingActionOpt.get();
                if (action.getState() == ActionState.SUCCEEDED) {
                    logger.debug("Action {} has been executed successfully recently"
                            + " but is now being recommended again by the market.",
                        action.getDescription());
                }
                // If we are re-using an existing action, we should update the recommendation
                // so other properties that may have changed (e.g. importance, executability)
                // reflect the most recent recommendation from the market. However, we only
                // do this for "READY", "ACCEPTED", "REJECTED" actions.
                // An IN_PROGRESS or QUEUED action is considered "fixed" until it either succeeds or fails.
                // TODO (roman, Oct 31 2018): If a QUEUED action becomes non-executable, it
                // may be worth clearing it.
                if (action.getState() == ActionState.READY
                    || action.getState() == ActionState.ACCEPTED
                    || action.getState() == ActionState.REJECTED) {
                    action.updateRecommendationAndCategory(identifiedAction.action);
                }
            } else {
                newActionCount++;
                action = actionFactory.newAction(identifiedAction.action, getContext().getActionPlanId(),
                    recommendationOid);
            }
            return action;
        }

        private int getNewActionCount() {
            return newActionCount;
        }
    }

    /**
     * The {@link TranslateActionsStage} translates actions from the market's domain-agnostic actions
     * into real-world domain-specific actions. {@see ActionTranslator}.
     */
    public static class TranslateActionsStage extends Stage<ActionsToTranslateAndStore, LiveActionStore> {
        private static final Logger logger = LogManager.getLogger();

        private final ActionTranslator actionTranslator;
        private final FromContext<EntitiesAndSettingsSnapshot> entitiesAndSettingsSnapshot =
            requiresFromContext(ENTITIES_AND_SETTINGS_SNAPSHOT);
        private final FromContext<ActionDifference> actionDifference;

        /**
         * Create a new {@link TranslateActionsStage}.
         *
         * @param actionTranslator The {@link ActionTranslator} to use when translating the actions.
         * @param actionDifferenceMember The context member to use for looking up the {@link ActionDifference} for
         *                               the current processing segment (either market or realtime).
         */
        public TranslateActionsStage(@Nonnull final ActionTranslator actionTranslator,
                                     @Nonnull final PipelineContextMemberDefinition<ActionDifference> actionDifferenceMember) {
            this.actionTranslator = Objects.requireNonNull(actionTranslator);
            actionDifference = requiresFromContext(actionDifferenceMember);
        }

        @Nonnull
        @Override
        protected StageResult<LiveActionStore> executeStage(@Nonnull ActionsToTranslateAndStore input) {
            final Stream<Action> translatedReadyActions =
                actionTranslator.translate(input.actionsToTranslate.stream(), entitiesAndSettingsSnapshot.get());

            final List<Action> toRemove = actionDifference.get().actionsToRemove;
            final List<Action> toAdd = actionDifference.get().actionsToAdd;
            final int initialRemoveCount = toRemove.size();
            final int initialAddCount = toAdd.size();

            // This actually drains the stream defined above, updating the recommendation, creating
            // new actions, and so on.
            translatedReadyActions.forEach(action -> {
                if (action.getTranslationStatus() == TranslationStatus.TRANSLATION_FAILED) {
                    // Make sure to remove the actions with failed translations.
                    // We don't send NotRecommendedEvent-s because the action is still recommended
                    // but it's useless.
                    toRemove.add(action);
                    logger.trace("Removed action {} with failed translation. Full action: {}",
                        action.getId(), action);
                } else {
                    toAdd.add(action);
                }
            });

            // We don't explicitly clear actions that were not successfully translated.
            final int removeCount = toRemove.size() - initialRemoveCount;
            final int addCount = toAdd.size() - initialAddCount;
            if (removeCount > 0) {
                logger.warn("Dropped {} actions due to failed translations.", removeCount);
            }

            final StringBuilder builder = new StringBuilder()
                .append("Successfully translated ").append(String.format("%,d", addCount)).append(" actions\n")
                .append("Dropped ").append(String.format("%,d", removeCount))
                .append(" actions due to failed translations");

            return StageResult.withResult(input.actionStore)
                .andStatus(Status.success(builder.toString()));
        }
    }

    /**
     * The {@link AddRemoveAndClearActionsStage} adds, removes, or clears actions from the action store.
     */
    public abstract static class AddRemoveAndClearActionsStage extends RequiredPassthroughStage<LiveActionStore> {
        private static final Logger logger = LogManager.getLogger();

        private final FromContext<ActionDifference> actionDifference;
        private final FromContext<EntitiesAndSettingsSnapshot> entitiesAndSettingsSnapshot =
            requiresFromContext(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT);
        private final FromContext<Integer> newActionCount;
        private final FromContext<Integer> inputActionCount;

        /**
         * Create a new {@link AddRemoveAndClearActionsStage}.
         *
         * @param actionDifferenceMember The context member for the {@link ActionDifference} used in
         *                               this pipeline segment.
         * @param inputActionCountMember The context member for the total number of actions input to the segment.
         * @param newActionCountMember The context member for the number of new actions handled by the segment.
         * @param actionsToAdd The list of actions to add to the store.
         */
        public AddRemoveAndClearActionsStage(
            @Nonnull final PipelineContextMemberDefinition<ActionDifference> actionDifferenceMember,
            @Nonnull final PipelineContextMemberDefinition<Integer> inputActionCountMember,
            @Nonnull final PipelineContextMemberDefinition<Integer> newActionCountMember,
            @Nonnull final PipelineContextMemberDefinition<List<Action>> actionsToAdd) {
            actionDifference = requiresFromContext(actionDifferenceMember);
            newActionCount = requiresFromContext(newActionCountMember);
            inputActionCount = requiresFromContext(inputActionCountMember);
            providesToContext(actionsToAdd, (Supplier<List<Action>>)() -> actionDifference.get().actionsToAdd);
        }

        @Nonnull
        @Override
        public Status passthrough(LiveActionStore input) {
            updateActions(input, entitiesAndSettingsSnapshot.get(),
                actionDifference.get().actionsToRemove, actionDifference.get().actionsToAdd);

            final long actionPlanId = getContext().getActionPlanId();
            final int reRecommendedCount = (inputActionCount.get() - newActionCount.get());
            logger.info("Number of Re-Recommended actions={}, Newly created actions={}",
                reRecommendedCount, newActionCount.get());

            // Clear READY or QUEUED actions that were not re-recommended. If they were
            // re-recommended, they would have been removed from the RecommendationTracker
            // above.
            final MutableInt notRecommendedCount = new MutableInt(0);
            StreamSupport.stream(actionDifference.get().previousRecommendations.spliterator(), false)
                .filter(action -> (action.getState() == ActionState.READY
                    || action.getState() == ActionState.QUEUED))
                .forEach(action -> {
                    notRecommendedCount.increment();
                    action.receive(new NotRecommendedEvent(actionPlanId));
                });

            final StringBuilder builder = new StringBuilder()
                .append("Re-recommended actions:  ")
                .append(String.format("%,d", reRecommendedCount))
                .append("\nNew actions: ")
                .append(String.format("%,d", newActionCount.get()))
                .append("\nCleared actions: ")
                .append(String.format("%,d", notRecommendedCount.getValue()));

            return Status.success(builder.toString());
        }

        /**
         * Performs the additional and removal from the action store. Separate implementations for
         * market vs atomic actions.
         *
         * @param actionStore The {@link ActionStore}.
         * @param entitiesAndSettingsSnapshot The {@link EntitiesAndSettingsSnapshot}.
         * @param actionsToRemove The actions to remove from the store.
         * @param actionsToAdd The actions to add to the store.
         */
        protected abstract void updateActions(@Nonnull LiveActionStore actionStore,
                                              @Nonnull EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot,
                                              @Nonnull List<Action> actionsToRemove,
                                              @Nonnull List<Action> actionsToAdd);
    }

    /**
     * Add, remove, and clear market actions.
     */
    public static class AddRemoveAndClearMarketActionsStage extends AddRemoveAndClearActionsStage {
        /**
         * Create a new {@link AddRemoveAndClearMarketActionsStage}.
         *
         * @param actionDifferenceMember The context member for the {@link ActionDifference} used in
         *                               this pipeline segment.
         * @param inputActionCountMember The context member for the total number of actions input to the segment.
         * @param newActionCountMember The context member for the number of new actions handled by the segment.
         * @param actionsToAdd The list of actions to add to the store.
         */
        public AddRemoveAndClearMarketActionsStage(
            @Nonnull PipelineContextMemberDefinition<ActionDifference> actionDifferenceMember,
            @Nonnull PipelineContextMemberDefinition<Integer> inputActionCountMember,
            @Nonnull PipelineContextMemberDefinition<Integer> newActionCountMember,
            @Nonnull PipelineContextMemberDefinition<List<Action>> actionsToAdd) {
            super(actionDifferenceMember, inputActionCountMember, newActionCountMember, actionsToAdd);
        }

        @Override
        protected void updateActions(@Nonnull LiveActionStore actionStore,
                                     @Nonnull EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot,
                                     @Nonnull List<Action> actionsToRemove,
                                     @Nonnull List<Action> actionsToAdd) {
            actionStore.updateMarketActions(entitiesAndSettingsSnapshot, actionsToRemove, actionsToAdd);
        }
    }

    /**
     * Add, remove, and clear atomic actions.
     */
    public static class AddRemoveAndClearAtomicActionsStage extends AddRemoveAndClearActionsStage {
        private final FromContext<List<Action>> mergedActions = requiresFromContext(
            ActionPipelineContextMembers.MERGED_MARKET_ACTIONS_FOR_ATOMIC);

        /**
         * Create a new {@link AddRemoveAndClearAtomicActionsStage}.
         *
         * @param actionDifferenceMember The context member for the {@link ActionDifference} used in
         *                               this pipeline segment.
         * @param inputActionCountMember The context member for the total number of actions input to the segment.
         * @param newActionCountMember The context member for the number of new actions handled by the segment.
         * @param actionsToAdd The list of actions to add to the store.
         */
        public AddRemoveAndClearAtomicActionsStage(
            @Nonnull PipelineContextMemberDefinition<ActionDifference> actionDifferenceMember,
            @Nonnull PipelineContextMemberDefinition<Integer> inputActionCountMember,
            @Nonnull PipelineContextMemberDefinition<Integer> newActionCountMember,
            @Nonnull PipelineContextMemberDefinition<List<Action>> actionsToAdd) {
            super(actionDifferenceMember, inputActionCountMember, newActionCountMember, actionsToAdd);
        }

        @Override
        protected void updateActions(@Nonnull LiveActionStore actionStore,
                                     @Nonnull EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot,
                                     @Nonnull List<Action> actionsToRemove,
                                     @Nonnull List<Action> actionsToAdd) {
            actionStore.updateAtomicActions(entitiesAndSettingsSnapshot, actionsToRemove,
                actionsToAdd, mergedActions.get());
        }
    }

    /**
     * The {@link ActionStatisticsStage} records statistics about the actions in the store
     * to the database.
     */
    public static class ActionStatisticsStage extends RequiredPassthroughStage<LiveActionStore> {

        private static final Logger logger = LogManager.getLogger();

        /**
         * Specifies action count gauge.
         */
        private static final DataMetricGauge ACTION_COUNTS_GAUGE = DataMetricGauge.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "current_actions")
            .withHelp("Number of actions in the action orchestrator live store.")
            .withLabelNames("action_type", "entity_type", "environment", "category",
                "severity", "state")
            .build()
            .register();

        private final LiveActionsStatistician actionsStatistician;

        private final FromContext<List<ActionView>> completedSinceLastPopulate =
            requiresFromContext(ActionPipelineContextMembers.COMPLETED_SINCE_LAST_POPULATE);

        /**
         * Create a new {@link ActionStatisticsStage}.
         *
         * @param actionsStatistician The {@link LiveActionsStatistician} to use to generate and
         *                            persist the action statistics.
         */
        public ActionStatisticsStage(@Nonnull final LiveActionsStatistician actionsStatistician) {
            this.actionsStatistician = Objects.requireNonNull(actionsStatistician);
        }

        @Nonnull
        @Override
        public Status passthrough(LiveActionStore input) {
            final ActionStore actionStore = input;
            final TopologyInfo sourceTopologyInfo = getContext()
                .getActionPlanInfo()
                .getMarket()
                .getSourceTopologyInfo();
            // Need to make a copy because it's not safe to iterate otherwise.
            final Collection<Action> actions = actionStore.getActions().values();

            // Record the action stats.
            // TODO (roman, Nov 15 2018): For actions completed since the last snapshot, it may make
            // sense to use the last snapshot's time instead of the current snapshot's time.
            // Not doing it for now because of the extra complexity - and it's not clear if anyone
            // cares if the counts are off by ~10 minutes.
            actionsStatistician.recordActionStats(sourceTopologyInfo,
                // Only record user-visible actions.
                Stream.concat(completedSinceLastPopulate.get().stream(), actions.stream())
                    .filter(actionStore.getVisibilityPredicate()));

            updateActionMetricsDescriptor(actions, actionStore.getVisibilityPredicate());
            return Status.success("Recorded statistics and metrics for "
                + String.format("%,d", actions.size()) + " actions");
        }

        /**
         * Generate a descriptor string for the action and push to the metrics end point.
         *
         * @param actions the actions whose metrics should be updated.
         * @param visibilityPredicate A predicate function to determine which actions are visible
         *                            to the end user. We only track actions for visible actions.
         */
        private void updateActionMetricsDescriptor(@Nonnull final Collection<Action> actions,
                                                   @Nonnull final Predicate<ActionView> visibilityPredicate) {
            // Reset the values in the Gauge.
            ACTION_COUNTS_GAUGE.getLabeledMetrics().forEach((key, val) -> val.setData(0.0));
            actions.stream()
                // filter out invisible actions, same as what we do in LiveActions#get(ActionQueryFilter)
                // only visible actions are shown in UI
                .filter(visibilityPredicate)
                .forEach(this::updateActionMetricsDescriptor);
        }

        /**
         * Generate a descriptor string for the action and push to the metrics end point.
         *
         * @param action record the given action
         */
        private void updateActionMetricsDescriptor(final Action action) {
            String actionSeverity  = action.getActionSeverity().name();
            String actionCategory  = action.getActionCategory().name();
            String actionState  = action.getState().name();
            String entityType = null;
            String env = null;
            try {
                final ActionEntity actionTarget = ActionDTOUtil.getPrimaryEntity(
                    action.getTranslationResultOrOriginal(), false);
                if (actionTarget != null) {
                    entityType = EntityType.forNumber(actionTarget.getType()).name();
                }

                final ActionEnvironmentType envType =
                    ActionEnvironmentType.forAction(action.getTranslationResultOrOriginal());
                switch (envType) {
                    case ON_PREM:
                        env = EnvironmentType.ON_PREM.name();
                        break;
                    case CLOUD:
                        env = EnvironmentType.CLOUD.name();
                        break;
                    case ON_PREM_AND_CLOUD:
                        env = EnvironmentType.HYBRID.name();
                        break;
                }
            } catch (UnsupportedActionException e) {
                logger.error("Unsupported action {} found in action store: {}", action, e.getMessage());
            }

            ACTION_COUNTS_GAUGE.labels(action.getTranslationResultOrOriginal().getInfo().getActionTypeCase().name(),
                entityType, env, actionCategory, actionSeverity, actionState).increment();
        }
    }

    /**
     * The {@link AuditAndCacheBookkeepingStage} performs action auditing and various cache bookkeeping
     * (ie the {@link com.vmturbo.action.orchestrator.store.EntitiesWithNewStateCache} and
     * {@link com.vmturbo.action.orchestrator.store.EntitySeverityCache}.
     */
    public static class AuditAndCacheBookkeepingStage extends RequiredPassthroughStage<LiveActionStore> {

        private static final Logger logger = LogManager.getLogger();

        private final ActionAuditSender actionAuditSender;

        private final FromContext<EntitiesAndSettingsSnapshot> entitiesAndSettingsSnapshot =
            requiresFromContext(ActionPipelineContextMembers.ENTITIES_AND_SETTINGS_SNAPSHOT);
        private final FromContext<List<Action>> actionsToAdd = requiresFromContext(
            ActionPipelineContextMembers.MARKET.getActionsToAdd());
        private final FromContext<List<Action>> atomicActionsToAdd = requiresFromContext(
            ActionPipelineContextMembers.ATOMIC.getActionsToAdd());

        /**
         * Create a new {@link AuditAndCacheBookkeepingStage}.
         *
         * @param actionAuditSender The {@link ActionAuditSender} for performing auditing of actions.
         */
        public AuditAndCacheBookkeepingStage(@Nonnull final ActionAuditSender actionAuditSender) {
            this.actionAuditSender = Objects.requireNonNull(actionAuditSender);
        }

        @Nonnull
        @Override
        public Status passthrough(LiveActionStore actionStore) throws InterruptedException {
            final TopologyInfo sourceTopologyInfo = getContext()
                .getActionPlanInfo()
                .getMarket()
                .getSourceTopologyInfo();

            final int deletedActions = actionStore
                .getEntitiesWithNewStateCache()
                .clearActionsAndUpdateCache(sourceTopologyInfo.getTopologyId());
            // Need to make a copy because it's not safe to iterate otherwise.
            final Map<Long, Action> actions = actionStore.getActions();

            // Get actions for audit. Don't use directly actionsToAdd because it could
            // contain actions that were merged (during populating atomic actions) and as a result
            // some of initially recommended actions were removed.
            final List<ActionView> actionsForAudit =
                Streams.concat(actionsToAdd.get().stream(),
                    // The atomic actions will not appear in actionsToAdd.
                    // populateAtomicActions places them in translatedAtomicActionsToAdd
                    atomicActionsToAdd.get().stream())
                    .map(action -> actions.get(action.getId()))
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
            // We need to call auditOnGeneration once so that the book keeping can determine what
            // is new and what needs to be cleared by comparing what was provided in the last cycle.
            // As a result, this should not be called multiple times per market cycle.
            auditOnGeneration(actionsForAudit, entitiesAndSettingsSnapshot.get());

            if (deletedActions > 0) {
                actionStore.getEntitySeverityCache().ifPresent(cache -> cache.refresh(actionStore));
            }

            return Status.success("Audit on generation for "
                + String.format("%,d", actionsForAudit.size()) + " actions");
        }

        private void auditOnGeneration(
            @Nonnull Collection<? extends ActionView> newActions,
            @Nonnull EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot)
            throws InterruptedException {
            try {
                // Even if the list is empty, we need ActionAuditSender to update it's book keeping.
                // Previously there was an optimization that checked if the list was non-empty.
                actionAuditSender.sendOnGenerationEvents(newActions, entitiesAndSettingsSnapshot);
            } catch (CommunicationException | UnsupportedActionException | ExecutionInitiationException e) {
                logger.warn(
                    "Failed sending audit event \"on generation event\" for actions " + newActions,
                    e);
            }
        }
    }

    /**
     * The {@link MarketActionsSegment} is responsible for processing market-generated actions
     * (as opposed to atomic actions). The {@link SegmentStage} wraps a mini-pipeline of other
     * stages that together are responsible for processing market actions. A separate, similar
     * segment processes atomic actions.
     */
    public static class MarketActionsSegment extends SegmentStage<
        ActionPlanAndStore, ActionDTOsAndStore, LiveActionStore, LiveActionStore, ActionPipelineContext> {

        /**
         * Construct a new {@link MarketActionsSegment}.
         *
         * @param segmentDefinition The definition for the stages in the interior pipeline segment to be run when
         *                          this stage runs.
         */
        public MarketActionsSegment(
            @Nonnull SegmentDefinition<ActionDTOsAndStore, LiveActionStore, ActionPipelineContext> segmentDefinition) {
            super(segmentDefinition);
        }

        @Nonnull
        @Override
        protected ActionDTOsAndStore setupExecution(@Nonnull ActionPlanAndStore input) {
            // Transform the input to the stage into the input for the first stage of the segment.
            return new ActionDTOsAndStore(input.actionPlan.getActionList(), input.actionStore);
        }

        @Nonnull
        @Override
        protected StageResult<LiveActionStore> completeExecution(
            @Nonnull final StageResult<LiveActionStore> segmentResult) {
            return segmentResult;
        }
    }

    /**
     * The {@link PopulateLiveActionsSegment} is a {@link ExclusiveLockedSegmentStage} that wraps an inner
     * mini-pipeline for processing live actions. When created, a lock is injected which is acquired
     * when the segment starts and is released when finished, preventing multiple
     * {@link PopulateLiveActionsSegment}s from running at the same time and trashing some of the
     * data structures and caches that are shared across pipeline runs by overwriting each other's data.
     * <p/>
     * Note that some stages are safe to run outside this lock because they do not access any data
     * that could be stepped on by another, simultaneous action pipeline, or because all shared data
     * structures they access are safe to access by multiple pipelines simultaneously.
     */
    public static class PopulateLiveActionsSegment extends ExclusiveLockedSegmentStage<
            ActionPlanAndStore, ActionPlanAndStore, LiveActionStore, ActionStore, ActionPipelineContext> {

        /**
         * Construct a new {@link PopulateLiveActionsSegment}.
         *
         *  @param sharedLiveActionsLock All stages in the interior segment are executed while holding this shared lock.
         *                              This prevents multiple action processing pipelines from stepping on each
         *                              other when using some of the shared data structures used in the segment.
         * @param maxLockAcquireTimeMinutes Maximum amount of time to wait in minutes when attempting to acquire the
         *                                  lock. If unable to acquire the lock in this time, we throw an
         *                                  {@link InterruptedException}.
         * @param segmentDefinition The definition for the stages in the interior pipeline segment to be run when
         *                                  this stage runs.
         */
        public PopulateLiveActionsSegment(@Nonnull final ReentrantLock sharedLiveActionsLock,
                                          final long maxLockAcquireTimeMinutes,
                                          @Nonnull SegmentDefinition<ActionPlanAndStore, LiveActionStore, ActionPipelineContext> segmentDefinition) {
            super(sharedLiveActionsLock, maxLockAcquireTimeMinutes, TimeUnit.MINUTES, segmentDefinition);
            Preconditions.checkArgument(maxLockAcquireTimeMinutes > 0,
                "Illegal value %s for maxLockAcquireTimeMinutes", maxLockAcquireTimeMinutes);
        }

        /**
         * Sets up execution by passing through stage input to the segment.
         *
         * @param input The input to the stage.
         * @return The input to the segment, which is the same as the input to the stage.
         */
        @Nonnull
        @Override
        protected ActionPlanAndStore setupExecution(@Nonnull ActionPlanAndStore input) {
            return input;
        }

        @Nonnull
        @Override
        protected StageResult<ActionStore> completeExecution(@Nonnull StageResult<LiveActionStore> segmentResult) {
            // Need to convert Result<LiveActionStore> -> Result<ActionStore>
            return segmentResult.transpose(segmentResult.getResult());
        }
    }

    /**
     * The {@link ProcessLiveBuyRIActionsStage} processes a BuyRI action plan.
     */
    public static class ProcessLiveBuyRIActionsStage extends Stage<ActionPlanAndStore, LiveActionStore> {
        @Nonnull
        @Override
        protected StageResult<LiveActionStore> executeStage(@Nonnull ActionPlanAndStore input) {
            input.actionStore.populateBuyRIActions(input.actionPlan);

            return StageResult.withResult(input.actionStore)
                .andStatus(Status.success());
        }
    }

    /**
     * A class that gathers helpful statistics about action processing from the pipeline.
     */
    public static class ActionProcessingInfoStage extends Stage<ActionStore, ActionProcessingInfo> {
        @Nonnull
        @Override
        public StageResult<ActionProcessingInfo> executeStage(@Nonnull ActionStore input) {
            return StageResult.withResult(new ActionProcessingInfo(input.size()))
                .andStatus(Status.success());
        }
    }

    /**
     * Stage to update automated actions.
     */
    public static class UpdateAutomationStage extends RequiredPassthroughStage<ActionStore> {

        private final ActionAutomationManager automationManager;

        /**
         * Create a new UpdateAutomationStage.
         *
         * @param automationManager The automation manager to use to update automation.
         */
        public UpdateAutomationStage(@Nonnull final ActionAutomationManager automationManager) {
            this.automationManager = Objects.requireNonNull(automationManager);
        }

        @Nonnull
        @Override
        public Status passthrough(ActionStore actionStore) throws InterruptedException {
            automationManager.updateAutomation(actionStore);
            return Status.success();
        }
    }

    /**
     * Stage to update automated actions.
     */
    public static class UpdateSeverityCacheStage extends RequiredPassthroughStage<ActionStore> {

        @Nonnull
        @Override
        public Status passthrough(ActionStore actionStore)  {
            // severity cache must be refreshed after actions change (see EntitySeverityCache javadoc)
            actionStore.getEntitySeverityCache().ifPresent(cache -> cache.refresh(actionStore));
            return Status.success();
        }
    }

    /**
     * Bundles an {@link ActionPlan} and {@link ActionStore} in a small helper object.
     */
    public static class ActionPlanAndStore {
        /**
         * The action plan being processed.
         */
        @Nonnull
        private final ActionPlan actionPlan;

        /**
         * The {@link LiveActionStore}.
         */
        @Nonnull
        private final LiveActionStore actionStore;

        /**
         * Create a new {@link ActionPlanAndStore}.
         *
         * @param actionPlan The action plan.
         * @param actionStore The {@link LiveActionStore}.
         */
        public ActionPlanAndStore(@Nonnull final ActionPlan actionPlan,
                                  @Nonnull final LiveActionStore actionStore) {
            this.actionPlan = Objects.requireNonNull(actionPlan);
            this.actionStore = Objects.requireNonNull(actionStore);
        }

        /**
         * Get the action plan.
         *
         * @return the action plan.
         */
        public ActionPlan getActionPlan() {
            return actionPlan;
        }

        /**
         * Get the action store.
         *
         * @return the action store.
         */
        public ActionStore getActionStore() {
            return actionStore;
        }
    }

    /**
     * Bundles a market action ({@link ActionDTO.Action}) together with its OID. We have to generate action
     * OIDs before we actually create the {@link Action} model objects that contain both the OID and the DTO.
     * By bundling the two together, we can avoid having to look up the OID for the ActionDTO twice because
     * doing so has a high performance cost.
     */
    public static class IdentifiedActionDTO {
        /**
         * The market action.
         */
        @Nonnull
        private final ActionDTO.Action action;

        /**
         * The assigned identity for the market action.
         */
        private final long oid;

        /**
         * Create a new {@link IdentifiedActionDTO}.
         *
         * @param action The action.
         * @param oid The oid for the action.
         */
        public IdentifiedActionDTO(@Nonnull final ActionDTO.Action action,
                                   final long oid) {
            this.oid = oid;
            this.action = action;
        }

        @VisibleForTesting
        long getOid() {
            return oid;
        }

        @VisibleForTesting
        ActionDTO.Action getAction() {
            return action;
        }
    }

    /**
     * Bundles a list of actions with their OIDs assigned (the order will be the same as
     * the order in which they appear in the store) along with an {@link ActionStore}
     * in a small helper object.
     */
    public static class IdentifiedActionsAndStore {
        /**
         * The identified actions.
         */
        @Nonnull
        private final List<IdentifiedActionDTO> actions;

        /**
         * The {@link LiveActionStore}.
         */
        @Nonnull
        private final LiveActionStore actionStore;

        /**
         * Create a new {@link IdentifiedActionsAndStore}.
         *
         * @param actions The actions.
         * @param actionStore The {@link LiveActionStore}.
         */
        public IdentifiedActionsAndStore(@Nonnull final List<IdentifiedActionDTO> actions,
                                         @Nonnull final LiveActionStore actionStore) {
            this.actions = actions;
            this.actionStore = actionStore;
        }

        /**
         * Get the actions.
         *
         * @return the actions.
         */
        @VisibleForTesting
        @Nonnull
        List<IdentifiedActionDTO> getActions() {
            return actions;
        }
    }

    /**
     * Small helper class bundling a list of {@link ActionDTO.Action}s together with a
     * {@link LiveActionStore} for use as the input/output of several stages.
     */
    public static class ActionDTOsAndStore {
        /**
         * The actions.
         */
        @Nonnull
        private final List<ActionDTO.Action> actionDTOs;

        /**
         * The action store.
         */
        @Nonnull
        private final LiveActionStore actionStore;

        /**
         * Create a new {@link ActionDTOsAndStore}.
         *
         * @param actionDTOs The actions.
         * @param actionStore The store.
         */
        public ActionDTOsAndStore(@Nonnull final List<ActionDTO.Action> actionDTOs,
                                  @Nonnull final LiveActionStore actionStore) {
            this.actionDTOs = Objects.requireNonNull(actionDTOs);
            this.actionStore = Objects.requireNonNull(actionStore);
        }

        @Nonnull
        @VisibleForTesting
        List<ActionDTO.Action> getActionDTOs() {
            return actionDTOs;
        }
    }

    /**
     * Small helper class bundling a list of actions to translate (for the {@link TranslateActionsStage}
     * together with the {@link ActionStore}.
     */
    public static class ActionsToTranslateAndStore {
        /**
         * Actions that should be translated from the market's original recommendation into
         * a real-world executable action. An example of this is a VCPU resize that needs to
         * be translated from ie resize up from 500 to 1000 MHz to resize up from 1 CPU core
         * to 2 CPU cores.
         */
        @Nonnull
        private final List<Action> actionsToTranslate;

        /**
         * The {@link ActionStore} for use in processing the live actions.
         */
        @Nonnull
        private final LiveActionStore actionStore;

        /**
         * Create a new {@link ActionsToTranslateAndStore}.
         *
         * @param actionsToTranslate The list of actions to translate.
         * @param actionStore The {@link LiveActionStore}.
         */
        public ActionsToTranslateAndStore(@Nonnull final List<Action> actionsToTranslate,
                                          @Nonnull final LiveActionStore actionStore) {
            this.actionsToTranslate = Objects.requireNonNull(actionsToTranslate);
            this.actionStore = Objects.requireNonNull(actionStore);
        }

        /**
         * Get the actions to translate.
         *
         * @return the actions to translate.
         */
        @VisibleForTesting
        @Nonnull
        List<Action> getActionsToTranslate() {
            return actionsToTranslate;
        }
    }

    /**
     * A helper class that bundles a {@link RecommendationTracker} initially composed of actions in the
     * {@link ActionStore} prior to processing the action plan,  as well as lists of actions to
     * add to and remove from the store.
     */
    public static class ActionDifference {
        /**
         * Previous recommendations stored in a {@link RecommendationTracker}. Initially this will
         * be most of the actions in the store, and then actions will be taken from the
         * {@link RecommendationTracker}. Re-recommended actions will be taken from the tracker
         * over time, and eventually only actions that are no longer recommended will remain
         * in the tracker. These actions will be sent a {@link NotRecommendedEvent} in order
         * to clear them.
         */
        @Nonnull
        private final RecommendationTracker previousRecommendations;

        /**
         * The actions to remove will be removed from the store.
         * Note that we add many actions to actionsToRemove that we don't actually want to
         * remove from the store. Some actions in actionsToRemove also get added to
         * the actionsToAdd list in other parts of the code. Because we process actionsToRemove
         * first, actions that are in both lists are first removed, and then immediately
         * re-added. This is done intentionally.
         * <p/>
         * TODO: Make actionsToRemove and actionsToAdd disjoint collections and remove the
         * order dependency on processing them to make it less likely someone unfamiliar
         * with the use of these collections misuses of them to cause a bug.
         */
        @Nonnull
        private final List<Action> actionsToRemove;

        /**
         * The actions to add wil be added to the store. Note that this list is NOT disjoint
         * from actionsToRemove, but we process this after actionsToRemove, so any actions
         * in both lists result in the actions remaining in the store.
         */
        @Nonnull
        private final List<Action> actionsToAdd;

        /**
         * Create a new {@link ActionDifference}.
         */
        public ActionDifference() {
            previousRecommendations = new RecommendationTracker();
            actionsToRemove = new ArrayList<>();
            actionsToAdd = new ArrayList<>();
        }

        /**
         * Get PreviousRecommendations.
         *
         * @return PreviousRecommendations.
         */
        @VisibleForTesting
        @Nonnull
        RecommendationTracker getPreviousRecommendations() {
            return previousRecommendations;
        }

        /**
         * Get ActionsToRemove.
         *
         * @return the ActionsToRemove.
         */
        @VisibleForTesting
        @Nonnull
        List<Action> getActionsToRemove() {
            return actionsToRemove;
        }

        /**
         * Get the actions to add.
         *
         * @return the actions to add.
         */
        @VisibleForTesting
        @Nonnull
        List<Action> getActionsToAdd() {
            return actionsToAdd;
        }

        /**
         * Get a description of the size of the {@link ActionDifference}.
         *
         * @return a description of the size of the {@link ActionDifference}.
         */
        public String sizeDescription() {
            return "previousRecommendations=" + previousRecommendations.size()
                + ", actionsToRemove=" + actionsToRemove.size()
                + ", actionsToAdd=" + actionsToAdd.size();
        }
    }
}
