package com.vmturbo.action.orchestrator.store;

import static com.vmturbo.action.orchestrator.db.tables.ActionPlan.ACTION_PLAN;
import static com.vmturbo.action.orchestrator.db.tables.MarketAction.MARKET_ACTION;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import io.prometheus.client.Summary;

import org.apache.commons.collections4.MapUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep7;
import org.jooq.impl.DSL;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionDescriptionBuilder;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.db.tables.pojos.MarketAction;
import com.vmturbo.action.orchestrator.db.tables.records.MarketActionRecord;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.query.MapBackedActionViews;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.BuyRI;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.platform.sdk.common.util.ProbeLicense;

/**
 * {@inheritDoc}
 *
 * Stores actions in a persistent manner. Actions in this store should not be mutated.
 * Clients who mutate their local copy of actions retrieved from the store are not
 * provided a means to persist those mutations.
 * TODO: have the store return actions that are actually immutable.
 *
 * Actions in stores of this type are not kept in memory. Rather, they are saved
 * to persistent storage and reloaded when clients query information about them.
 *
 * A {@link PlanActionStore} is the right place to store actions that will
 * never be executed (ie actions resulting from a plan - what does it even mean to run an action
 * on an entity added to the plan that doesn't exist in the real world?) but the wrong place
 * to store actions that may at some point be executed (ie actions from the real-time market).
 * See also {@link LiveActionStore}.
 *
 * Data consistency is maintained by taking advantage of locking provided by database interactions.
 * All multi-step interactions with the database (ie clear the old actions, add new ones)
 * are performed transactionally.
 *
 * TODO: consider error handling for all the getters that interact with the DB.
 * TODO: If necessary, cache translation results for plan actions so they do not
 * have to be translated multiple times.
 */
@ThreadSafe
public class PlanActionStore implements ActionStore {
    private static final int BATCH_SIZE = 1000;
    private static final Logger logger = LogManager.getLogger();

    private final IActionFactory actionFactory;

    /**
     * Interface for interacting with persistent storage layer.
     */
    private final DSLContext dsl;

    /**
     * One Plan Action Store can store one action plan per action plan type.
     * For ex., there can be one action plan each for market, BuyRI and Delete wasted storage.
     *
     * A map of action plan type to the corresponding action plan id.
     */
    private Map<ActionPlanType, Long> actionPlanIdByActionPlanType;

    /**
     * A map of action plan id to the recommendation time.
     */
    private Map<Long, LocalDateTime> recommendationTimeByActionPlanId;

    /**
     * The context id for the topology context.
     */
    private final long topologyContextId;

    // TODO: a temp fix to get the source entities used for buy RI. We can get rid of it
    // if we have a source topology saved for every plan.
    private final long realtimeTopologyContextId;

    /**
     * The severity cache to be used for looking up severities for entities associated with actions
     * in this action store.
     */
    private final EntitySeverityCache severityCache;

    /**
     * All immutable (plan) actions are considered visible (from outside the Action Orchestrator's perspective).
     *
     * <p>Non-visible actions can still be valuable for debugging
     * purposes, but they shouldn't be exposed externally.
     *
     * @param actionView The {@link ActionView} to test for visibility.
     * @return Always true.
     */
    public static final Predicate<ActionView> VISIBILITY_PREDICATE = actionView ->
        actionView.getVisibilityLevel().checkVisibility(null);

    public static final String STORE_TYPE_NAME = "Plan";

    private static final Summary DELETE_PLAN_ACTION_PLAN_DURATION_SUMMARY = Summary.build()
        .name("ao_delete_plan_action_plan_duration_seconds")
        .help("Duration in seconds it takes to delete a plan action plan from persistent storage.")
        .register();

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    private final ActionTranslator actionTranslator;

    private final ActionTargetSelector actionTargetSelector;

    private final LicenseCheckClient licenseCheckClient;

    /**
     * Create a new {@link PlanActionStore}.
     *
     * @param actionFactory The factory for creating actions that live in this store.
     * @param dsl The interface for interacting with persistent storage layer where actions will be persisted.
     * @param topologyContextId the topology context id
     * @param supplyChainService used for constructing the EntitySeverityCache.
     * @param repositoryService used for constructing the EntitySeverityCache.
     * @param actionTranslator the action translator class
     * @param realtimeTopologyContextId real time topology id
     * @param actionTargetSelector For calculating target specific action pre-requisites
     */
    public PlanActionStore(@Nonnull final IActionFactory actionFactory,
                           @Nonnull final DSLContext dsl,
                           final long topologyContextId,
                           @Nonnull final SupplyChainServiceBlockingStub supplyChainService,
                           @Nonnull final RepositoryServiceBlockingStub repositoryService,
                           @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache,
                           @Nonnull final ActionTranslator actionTranslator,
                           final long realtimeTopologyContextId,
                           @Nonnull ActionTargetSelector actionTargetSelector,
                           @Nonnull LicenseCheckClient licenseCheckClient) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.dsl = dsl;
        this.actionPlanIdByActionPlanType = Maps.newHashMap();
        this.recommendationTimeByActionPlanId = Maps.newHashMap();
        this.topologyContextId = topologyContextId;
        this.severityCache = new EntitySeverityCache(repositoryService, false);
        this.entitySettingsCache = entitySettingsCache;
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.actionTargetSelector = actionTargetSelector;
        this.licenseCheckClient = licenseCheckClient;
    }

    /**
     * {@inheritDoc}
     *
     * Replaces all actions in the store with the recommendations in the new {@link ActionPlan}.
     * All prior recommendations matching the topologyContextId in the plan are removed from
     * persistent storage and all recommendations in the new plan are saved to persistent storage
     * where they can be retrieved in the future.
     */
    @Override
    public boolean populateRecommendedActions(@Nonnull final ActionPlan actionPlan) {
        if (ActionDTOUtil.getActionPlanContextId(actionPlan.getInfo()) != topologyContextId) {
            throw new IllegalArgumentException("Attempt to set store with topologyContextId " + topologyContextId +
                " with information from an action plan with topologyContextId " + actionPlan);
        }
        return replaceAllActions(actionPlan.getActionList(),
            actionPlanData(actionPlan, LocalDateTime.now()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return dsl.fetchCount(dsl.selectFrom(MARKET_ACTION)
            .where(MARKET_ACTION.TOPOLOGY_CONTEXT_ID.eq(topologyContextId)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean allowsExecution() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<ActionView> getActionView(long actionId) {
        // The map operation is necessary because of how Java handles generics via type erasure.
        // An Optional<Action> is not directly assignable to an Optional<ActionView> even though an
        // Action is an ActionView.
        return getAction(actionId)
            .map(Function.identity());
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<Action> getAction(long actionId) {
        // this operation requires the planner feature to be enabled
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);

        return loadAction(actionId)
            .map(action -> actionFactory.newPlanAction(action.getRecommendation(),
                recommendationTimeByActionPlanId.get(action.getActionPlanId()),
                action.getActionPlanId(), action.getDescription(),
                action.getAssociatedAccountId(), action.getAssociatedResourceGroupId()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public QueryableActionViews getActionViews() {
        // In the future this can be optimized a lot more by (among other things) moving more fields to the
        // database, caching the retrieved actions for a particular plan for a configurable
        // period of time, etc.
        return new MapBackedActionViews(Collections.unmodifiableMap(getActions()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, Action> getActions() {
        // this operation requires the planner feature to be enabled
        licenseCheckClient.checkFeatureAvailable(ProbeLicense.PLANNER);

        final Map<Long, Action> actions = loadActions().stream()
            .map(action -> actionFactory.newPlanAction(action.getRecommendation(),
                recommendationTimeByActionPlanId.get(action.getActionPlanId()),
                action.getActionPlanId(),
                action.getDescription(),
                action.getAssociatedAccountId(), action.getAssociatedResourceGroupId()))
            .collect(Collectors.toMap(Action::getId, Function.identity()));
        return actions;
    }

    @Nonnull
    @Override
    public Map<ActionPlanType, Collection<Action>> getActionsByActionPlanType() {
        return actionPlanIdByActionPlanType.entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, e -> loadActions(e.getValue()).stream()
                .map(action -> actionFactory.newPlanAction(action.getRecommendation(),
                    recommendationTimeByActionPlanId.get(action.getActionPlanId()),
                    action.getActionPlanId(),
                    action.getDescription(),
                    action.getAssociatedAccountId(), action.getAssociatedResourceGroupId()))
                .collect(Collectors.toList())));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean overwriteActions(@Nonnull final Map<ActionPlanType, List<Action>> newActions) {
        boolean overwriteResult = true;
        for (Entry<ActionPlanType, List<Action>> e : newActions.entrySet()) {
            ActionPlanType actionPlanType = e.getKey();
            List<Action> actions = e.getValue();
            com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan planData =
                actionPlanData(actionPlanType, actions);
            if (replaceAllActions(actions.stream()
                .map(Action::getRecommendation)
                .collect(Collectors.toList()), planData)) {
                logger.info("Successfully overwrote {} actions in the store with {} new actions.",
                    actionPlanType, actions.size());
            } else {
                overwriteResult = false;
            }
        }
        return overwriteResult;
    }

    /**
     * {@inheritDoc}
     * The {@link PlanActionStore} permits the clear operation.
     */
    @Override
    public boolean clear() {
        try {
            actionPlanIdByActionPlanType.values().forEach(planId -> cleanActions(dsl, planId));
            actionPlanIdByActionPlanType.clear();
            recommendationTimeByActionPlanId.clear();
            return true;
        } catch (RuntimeException e) {
            logger.error("Clear store failed with error: ", e);
            return false;
        }
    }

    @Override
    public long getTopologyContextId() {
        return topologyContextId;
    }

    @Override
    @Nonnull
    public EntitySeverityCache getEntitySeverityCache() {
        return severityCache;
    }

    @Override
    @Nonnull
    public Predicate<ActionView> getVisibilityPredicate() {
        return VISIBILITY_PREDICATE;
    }

    @Override
    @Nonnull
    public String getStoreTypeName() {
        return STORE_TYPE_NAME;
    }

    /**
     * Get the ids of the last action plans whose actions were successfully stored in this store.
     *
     * @return the ids of the last action plans whose actions were successfully stored in this store.
     */
    public Optional<Long> getActionPlanId(@Nonnull ActionPlanType actionPlanType) {
        return Optional.ofNullable(actionPlanIdByActionPlanType.get(actionPlanType));
    }

    /**
     * Implements the logic for populating the store with recommended actions
     * (see {@link #populateRecommendedActions(ActionPlan)}.
     *
     * @param actions The {@link ActionPlan} whose actions should be stored.
     * @param planData Data for the plan whose actions are being populated.
     *                 If empty, does not attempt to write new action information.
     * @return Whether actions were successfully replaced with those in the list of actions.
     */
    private boolean replaceAllActions(@Nonnull final List<ActionDTO.Action> actions,
                                      @Nonnull final com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan planData) {
        try {
            final List<ActionAndInfo> translatedActionsToAdd = translatePlanActions(actions, planData);
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                // Delete existing action plan if it exists for the incoming action plan type
                ActionPlanType incomingActionPlanType = ActionPlanType.forNumber(planData.getActionPlanType());
                deleteActionPlanIfExists(transactionDsl, incomingActionPlanType);

                if (!translatedActionsToAdd.isEmpty()) {
                    // Store the action plan
                    transactionDsl
                        .newRecord(ACTION_PLAN, planData)
                        .store();

                    // Store the associated actions for the plan in batches, but keep
                    // the batches in the same transaction.
                    Iterators.partition(translatedActionsToAdd.iterator(), BATCH_SIZE).forEachRemaining(actionBatch -> {
                        // If we expand the Market Action table we need to modify this insert
                        // statement and the subsequent "values" bindings.
                        InsertValuesStep7<MarketActionRecord, Long, Long, Long, ActionDTO.Action,
                            String, Long, Long> step =
                            transactionDsl.insertInto(MARKET_ACTION,
                                MARKET_ACTION.ID,
                                MARKET_ACTION.ACTION_PLAN_ID,
                                MARKET_ACTION.TOPOLOGY_CONTEXT_ID,
                                MARKET_ACTION.RECOMMENDATION,
                                MARKET_ACTION.DESCRIPTION,
                                MARKET_ACTION.ASSOCIATED_ACCOUNT_ID, MARKET_ACTION.ASSOCIATED_RESOURCE_GROUP_ID);

                        for (ActionAndInfo actionAndInfo : actionBatch) {
                            step = step.values(actionAndInfo.translatedAction().getId(),
                                planData.getId(),
                                planData.getTopologyContextId(),
                                actionAndInfo.translatedAction(),
                                actionAndInfo.description(),
                                actionAndInfo.associatedAccountId().orElse(null),
                                actionAndInfo.associatedResourceGroupId().orElse(null));
                        }
                        step.execute();
                    });
                    // Update internal state tracking only on success of database operations.
                    updateInternalState(incomingActionPlanType, planData.getId(), planData.getCreateTime());
                    logger.info("Successfully stored action plan id {} of type {} with {} actions",
                        planData.getId(), planData.getActionPlanType(), translatedActionsToAdd.size());
                }
            });
            return true;
        } catch (RuntimeException e) {
            logger.error("Replace all actions transaction failed with error: ", e);
            return false;
        }
    }

    /**
     * Translates and adds description for each of the given list of plan actions.
     *
     * @param actions The {@link ActionPlan} whose actions should be stored.
     * @param planData Data for the plan whose actions are being populated.
     * @return A list of translated actions and their descriptions.
     */
    private List<ActionAndInfo> translatePlanActions(@Nonnull final List<ActionDTO.Action> actions,
                                                     @Nonnull final com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan planData) {
        // Check if there are any delete volume actions.  If so we need to
        // get these from the real-time SOURCE topology as plan projected topology
        // won't have them.
        Set<ActionDTO.Action> deleteVolumeActions = actions.stream()
                .filter(action -> action.getInfo().getActionTypeCase() == ActionTypeCase.DELETE)
                .collect(Collectors.toSet());
        final Set<Long> deleteVolumesToRetrieve = ActionDTOUtil.getInvolvedEntityIds(deleteVolumeActions);

        //TODO: Remove deleteVolumesToRetrieve from entitiesToRetrieve
        final Set<Long> entitiesToRetrieve =
            new HashSet<>(ActionDTOUtil.getInvolvedEntityIds(actions));
        // snapshot contains the entities information that is required for the actions descriptions
        long planContextId = planData.getTopologyContextId();
        // TODO: a temp fix to get the  entities used for buy RI.
        if (planData.getActionPlanType() == ActionPlanType.BUY_RI.getNumber()) {
            planContextId = realtimeTopologyContextId;
        }
        // TODO: remove hack to go to realtime if source plan topology is not available.  Needed to
        // compute action descriptions.
        EntitiesAndSettingsSnapshot snapshotHack = entitySettingsCache.newSnapshot(
                entitiesToRetrieve,deleteVolumesToRetrieve, planContextId);
        if (MapUtils.isEmpty(snapshotHack.getEntityMap())) {
            // Hack: if the plan source topology is not ready, use realtime.
            // This should only occur initially when the plan  created.
            logger.warn("translatePlanActions: failed for topologyContextId={} topologyId={}, try realtime",
                planContextId, planData.getTopologyId());
            snapshotHack = entitySettingsCache.newSnapshot(entitiesToRetrieve,
                                                           deleteVolumesToRetrieve,
                                                           realtimeTopologyContextId);
        }
        final EntitiesAndSettingsSnapshot snapshot = snapshotHack;
        final List<ActionDTO.Action> actionsStream = new ArrayList<>(actions.size());

        Map<Long, ActionTargetInfo> actionTargetInfo = actionTargetSelector.getTargetsForActions(actions.stream(), snapshot);
        Stream<ActionDTO.Action> actionStream = actions.stream().map(
            action -> action.toBuilder().addAllPrerequisite(actionTargetInfo.get(action.getId()).prerequisites()).build());

        final Stream<Action> translatedActions = actionTranslator.translate(actionStream
            .map(recommendedAction -> actionFactory.newAction(recommendedAction, planData.getId(), IdentityGenerator.next())),
            snapshot);

        final List<ActionAndInfo> translatedActionsToAdd = new ArrayList<>();

        final Set<ActionTypeCase> unsupportedActionTypes = new HashSet<>();
        translatedActions.forEach(action -> {
            // Ignoring actions with failed translation
            if (action.getActionTranslation().getTranslationStatus() != TranslationStatus.TRANSLATION_FAILED) {
                final ActionDTO.Action recommendation = action.getActionTranslation().getTranslationResultOrOriginal();
                try {
                    final long primaryEntity = ActionDTOUtil.getPrimaryEntityId(recommendation);
                    translatedActionsToAdd.add(ImmutableActionAndInfo.builder()
                        .translatedAction(recommendation)
                        .description(ActionDescriptionBuilder.buildActionDescription(snapshot,
                                                                      recommendation))
                        .associatedAccountId(PlanActionStore.getAssociatedAccountId(recommendation, snapshot, primaryEntity))
                        .build());
                } catch (UnsupportedActionException e) {
                    unsupportedActionTypes.add(recommendation.getInfo().getActionTypeCase());
                }
            }
        });

        if (!unsupportedActionTypes.isEmpty()) {
            logger.error("Action plan contained unsupported action types: {}", unsupportedActionTypes);
        }
        return translatedActionsToAdd;
    }

    private static Optional<Long> getAssociatedAccountId(@Nonnull final ActionDTO.Action action,
                                                         @Nonnull final EntitiesAndSettingsSnapshot entitiesSnapshot,
                                                         long primaryEntity) {
        final ActionInfo actionInfo = action.getInfo();
        switch (actionInfo.getActionTypeCase()) {
            case BUYRI:
                final BuyRI buyRi = actionInfo.getBuyRi();
                return Optional.of(buyRi.getMasterAccount().getId());
            default:
                return entitiesSnapshot.getOwnerAccountOfEntity(primaryEntity)
                        .map(EntityWithConnections::getOid);
        }
    }


    /**
     * Load all market actions in the persistent store by their associated topologyContextId.
     *
     * @return A last known {@link MarketAction}s for the topology context.
     */
    private List<MarketAction> loadActions() {
        return dsl.selectFrom(MARKET_ACTION)
            .where(MARKET_ACTION.TOPOLOGY_CONTEXT_ID.eq(topologyContextId))
            .fetch()
            .into(MarketAction.class);
    }

    /**
     * Load all market actions in the persistent store for an action plan id.
     *
     * @return A last known {@link MarketAction}s for the action plan id.
     */
    private List<MarketAction> loadActions(long actionPlanId) {
        return dsl.selectFrom(MARKET_ACTION)
            .where(MARKET_ACTION.ACTION_PLAN_ID.eq(actionPlanId))
            .fetch()
            .into(MarketAction.class);
    }

    /**
     * Load the market action with the given ID in the persistent store by their associated topologyContextId.
     *
     * @param actionId The topologyContextId whose last known actions should be loaded.
     * @return A last known {@link MarketAction}s for a the given topology context.
     */
    private Optional<MarketAction> loadAction(final long actionId) {
        return Optional.ofNullable(dsl.selectFrom(MARKET_ACTION)
            .where(MARKET_ACTION.ID.eq(actionId))
            .fetchAny()).map(rec -> rec.into(MarketAction.class));
    }

    /**
     * Delete the actions associated with th store's topology context from the persistence layer.
     *
     * @param context The DSL context in which to execute the clean.
     * @param actionPlanId The id of the action plan whose actions should be cleaned.
     */
    private void cleanActions(@Nonnull final DSLContext context, final long actionPlanId) {
        DELETE_PLAN_ACTION_PLAN_DURATION_SUMMARY.time(() -> {
            // Deleting the action plan causes a cascading clear for all associated actions.
            context.deleteFrom(ACTION_PLAN)
                .where(ACTION_PLAN.ID.eq(actionPlanId))
                .execute();
        });
    }

    /**
     * Setup the internal information of the store to match the information in the jOOQ POJO ActionPlan.
     *
     * @param actionPlan The plan whose information should be set for this store.
     */
    private void setupPlanInformation(@Nonnull final com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan actionPlan) {
        if (actionPlan.getTopologyContextId() != topologyContextId) {
            throw new IllegalArgumentException("Attempt to set store with topologyContextId " + topologyContextId +
                " with information from an action plan with topologyContextId " + actionPlan.getTopologyContextId());
        }
        updateInternalState(ActionPlanType.forNumber(actionPlan.getActionPlanType()),
            actionPlan.getId(), actionPlan.getCreateTime());
    }

    /**
     * Updates maps actionPlanIdByActionPlanType and recommendationTimeByActionPlanId with the action plan details
     * like action plan id and recommendation time.
     *
     * @param actionPlanType the action plan type
     * @param actionPlanId the action plan id
     * @param recommendationTime the recommendation time
     */
    private void updateInternalState(ActionPlanType actionPlanType, long actionPlanId,
                                     LocalDateTime recommendationTime) {
        actionPlanIdByActionPlanType.put(actionPlanType, actionPlanId);
        recommendationTimeByActionPlanId.put(actionPlanId, recommendationTime);
    }

    /**
     * Delete action plan of the incoming action plan type if it exists.
     * But this should never happen as of writing this {4/9/2019}.
     * One plan action store should store one action plan type only once.
     * For ex. market actions should be stored once, Buy RI actions should be stored once.
     */
    private void deleteActionPlanIfExists(DSLContext transactionDsl, ActionPlanType actionPlanType) {
        Long oldActionPlanId = actionPlanIdByActionPlanType.get(actionPlanType);
        if (oldActionPlanId != null) {
            logger.error("Existing action plan id {} of action plan type {} exists. " +
                "Deleting action plan {}", oldActionPlanId, actionPlanType, oldActionPlanId);
            cleanActions(transactionDsl, oldActionPlanId);
            recommendationTimeByActionPlanId.remove(oldActionPlanId);
            actionPlanIdByActionPlanType.remove(actionPlanType);
        }
    }

    /**
     * A loader for loading {@link PlanActionStore}s from the database.
     */
    public static class StoreLoader implements IActionStoreLoader {
        private final DSLContext dsl;
        private final IActionFactory actionFactory;
        private final ActionModeCalculator actionModeCalculator;
        private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;
        private final ActionTranslator actionTranslator;
        private final long realtimeTopologyContextId;
        private final SupplyChainServiceBlockingStub supplyChainService;
        private final RepositoryServiceBlockingStub repositoryService;
        private final ActionTargetSelector actionTargetSelector;
        private final LicenseCheckClient licenseCheckClient;

        public StoreLoader(@Nonnull final DSLContext dsl,
                           @Nonnull final IActionFactory actionFactory,
                           @Nonnull final ActionModeCalculator actionModeCalculator,
                           @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache,
                           @Nonnull final ActionTranslator actionTranslator,
                           final long realtimeTopologyContextId,
                           @Nonnull final SupplyChainServiceBlockingStub supplyChainService,
                           @Nonnull final RepositoryServiceBlockingStub repositoryService,
                           @Nonnull final ActionTargetSelector actionTargetSelector,
                           @Nonnull final LicenseCheckClient licenseCheckClient) {
            this.dsl = Objects.requireNonNull(dsl);
            this.actionFactory = Objects.requireNonNull(actionFactory);
            this.actionModeCalculator = Objects.requireNonNull(actionModeCalculator);
            this.entitySettingsCache = entitySettingsCache;
            this.actionTranslator = actionTranslator;
            this.realtimeTopologyContextId = realtimeTopologyContextId;
            this.supplyChainService = supplyChainService;
            this.repositoryService = repositoryService;
            this.actionTargetSelector = actionTargetSelector;
            this.licenseCheckClient = licenseCheckClient;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<ActionStore> loadActionStores() {
            try {
                final Map<Long, PlanActionStore> planActionStoresByTopologyContextId = Maps.newHashMap();
                dsl.selectFrom(ACTION_PLAN)
                    .fetch()
                    .into(com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan.class)
                    .forEach(actionPlan -> {
                        final PlanActionStore store = planActionStoresByTopologyContextId
                            .computeIfAbsent(actionPlan.getTopologyContextId(),
                                k -> new PlanActionStore(actionFactory, dsl,
                                    actionPlan.getTopologyContextId(),
                                    supplyChainService, repositoryService,
                                    entitySettingsCache, actionTranslator,
                                    realtimeTopologyContextId, actionTargetSelector,
                                    licenseCheckClient));
                        store.setupPlanInformation(actionPlan);
                    });
                return planActionStoresByTopologyContextId.values().stream().collect(Collectors.toList());
            } catch (Exception e) {
                logger.error("Unable to load action stores due to exception: ", e);
                return Collections.emptyList();
            }
        }
    }

    /**
     * A helper for creating a POJO ActionPlan from a protobuf ActionPlan.
     *
     * @param actionPlan The protobuf plan to translate.
     * @param recommendationTime The time at which the recommendations were made.
     * @return A POJO ActionPlan equivalent of the protobuf ActionPlan.
     */
    private com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan actionPlanData(
        @Nonnull final ActionPlan actionPlan, @Nonnull final LocalDateTime recommendationTime) {

        return new com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan(
            actionPlan.getId(),
            // In case of Buy RI, topologyId will be 0
            actionPlan.getInfo().getMarket().getSourceTopologyInfo().getTopologyId(),
            topologyContextId,
            recommendationTime,
            (short) ActionDTOUtil.getActionPlanType(actionPlan.getInfo()).getNumber()
        );
    }

    /**
     * A helper for creating a POJO ActionPlan from a single protobuf Action.
     * The generated plan information does not include topologyId information. The topologyContextId
     * comes from the store's associated topologyContextId.
     *
     * @param actionPlanType The action plan type which is an input for the ActionPlan db pojo.
     * @praam actions List of {@link Action} to use as a source for plan information.
     * @return A POJO ActionPlan.
     */
    private com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan actionPlanData(
        @Nonnull ActionPlanType actionPlanType, @Nonnull final List<Action> actions) {
        long actionPlanId = -1;
        LocalDateTime recommendationTime = null;
        if (!actions.isEmpty()) {
            Action action = actions.get(0);
            actionPlanId = action.getActionPlanId();
            recommendationTime = action.getRecommendationTime();
        }
        return new com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan(
            actionPlanId,
            -1L,
            topologyContextId,
            recommendationTime,
            (short)actionPlanType.getNumber()
        );
    }

    /**
     * An Immutable interface that holds both the translated action and other information we need
     * to persist alongside it.
     */
    @Value.Immutable
    public interface ActionAndInfo {

        /**
         * The translated action.
         *
         * @return {@link ActionDTO.Action}
         */
        ActionDTO.Action translatedAction();

        /**
         * The action description created by {@link ActionDescriptionBuilder}.
         *
         * @return The action description.
         */
        String description();

        /**
         * The ID of the business account associated with the action (cloud only).
         *
         * @return {@link Optional} containing the account ID, if the action can be associated
         * with an account.
         */
        Optional<Long> associatedAccountId();

        /**
         * The ID of the resource group associated with the action (cloud only).
         *
         * @return {@link Optional} containing the resource group Id, if the action can be
         * associated with resource group.
         */
        Optional<Long> associatedResourceGroupId();
    }
}
