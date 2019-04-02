package com.vmturbo.action.orchestrator.store;

import static com.vmturbo.action.orchestrator.db.tables.ActionPlan.ACTION_PLAN;
import static com.vmturbo.action.orchestrator.db.tables.MarketAction.MARKET_ACTION;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.InsertValuesStep4;
import org.jooq.impl.DSL;

import com.google.common.collect.Iterators;

import io.prometheus.client.Summary;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.action.orchestrator.db.tables.pojos.MarketAction;
import com.vmturbo.action.orchestrator.db.tables.records.MarketActionRecord;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;

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
     * The ID of the action plan whose actions are currently in the store.
     */
    private Optional<Long> actionPlanId;

    /**
     * The time at which the currently stored action plan was recommended.
     */
    private Optional<LocalDateTime> planRecommendationTime;

    /**
     * The context id for the topology context.
     */
    private final long topologyContextId;

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
    public static final Predicate<ActionView> VISIBILITY_PREDICATE = actionView -> true;

    private static final String STORE_TYPE_NAME = "Plan";

    private static final Summary DELETE_PLAN_ACTION_PLAN_DURATION_SUMMARY = Summary.build()
        .name("ao_delete_plan_action_plan_duration_seconds")
        .help("Duration in seconds it takes to delete a plan action plan from persistent storage.")
        .register();

    /**
     * Create a new {@link PlanActionStore}.
     *
     * @param actionFactory The factory for creating actions that live in this store.
     * @param dsl The interface for interacting with persistent storage layer where actions will be persisted.
     */
    public PlanActionStore(@Nonnull final IActionFactory actionFactory,
                           @Nonnull final DSLContext dsl,
                           final long topologyContextId) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.dsl = dsl;
        this.actionPlanId = Optional.empty();
        this.planRecommendationTime = Optional.empty();
        this.topologyContextId = topologyContextId;
        this.severityCache = new EntitySeverityCache(QueryFilter.VISIBILITY_FILTER);
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
        if (actionPlan.getInfo().hasBuyRi()) {
            // Add support for storing RI actions when we have RI purchasing plans.
            logger.error("Plan action store cannot support a buy RI action plan (id: {})",
                actionPlan.getId());
            return false;
        }
        return replaceAllActions(actionPlan.getActionList(),
            Optional.of(actionPlanData(actionPlan, LocalDateTime.now())));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return actionPlanId.map(planId -> dsl.fetchCount(dsl
                .selectFrom(MARKET_ACTION)
                .where(MARKET_ACTION.ACTION_PLAN_ID.eq(planId)))
        ).orElse(0);
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
        return actionPlanId.flatMap(planId -> loadAction(actionId))
            .map(marketAction -> actionFactory.newAction(marketAction.getRecommendation(),
                planRecommendationTime.get(),
                marketAction.getActionPlanId()));
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, ActionView> getActionViews() {
        return Collections.unmodifiableMap(getActions());
    }

    /**
     * Plan action store doesn't support get action views by date, will just return all the actions.
     */
    @Nonnull
    @Override
    public Map<Long, ActionView> getActionViewsByDate(final LocalDateTime startDate, final LocalDateTime endDate) {
        return Collections.unmodifiableMap(getActions());
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, Action> getActions() {
        return actionPlanId.map(planId -> loadActions(planId).stream()
                .map(marketAction -> actionFactory.newAction(marketAction.getRecommendation(),
                    planRecommendationTime.get(),
                    marketAction.getActionPlanId()))
                .collect(Collectors.toMap(Action::getId, Function.identity()))
        ).orElse(Collections.emptyMap());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean overwriteActions(@Nonnull final List<Action> newActions) {
        // Data for the plan is pulled from the first action in the list.
        // Since in the PersistentImmutableStore all actions are from the same plan, this
        // should be fine. If data from mixed plans are to be put in a PersistentImmutableDataStore
        // this logic should be reconsidered.
        final Optional<com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan> planData = newActions.isEmpty() ?
            Optional.empty() :
            Optional.of(actionPlanData(newActions.get(0)));

        if (replaceAllActions(newActions.stream()
                .map(Action::getRecommendation)
                .collect(Collectors.toList()), planData)) {
            logger.info("Successfully overwrote actions in the store with {} new actions.", newActions.size());
            return true;
        } else {
            return false;
        }
    }

    /**
     * {@inheritDoc}
     * The {@link PlanActionStore} permits the clear operation.
     */
    public boolean clear() {
        try {
            actionPlanId.ifPresent(planId -> cleanActions(dsl, planId));
            actionPlanId = Optional.empty();
            planRecommendationTime = Optional.empty();

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
     * Get the id of the last action plan whose actions were successfully stored in this store.
     *
     * @return the id of the last action plan whose actions were successfully stored in this store.
     */
    public Optional<Long> getActionPlanId() {
        return actionPlanId;
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
                                      @Nonnull final Optional<com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan> planData) {
        try {
            dsl.transaction(configuration -> {
                DSLContext transactionDsl = DSL.using(configuration);
                actionPlanId.ifPresent(planId -> cleanActions(transactionDsl, planId));

                if (planData.isPresent()) {
                    final com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan data = planData.get();

                    // Store the action plan
                    transactionDsl
                        .newRecord(ACTION_PLAN, data)
                        .store();

                    // Store the associated actions for the plan in batches, but keep
                    // the batches in the same transaction.
                    Iterators.partition(actions.iterator(), BATCH_SIZE).forEachRemaining(actionBatch -> {
                        // If we expand the Market Action table we need to modify this insert
                        // statement and the subsequent "values" bindings.
                        InsertValuesStep4<MarketActionRecord, Long, Long, Long, ActionDTO.Action> step =
                                transactionDsl.insertInto(MARKET_ACTION,
                                        MARKET_ACTION.ID,
                                        MARKET_ACTION.ACTION_PLAN_ID,
                                        MARKET_ACTION.TOPOLOGY_CONTEXT_ID,
                                        MARKET_ACTION.RECOMMENDATION);
                        for (ActionDTO.Action action : actionBatch) {
                            step = step.values(action.getId(),
                                    data.getId(),
                                    data.getTopologyContextId(),
                                    action);
                        }
                        step.execute();
                    });

                    // Update internal state tracking only on success of database operations.
                    this.planRecommendationTime = Optional.of(data.getCreateTime());
                    actionPlanId = Optional.of(data.getId());
                } else {
                    logger.info("Clearing plan information.");
                    this.planRecommendationTime = Optional.empty();
                    actionPlanId = Optional.empty();
                }
            });

            return true;
        } catch (RuntimeException e) {
            logger.error("Replace all actions transaction failed with error: ", e);
            return false;
        }
    }

    /**
     * Load all market actions in the persistent store by their associated topologyContextId.
     *
     * @param contextId The topologyContextId whose last known actions should be loaded.
     * @return A last known {@link MarketAction}s for a the given topology context.
     */
    private List<MarketAction> loadActions(final long contextId) {
        return dsl.selectFrom(MARKET_ACTION)
            .where(MARKET_ACTION.ACTION_PLAN_ID.eq(contextId))
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
            .fetchAny()
            .into(MarketAction.class));
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

        this.actionPlanId = Optional.of(actionPlan.getId());
        this.planRecommendationTime = Optional.of(actionPlan.getCreateTime());
    }

    /**
     * A loader for loading {@link PlanActionStore}s from the database.
     */
    public static class StoreLoader implements IActionStoreLoader {
        private final DSLContext dsl;
        private final IActionFactory actionFactory;
        private final ActionModeCalculator actionModeCalculator;

        public StoreLoader(@Nonnull final DSLContext dsl,
                           @Nonnull final IActionFactory actionFactory,
                           @Nonnull final ActionModeCalculator actionModeCalculator) {
            this.dsl = Objects.requireNonNull(dsl);
            this.actionFactory = Objects.requireNonNull(actionFactory);
            this.actionModeCalculator = Objects.requireNonNull(actionModeCalculator);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public List<ActionStore> loadActionStores() {
            try {
                return dsl.selectFrom(ACTION_PLAN)
                    .fetch()
                    .into(com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan.class)
                    .stream()
                    .map(actionPlan -> {
                        final PlanActionStore store =
                            new PlanActionStore(actionFactory, dsl, actionPlan.getTopologyContextId());
                        store.setupPlanInformation(actionPlan);
                        return store;
                    }).collect(Collectors.toList());
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
            actionPlan.getInfo().getMarket().getSourceTopologyInfo().getTopologyId(),
            actionPlan.getInfo().getMarket().getSourceTopologyInfo().getTopologyContextId(),
            recommendationTime
        );
    }

    /**
     * A helper for creating a POJO ActionPlan from a single protobuf Action.
     * The generated plan information does not include topologyId information. The topologyContextId
     * comes from the store's associated topologyContextId.
     *
     * @param action The action to use as a source for plan information.
     * @return A POJO ActionPlan.
     */
    private com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan actionPlanData(@Nonnull final Action action) {
        return new com.vmturbo.action.orchestrator.db.tables.pojos.ActionPlan(
            action.getActionPlanId(),
            -1L,
            topologyContextId,
            action.getRecommendationTime()
        );
    }
}
