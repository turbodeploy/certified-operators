package com.vmturbo.action.orchestrator.store;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.apache.commons.collections4.SetUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.atomic.AggregatedAction;
import com.vmturbo.action.orchestrator.store.atomic.AtomicActionFactory;
import com.vmturbo.action.orchestrator.store.pipeline.ActionCounts;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.licensing.LicenseCheckClient;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntitiesWithNewState;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;
import com.vmturbo.identity.IdentityService;
import com.vmturbo.identity.exceptions.IdentityServiceException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * {@inheritDoc}
 *
 * Stores actions mainly in memory except executed (SUCCEEDED and FAILED) actions
 * For use with actions from the live market (sometimes also called "real time").
 */
@ThreadSafe
public class LiveActionStore implements ActionStore {
    private static final Logger logger = LogManager.getLogger();

    private final LiveActions actions;

    private final IActionFactory actionFactory;

    private final long topologyContextId;

    private final EntitySeverityCache severityCache;

    private final EntitiesAndSettingsSnapshotFactory entitySettingsCache;

    public static final String STORE_TYPE_NAME = "Live";

    private final ActionTargetSelector actionTargetSelector;

    private final ActionTranslator actionTranslator;

    private final LicenseCheckClient licenseCheckClient;

    private final IdentityService<ActionInfo> actionIdentityService;

    private final EntitiesWithNewStateCache entitiesWithNewStateCache;

    /**
     * For certain actions that have already executed successfully, it is valid to execute them
     * again if market re-recommends them, and we don't want them to be removed from the action plan.
     * For example, container pod provision actions are light weight and may need to be executed
     * multiple times in a timely fashion to achieve performance in SLO driven horizontal scaling.
     * OM-62427.
     */
    private static final Map<ActionTypeCase, Set<EntityType>> REPEATABLE_ACTIONS =
            ImmutableMap.of(ActionTypeCase.PROVISION, ImmutableSet.of(EntityType.CONTAINER_POD));

    /**
     * A mutable (real-time) action is considered visible (from outside the Action Orchestrator's perspective)
     * if it's not disabled. We shouldn't consider the action state or executability as before.
     *
     * <p>Non-visible actions can still be valuable for debugging
     * purposes, but they shouldn't be exposed externally.
     *
     * @param actionView The {@link ActionView} to test for visibility.
     * @return True if the spec is visible to the UI, false otherwise.
     */
    public static final Predicate<ActionView> VISIBILITY_PREDICATE = actionView ->
        actionView.getVisibilityLevel().checkVisibility(true);

    public enum ActionSource {
        MARKET("market"),
        ATOMIC("atomic");

        private final String sourceName;

        ActionSource(@Nonnull final String sourceName) {
            this.sourceName = Objects.requireNonNull(sourceName);
        }

        public String getSourceName() {
            return sourceName;
        }
    }

    /**
     * Create a new {@link ActionStore} for storing live actions (actions generated by the realtime
     * market).
     *  @param actionFactory the action factory
     * @param topologyContextId the topology context id
     * @param actionTargetSelector selects which target/probe to execute each action against
     * @param entitySettingsCache an entity snapshot factory used for creating entity snapshot
     * @param actionHistoryDao dao layer working with executed actions
     * @param actionTranslator the action translator class
     * @param clock the {@link Clock}
     * @param userSessionContext the user session context
     * @param acceptedActionsStore dao layer working with accepted actions
     * @param rejectedActionsStore dao layer working with rejected actions
     * @param actionIdentityService identity service to fetch OIDs for actions
     * @param involvedEntitiesExpander used for expanding entities and determining how involved
*                                 entities should be filtered.
     * @param entitySeverityCache The {@link EntitySeverityCache}.
     * @param workflowStore the store for all the known {@link WorkflowDTO.Workflow} items
     */
    public LiveActionStore(@Nonnull final IActionFactory actionFactory,
                           final long topologyContextId,
                           @Nonnull final ActionTargetSelector actionTargetSelector,
                           @Nonnull final EntitiesAndSettingsSnapshotFactory entitySettingsCache,
                           @Nonnull final ActionHistoryDao actionHistoryDao,
                           @Nonnull final ActionTranslator actionTranslator,
                           @Nonnull final Clock clock,
                           @Nonnull final UserSessionContext userSessionContext,
                           @Nonnull final LicenseCheckClient licenseCheckClient,
                           @Nonnull final AcceptedActionsDAO acceptedActionsStore,
                           @Nonnull final RejectedActionsDAO rejectedActionsStore,
                           @Nonnull final IdentityService<ActionInfo> actionIdentityService,
                           @Nonnull final InvolvedEntitiesExpander involvedEntitiesExpander,
                           @Nonnull final EntitySeverityCache entitySeverityCache,
                           @Nonnull final WorkflowStore workflowStore
    ) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.topologyContextId = topologyContextId;
        this.actionTargetSelector = Objects.requireNonNull(actionTargetSelector);
        this.entitySettingsCache = Objects.requireNonNull(entitySettingsCache);
        this.actions =
                new LiveActions(actionHistoryDao, Objects.requireNonNull(acceptedActionsStore),
                        Objects.requireNonNull(rejectedActionsStore), clock,
                        Objects.requireNonNull(userSessionContext),
                        Objects.requireNonNull(involvedEntitiesExpander),
                        Objects.requireNonNull(workflowStore));
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
        this.licenseCheckClient = Objects.requireNonNull(licenseCheckClient);
        this.actionIdentityService = Objects.requireNonNull(actionIdentityService);
        this.entitiesWithNewStateCache = new EntitiesWithNewStateCache(actions);
        this.severityCache = entitySeverityCache;
    }

    /**
     * {@inheritDoc}
     *
     * All recommendations in the {@link ActionPlan} that correspond to a READY action in the store
     * or are new to the store will be added in the same order they share in the {@link ActionPlan}.
     * The order of actions in the plan that correspond to an action in the store is not guaranteed.
     * <p/>
     * Any actions in the {@link ActionPlan} that match an action already in the store retain the ID
     * of the recommendation in the store, rather than the new recommendation ID.
     * <p/>
     * All recommendations in the {@link ActionPlan} that do not match an action already in the store
     * are added to the store as an {@link Action}.
     * <p/>
     * All QUEUED and IN_PROGRESS {@link Action}s are retained in the store.
     * <p/>
     * All CLEARED, SUCCEEDED, and FAILED {@link Action}s are removed from the store.
     * <p/>
     * All currently READY {@link Action}s are transitioned to CLEARED and removed from the store
     * unless they are re-recommended in the new {@link ActionPlan}.
     * <p/>
     * Consider the following scenarios where the store has one plan in it and the plan has one recommendation in it:
     * <ul>
     *     <li>PRE-STORE:  Move VM1: HostA -> HostB (IN_PROGRESS)</li>
     *     <li>PLAN:       Move VM1: HostA -> HostB</li>
     *     <li>POST-STORE: Move VM1: HostA -> HostB (IN_PROGRESS)</li>
     * </ul>
     * The market re-recommended an IN_PROGRESS action. In this case, there were 0 READY actions in the
     * store at the end of the populate call even though there was 1 ActionInfo in the plan.
     * <ul>
     *     <li>PRE-STORE:  Move VM1: HostA -> HostB (SUCCEEDED)</li>
     *     <li>PLAN:       Move VM1: HostA -> HostB</li>
     *     <li>POST-STORE: empty</li>
     * </ul>
     * The market re-recommended an action that already ran and succeeded.
     * <ul>
     *     <li>PRE-STORE:  Move VM1: HostA -> HostB (READY)</li>
     *     <li>PLAN:       Move VM1: HostA -> HostB</li>
     *     <li>POST-STORE: Move VM1: HostA -> HostB (READY)</li>
     * </ul>
     * The market re-recommended an action that was undecided.
     * <ul>
     *     <li>PRE-STORE:  Move VM1: HostA -> HostB (READY)</li>
     *     <li>PLAN:       empty</li>
     *     <li>POST-STORE: empty</li>
     * </ul>
     * The market did not re-recommend a READY action in the store.
     *
     * When the action plan is received, the market actions are passed through
     * the {@link AtomicActionFactory} to be de-duplicated and aggregated into {@link AggregatedAction}.
     * After the market actions are processed, AggregatedActions are
     * converted to atomic action DTOs and then then to {@link Action}'s
     * and saved in the LiveActionsStore.
     *
     * @throws InterruptedException if current thread has been interrupted
     */
    @Override
    public boolean populateRecommendedActions(@Nonnull final ActionPlan actionPlan)
            throws InterruptedException {

        logger.error("Do not call LiveActionsStore#populateRecommendedActions. "
            + "Instead prefer LiveActionsPipelineFactory#buildLiveMarketActionsPipeline.");
        return false;
    }

    /**
     * Update the market actions atomically. The update includes updating things like the action description
     * for re-recommended actions, etc.
     *
     * @param snapshot The {@link EntitiesAndSettingsSnapshot} associated with the topology for the
     *                 action plan being processed.
     * @param actionsToRemove The list of market actions to remove from the store.
     * @param actionsToAdd The list of market actions to add to the store.
     */
    public void updateMarketActions(@Nonnull final EntitiesAndSettingsSnapshot snapshot,
                                    @Nonnull final List<Action> actionsToRemove,
                                    @Nonnull final List<Action> actionsToAdd) {
        // Some of these may be noops - if we're re-adding an action that was already in
        // the map from a previous action plan.
        // THis also updates the snapshot in the entity settings cache.
        actions.updateMarketActions(actionsToRemove, actionsToAdd, snapshot, actionTargetSelector);
    }

    /**
     * Update the atomic actions atomically. The update includes updating things like the action
     * description for re-recommended actions, etc. The mergedActions are market actions that
     * were merged together to form atomic actions and they should be removed from the store
     * of market actions.
     *
     * @param snapshot The {@link EntitiesAndSettingsSnapshot} associated with the topology for the
     *                 action plan being processed.
     * @param actionsToRemove The list of atomic actions to remove from the store.
     * @param actionsToAdd The list of atomic actions to add to the store.
     * @param mergedActions The merged market actions to remove from the store of market actions.
     */
    public void updateAtomicActions(@Nonnull final EntitiesAndSettingsSnapshot snapshot,
                                    @Nonnull final List<Action> actionsToRemove,
                                    @Nonnull final List<Action> actionsToAdd,
                                    @Nonnull final List<Action> mergedActions) {
        actions.updateAtomicActions(actionsToRemove, actionsToAdd, mergedActions, snapshot);
    }

    /**
     * Check if the action is non-repeatable (i.e., cannot be re-executed).
     *
     * @param actionView the action to check
     * @return true if the action is non-repeatable
     */
    public static boolean isNonRepeatableAction(@Nonnull final ActionView actionView) {
        final ActionDTO.Action action = actionView.getRecommendation();
        try {
            return !SetUtils.emptyIfNull(REPEATABLE_ACTIONS.get(action.getInfo().getActionTypeCase()))
                    .contains(EntityType.forNumber(ActionDTOUtil.getPrimaryEntity(action).getType()));
        } catch (UnsupportedActionException e) {
            return true;
        }
    }

    public boolean populateBuyRIActions(@Nonnull ActionPlan actionPlan) {
        final long planId = actionPlan.getId();
        // (Oct 24 2019): When processing BuyRI actions we always use the realtime snapshot.
        // This is necessary because in a pure BuyRI plan we don't have a plan-specific source
        // topology. This is safe because we only need the names of the related regions and tiers,
        // which don't change between realtime and plan.
        final EntitiesAndSettingsSnapshot snapshot = entitySettingsCache.newSnapshot(
            ActionDTOUtil.getInvolvedEntityIds(actionPlan.getActionList()),
                Collections.emptySet(), topologyContextId);

        final Iterator<Long> recommendationOids;
        try {
            recommendationOids = actionIdentityService.getOidsForObjects(
                    Lists.transform(actionPlan.getActionList(), ActionDTO.Action::getInfo))
                    .iterator();
        } catch (IdentityServiceException e) {
            logger.error("Failed assigning OIDs to actions from plan " + actionPlan.getId(), e);
            return false;
        }
        final List<Action> actionsFromPlan = new ArrayList<>(actionPlan.getActionCount());
        for (ActionDTO.Action recommendedAction: actionPlan.getActionList()) {
            final long recommendationOid = recommendationOids.next();
            final Action action = actionFactory.newAction(recommendedAction, planId, recommendationOid);
            actionsFromPlan.add(action);
        }
        // All RI translations should be passthrough, but we do it here anyway for consistency
        // with the "normal" action case.
        actions.replaceRiActions(actionTranslator.translate(actionsFromPlan.stream(), snapshot));
        actions.updateBuyRIActions(snapshot);
        logger.info("Number of buy RI actions={}", actionPlan.getActionCount());
        return true;
    }

    /**
     * Get the {@link EntitiesWithNewStateCache}.
     *
     * @return the {@link EntitiesWithNewStateCache}.
     */
    public EntitiesWithNewStateCache getEntitiesWithNewStateCache() {
        return entitiesWithNewStateCache;
    }

    public ActionCounts getActionCounts() {
        return actions.getActionCounts();
    }

    /**
     * Compile previously recommended actions into the action store into the the associated
     * RecommendationTracker, completedSinceLastPopulate, and actionsToRemove data structures.
     * The {@link ActionSource} parameter controls whether we compile market or atomic
     * actions.
     *
     * @param recommendations The {@link RecommendationTracker} into which we put existing previous
     *                        recommendations that we may want to keep.
     * @param completedSinceLastPopulate Actions that have completed since the last action plan
     *                                   was processed.
     * @param actionsToRemove The list of actions to remove from the store.
     * @param actionSource controls whether we compile market or atomic actions.
     */
    public void compilePreviousActions(@Nonnull final RecommendationTracker recommendations,
                                       @Nonnull final List<ActionView> completedSinceLastPopulate,
                                       @Nonnull final List<Action> actionsToRemove,
                                       @Nonnull  final ActionSource actionSource) {
        final Consumer<Consumer<Action>> forEachAction = actionSource == ActionSource.MARKET
            ? actions::doForEachMarketAction
            : actions::doForEachAtomicAction;
        forEachAction.accept(action -> {
            // Retain QUEUED, PRE_IN_PROGRESS, IN-PROGRESS, POST_IN_PROGRESS, ACCEPTED,
            // REJECTED and READY actions which are re-recommended.
            switch (action.getState()) {
                case PRE_IN_PROGRESS:
                case IN_PROGRESS:
                case POST_IN_PROGRESS:
                case QUEUED:
                case FAILING:
                    recommendations.add(action);
                    break;
                case READY:
                case ACCEPTED:
                case REJECTED:
                    recommendations.add(action);
                    actionsToRemove.add(action);
                    break;
                case SUCCEEDED:
                    if (isNonRepeatableAction(action)) {
                        // Address the issue where market re-recommends the same action again
                        // due to outdated topology. OM-62071.
                        // Only do this for non-repeatable actions. OM-62427.
                        recommendations.add(action);
                    }
                case FAILED:
                    completedSinceLastPopulate.add(action);
                    actionsToRemove.add(action);
                    break;
                case CLEARED:
                    actionsToRemove.add(action);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown action state " + action.getState()
                        + " on action with OID " + action.getId());
            }
        });
    }

    @Override
    public int size() {
        return actions.size();
    }

    @Override
    public boolean allowsExecution() {
        // if the license is invalid, then disallow execution.
        return licenseCheckClient.hasValidNonExpiredLicense();
    }

    @Nonnull
    @Override
    public Optional<Action> getAction(long actionId) {
        return actions.getAction(actionId);
    }

    @Nonnull
    @Override
    public Optional<Action> getActionByRecommendationId(long recommendationId) {
        return actions.getActionByRecommendationId(recommendationId);
    }

    @Nonnull
    @Override
    public Optional<ActionView> getActionViewByRecommendationId(long recommendationId) {
        // The map operation is necessary because of how Java handles generics via type erasure.
        // An Optional<Action> is not directly assignable to an Optional<ActionView> even though an
        // Action is an ActionView.
        return getActionByRecommendationId(recommendationId)
            .map(Function.identity());
    }

    @Nonnull
    @Override
    public Map<Long, Action> getActions() {
        return actions.copy();
    }

    @Nonnull
    @Override
    public Map<ActionPlanType, Collection<Action>> getActionsByActionPlanType() {
        return actions.getActionsByPlanType();
    }

    @Nonnull
    @Override
    public Optional<ActionView> getActionView(long actionId) {
        // The map operation is necessary because of how Java handles generics via type erasure.
        // An Optional<Action> is not directly assignable to an Optional<ActionView> even though an
        // Action is an ActionView.
        return getAction(actionId)
            .map(Function.identity());
    }

    @Nonnull
    @Override
    public QueryableActionViews getActionViews() {
        return actions;
    }

    @Override
    public boolean overwriteActions(@Nonnull final Map<ActionPlanType, List<Action>> newActions) {
        // To be 100% correct we should probably add a method to overwrite both action types
        // atomically, but we only use this method when loading diags so it's ok to do this.
        newActions.forEach((actionType, actionsOfType) -> {
            switch (actionType) {
                case MARKET:
                    actions.replaceMarketActions(actionsOfType.stream());
                    break;
                case BUY_RI:
                    actions.replaceRiActions(actionsOfType.stream());
                    break;
            }
        });

        logger.info("Successfully overwrote actions in the store with {} new actions.", actions.size());
        return true;
    }

    /**
     * The {@link LiveActionStore} does not permit this operation.
     *
     * @return nothing will ever return since this operation always throws an exception.
     */
    @Override
    public boolean clear() {
        throw new IllegalStateException("Actions for the live market context " +
            topologyContextId + " may not be deleted.");
    }

    @Override
    public long getTopologyContextId() {
        return topologyContextId;
    }

    @Override
    @Nonnull
    public Optional<EntitySeverityCache> getEntitySeverityCache() {
        return Optional.of(severityCache);
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
     * Updates the enitities with new state cache.
     *
     * @param entitiesWithNewState ids of hosts that went into maintenance.
     */
    public void updateActionsBasedOnNewStates(@Nonnull final EntitiesWithNewState entitiesWithNewState) {

        entitiesWithNewStateCache.updateHostsWithNewState(entitiesWithNewState);
    }

    /**
     * An acceleration structure used to permit ordered lookups of actions by their {@link ActionInfo}.
     * This assists in rapidly matching {@link ActionInfo}s in a new {@link ActionPlan} with their
     * corresponding {@link Action}s currently in the store.
     * <p/>
     * Internally keeps a map of queues where the key is the ActionInfo for a recommended Action by the
     * market and the values are an ordered queue of the corresponding domain model {@link Action}s.
     */
    public static class RecommendationTracker implements Iterable<Action> {
        final Map<Long, Queue<Action>> recommendations = new HashMap<>();

        /**
         * Add an action to the tracker. Inserts an entry at the back of the queue
         * corresponding to the {@link ActionInfo} associated with the action.
         *
         * @param action The action to add to the tracker.
         */
        public void add(@Nonnull final Action action) {
            final Queue<Action> actions = recommendations.computeIfAbsent(action.getRecommendationOid(),
                k -> new LinkedList<>());
            actions.add(action);
        }

        /**
         *  Remove and return the action which matches the given
         *  ActionInfo.
         *
         * @param actionInfoId ActionInfo id - the id of the market recommendation
         * @return Action which has the ActionInfo info
         *
         * If the action exists, it is removed from the queue
         * and returned to the caller.
         */
        public Optional<Action> take(long actionInfoId) {
            Queue<Action> actions = recommendations.get(actionInfoId);
            if (actions == null) {
                return Optional.empty();
            } else {
                return actions.isEmpty() ? Optional.empty() : Optional.of(actions.remove());
            }
        }

        @Override
        public RemainingActionsIterator iterator() {
            return new RemainingActionsIterator();
        }

        /**
         * Get the number of actions in the tracker. Note this count can change over time as actions
         * are taken from the tracker.
         *
         * @return the number of actions in the tracker.
         */
        public int size() {
            return recommendations.size();
        }

        /**
         * Iterates over the remaining actions in the {@link RecommendationTracker}.
         */
        private class RemainingActionsIterator implements Iterator<Action> {
            private final Iterator<Queue<Action>> mapIterator =
                recommendations.values().iterator();
            private Iterator<Action> queueIterator = mapIterator.hasNext()
                ? mapIterator.next().iterator()
                : Collections.emptyIterator();

            @Override
            public boolean hasNext() {
                if (queueIterator.hasNext()) {
                    return true;
                } else if (!mapIterator.hasNext()) {
                    return false;
                }

                // try to get the 1st non-empty queue.
                while (mapIterator.hasNext() && !queueIterator.hasNext()) {
                    queueIterator = mapIterator.next().iterator();
                }
                return queueIterator.hasNext();
            }

            @Override
            public Action next() {
                return queueIterator.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public String toString() {
            return recommendations.toString();
        }
    }
}
