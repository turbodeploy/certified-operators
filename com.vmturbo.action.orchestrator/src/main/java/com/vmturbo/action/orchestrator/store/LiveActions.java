package com.vmturbo.action.orchestrator.store;

import static com.vmturbo.auth.api.authorization.scoping.UserScopeUtils.STATIC_CLOUD_ENTITY_TYPES;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.AcceptedActionsDAO;
import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.AcceptanceRemovalEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.ManualAcceptanceEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RejectionEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.RejectionRemovalEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.RejectedActionInfo;
import com.vmturbo.action.orchestrator.action.RejectedActionsDAO;
import com.vmturbo.action.orchestrator.exception.ActionStoreOperationException;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.InvolvedEntitiesExpander.InvolvedEntitiesFilter;
import com.vmturbo.action.orchestrator.store.query.QueryFilter;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessScopeException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.scoping.EntityAccessScope;
import com.vmturbo.auth.api.authorization.scoping.UserScopeUtils;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.components.common.identity.OidSet;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * A wrapper object for actions in the {@link LiveActionStore}.
 * It's closely coupled to the {@link LiveActionStore} implementation, and not meant
 * to be used outside of that.
 *
 * Meant to provide utilities for updating and accessing actions
 */
@ThreadSafe
class LiveActions implements QueryableActionViews {

    private static Logger logger = LogManager.getLogger();

    /**
     * Shared lock protecting access to actions and riActions maps.
     */
    private final ReadWriteLock actionsLock = new ReentrantReadWriteLock();

    private final ActionHistoryDao actionHistoryDao;

    private final Clock clock;

    private final AcceptedActionsDAO acceptedActionsStore;
    private final RejectedActionsDAO rejectedActionsStore;

    /**
     * (action id) -> ({@link Action}), where the {@link Action} is generated by
     * the market analysis.
     */
    @GuardedBy("actionsLock")
    private final Map<Long, Action> marketActions = new HashMap<>();

    /**
     * (recommendation id) -> ({@link Action}), where the {@link Action} is generated by
     * the market analysis.
     */
    @GuardedBy("actionsLock")
    private final Map<Long, Action> recommendationIdToMarketActionMap = new HashMap<>();

    /**
     * (action id) -> ({@link Action}), where the {@link Action} is generated by
     * the BuyRI algorithm.
     */
    @GuardedBy("actionsLock")
    private final Map<Long, Action> riActions = new HashMap<>();

    /**
     * (recommendation id) -> ({@link Action}), where the {@link Action} is generated by
     * the BuyRI algorithm.
     */
    @GuardedBy("actionsLock")
    private final Map<Long, Action> recommendationIdToRiActionMap = new HashMap<>();

    /**
     * (entity id) -> (IDs of actions the entity is involved in)
     *
     * The actions ids may refer to both BuyRI and market actions.
     */
    private final Map<Long, Set<Long>> actionsByEntityIdx = new HashMap<>();

    private final QueryFilterFactory queryFilterFactory;

    private final UserSessionContext userSessionContext;

    private final InvolvedEntitiesExpander involvedEntitiesExpander;

    LiveActions(@Nonnull final ActionHistoryDao actionHistoryDao,
                @Nonnull final AcceptedActionsDAO acceptedActionsStore,
                @Nonnull final RejectedActionsDAO rejectedActionsStore,
                @Nonnull final Clock clock,
                @Nonnull final UserSessionContext userSessionContext,
                @Nonnull final InvolvedEntitiesExpander involvedEntitiesExpander) {
        this(actionHistoryDao, acceptedActionsStore, rejectedActionsStore,
            clock,
            QueryFilter::new,
            userSessionContext,
            involvedEntitiesExpander);
    }

    @VisibleForTesting
    LiveActions(@Nonnull final ActionHistoryDao actionHistoryDao,
                @Nonnull final AcceptedActionsDAO acceptedActionsStore,
                @Nonnull final RejectedActionsDAO rejectedActionsStore,
                @Nonnull final Clock clock,
                @Nonnull QueryFilterFactory queryFilterFactory,
                @Nonnull final UserSessionContext userSessionContext,
                @Nonnull final InvolvedEntitiesExpander involvedEntitiesExpander) {
        this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
        this.clock = Objects.requireNonNull(clock);
        this.queryFilterFactory = Objects.requireNonNull(queryFilterFactory);
        this.userSessionContext = Objects.requireNonNull(userSessionContext);
        this.acceptedActionsStore = Objects.requireNonNull(acceptedActionsStore);
        this.rejectedActionsStore = Objects.requireNonNull(rejectedActionsStore);
        this.involvedEntitiesExpander = Objects.requireNonNull(involvedEntitiesExpander);
    }

    /**
     * Apply a function to every known market action.
     * Intended to provide a thread-safe way to iterate over the actions.
     *
     * @param consumer The function to apply.
     */
    void doForEachMarketAction(@Nonnull final Consumer<Action> consumer) {
        // Since we're not modifying the maps, we can get by with a read lock.
        actionsLock.readLock().lock();
        try {
            marketActions.values().forEach(consumer);
        } finally {
            actionsLock.readLock().unlock();
        }
    }

    private void addInvolvedEntitiesToIndex(@Nonnull final Collection<Action> actions) {
        final Iterator<Action> actionsIt = actions.iterator();
        int unsupportedActions = 0;
        while (actionsIt.hasNext()) {
            final Action action = actionsIt.next();
            try {
                ActionDTOUtil.getInvolvedEntityIds(action.getRecommendation())
                    .forEach(involvedEntityId ->
                        actionsByEntityIdx.computeIfAbsent(involvedEntityId, k -> new HashSet<>())
                            .add(action.getId()));
            } catch (UnsupportedActionException e) {
                // This shouldn't happen, because we shouldn't have put unsupported actions
                // into the store. But if it does happen, just remove the action.
                unsupportedActions++;
                actionsIt.remove();
            }
        }

        if (unsupportedActions > 0) {
            logger.error("Found {} unsupported actions when adding {} actions to index.",
                unsupportedActions, actions.size());
        }
    }

    /**
     * Update the indices - should be called every time actions change.
     */
    private void updateIndices() {
        // Grab the lock again just in case the caller forgot or released early.
        actionsLock.writeLock().lock();
        try {
            actionsByEntityIdx.clear();
            involvedEntitiesExpander.expandAllARMEntities();
            addInvolvedEntitiesToIndex(marketActions.values());
            addInvolvedEntitiesToIndex(riActions.values());
        } finally {
            actionsLock.writeLock().unlock();
        }
    }

    /**
     * Replace all market actions. THIS SHOULD ONLY BE CALLED WHEN RESTORING FROM DIAGS!
     * For normal operation use {@link LiveActions#
     * updateMarketActions(Collection, Collection, EntitiesAndSettingsSnapshot)}.
     *
     * @param newActions The new market actions.
     */
    void replaceMarketActions(@Nonnull final Stream<Action> newActions) {
        actionsLock.writeLock().lock();
        try {
            marketActions.clear();
            recommendationIdToMarketActionMap.clear();
            for (Iterator<Action> it = newActions.iterator(); it.hasNext(); ) {
                final Action action = it.next();
                marketActions.put(action.getId(), action);
                recommendationIdToMarketActionMap.put(action.getRecommendationOid(), action);
            }
            updateIndices();
        } finally {
            actionsLock.writeLock().unlock();
        }
    }

    /**
     * Replace all RI actions.
     *
     * @param newRiActions The new RI actions.
     */
    void replaceRiActions(@Nonnull final Stream<Action> newRiActions) {
        actionsLock.writeLock().lock();
        try {
            riActions.clear();
            recommendationIdToRiActionMap.clear();
            for (Iterator<Action> it = newRiActions.iterator(); it.hasNext(); ) {
                final Action action = it.next();
                riActions.put(action.getId(), action);
                recommendationIdToRiActionMap.put(action.getRecommendationOid(), action);
            }
            updateIndices();
        } finally {
            actionsLock.writeLock().unlock();
        }
    }

    /**
     * Update the RI Actions.
     * @param newEntitiesSnapshot The new {@link EntitiesAndSettingsSnapshot} to put into the entities
     *                            cache. This needs to be done atomically with the action addition,
     *                            because the mode calculation of those actions will depend on
     *                            the snapshot in the {@link EntitiesAndSettingsSnapshotFactory}.
     */
    void updateBuyRIActions(@Nonnull final EntitiesAndSettingsSnapshot newEntitiesSnapshot) {
        actionsLock.writeLock().lock();
        try {
            riActions.values().forEach(action -> refreshAction(action, newEntitiesSnapshot));
        } finally {
            actionsLock.writeLock().unlock();
        }
    }

    /**
     * Update the market actions atomically.
     *
     * @param actionsToRemove The {@link Action}s to remove.
     * @param actionsToAdd The {@link Action}s to add.
     * @param actionTargetSelector selects which target/probe to execute each action against
     * @param newEntitiesSnapshot The new {@link EntitiesAndSettingsSnapshot} to put into the entities
     *                            cache. This needs to be done atomically with the action addition,
     *                            because the mode calculation of those actions will depend on
     *                            the snapshot in the {@link EntitiesAndSettingsSnapshotFactory}.
     */
    void updateMarketActions(@Nonnull final Collection<Action> actionsToRemove,
            @Nonnull final Collection<Action> actionsToAdd,
            @Nonnull final EntitiesAndSettingsSnapshot newEntitiesSnapshot,
            @Nonnull final ActionTargetSelector actionTargetSelector) {
        actionsLock.writeLock().lock();
        try {
            // We used to do a marketActions.keySet().removeAll(actionsToRemove) here, but that
            // method had serious performance issues when the argument is a List that is bigger than
            // the set. (which was the case for us) The AbstractSet.removeAll() implementation ends
            // up being n^2 due to repetitive iterations through the List.
            for (final Action action : actionsToRemove) {
                marketActions.remove(action.getId());
                recommendationIdToMarketActionMap.remove(action.getRecommendationOid());
            }

            actionsToAdd.forEach(action -> {
                marketActions.put(action.getId(), action);
                recommendationIdToMarketActionMap.put(action.getRecommendationOid(), action);
            });

            updateIndices();

            // Now that we updated the entities + settings cache, refresh the action modes
            // of all market actions and set action description.
            marketActions.values().forEach(action -> refreshAction(action, newEntitiesSnapshot));

            // update state for accepted actions and for actions with removed acceptance and
            // update latest recommendation time for accepted actions
            processAcceptedActions(marketActions.values(), newEntitiesSnapshot,
                    actionTargetSelector);

            updateStateForRejectedActions(marketActions.values());

            marketActions.values().stream()
                .collect(Collectors.groupingBy(a ->
                    a.getRecommendation().getInfo().getActionTypeCase(), Collectors.counting())
                ).forEach((actionType, count) -> Metrics.ACTION_COUNTS_SUMMARY
                .labels(actionType.name())
                .observe((double) count));
        } finally {
            actionsLock.writeLock().unlock();
        }
    }

    private void updateStateForRejectedActions(@Nonnull final Collection<Action> marketActions) {
        final Set<Long> rejectedActions = rejectedActionsStore.getAllRejectedActions()
                .stream()
                .map(RejectedActionInfo::getRecommendationId)
                .collect(Collectors.toSet());
        for (Action action : marketActions) {
            if (rejectedActions.contains(action.getRecommendationOid())
                    && action.getState() == ActionState.READY) {
                if (action.receive(new RejectionEvent()).transitionNotTaken()) {
                    logger.error("Failed to transit action {} to REJECTED state", action.getId());
                }
            } else if (action.getState() == ActionState.REJECTED && !rejectedActions.contains(
                    action.getRecommendationOid())) {
                if (action.receive(new RejectionRemovalEvent()).transitionNotTaken()) {
                    logger.error("Failed to transit action {} from REJECTED state", action.getId());
                }
            }
        }
    }

    private void processAcceptedActions(@Nonnull final Collection<Action> allMarketActions,
            @Nonnull final EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot,
            @Nonnull final ActionTargetSelector actionTargetSelector) {
        updateStateForActionsWithRemovedAcceptance(allMarketActions);
        updateStateForAcceptedActions(allMarketActions,
                entitiesAndSettingsSnapshot, actionTargetSelector);
        updateLatestRecommendationTimeForAcceptedActions(allMarketActions);
    }

    private void updateStateForAcceptedActions(@Nonnull final Collection<Action> allMarketActions,
            @Nonnull final EntitiesAndSettingsSnapshot entitiesAndSettingsSnapshot,
            @Nonnull final ActionTargetSelector actionTargetSelector) {
        final List<Action> acceptedActions = allMarketActions.stream()
                .filter(action -> action.getSchedule().isPresent()
                        && action.getSchedule().get().getAcceptingUser() != null
                        && action.getState() == ActionState.READY)
                .collect(Collectors.toList());

        if (!acceptedActions.isEmpty()) {
            final Map<Long, ActionTargetInfo> targetsForAcceptedActions =
                    actionTargetSelector.getTargetsForActions(
                            acceptedActions.stream().map(Action::getRecommendation),
                            entitiesAndSettingsSnapshot);
            acceptedActions.forEach(action -> updateActionState(targetsForAcceptedActions, action));
        }
    }

    private void updateActionState(@Nonnull final Map<Long, ActionTargetInfo> targetsForAcceptedActions,
            @Nonnull final Action action) {
        final ActionTargetInfo actionTargetInfo =
                targetsForAcceptedActions.get(action.getId());
        if (actionTargetInfo.targetId().isPresent()) {
            if (actionTargetInfo.supportingLevel() == SupportLevel.SUPPORTED) {
                action.receive(new ManualAcceptanceEvent(
                        action.getSchedule().get().getAcceptingUser(),
                        targetsForAcceptedActions.get(action.getId()).targetId().get()));
            } else {
                logger.error("Action {} cannot be executed because it is not supported. "
                                + "Support level: {}", action.getRecommendationOid(),
                        actionTargetInfo.supportingLevel());
                action.receive(new FailureEvent(
                        "Action cannot be executed because it is not supported. Support level: "
                                + actionTargetInfo.supportingLevel()));
            }
        } else {
            logger.error("There is no target for accepted action {}.", action.toString());
            action.receive(new FailureEvent("Action cannot be executed by any target."));
        }
    }

    private void updateStateForActionsWithRemovedAcceptance(
            @Nonnull final Collection<Action> allMarketActions) {
        final List<Action> actionsWithRemovedAcceptance = allMarketActions.stream()
                .filter(action -> action.getSchedule().isPresent()
                        && action.getSchedule().get().getAcceptingUser() == null
                        && action.getState() == ActionState.ACCEPTED)
                .collect(Collectors.toList());

        actionsWithRemovedAcceptance.forEach(action -> action.receive(new AcceptanceRemovalEvent()));
    }

    private void updateLatestRecommendationTimeForAcceptedActions(
            @Nonnull Collection<Action> allMarketActions) {
        final List<Long> acceptedRecommendationsIds = allMarketActions.stream()
                .filter(action -> action.getSchedule().isPresent()
                        && action.getSchedule().get().getAcceptingUser() != null)
                .map(Action::getRecommendationOid)
                .collect(Collectors.toList());
        if (!acceptedRecommendationsIds.isEmpty()) {
            try {
                acceptedActionsStore.updateLatestRecommendationTime(acceptedRecommendationsIds);
            } catch (ActionStoreOperationException ex) {
                logger.warn("Last recommended time wasn't updated for all accepted actions.", ex);
            }
        }
    }

    /**
     * Delete certain actions.
     *
     * @param actionsToRemove Set of actions to remove
     */
    void deleteActions(@Nonnull final Set<Action> actionsToRemove) {
        actionsLock.writeLock().lock();
        try {
            for (Action actionToRemove : actionsToRemove) {
                marketActions.remove(actionToRemove.getId());
                recommendationIdToMarketActionMap.remove(actionToRemove.getRecommendationOid());
            }
        } finally {
            actionsLock.writeLock().unlock();
        }
    }

    private static void refreshAction(
            @Nonnull final Action action,
            @Nonnull final EntitiesAndSettingsSnapshot newEntitiesSnapshot) {
        // We want to update dynamic information not only for "READY" actions, but also for
        // "ACCEPTED" and "QUEUED" in order to update information about action schedule.
        // For other action states it doesn't make sense to retroactively modify the action mode or other dynamic information
        final ActionState actionState = action.getState();
        if (actionState == ActionState.READY || actionState == ActionState.ACCEPTED
                || actionState == ActionState.QUEUED) {
            try {
                action.refreshAction(newEntitiesSnapshot);
            } catch (UnsupportedActionException e) {
                logger.error("Failed to refresh action " + action, e);
            }
        }
    }

    /**
     * Get the total number of actions.
     */
    public int size() {
        actionsLock.readLock().lock();
        try {
            return marketActions.size() + riActions.size();
        } finally {
            actionsLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<ActionView> get(@Nonnull final Collection<Long> actionIds) {
        // Build up the result set while holding the lock.
        final List<ActionView> results = new ArrayList<>(actionIds.size());
        actionsLock.readLock().lock();
        try {
            actionIds.stream()
                .map(this::get)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(results::add);
        } finally {
            actionsLock.readLock().unlock();
        }
        return results.stream();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    public Optional<ActionView> get(final long actionId) {
        actionsLock.readLock().lock();
        try {
            Action result = marketActions.get(actionId);
            if (result == null) {
                result = riActions.get(actionId);
            }
            checkActionAccess(result);
            return Optional.ofNullable(result);
        } finally {
            actionsLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<ActionView> get(@Nonnull final ActionQueryFilter actionQueryFilter) {
        final Stream<ActionView> currentActions;

        // if the user has an access scope restriction, then we'll apply it here. The logic will be:
        // 1) if the request is for a specific set of entities, process it -- the request will
        //    get rejected if any requested entities are out of scope in the "getByEntity" method.
        // 2) if the request is for the whole market, and the user is scoped -- set the entities
        //    restriction to the set of accessible oids for the user. (NOTE: this can be a very
        //    large set!!)
        // 3) otherwise, the request is for the whole market and the user is NOT scoped -- fetch
        //    all entities without restriction.
        InvolvedEntityCalculation involvedEntityCalculation = InvolvedEntityCalculation.INCLUDE_ALL_INVOLVED_ENTITIES;
        final Set<Long> entitiesRestriction;
        if (actionQueryFilter.hasInvolvedEntities()) {
            final List<Long> oidsList = actionQueryFilter.getInvolvedEntities().getOidsList();
            // If involved entities contains only ARM entities, it will be expanded.
            // If it contains at least one non-arm entity, it will not be expanded.
            InvolvedEntitiesFilter filter =
                involvedEntitiesExpander.expandInvolvedEntitiesFilter(oidsList);
            entitiesRestriction = filter.getEntities();
            involvedEntityCalculation = filter.getCalculationType();
        } else if (userSessionContext.isUserScoped()) {
            entitiesRestriction = userSessionContext.getUserAccessScope().accessibleOids().toSet();
        } else {
            // null indicates to use this.getAll() later on and to not filter
            // succeededOrFailedActionList by involved entities
            entitiesRestriction = null;
        }

        // The getAll() and getByEntity() methods re-acquire the lock, but we do it here just
        // to be defensive.
        actionsLock.readLock().lock();
        try {
            if (entitiesRestriction != null) {
                currentActions = this.getByEntity(entitiesRestriction);
            } else {
                currentActions = this.getAll();
            }
        } finally {
            actionsLock.readLock().unlock();
        }

        final Stream<ActionView> candidateActionViews;
        if (actionQueryFilter.hasStartDate() && actionQueryFilter.hasEndDate()) {
            final LocalDateTime startDate = getLocalDateTime(actionQueryFilter.getStartDate());
            final LocalDateTime endDate = getLocalDateTime(actionQueryFilter.getEndDate());
            final List<ActionView> succeededOrFailedActionList =
                actionHistoryDao.getActionHistoryByDate(startDate, endDate);
            Stream<ActionView> historical = succeededOrFailedActionList.stream()
                .filter(view -> {
                    if (entitiesRestriction != null) {
                        try {
                            // include actions with ANY involved entities in the set.
                            return ActionDTOUtil.getInvolvedEntityIds(view.getRecommendation()).stream()
                                .anyMatch(entitiesRestriction::contains);
                        } catch (UnsupportedActionException e) {
                            return false;
                        }
                    } else {
                        return true;
                    }
                });
            final Stream<ActionView> current = currentActions
                .filter(action -> !isSucceededorFailed(action))
                .filter(action -> endDate.compareTo(action.getRecommendationTime()) > 0);

            candidateActionViews = Stream.concat(historical, current);
        } else {
            candidateActionViews = currentActions;
        }

        final QueryFilter queryFilter =
            queryFilterFactory.newQueryFilter(
                actionQueryFilter,
                LiveActionStore.VISIBILITY_PREDICATE,
                entitiesRestriction != null ? entitiesRestriction : Collections.emptySet(),
                involvedEntityCalculation);
        return candidateActionViews.filter(queryFilter::test);
    }

    private boolean isSucceededorFailed(@Nonnull final ActionView action) {
        final ActionState state = action.getState();
        return (state == ActionState.SUCCEEDED || state == ActionState.FAILED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<ActionView> getByEntity(@Nonnull final Collection<Long> involvedEntities) {
        // if the user is scoped, verify that the user has access to all of the requested entities.
        if (userSessionContext.isUserScoped()) {
            UserScopeUtils.checkAccess(userSessionContext, involvedEntities);
        }

        // We need to copy the matching action views into an intermediate list
        // while holding the lock.
        final List<ActionView> results = new ArrayList<>();
        actionsLock.readLock().lock();
        try {
            involvedEntities.stream()
                .map(entityId -> actionsByEntityIdx.getOrDefault(entityId, Collections.emptySet()))
                .flatMap(Collection::stream)
                // De-dupe the target action ids.
                .distinct()
                .map(actionId -> {
                    try { // filter out inaccessible actions
                        return get(actionId);
                    } catch (UserAccessScopeException uase) {
                        return Optional.<ActionView>empty();
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(results::add);
        } finally {
            actionsLock.readLock().unlock();
        }
        return results.stream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<ActionView> getAll() {
        // if the user is scoped, get the subset of actions for the entities they have access to.
        if (userSessionContext.isUserScoped()) {
            return getByEntity(userSessionContext.getUserAccessScope().accessibleOids().toSet());
        }
        // otherwise, the user is not scoped and we can pull them all right out of the map.

        // We need to make a copy, because we can't stream while holding the lock.
        final Map<Long, ActionView> copy = Collections.unmodifiableMap(copy());
        return copy.values().stream();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isEmpty() {
        actionsLock.readLock().lock();
        try {
            return riActions.isEmpty() && marketActions.isEmpty();
        } finally {
            actionsLock.readLock().unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    public Optional<Action> getAction(final long actionId) {
        actionsLock.readLock().lock();
        try {
            Action result = marketActions.get(actionId);
            if (result == null) {
                result = riActions.get(actionId);
            }
            checkActionAccess(result);
            return Optional.ofNullable(result);
        } finally {
            actionsLock.readLock().unlock();
        }
    }

    /**
     * Get action by recommendation id.
     *
     * @param recommendationId stable identifier of action
     * @return the Optional.of(Action) if action is present, otherwise
     * Optional.empty()
     */
    @Nonnull
    public Optional<Action> getActionByRecommendationId(final long recommendationId) {
        actionsLock.readLock().lock();
        try {
            Action result = recommendationIdToMarketActionMap.get(recommendationId);
            if (result == null) {
                result = recommendationIdToRiActionMap.get(recommendationId);
            }
            checkActionAccess(result);
            return Optional.ofNullable(result);
        } finally {
            actionsLock.readLock().unlock();
        }
    }

    /**
     * See {@link ActionStore#getActionsByActionPlanType()}.
     */
    @Nonnull
    Map<ActionPlanType, Collection<Action>> getActionsByPlanType() {
        actionsLock.readLock().lock();
        try {
            Map<ActionPlanType, Collection<Action>> results = Maps.newHashMap();
            if (!marketActions.isEmpty()) {
                results.put(ActionPlanType.MARKET, marketActions.values());
            }
            if (!riActions.isEmpty()) {
                results.put(ActionPlanType.BUY_RI, riActions.values());
            }
            return results;
        } finally {
            actionsLock.readLock().unlock();
        }
    }

    /**
     * Return a copy of all the actions in the store.
     * There is no way to get the actions without copying because that would violate thread-safety.
     */
    @Nonnull
    public Map<Long, Action> copy() {
        actionsLock.readLock().lock();
        try {
            final Map<Long, Action> actionMap = new HashMap<>(marketActions);
            actionMap.putAll(riActions);
            return Collections.unmodifiableMap(actionMap);
        } finally {
            actionsLock.readLock().unlock();
        }
    }

    /**
     * Check the current user's access to the action. The default case is that access is allowed,
     * but if the user is scoped, then we'll check to make sure that at least one involved entity is in
     * the user's accessible entity set.
     *
     * @param action The action to check access on.
     */
    private void checkActionAccess(ActionView action) {
        if (action == null) {
            return;
        }

        if (userSessionContext.isUserScoped()) {
            try {
                EntityAccessScope entityAccessScope = userSessionContext.getUserAccessScope();
                Set<Long> involvedEntityIds = ActionDTOUtil.getInvolvedEntityIds(action.getRecommendation());
                if (!entityAccessScope.containsAny(involvedEntityIds)) {
                    throw new UserAccessScopeException("User does not have access to all entities involved in action.");
                }
                OidSet cloudStaticEntities = entityAccessScope
                        .getAccessibleOidsByEntityTypes(STATIC_CLOUD_ENTITY_TYPES);
                // Check if involved entities are accessible for scoped user and is not a Cloud Static Entities.
                boolean hasAccess =
                        involvedEntityIds.stream().anyMatch(entityId ->
                                (entityAccessScope.contains(entityId) &&
                                        !cloudStaticEntities.contains(entityId)));
                if (!hasAccess) {
                    throw new UserAccessScopeException("User does not have access to entity involved in action");
                }
            } catch (UnsupportedActionException uae) {
                logger.error("Unsupported action", uae);
            }
        }
    }

    private static class Metrics {
        private static final DataMetricSummary ACTION_COUNTS_SUMMARY = DataMetricSummary.builder()
            .withName("ao_live_action_counts")
            .withHelp("Number of actions in the action orchestrator live store.")
            .withLabelNames("action_type")
            .build()
            .register();
    }

    /**
     * An acceleration structure used to permit ordered lookups of actions by their {@link ActionInfo}.
     * This assists in rapidly matching {@link ActionInfo}s in a new {@link ActionPlan} with their
     * corresponding {@link Action}s currently in the store.
     *
     * Internally keeps a map of queues where the key is the ActionInfo for a recommended Action by the
     * market and the values are an ordered queue of the corresponding domain model {@link Action}s.
     */
    static class RecommendationTracker implements Iterable<Action> {
        final Map<Long, Queue<Action>> recommendations = new HashMap<>();

        /**
         * Add an action to the tracker. Inserts an entry at the back of the queue
         * corresponding to the {@link ActionInfo} associated with the action.
         *
         * @param action The action to add to the tracker.
         */
        void add(@Nonnull final Action action) {
            Queue<Action> actions = recommendations.get(action.getRecommendationOid());
            if (actions == null) {
                actions = new LinkedList<>();
                recommendations.put(action.getRecommendationOid(), actions);
            }

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
        Optional<Action> take(long actionInfoId) {
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
         * Iterates over the remaining actions in the {@link RecommendationTracker}.
         */
        private class RemainingActionsIterator implements Iterator<Action> {
            private final Iterator<Queue<Action>> mapIterator =
                    recommendations.values().iterator();
            private Iterator<Action> queueIterator = mapIterator.hasNext() ?
                    mapIterator.next().iterator() :
                    Collections.emptyIterator();

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

    /**
     * Convert date time to local date time.
     *
     * @param dateTime date time with long type.
     * @return local date time with LocalDateTime type.
     */
    private LocalDateTime getLocalDateTime(final long dateTime) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(dateTime), clock.getZone());
    }

    @FunctionalInterface
    interface QueryFilterFactory {

        QueryFilter newQueryFilter(@Nonnull ActionQueryFilter filter,
                                   @Nonnull Predicate<ActionView> visibilityPredicate,
                                   @Nullable Set<Long> entitiesRestriction,
                                   @Nonnull InvolvedEntityCalculation involvedEntityCalculation);

    }
}
