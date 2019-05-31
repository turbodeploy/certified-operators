package com.vmturbo.action.orchestrator.store;

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
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.store.query.QueryFilter;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
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

    /**
     * (action id) -> ({@link Action}), where the {@link Action} is generated by
     * the market analysis.
     */
    @GuardedBy("actionsLock")
    private final Map<Long, Action> marketActions = new HashMap<>();

    /**
     * (action id) -> ({@link Action}), where the {@link Action} is generated by
     * the BuyRI algorithm.
     */
    @GuardedBy("actionsLock")
    private final Map<Long, Action> riActions = new HashMap<>();

    /**
     * (entity id) -> (IDs of actions the entity is involved in)
     *
     * The actions ids may refer to both BuyRI and market actions.
     */
    private final Map<Long, Set<Long>> actionsByEntityIdx = new HashMap<>();

    private final QueryFilterFactory queryFilterFactory;

    LiveActions(@Nonnull final ActionHistoryDao actionHistoryDao,
                @Nonnull final Clock clock) {
        this(actionHistoryDao,
            clock,
            QueryFilter::new);
    }

    @VisibleForTesting
    LiveActions(@Nonnull final ActionHistoryDao actionHistoryDao,
                @Nonnull final Clock clock,
                QueryFilterFactory queryFilterFactory) {
        this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
        this.clock = Objects.requireNonNull(clock);
        this.queryFilterFactory = Objects.requireNonNull(queryFilterFactory);
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
            marketActions.values().forEach(consumer::accept);
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
            addInvolvedEntitiesToIndex(marketActions.values());
            addInvolvedEntitiesToIndex(riActions.values());
        } finally {
            actionsLock.writeLock().unlock();
        }
    }

    /**
     * Replace all market actions. THIS SHOULD ONLY BE CALLED WHEN RESTORING FROM DIAGS!
     * For normal operation use {@link LiveActions#updateMarketActions(Collection, Collection, EntitiesAndSettingsSnapshot)}.
     *
     * @param newActions The new market actions.
     */
    void replaceMarketActions(@Nonnull final Stream<Action> newActions) {
        actionsLock.writeLock().lock();
        try {
            marketActions.clear();
            newActions.forEach(action -> marketActions.put(action.getId(), action));
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
            newRiActions.forEach(action -> riActions.put(action.getId(), action));
            updateIndices();
        } finally {
            actionsLock.writeLock().unlock();
        }
    }

    /**
     * Update the market actions atomically.
     *
     * @param actionsToRemove The ids of actions to remove.
     * @param actionsToAdd The {@link Action}s to add.
     * @param newEntitiesSnapshot The new {@link EntitiesAndSettingsSnapshot} to put into the entities
     *                            cache. This needs to be done atomically with the action addition,
     *                            because the mode calculation of those actions will depend on
     *                            the snapshot in the {@link EntitiesAndSettingsSnapshotFactory}.
     */
    void updateMarketActions(@Nonnull final Collection<Long> actionsToRemove,
                             @Nonnull final Collection<Action> actionsToAdd,
                             @Nonnull final EntitiesAndSettingsSnapshot newEntitiesSnapshot) {
        actionsLock.writeLock().lock();
        try {
            marketActions.keySet().removeAll(actionsToRemove);
            actionsToAdd.forEach(action -> marketActions.put(action.getId(), action));
            updateIndices();

            // Now that we updated the entities + settings cache, refresh the action modes
            // of all market actions.
            marketActions.values().forEach(action -> action.refreshActionMode(newEntitiesSnapshot));

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
                return Optional.ofNullable(riActions.get(actionId));
            }
            return Optional.of(result);
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

        final Optional<Set<Long>> entitiesRestriction = actionQueryFilter.hasInvolvedEntities() ?
            Optional.of(Sets.newHashSet(actionQueryFilter.getInvolvedEntities().getOidsList())) :
            Optional.empty();

        // The getAll() and getByEntity() methods re-acquire the lock, but we do it here just
        // to be defensive.
        actionsLock.readLock().lock();
        try {
            currentActions = entitiesRestriction
                .map(this::getByEntity)
                .orElseGet(this::getAll);
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
                .filter(view -> entitiesRestriction
                    .map(involvedEntities -> {
                        try {
                            return ActionDTOUtil.getInvolvedEntityIds(view.getRecommendation()).stream()
                                .anyMatch(involvedEntities::contains);
                        } catch (UnsupportedActionException e) {
                            return false;
                        }
                    }).orElse(true)
                );
            final Stream<ActionView> current = currentActions
                .filter(action -> !isSucceededorFailed(action))
                .filter(action -> endDate.compareTo(action.getRecommendationTime()) > 0);

            candidateActionViews = Stream.concat(historical, current);
        } else {
            candidateActionViews = currentActions;
        }

        final QueryFilter queryFilter =
            queryFilterFactory.newQueryFilter(actionQueryFilter, LiveActionStore.VISIBILITY_PREDICATE);
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
    @Override
    public Stream<ActionView> getAll() {
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
                return Optional.ofNullable(riActions.get(actionId));
            }
            return Optional.of(result);
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
        final Map<ActionInfo, Queue<Action>> recommendations = new HashMap<>();

        /**
         * Add an action to the tracker. Inserts an entry at the back of the queue
         * corresponding to the {@link ActionInfo} associated with the action.
         *
         * @param action The action to add to the tracker.
         */
        void add(@Nonnull final Action action) {
            final ActionInfo info = action.getRecommendation().getInfo();
            Queue<Action> actions = recommendations.get(info);
            if (actions == null) {
                actions = new LinkedList<>();
                recommendations.put(info, actions);
            }

            actions.add(action);
        }

        /**
         *  Remove and return the action which matches the given
         *  ActionInfo.
         *
         * @param info ActionInfo
         * @return Action which has the ActionInfo info
         *
         * If the action exists, it is removed from the queue
         * and returned to the caller.
         */
        Optional<Action> take(@Nonnull final ActionInfo info) {
            Queue<Action> actions = recommendations.get(info);
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

        QueryFilter newQueryFilter(@Nonnull final ActionQueryFilter actionQueryFilter,
                                   @Nonnull final Predicate<ActionView> actionViewPredicate);

    }
}
