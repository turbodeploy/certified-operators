package com.vmturbo.action.orchestrator.store;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * {@inheritDoc}
 *
 * Stores actions mainly in memory except executed (SUCCEEDED and FAILED) actions
 * For use with actions from the live market (sometimes also called "real time").
 */
@ThreadSafe
public class LiveActionStore implements ActionStore {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Wrap with {@link Collections#synchronizedMap} to ensure correct concurrent access.
     */
    @GuardedBy("actionsLock")
    private final Map<Long, Action> actions = Collections.synchronizedMap(new LinkedHashMap<>());

    @GuardedBy("actionsLock")
    private final Map<Long, Action> riActions = Collections.synchronizedMap(new LinkedHashMap<>());

    // shared lock protecting access to actions and riActions maps.
    private static Object actionsLock = new Object();

    private final IActionFactory actionFactory;

    private final long topologyContextId;

    private final EntitySeverityCache severityCache;

    private final EntitySettingsCache entitySettingsCache;

    private final ActionHistoryDao actionHistoryDao;

    private static final DataMetricSummary ACTION_COUNTS_SUMMARY = DataMetricSummary.builder()
        .withName("ao_live_action_counts")
        .withHelp("Number of actions in the action orchestrator live store.")
        .withLabelNames("action_type")
        .build()
        .register();

    private static final String STORE_TYPE_NAME = "Live";

    private final ActionSupportResolver actionSupportResolver;

    private final LiveActionsStatistician actionsStatistician;

    /**
     * A mutable (real-time) action is considered visible (from outside the Action Orchestrator's perspective)
     * if it's not disabled and has either had a decision, or is executable.
     *
     * <p>Non-visible actions can still be valuable for debugging
     * purposes, but they shouldn't be exposed externally.
     *
     * @param actionView The {@link ActionView} to test for visibility.
     * @return True if the spec is visible to the UI, false otherwise.
     */
    public static final Predicate<ActionView> VISIBILITY_PREDICATE = actionView ->
        !ActionMode.DISABLED.equals(actionView.getMode()) &&
            (!ActionState.READY.equals(actionView.getState()) || actionView
                    .determineExecutability());

    /**
     * Create a new {@link ActionStore} for storing live actions (actions generated by the realtime market).
     *
     * @param actionFactory The factory for generating new actions.
     * @param topologyContextId The contextId for the live (realtime) market.
     * @param actionSupportResolver used to determine action capabilities.
     * @param entitySettingsCache caches the entity capabilities per entity
     * @param actionHistoryDao obtains history of actions
     */
    public LiveActionStore(@Nonnull final IActionFactory actionFactory,
                           final long topologyContextId,
                           @Nonnull final ActionSupportResolver actionSupportResolver,
                           @Nonnull final EntitySettingsCache entitySettingsCache,
                           @Nonnull final ActionHistoryDao actionHistoryDao,
                           @Nonnull final LiveActionsStatistician liveActionsStatistician) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.topologyContextId = topologyContextId;
        this.severityCache = new EntitySeverityCache(QueryFilter.VISIBILITY_FILTER);
        this.actionSupportResolver = actionSupportResolver;
        this.entitySettingsCache = entitySettingsCache;
        this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
        this.actionsStatistician = Objects.requireNonNull(liveActionsStatistician);
    }

    /**
     * An acceleration structure used to permit ordered lookups of actions by their {@link ActionInfo}.
     * This assists in rapidly matching {@link ActionInfo}s in a new {@link ActionPlan} with their
     * corresponding {@link Action}s currently in the store.
     *
     * Internally keeps a map of queues where the key is the ActionInfo for a recommended Action by the
     * market and the values are an ordered queue of the corresponding domain model {@link Action}s.
     */
    @VisibleForTesting
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
     */
    @Override
    public boolean populateRecommendedActions(@Nonnull final ActionPlan actionPlan) {

        if (actionPlan.hasActionPlanType() && actionPlan.getActionPlanType() == ActionPlanType.BUY_RI) {
            return populateBuyRIActions(actionPlan);
        }

        // RecommendationTracker to accelerate lookups of recommendations.
        RecommendationTracker recommendations = new RecommendationTracker();

        // Apply addition and removal to the internal store atomically.
        // It is generally not safe to call synchronized on a complex object not dedicated
        // to the purpose of locking because of the possibility of deadlock, but
        // SynchronizedCollections are an exception to this rule.
        final List<ActionView> completedSinceLastPopulate = new ArrayList<>();
        synchronized (actionsLock) {

            // Only retain IN-PROGRESS, QUEUED and READY actions which are re-recommended.
            List<Long> actionsToRemove = new ArrayList<>();
            actions.values().forEach(action -> {
                switch (action.getState()) {
                    case IN_PROGRESS:
                    case QUEUED:
                        recommendations.add(action);
                        break;
                    case READY:
                        recommendations.add(action);
                        actionsToRemove.add(action.getId());
                        break;
                    case SUCCEEDED:
                    case FAILED:
                        completedSinceLastPopulate.add(action);
                        actionsToRemove.add(action.getId());
                        break;
                    default:
                        actionsToRemove.add(action.getId());
                }
            });

            actions.keySet().removeAll(actionsToRemove);

            final long planId = actionPlan.getId();
            final Set<Long> entitiesToRetrieve = new HashSet<>();
            final AtomicInteger newActionCounts = new AtomicInteger(0);
            for (ActionDTO.Action recommendedAction : actionPlan.getActionList() ) {
                final Optional<Action> existingActionOpt = recommendations.take(recommendedAction.getInfo());
                final Action action;
                if (existingActionOpt.isPresent()) {
                    action = existingActionOpt.get();

                    // If we are re-using an existing action, we should update the recommendation
                    // so other properties that may have changed (e.g. importance, executability)
                    // reflect the most recent recommendation from the market. However, we only
                    // do this for "READY" actions. An IN_PROGRESS or QUEUED action is considered
                    // "fixed" until it either succeeds or fails.
                    // TODO (roman, Oct 31 2018): If a QUEUED action becomes non-executable, it
                    // may be worth clearing it.
                    if (action.getState() == ActionState.READY) {
                        action.updateRecommendation(recommendedAction);
                    }
                } else {
                    newActionCounts.getAndIncrement();
                    action = actionFactory.newAction(recommendedAction, entitySettingsCache, planId);
                }

                if (action.getState() == ActionState.READY) {
                    try {
                        entitiesToRetrieve.addAll(ActionDTOUtil.getInvolvedEntityIds(recommendedAction));
                        actions.put(action.getId(), action);
                    } catch (UnsupportedActionException e) {
                        logger.error("Recommendation contains unsupported action", e);
                    }
                }
            }
            logger.info("Number of Re-Recommended actions={}, Newly created actions={}",
                    (actionPlan.getActionCount() - newActionCounts.get()),
                    newActionCounts);

            entitySettingsCache.update(entitiesToRetrieve,
                    actionPlan.getTopologyContextId(), actionPlan.getTopologyId());

            filterActionsByCapabilityForUiDisplaying();

            // Clear READY or QUEUED actions that were not re-recommended. If they were
            // re-recommended, they would have been removed from the RecommendationTracker
            // above.
            StreamSupport.stream(recommendations.spliterator(), false)
                .filter(action -> (action.getState() == ActionState.READY
                            || action.getState() == ActionState.QUEUED))
                .forEach(action -> action.receive(new NotRecommendedEvent(planId)));

            actions.values().stream()
                .collect(Collectors.groupingBy(a ->
                    a.getRecommendation().getInfo().getActionTypeCase(), Collectors.counting())
                ).forEach((actionType, count) -> ACTION_COUNTS_SUMMARY
                    .labels(actionType.name())
                    .observe((double)count));

            // Record the action stats.
            // TODO (roman, Nov 15 2018): For actions completed since the last snapshot, it may make
            // sense to use the last snapshot's time instead of the current snapshot's time.
            // Not doing it for now because of the extra complexity - and it's not clear if anyone
            // cares if the counts are off by ~10 minutes.
            actionsStatistician.recordActionStats(actionPlan.getTopologyId(),
                    // Only record user-visible actions.
                    Stream.concat(completedSinceLastPopulate.stream(), actions.values().stream())
                            .filter(VISIBILITY_PREDICATE));
        }

        return true;
    }

    private boolean populateBuyRIActions(@Nonnull ActionPlan actionPlan) {

        synchronized (actionsLock) {
            final long planId = actionPlan.getId();
            riActions.clear();
            for (ActionDTO.Action recommendedAction : actionPlan.getActionList() ) {
                ActionInfo actionInfo = recommendedAction.getInfo();
                final Action action = actionFactory.newAction(recommendedAction, planId);
                riActions.put(action.getId(), action);
            }
            logger.info("Number of buy RI actions={}", actionPlan.getActionCount());
        }
        return true;
    }

    private void filterActionsByCapabilityForUiDisplaying() {
        try {
            // This method is called within a synchronized block in method populateRecommendedActions.
            // Therefore, the access to the actions object is not synchronized in this method.
            final Collection<Action> filteredForUiActions = actionSupportResolver
                .resolveActionsSupporting(actions.values());
            filteredForUiActions.forEach(action -> actions.put(action.getId(), action));
        } catch (RuntimeException e) {
            logger.error("Error filtering actions by capability: ", e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        synchronized (actionsLock) {
            return actions.size() + riActions.size();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean allowsExecution() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<Action> getAction(long actionId) {
        synchronized (actionsLock) {
            Action result = actions.get(actionId);
            if (result == null) {
               return Optional.ofNullable(riActions.get(actionId));
            }
            return Optional.of(result);
        }
    }

    @Nonnull
    @Override
    public Map<Long, Action> getActions() {
        synchronized (actionsLock) {
            Map<Long, Action> actionMap = new HashMap<>(actions);
            actionMap.putAll(riActions);
            return Collections.unmodifiableMap(actionMap);
        }
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


    /**
     * SUCCEEDED and FAILED {@link Action} are from DB.
     * All other {@link Action} are from actions map.
     *
     * @return merged actions
     */
    @Nonnull
    @Override
    public Map<Long, ActionView> getActionViews() {
        // TODO: (DavidBlinn) This imposes an unacceptable performance hit and
        // seems unnecessary. Refactor to improve performance. We should probably
        // have separate RPCs and interfaces for querying operational vs historical actions
        // since we have to hand-roll the code to join the two in any case.
        List<ActionView> succeededOrFailedActionList = actionHistoryDao.getAllActionHistory();
        List<ActionView> otherActionList;
        synchronized (actionsLock) {
            otherActionList = actions.values().stream()
                    .filter(action -> !isSucceededorFailed(action))
                    .collect(Collectors.toList());
        }
        List<ActionView> combinedActionList = Stream.of(succeededOrFailedActionList, otherActionList)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
        Map<Long, ActionView> combinedActionMap = combinedActionList.stream()
                .collect(Collectors.toMap(ActionView::getId, Function.identity()));
        return Collections.unmodifiableMap(combinedActionMap);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, ActionView> getActionViewsByDate(@Nonnull final LocalDateTime startDate,
                                                      @Nonnull final LocalDateTime endDate) {
        List<ActionView> succeededOrFailedActionList = actionHistoryDao.getActionHistoryByDate(startDate, endDate);
        List<ActionView> otherActionList;
        synchronized (actionsLock) {
            otherActionList = actions.values().stream()
                    .filter(action -> !isSucceededorFailed(action))
                    .filter(action -> isEndDateAfterRecommendationTime(action, endDate))
                    .collect(Collectors.toList());
        }
        Map<Long, ActionView> combinedActionMap = Stream.of(succeededOrFailedActionList, otherActionList)
                .flatMap(Collection::stream)
                .collect(Collectors.toMap(ActionView::getId, Function.identity()));
        return Collections.unmodifiableMap(combinedActionMap);
    }

    private boolean isEndDateAfterRecommendationTime(@Nonnull final ActionView acionView,
                                                     @Nonnull final LocalDateTime endDate) {
        return endDate.compareTo(acionView.getRecommendationTime()) > 0;
    }

    @Override
    public boolean overwriteActions(@Nonnull final List<Action> newActions) {
        synchronized (actionsLock) {
            actions.clear();
            newActions.forEach(action -> actions.put(action.getId(), action));
        }

        logger.info("Successfully overwrote actions in the store with {} new actions.", newActions.size());
        return true;
    }

    /**
     * {@inheritDoc}
     * The {@link LiveActionStore} does not permit this operation.
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

    private boolean isSucceededorFailed(@Nonnull final Action action) {
        final ActionState state = action.getState();
        return (state == ActionState.SUCCEEDED || state == ActionState.FAILED);
    }
}
