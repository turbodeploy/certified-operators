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

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Collections2;
import com.google.common.collect.Maps;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionHistoryDao;
import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector;
import com.vmturbo.action.orchestrator.execution.ActionTargetSelector.ActionTargetInfo;
import com.vmturbo.action.orchestrator.execution.ProbeCapabilityCache;
import com.vmturbo.action.orchestrator.stats.LiveActionsStatistician;
import com.vmturbo.action.orchestrator.store.EntitiesCache.Snapshot;
import com.vmturbo.action.orchestrator.translation.ActionTranslator;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Action.SupportLevel;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.proactivesupport.DataMetricGauge;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

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
    private static final Object actionsLock = new Object();

    /**
     * Lock protecting population of the store. Within a single "population" of the store we may
     * want to hold and release the actions lock, but we don't want to allow other "population"
     * operations to take place in the meantime.
     */
    private static final Object storePopulationLock = new Object();

    private final IActionFactory actionFactory;

    private final long topologyContextId;

    private final EntitySeverityCache severityCache;

    private final EntitiesCache entitySettingsCache;

    private final ActionHistoryDao actionHistoryDao;

    private static final String STORE_TYPE_NAME = "Live";

    private final ActionTargetSelector actionTargetSelector;

    private final ProbeCapabilityCache probeCapabilityCache;

    private final LiveActionsStatistician actionsStatistician;

    private final ActionTranslator actionTranslator;

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
        !ActionMode.DISABLED.equals(actionView.getMode());

    /**
     * Create a new {@link ActionStore} for storing live actions (actions generated by the realtime market).
     */
    public LiveActionStore(@Nonnull final IActionFactory actionFactory,
                           final long topologyContextId,
                           @Nonnull final ActionTargetSelector actionTargetSelector,
                           @Nonnull final ProbeCapabilityCache probeCapabilityCache,
                           @Nonnull final EntitiesCache entitySettingsCache,
                           @Nonnull final ActionHistoryDao actionHistoryDao,
                           @Nonnull final LiveActionsStatistician liveActionsStatistician,
                           @Nonnull final ActionTranslator actionTranslator) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.topologyContextId = topologyContextId;
        this.severityCache = new EntitySeverityCache(QueryFilter.VISIBILITY_FILTER);
        this.actionTargetSelector = actionTargetSelector;
        this.probeCapabilityCache = probeCapabilityCache;
        this.entitySettingsCache = entitySettingsCache;
        this.actionHistoryDao = Objects.requireNonNull(actionHistoryDao);
        this.actionsStatistician = Objects.requireNonNull(liveActionsStatistician);
        this.actionTranslator = Objects.requireNonNull(actionTranslator);
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

        synchronized (storePopulationLock) {
            if (actionPlan.getInfo().hasBuyRi()) {
                return populateBuyRIActions(actionPlan);
            }

            final TopologyInfo sourceTopologyInfo =
                actionPlan.getInfo().getMarket().getSourceTopologyInfo();

            // This call requires some computation and an RPC call, so do it outside of the
            // action lock.
            Collection<ActionDTO.Action> actionsWithSupportLevel = actionsWithSupportLevel(actionPlan);
            // RecommendationTracker to accelerate lookups of recommendations.
            RecommendationTracker recommendations = new RecommendationTracker();

            // Apply addition and removal to the internal store atomically.
            // It is generally not safe to call synchronized on a complex object not dedicated
            // to the purpose of locking because of the possibility of deadlock, but
            // SynchronizedCollections are an exception to this rule.
            final List<ActionView> completedSinceLastPopulate = new ArrayList<>();
            final List<Long> actionsToRemove = new ArrayList<>();
            synchronized (actionsLock) {

                // Only retain IN-PROGRESS, QUEUED and READY actions which are re-recommended.
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
            }

            // We released the actions lock, but we are still holding the population lock, so
            // the actions map shouldn't get modified in the meantime.
            //
            // Actions may change state due to user behaviour, but that's fine.
            //
            // We now build up the list of actions to add, and apply translations.

            final long planId = actionPlan.getId();
            final MutableInt newActionCounts = new MutableInt(0);

            final Stream<Action> translatedReadyActions = actionTranslator.translate(actionsWithSupportLevel.stream()
                .map(recommendedAction -> {
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
                        return action;
                    } else {
                        return null;
                    }
                })
                .filter(Objects::nonNull));

            final MutableInt removedCount = new MutableInt(0);
            final Set<ActionTypeCase> unsupportedActionTypes = new HashSet<>();
            final List<Action> translatedActionsToAdd = new ArrayList<>();
            final Set<Long> entitiesToRetrieve = new HashSet<>();

            // This actually drains the stream defined above, updating the recommendation, creating
            // new actions, and so on.
            translatedReadyActions.forEach(action -> {
                if (action.getTranslationStatus() == TranslationStatus.TRANSLATION_FAILED) {
                    removedCount.increment();
                    // Make sure to remove the actions with failed translations.
                    // We don't send NotRecommendedEvent-s because the action is still recommended
                    // but it's useless.
                    actionsToRemove.add(action.getId());
                    logger.trace("Removed action {} with failed translation. Full action: {}",
                            action.getId(), action);
                } else {
                    try {
                        entitiesToRetrieve.addAll(ActionDTOUtil.getInvolvedEntityIds(action.getRecommendation()));
                        translatedActionsToAdd.add(action);
                    } catch (UnsupportedActionException e) {
                        unsupportedActionTypes.add(e.getActionType());
                    }
                }
            });


            // We don't explicitly clear actions that were not successfully translated.
            if (removedCount.intValue() > 0) {
                logger.warn("Dropped {} actions due to failed translations.", removedCount);
            }

            if (!unsupportedActionTypes.isEmpty()) {
                logger.error("Action plan contained unsupported action types: {}", unsupportedActionTypes);
            }

            // Get a new snapshot for the entity settings cache. We do this outside of lock scope
            // so as to not block the readers on the RPC.
            final Snapshot snapshot = entitySettingsCache.newSnapshot(entitiesToRetrieve,
                sourceTopologyInfo.getTopologyContextId(), sourceTopologyInfo.getTopologyId());

            // Grab the lock again (this will wait for all readers to leave), and apply the changes.
            synchronized (actionsLock) {
                actions.keySet().removeAll(actionsToRemove);
                // Some of these may be noops - if we're re-adding an action that was already in
                // the map from a previous action plan.
                translatedActionsToAdd.forEach(action -> actions.put(action.getId(), action));

                // Update the entity settings cache.
                entitySettingsCache.update(snapshot);

                logger.info("Number of Re-Recommended actions={}, Newly created actions={}",
                    (actionPlan.getActionCount() - newActionCounts.intValue()),
                    newActionCounts);

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
                    ).forEach((actionType, count) -> Metrics.ACTION_COUNTS_SUMMARY
                    .labels(actionType.name())
                    .observe((double) count));

                // Record the action stats.
                // TODO (roman, Nov 15 2018): For actions completed since the last snapshot, it may make
                // sense to use the last snapshot's time instead of the current snapshot's time.
                // Not doing it for now because of the extra complexity - and it's not clear if anyone
                // cares if the counts are off by ~10 minutes.
                actionsStatistician.recordActionStats(sourceTopologyInfo,
                    // Only record user-visible actions.
                    Stream.concat(completedSinceLastPopulate.stream(), actions.values().stream())
                        .filter(VISIBILITY_PREDICATE));
            }
        }

        return true;
    }

    @Nonnull
    private Collection<ActionDTO.Action> actionsWithSupportLevel(@Nonnull final ActionPlan actionPlan) {
        try (DataMetricTimer timer = Metrics.SUPPORT_LEVEL_CALCULATION.startTimer()) {
            // Attempt to fully refresh the cache - this gets the most up-to-date target and
            // probe information from the topology processor.
            //
            // Note - we don't REALLY need to do this, because the cache tries to stay up to date by
            // listening for probe registrations and target additions/removals. But fully refreshing
            // it is cheap, so we do it to be safe.
            probeCapabilityCache.fullRefresh();

            final Map<Long, ActionTargetInfo> actionAndTargetInfo =
                actionTargetSelector.getTargetsForActions(actionPlan.getActionList().stream());

            // Increment the relevant counters.
            final Map<SupportLevel, Long> actionsBySupportLevel =
                actionAndTargetInfo.values().stream()
                    .collect(Collectors.groupingBy(ActionTargetInfo::supportingLevel, Collectors.counting()));
            logger.info("Action support counts: {}", actionsBySupportLevel);

            // First, zero out all the values.
            Metrics.SUPPORT_LEVELS.getLabeledMetrics().values()
                .forEach(gaugeData -> gaugeData.setData(0.0));
            actionsBySupportLevel.forEach((supportLevel, numActions) -> {
                Metrics.SUPPORT_LEVELS.labels(supportLevel.name()).setData((double)numActions);
            });

            return Collections2.transform(actionPlan.getActionList(), action -> {
                final SupportLevel supportLevel = Optional.ofNullable(
                    actionAndTargetInfo.get(action.getId()))
                    .map(ActionTargetInfo::supportingLevel)
                    .orElse(SupportLevel.UNSUPPORTED);
                if (action.getSupportingLevel() != supportLevel) {
                    return action.toBuilder()
                        .setSupportingLevel(supportLevel)
                        .build();
                } else {
                    return action;
                }
            });
        }
    }

    private boolean populateBuyRIActions(@Nonnull ActionPlan actionPlan) {

        synchronized (actionsLock) {
            final long planId = actionPlan.getId();
            riActions.clear();
            // All RI translations should be passthrough, but we do it here anyway for consistency
            // with the "normal" action case.
            actionTranslator.translate(actionPlan.getActionList().stream()
                    .map(recommendedAction -> actionFactory.newAction(recommendedAction, planId)))
                .forEach(action -> riActions.put(action.getId(), action));
            logger.info("Number of buy RI actions={}", actionPlan.getActionCount());
        }
        return true;
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
    public Map<ActionPlanType, Collection<Action>> getActionsByActionPlanType() {
        Map<ActionPlanType, Collection<Action>> results = Maps.newHashMap();
        if (!actions.isEmpty()) {
            results.put(ActionPlanType.MARKET, actions.values());
        }
        if (!riActions.isEmpty()) {
            results.put(ActionPlanType.BUY_RI, riActions.values());
        }
        return results;
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
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, ActionView> getActionViews() {
        return Collections.unmodifiableMap(getActions());
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Map<Long, ActionView> getActionViewsByDate(@Nonnull final LocalDateTime startDate,
                                                      @Nonnull final LocalDateTime endDate) {
        // TODO (roman, Feb 19 2019: OM-43247 - Refactor the historical action query interface.
        // Move the by-date functionality out of LiveActionStore.
        List<ActionView> succeededOrFailedActionList = actionHistoryDao.getActionHistoryByDate(startDate, endDate);
        List<ActionView> otherActionList;
        synchronized (actionsLock) {
            otherActionList = getActions().values().stream()
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
    public boolean overwriteActions(@Nonnull final Map<ActionPlanType, List<Action>> newActions) {
        synchronized (actionsLock) {
            actions.clear();
            newActions.values().forEach(actionsList ->
                    actionsList.forEach(action -> actions.put(action.getId(), action)));
        }

        logger.info("Successfully overwrote actions in the store with {} new actions.", actions.size());
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

    private static class Metrics {
        private static final DataMetricSummary ACTION_COUNTS_SUMMARY = DataMetricSummary.builder()
            .withName("ao_live_action_counts")
            .withHelp("Number of actions in the action orchestrator live store.")
            .withLabelNames("action_type")
            .build()
            .register();

        private static final DataMetricGauge SUPPORT_LEVELS = DataMetricGauge.builder()
            .withName("ao_live_action_support_level_gauge")
            .withHelp("Current number of actions of various support levels in the live action store.")
            .withLabelNames("support_level")
            .build();

        private static final DataMetricSummary SUPPORT_LEVEL_CALCULATION = DataMetricSummary.builder()
            .withName("ao_live_action_support_level_calculation_seconds")
            .withHelp("Time taken to calculate support levels in the live action store.")
            .build()
            .register();
    }
}
