package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.action.QueryFilter;
import com.vmturbo.common.protobuf.ActionDTOUtil;
import com.vmturbo.common.protobuf.UnsupportedActionException;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.setting.SettingPolicyServiceGrpc.SettingPolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EntitySettingFilter;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetEntitySettingsResponse.SettingsForEntity;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingProto.TopologySelection;
import com.vmturbo.proactivesupport.DataMetricSummary;

/**
 * {@inheritDoc}
 *
 * Stores actions entirely in memory with no persistence.
 * For use with actions from the live market (sometimes also called "real time").
 */
@ThreadSafe
public class LiveActionStore implements ActionStore {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Wrap with {@link Collections#synchronizedMap} to ensure correct concurrent access.
     */
    private final Map<Long, Action> actions = Collections.synchronizedMap(new LinkedHashMap<>());

    private final IActionFactory actionFactory;

    private final long topologyContextId;

    private final SettingPolicyServiceBlockingStub settingPolicyServiceStub;

    private final EntitySeverityCache severityCache;

    private static final DataMetricSummary ACTION_COUNTS_SUMMARY = DataMetricSummary.builder()
        .withName("ao_live_action_counts")
        .withHelp("Number of actions in the action orchestrator live store.")
        .withLabelNames("action_type")
        .build()
        .register();

    private static final String STORE_TYPE_NAME = "Live";

    private final ActionSupportResolver actionSupportResolver;

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
     */
    public LiveActionStore(@Nonnull final IActionFactory actionFactory,
                           final long topologyContextId,
                           @Nonnull final SettingPolicyServiceBlockingStub settingServiceStub,
                           @Nonnull final ActionSupportResolver actionSupportResolver) {
        this.actionFactory = Objects.requireNonNull(actionFactory);
        this.topologyContextId = topologyContextId;
        this.severityCache = new EntitySeverityCache(QueryFilter.VISIBILITY_FILTER);
        this.settingPolicyServiceStub = Objects.requireNonNull(settingServiceStub);
        this.actionSupportResolver = actionSupportResolver;
    }

    /**
     * An acceleration structure used to permit ordered lookups of actions by their {@link ActionInfo}.
     * This assists in rapidly matching {@link ActionInfo}s in a new {@link ActionPlan} with their
     * corresponding {@link Action}s currently in the store.
     *
     * Internally keeps a map of queues where the key is the ActionInfo for a recommended Action by the
     * market and the values are an ordered queue of the corresponding domain model {@link Action}s.
     */
    private static class RecommendationTracker implements Iterable<Action> {
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

                queueIterator = mapIterator.next().iterator();
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
        RecommendationTracker recommendations = new RecommendationTracker();

        // Apply addition and removal to the internal store atomically.
        // It is generally not safe to call synchronized on a complex object not dedicated
        // to the purpose of locking because of the possibility of deadlock, but
        // SynchronizedCollections are an exception to this rule.
        synchronized (actions) {
            // Build a RecommendationTracker to accelerate lookups of recommendations.
            actions.values().forEach(recommendations::add);

            // re-add all QUEUED and IN_PROGRESS actions.
            List<Action> acceptedIncompleteActions = actions.values().stream()
                .filter(this::isAcceptedAndIncomplete)
                .collect(Collectors.toList());
            actions.clear();
            acceptedIncompleteActions.forEach(action -> actions.put(action.getId(), action));

            /**
             * For a list of rules, refer to {@link ActionStore#populateRecommendedActions(ActionPlan)}
             */
            final long planId = actionPlan.getId();

            final List<Action> existingActions = new ArrayList<>();
            final List<ActionDTO.Action> newActions = new ArrayList<>();

            actionPlan.getActionList().forEach(recommendedAction -> {
                Optional<Action> action = recommendations.take(recommendedAction.getInfo());
                if (action.isPresent()){
                    existingActions.add(action.get());
                } else {
                    newActions.add(recommendedAction);
                }
            });
            // TODO: (Michelle Neuburger 2017-10-26) retrieve settings for all actions' entities,
            // TODO: not just new ones, and refresh existing actions' entity setting map (OM-26182)
            final Set<Long> entitiesToRetrieve = newActions.stream().flatMap(newAction -> {
                try {
                    return ActionDTOUtil.getInvolvedEntities(newAction).stream();
                } catch (UnsupportedActionException e) {
                    logger.error("Recommendation contains unsupported action", e);
                    return Stream.empty();
                }
            }).collect(Collectors.toSet());

            final Map<Long, List<Setting>> entitySettingMap =
                    retrieveEntityToSettingListMap(entitiesToRetrieve,
                            actionPlan.getTopologyContextId(), actionPlan.getTopologyId());

            Stream.concat(existingActions.stream(),
                          newActions.stream().map(newAction ->
                            actionFactory.newAction(newAction, entitySettingMap, planId)))
                    .filter(action -> action.getState() == ActionState.READY)
                    .forEach(action -> actions.put(action.getId(), action));

            filterActionsByCapabilityForUiDisplaying();

            // Clear READY actions that were not re-recommended (if they were re-recommended they would
            // have been removed from the RecommendationTracker above).
            StreamSupport.stream(recommendations.spliterator(), false)
                .filter(Action::isReady)
                .forEach(action -> action.receive(new NotRecommendedEvent(planId)));

            actions.values().stream()
                .collect(Collectors.groupingBy(a ->
                    a.getRecommendation().getInfo().getActionTypeCase(), Collectors.counting())
                ).forEach((actionType, count) -> ACTION_COUNTS_SUMMARY
                    .labels(actionType.name())
                    .observe((double) count));
        }

        return true;
    }

    private void filterActionsByCapabilityForUiDisplaying() {
        final Collection<Action> filteredForUiActions = actionSupportResolver
                .resolveActionsSupporting(actions.values());
        filteredForUiActions.forEach(action -> actions.put(action.getId(), action));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return actions.size();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Optional<Action> getAction(long actionId) {
        return Optional.ofNullable(actions.get(actionId));
    }

    @Nonnull
    @Override
    public Map<Long, Action> getActions() {
        return Collections.unmodifiableMap(new HashMap<>(actions));
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
    public Map<Long, ActionView> getActionViews() {
        return Collections.unmodifiableMap(new HashMap<>(actions));
    }

    @Override
    public boolean overwriteActions(@Nonnull final List<Action> newActions) {
        synchronized (actions) {
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

    private boolean isAcceptedAndIncomplete(@Nonnull final Action action) {
        final ActionState state = action.getState();
        return (state == ActionState.QUEUED || state == ActionState.IN_PROGRESS);
    }

    private Map<Long, List<Setting>> retrieveEntityToSettingListMap(final Set<Long> entities,
                                                                    final long topologyContextId,
                                                                    final long topologyId) {
        try {
            GetEntitySettingsRequest request = GetEntitySettingsRequest.newBuilder()
                    .setTopologySelection(TopologySelection.newBuilder()
                            .setTopologyContextId(topologyContextId)
                            .setTopologyId(topologyId))
                    .setSettingFilter(EntitySettingFilter.newBuilder()
                            .addAllEntities(entities))
                    .build();
            GetEntitySettingsResponse response = settingPolicyServiceStub.getEntitySettings(request);
            return Collections.unmodifiableMap(
                    response.getSettingsList().stream()
                            .collect(Collectors.toMap(SettingsForEntity::getEntityId,
                                    SettingsForEntity::getSettingsList)));
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve entity settings due to error: " + e.getMessage());
            return Collections.emptyMap();
        }
    }
}
