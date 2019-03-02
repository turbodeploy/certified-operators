package com.vmturbo.action.orchestrator.store;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionEvent.FailureEvent;
import com.vmturbo.action.orchestrator.action.ActionEvent.NotRecommendedEvent;
import com.vmturbo.action.orchestrator.action.ActionModeCalculator;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor;
import com.vmturbo.action.orchestrator.execution.AutomatedActionExecutor.ActionExecutionTask;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionInfo.ActionTypeCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;

/**
 * Houses a collection of {@link ActionStore}s indexed by their context ID.
 * Each topology context is allowed one action store in the storehouse which
 * is independent of each other store/cache pair.
 */
@ThreadSafe
public class ActionStorehouse {

    private static final Logger logger = LogManager.getLogger();

    private final IActionStoreFactory actionStoreFactory;
    private final Map<Long, ActionStore> storehouse;
    private final AutomatedActionExecutor automatedExecutor;
    // Stores the task futures/promises of the actions which have been submitted for execution.
    private final List<ActionExecutionTask> actionExecutionFutures = new ArrayList<>();
    private final ActionModeCalculator actionModeCalculator;

    private static final DataMetricSummary STORE_POPULATION_SUMMARY = DataMetricSummary.builder()
        .withName("ao_populate_store_duration_seconds")
        .withHelp("The amount of time it takes to populate an action store with a new action plan.")
        .withLabelNames("store_type")
        .build()
        .register();

    private static final DataMetricSummary ACTION_PLAN_COUNTS_SUMMARY = DataMetricSummary.builder()
        .withName("ao_action_plan_action_counts")
        .withHelp("Number of actions in received action plan. May be either plan or live.")
        .withLabelNames("context_type", "action_type")
        .build()
        .register();

    /**
     * Create a new action storehouse.
     *
     * @param actionStoreFactory The factory to use when creating new store instances.
     * @param storeLoader The loader to use at startup when loading previously saved action stores.
     */
    public ActionStorehouse(@Nonnull final IActionStoreFactory actionStoreFactory,
                            @Nonnull final AutomatedActionExecutor automatedActionExecutor,
                            @Nonnull final IActionStoreLoader storeLoader,
                            @Nonnull final ActionModeCalculator actionModeCalculator) {
        this.actionStoreFactory = actionStoreFactory;
        this.storehouse = new HashMap<>();
        this.automatedExecutor = automatedActionExecutor;
        storeLoader.loadActionStores().forEach(store -> storehouse.put(store.getTopologyContextId(), store));
        this.actionModeCalculator = actionModeCalculator;

        logger.info("ActionStorehouse initialized with data for {} action stores", size());
    }

    /**
     * Store the actions in the actionPlan into a {@link ActionStore}.
     * If an existing {@link ActionStore} exists for the topology context,
     * that store will be reused. If no such store exists, a new one will be created.
     *
     * Also refresh the corresponding {@link EntitySeverityCache}'s knowledge of entity severities
     * to reflect the new actions in the store.
     *
     * @param actionPlan The plan whose actions should be stored in a Store in the StoreHouse.
     * @return The store used to store the actions.
     * @throws IllegalArgumentException If the input is invalid.
     */
    @Nonnull
    public synchronized ActionStore storeActions(@Nonnull final ActionPlan actionPlan) {
        if (!actionPlan.hasTopologyContextId()) {
            throw new IllegalArgumentException("Cannot store actions in action plan " + actionPlan.getId() +
                " because it has no context ID.");
        }

        measureActionPlan(actionPlan);
        ActionStore store = getStore(actionPlan.getTopologyContextId()).orElseGet(() -> {
            ActionStore newStore = actionStoreFactory.newStore(actionPlan.getTopologyContextId());

            storehouse.put(actionPlan.getTopologyContextId(), newStore);
            return newStore;
        });

        DataMetricTimer populationTimer = STORE_POPULATION_SUMMARY
            .labels(store.getStoreTypeName())
            .startTimer();
        store.populateRecommendedActions(actionPlan);
        populationTimer.observe();

        if (store.allowsExecution()) {
            try {
                actionExecutionFutures.removeIf(actionExecutionTask ->
                        actionExecutionTask.getFuture().isDone() ||
                            actionExecutionTask.getAction().getState() == ActionState.CLEARED ||
                            actionExecutionTask.getAction().getState() == ActionState.FAILED ||
                            actionExecutionTask.getAction().getState() == ActionState.SUCCEEDED);
                actionExecutionFutures.addAll(automatedExecutor.executeAutomatedFromStore(store));
            } catch (RuntimeException e) {
                logger.info("Unable to execute automated actions: ", e);
            }
        }
        // severity cache must be refreshed after actions change (see EntitySeverityCache javadoc)
        store.getEntitySeverityCache().refresh(store);

        return store;
    }

    /**
     * Replace the contents of the storehouse with those from another storehouse.
     *
     * @param storehouse The storehouse whose action stores should replace the contents of this one's
     */
    public synchronized void restoreStorehouse(@Nonnull final Map<Long, ActionStore> storehouse) {
        clearStore();
        this.storehouse.putAll(storehouse);
    }

    /**
     * Get the {@link ActionStore} for a topology context.
     *
     * @param topologyContextId The ID of the topology context whose store should be retrieved.
     * @return The store corresponding to the topology context.
     *         {@link Optional#empty()} if no store exists corresponding to the topology context.
     */
    public synchronized Optional<ActionStore> getStore(final long topologyContextId) {
        return Optional.ofNullable(storehouse.get(topologyContextId));
    }

    /**
     * Get the {@link EntitySeverityCache} for a topology context.
     *
     * @param topologyContextId The ID of the topology context whose store should be retrieved.
     * @return The severity cache corresponding to the topology context.
     *         {@link Optional#empty()} if no store exists corresponding to the topology context.
     */
    public synchronized Optional<EntitySeverityCache> getSeverityCache(final long topologyContextId) {
        final ActionStore store = storehouse.get(topologyContextId);
        return store == null ?
            Optional.empty() :
            Optional.of(store.getEntitySeverityCache());
    }

    /**
     * Remove the store and cache for the topology context. If no store exists for the
     * topology context, it will not be removed.
     *
     * @param topologyContextId The ID of the topology context whose store should be removed.
     * @return The store corresponding to the topology context that was removed.
     *         {@link Optional#empty()} if no store exists corresponding to the topology context.
     */
    public synchronized Optional<ActionStore> removeStore(final long topologyContextId) {
        return Optional.ofNullable(storehouse.remove(topologyContextId));
    }

    /**
     * Delete and remove the store from the storehouse. If no store exists for the
     * topology context, it will not be removed. If deleting the store fails or is not permitted,
     * an exception will be thrown.
     *
     * @param topologyContextId The ID of the topology context whose store should be removed.
     * @return The store corresponding to the topology context that was removed.
     *         {@link Optional#empty()} if no store exists corresponding to the topology context.
     * @throws StoreDeletionException if the operation fails.
     */
    public synchronized Optional<ActionStore> deleteStore(final long topologyContextId) throws StoreDeletionException {
        Optional<ActionStore> actionStore = getStore(topologyContextId);
        if (!actionStore.isPresent()) {
            return Optional.empty();
        }

        final Optional<StoreDeletionException> deletionException = getStore(topologyContextId)
            .map(ActionStore::clear)
            .flatMap(wasCleared -> wasCleared ?
                Optional.empty() :
                Optional.of(new StoreDeletionException("Failed to delete actions for store " + topologyContextId)));

        if (deletionException.isPresent()) {
            throw deletionException.get();
        } else {
            return removeStore(topologyContextId);
        }
    }

    /**
     * Cancel actions which are waiting in the queue to be executed.
     *
     * @return The number of actions which were cancelled and removed from the queue.
     */
    public synchronized int cancelQueuedActions() {
        // Don't cancel actions in progress. Cancel only those tasks which are yet to be executed.
        logger.info("Cancelling all pending automated actions which are waiting to be executed");
        int cancelledCount = actionExecutionFutures.stream()
                .filter(actionTask -> actionTask.getAction().getState() == ActionState.QUEUED)
                .map(actionTask-> {
                    Action action = actionTask.getAction();
                    actionTask.getFuture().cancel(false);
                    action.receive(new NotRecommendedEvent(action.getId()));
                    return 1;
                })
                .reduce(Integer::sum)
                .orElse(0);

        logger.info("Cancelled execution of {} queued automated actions. Total automated actions: {}",
                cancelledCount, actionExecutionFutures.size());
        actionExecutionFutures.clear();

        return cancelledCount;
    }

    /**
     * Remove all stores and caches in the storehouse.
     */
    public synchronized void clearStore() {
        storehouse.clear();
    }

    /**
     * Get a collection of all {@link ActionStore}s in the {@link ActionStorehouse}
     * @return The collection of all {@link ActionStore}s in the {@link ActionStorehouse}
     */
    public synchronized Map<Long, ActionStore> getAllStores() {
        return ImmutableMap.copyOf(storehouse);
    }

    /**
     * Get the number of stores in the storehouse.
     *
     * @return The number of stores in the storehouse.
     */
    public synchronized int size() {
        return storehouse.size();
    }

    /**
     * Get the action store factory used by the storehouse.
     *
     * @return The action store factory used by the storehouse.
     */
    public IActionStoreFactory getActionStoreFactory() {
        return actionStoreFactory;
    }

    /**
     * An exception thrown when deletion of an {@link ActionStore} fails.
     */
    public static class StoreDeletionException extends Exception {

        /**
         * Create a new {@link StoreDeletionException}.
         *
         * @param message The message describing the exception.
         */
        public StoreDeletionException(@Nonnull final String message) {
            super(message);
        }
    }

    /**
     * Add logging and metrics instrumentation to note characteristics of the action plan
     * we received.
     *
     * @param actionPlan An action plan to be measured.
     */
    private void measureActionPlan(@Nonnull final ActionPlan actionPlan) {
        final long contextId = actionPlan.getTopologyContextId();
        final Map<ActionTypeCase, Long> actionCounts = actionPlan.getActionList().stream()
            .collect(Collectors.groupingBy(a -> a.getInfo().getActionTypeCase(), Collectors.counting()));
        logger.info("Processing action plan for context {} with the following actions: {}",
            contextId, actionCounts);
        actionCounts.forEach((actionType, count) -> ACTION_PLAN_COUNTS_SUMMARY
            .labels(actionStoreFactory.getContextTypeName(contextId), actionType.name())
            .observe((double) count));
    }
}
