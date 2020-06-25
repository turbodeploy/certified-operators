package com.vmturbo.action.orchestrator.store;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.action.Action;
import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.store.query.QueryableActionViews;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan.ActionPlanType;

/**
 * A store for actions that are active in the system.
 * Implementations may choose whether to provide persistence.
 *
 * Contains both recommended actions and actions that are in progress.
 * When actions are executed, they are moved from recommended to in progress.
 *
 * No interface is provided to directly remove actions from the store.
 * Actions that are no longer active (CLEARED, SUCCEEDED, or FAILED) are passively
 * removed from the store as a side effect of {@link #populateRecommendedActions(ActionPlan)}
 *
 * The {@link ActionStore} makes no guarantees about the order in which actions are returned.
 *
 * Actions in a store should be associated with a particular topology context.
 */
public interface ActionStore {
    /**
     * Updates the {@link ActionStore} with a new set of recommendations.
     * The particulars of the behavior of the store depend on the type of store
     * being populated.
     *
     * @param actionPlan The list of actions with which to populate the store.
     * @return If the store was successfully populated with actions from the plan.
     * @throws InterruptedException if current thread interrupted
     */
    boolean populateRecommendedActions(@Nonnull ActionPlan actionPlan) throws InterruptedException;

    /**
     * Return the number of elements in the store.
     *
     * @return the number of elements in the store.
     */
    int size();

    /**
     * Return whether the store contains actions that can be executed in the current topology.
     */
    boolean allowsExecution();

    /**
     * Get a view of an action that provides accessors to the property of that action.
     * Clients who do not need to mutate the state of an action should prefer
     * to access the actions via an {@link ActionView}.
     *
     * To get a mutable reference to an individual action, use {@link #getAction(long)}.
     *
     * @param actionId The ID of the action to retrieve.
     * @return A spec describing the Action with the corresponding ID, or Optional.empty
     *         if no action with the given ID can be found.
     */
    @Nonnull
    Optional<ActionView> getActionView(long actionId);

    /**
     * Get an action by its ID.
     * Unless it is necessary to mutate the action, prefer the use of {@link #getActionView(long}
     *
     * @param actionId The ID of the action to retrieve.
     * @return The Action with the corresponding ID, or Optional.empty if no action
     *         with the given ID can be found.
     */
    @Nonnull
    Optional<Action> getAction(long actionId);

    /**
     * Get views of the actions in the store that provide accessors to the properties of the actions.
     * Clients who do not need to mutate the state of actions should prefer to access actions
     * via their {@link ActionView}s.
     *
     * To get mutable access to actions in the store, use {@link #getActions()}.
     *
     * No guarantee is made to the order of the {@link ActionView}s returned.
     *
     * @return A map descriptions for all actions in the store where the key is the actionId and
     *         the value is the associated {@link ActionView}.
     */
    @Nonnull
    QueryableActionViews getActionViews();

    /**
     * Get the actions in the store.
     * Unless it is necessary to mutate the actions, prefer the use of {@link #getActionViews(}
     *
     * @return An unmodifiable map of actions, with the action ID as the key.
     */
    @Nonnull
    Map<Long, Action> getActions();

    /**
     * Get the actions in the store grouped by the action plan type.
     *
     * @return An unmodifiable map of action plan type to list of actions for that action plan type.
     */
    @Nonnull
    Map<ActionPlanType, Collection<Action>> getActionsByActionPlanType();

    /**
     * Overwrite the actions in the {@link ActionStore} with a supplied list
     * of actions. Clears the {@link ActionStore} of all existing actions.
     *
     * @param actions The actions to put into the store.
     * @return If the store successfully overwrote its actions with those in the list.
     */
    boolean overwriteActions(@Nonnull final Map<ActionPlanType, List<Action>> actions);

    /**
     * Clears the {@link ActionStore} of all existing actions.
     * Certain {@link ActionStore}s may not permit this operation.
     * In such a case, they will throw an {@link IllegalStateException} if called.
     *
     * @return If the store was able to successfully forceClear all its actions.
     *         May fail, for example, if attempts to clear records from the database fail.
     * @throws IllegalStateException if the store does not permit this operation.
     */
    boolean clear() throws IllegalStateException;

    /**
     * Get the topology context ID for the store.
     *
     * @return The topology context ID associated with this action store.
     */
    long getTopologyContextId();

    /**
     * Retrieve the cache that maintains entity severities for entities associated with actions
     * in this {@link ActionStore}. There is one severity cache per-store.
     *
     * Note that the store does not manage refreshing and updating the severity cache. It is up to clients
     * to do so because State changes to actions that happen external to updates to the {@link ActionStore}
     * can cause updates to an entity's severity.
     *
     * @return The {@link EntitySeverityCache} associated with this {@link ActionStore}.
     */
    @Nonnull
    EntitySeverityCache getEntitySeverityCache();

    /**
     * Get the visibility predicate for use in testing the visibility of actions in this store.
     * The visibility predicate is usd to test whether a particular action is visible.
     *
     * <p>Non-visible actions can still be valuable for debugging
     * purposes, but they shouldn't be exposed externally.
     *
     * @return The visibility predicate for use in testing the visibility of actions in this store.
     */
    @Nonnull
    Predicate<ActionView> getVisibilityPredicate();

    /**
     * Get the name of the type of the store (ie "Live", "Plan", etc.).
     *
     * @return the store type name, e.g. "Live" or "Plan"
     */
    @Nonnull
    String getStoreTypeName();
}
