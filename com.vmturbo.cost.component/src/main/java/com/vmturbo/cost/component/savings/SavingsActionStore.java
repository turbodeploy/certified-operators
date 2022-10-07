package com.vmturbo.cost.component.savings;

import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow;
import com.vmturbo.common.protobuf.action.ActionDTO.ExecutedActionsChangeWindow.LivenessState;

/**
 * Interface for store (cache) to the underlying action change DB table in AO.
 */
public interface SavingsActionStore {
    /**
     * Whether this cache has been initialized and ready to be used.
     * Set to true when first time the cache has been loaded successfully. Required so that clients
     * like TEM2 can know whether cache is ready to be used soon after startup, otherwise exception.
     *
     * @return True if cache can be used, otherwise don't use yet.
     */
    boolean isInitialized();

    /**
     * Gets all actions matching the input liveness state, from the store.
     *
     * @param state Liveness states to get actions for, typically LIVE.
     * @return Set of actions matching the input state.
     * @throws SavingsException Thrown if cache is not ready to be used yet.
     */
    @Nonnull
    Set<ExecutedActionsChangeWindow> getActions(@Nonnull LivenessState state) throws SavingsException;

    /**
     * Gets an action by the given action id.
     *
     * @param actionId Id of action.
     * @return Action live ness info, or null if not found.
     * @throws SavingsException Thrown if cache is not ready to be used yet.
     */
    @Nonnull
    Optional<ExecutedActionsChangeWindow> getAction(long actionId) throws SavingsException;

    /**
     * Makes an action as LIVE.
     *
     * @param actionId Action id.
     * @param startTime Start time to use for action.
     * @throws SavingsException Thrown if cache is not ready to be used yet.
     */
    void activateAction(long actionId, long startTime) throws SavingsException;

    /**
     * Makes a currently active action as inactive, with an end time and state.
     *
     * @param actionId Action id.
     * @param endTime End time to use for action.
     * @param state Action liveness state.
     * @throws SavingsException Thrown if cache is not ready to be used yet.
     */
    void deactivateAction(long actionId, long endTime, LivenessState state) throws SavingsException;

    /**
     * Saves any pending changes to the underlying store. Call once at the END of process().
     *
     * @throws SavingsException Thrown on save error.
     */
    void saveChanges() throws SavingsException;

    /**
     * Called by ExecutedActionsListener when it detects a new action has been successfully
     * executed, meaning that we have a NEW entry inserted in the change window table by AO, with
     * a start date and end date as null. So we use this call to update the internal cache.
     */
    void onNewAction(long actionId, long entityId);
}
