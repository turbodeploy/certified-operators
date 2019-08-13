package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.action.orchestrator.store.EntitiesAndSettingsSnapshotFactory.EntitiesAndSettingsSnapshot;
import com.vmturbo.action.orchestrator.workflow.store.WorkflowStore;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionCategory;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO;

/**
 * Provides a limited-access view of an action that permits accessors for retrieving properties
 * of that action, but provides minimal capability to clients for changing the properties of the action.
 *
 * Objects accessed through an {@link ActionView} may be mutable (for example, the {@link ActionTranslation})
 * but it must not be possible to use an {@link ActionView} to mutate properties of its associated action
 * that important enough that they may be audited (ie {@link ActionState}, or {@link ActionDecision}).
 *
 * When it is not necessary to mutate the state of an action, clients should prefer accessing the action
 * through its {@link ActionView} interface so as to minimize the chance of accidentally mutating
 * an {@link Action} inappropriately.
 *
 * Implementations of an ActionView must be thread-safe.
 */
@ThreadSafe
public interface ActionView {
    /**
     * Get the recommendation associated with this action.
     *
     * @return The recommendation associated with this action.
     */
    ActionDTO.Action getRecommendation();

    /**
     * Get the time at which the action was originally recommended.
     *
     * @return Get the time at which the action was originally recommended.
     */
    LocalDateTime getRecommendationTime();

    /**
     * Get the ID of the action plan that originally recommended this action.
     *
     * @return the ID of the action plan that originally recommended this action.
     */
    long getActionPlanId();

    /**
     * Get the action's current state as governed by its state machine.
     *
     * @return The action's current state.
     */
    ActionState getState();

    /**
     * Check if the action is in the READY state.
     *
     * @return true if in the READY state, false otherwise.
     */
    boolean isReady();

    /**
     * Get The mode of the action. The action mode is established by checking the policy for the action
     * when it is received by the action orchestrator.
     *
     * @return The {@link ActionMode} that currently applies to the action.
     */
    ActionMode getMode();

    /**
     * Get the ID of the action. This ID is the same as the one provided by the market in its
     * recommendation.
     *
     * @return The ID of the action.
     */
    long getId();

    /**
     * Get the decision associated with the action. Actions in the READY state do not have a decision.
     *
     * @return The decision associated with the action.
     */
    Optional<ActionDecision> getDecision();

    /**
     * Get the executable step associated with the action. An action must be accepted before it will
     * have an executable step.
     *
     * @return An optional of the executable step. Empty if the action has not been accepted.
     */
    Optional<ExecutableStep> getCurrentExecutableStep();

    /**
     * Get the translation of the action from the market's domain-agnostic representation into
     * the domain-specific real-world representation. See {@link ActionTranslation} for more details.
     *
     * @return The {@link ActionTranslation} associated with this action.
     */
    @Nonnull
    ActionTranslation getActionTranslation();

    /**
     * Get the status of the translation associated with this action. See
     * {@link ActionTranslation} for more details.
     *
     * @return the status of the translation associated with this action.
     */
    TranslationStatus getTranslationStatus();

    /**
     * Get the action category associated with this action.
     * This should return the same result as what's extracted from the recommendation's explanation,
     * but all users of the action should use this call.
     *
     * @return the category with the action.
     */
    @Nonnull
    ActionCategory getActionCategory();

    /**
     * Determine whether the action is executable.
     * An action is generally executable when its recommendation is marked as executable
     * by the market and its state is ready.
     *
     * @return True if the action is executable and ready, false otherwise.
     */
    default boolean determineExecutability() {
        // An action is considered "Executable" if the initial recommendation is marked as
        // executable by the market, and it has not been accepted/cleared.
        return getRecommendation().getExecutable() &&
            getState().equals(ActionState.READY);
    }

    /**
     * Determine whether the action has at least one remaining step to execute
     * This could include PRE and POST workflow executions, in addition to the main execution
     * of the action. Steps that are currently in-progress count as a pending execution.
     *
     * @return  true, if the action has at least one remaining step to execute
     */
    boolean hasPendingExecution();

    /**
     * Whether this action has at least one execution step in a failed state
     *
     * @return true, if this action has at least one execution step in a failed state
     */
    boolean hasFailures();

    /**
     * Fetch an Optional of the {@link WorkflowDTO.Workflow} corresponding to this Action, if any.
     * The Workflow is controlled by an Orchestration Setting whose name is based on the name of
     * this action. For example, a "Provision" action workflow is configured by creating a
     * "ProvisionActionWorkflow" setting. The ID of the workflow is given by the "value" of the
     * setting, if one is found, and that ID is used to look up the Workflow object in the
     * given 'workflowStore'.
     * If there is no such setting, or no Workflow with that ID is found in the store,
     * then return Optional.empty().
     *
     * @param workflowStore the store of all known Workflow objects
     * @return an Optional of the corresponding Workflow based on an Orchestration Setting for the
     * corresponding Action type.
     */
    Optional<WorkflowDTO.Workflow> getWorkflow(WorkflowStore workflowStore);

    /**
     * Gets the action description.
     *
     * @return The action description string.
     */
    @Nonnull
    String getDescription();
}
