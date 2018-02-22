package com.vmturbo.action.orchestrator.action;

import java.time.LocalDateTime;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.vmturbo.action.orchestrator.action.ActionTranslation.TranslationStatus;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionDecision;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionMode;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

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
    Optional<ExecutableStep> getExecutableStep();

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
     * Obtain a mapping from entity oid to entity type. Depends on the implementation, this
     * may return the entire map from the action store or just the types of the entities
     * involved in the action. The returned map might not contain entries for the involved
     * entities, and may even be empty (but not null).
     *
     * @return a map from entity oid to entity type
     */
    @Nonnull
    Map<Long, EntityType> getTypes();
}
