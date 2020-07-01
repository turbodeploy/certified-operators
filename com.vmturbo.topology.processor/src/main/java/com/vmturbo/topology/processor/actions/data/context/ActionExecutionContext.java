package com.vmturbo.topology.processor.actions.data.context;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;
import com.vmturbo.platform.common.dto.ActionExecution.ActionItemDTO;
import com.vmturbo.platform.common.dto.ActionExecution.Workflow;
import com.vmturbo.topology.processor.targets.TargetNotFoundException;

/**
 * An interface for collecting data needed for action execution
 *
 * The action execution context provides a single place to store all the data required for action
 * execution. The primary payload is the list of {@link ActionItemDTO}s, which is the SDK/probe
 * representation of an action.
 */
public interface ActionExecutionContext {

    /**
     * Get all of the action item DTOs associated with executing this action
     *
     * In constrast to {@link ActionDTO ActionDTOs}, which are used throughout XL to represent
     * actions, ActionItemDTOs are used to communicate actions to the probes.
     * This is the main carrier of data sent to the probes when executing an action.
     *
     * An action is represented as a list of action items because some actions, such as a move
     * together or a cross target move, will translate to multiple action items. For example, if a
     * VM is changing hosts and changing storage, it would have one action item for the hosts change
     * and another action item for the storage change. Any change of providers is represented as
     * a separate action item in the list. The entire list of action items executes atomically,
     * as a single logical action with multiple parts.
     *
     * By convention, the first ActionItem in the list will declare the overarching type of the
     *   action being executed, as well as include any additional ContextData needed to execute
     *   the action.
     *
     * @return a list of {@link ActionItemDTO} to send to the probe for action execution.
     * @throws ContextCreationException when the context cannot successfully create the action items
     */
    @Nonnull
    List<ActionItemDTO> getActionItems() throws ContextCreationException;

    /**
     * Get the SDK (probe-facing) type of the over-arching action being executed
     * {@link ActionItemDTO.ActionType} is what is used by the probes to identify the type of an
     * action.
     *
     * @return the SDK (probe-facing) type of the over-arching action being executed
     */
    @Nonnull
    ActionItemDTO.ActionType getSDKActionType();

    /**
     * The id of the overarching action. This is the ID that gets assigned by the Action Orchestrator.
     *
     * @return the id of the overarching action
     */
    long getActionId();

    /**
     * The id of the target which will be used to execute the action.
     *
     * @return the id of the target which will be used to execute the action.
     */
    long getTargetId();

    /**
     * Returns the workflow associated with this action.
     * Workflows allow actions to be executed through a third party action orchestrator.
     *
     * @return workflow or empty {@link Optional}.
     */
    @Nonnull
    Optional<Workflow> getWorkflow();

    /**
     * Return a Set of entities to that are directly involved in the action
     *
     * @return a Set of entities involved in the action
     */
    @Nonnull
    Set<Long> getControlAffectedEntities();

    /**
     * Get the ID of the secondary target involved in this action, or null if no secondary target is
     * involved
     *
     * @return the secondary target involved in this action, or null if no secondary target is
     *         involved
     * @throws ContextCreationException if error occurred while retrieving secondary target
     */
    @Nullable
    Long getSecondaryTargetId() throws ContextCreationException;

    /**
     * Creates {@link ActionExecutionDTO} for this action in a form suitable for SDK probes to
     * work with.
     *
     * @return action execution DTO
     * @throws ContextCreationException if failure occurred while constructing context
     */
    @Nonnull
    ActionExecutionDTO buildActionExecutionDto() throws ContextCreationException;
}
