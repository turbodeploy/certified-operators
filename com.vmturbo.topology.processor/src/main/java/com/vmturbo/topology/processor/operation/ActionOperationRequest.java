package com.vmturbo.topology.processor.operation;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.platform.common.dto.ActionExecution.ActionExecutionDTO;

/**
 * Action operation request to be sent to {@link IOperationManager}.
 */
public class ActionOperationRequest {

    private final ActionExecutionDTO actionExecutionDTO;
    private final Set<Long> controlAffectedEntities;

    /**
     * Constructor.
     *
     * @param actionExecutionDTO DTO with action details to be sent to mediation probe for execution.
     * @param controlAffectedEntities Set of entities directly affected by this action.
     */
    public ActionOperationRequest(
            @Nonnull final ActionExecutionDTO actionExecutionDTO,
            @Nonnull final Set<Long> controlAffectedEntities) {
        this.actionExecutionDTO = actionExecutionDTO;
        this.controlAffectedEntities = Objects.requireNonNull(controlAffectedEntities);
    }

    /**
     * Get DTO with action details to be sent to mediation probe for execution.
     *
     * @return Action execution DTO.
     */
    @Nonnull
    public ActionExecutionDTO getActionExecutionDTO() {
        return actionExecutionDTO;
    }

    /**
     * Get set of entities directly affected by this action.
     *
     * @return Set of entities directly affected by this action.
     */
    @Nonnull
    public Set<Long> getControlAffectedEntities() {
        return controlAffectedEntities;
    }
}
