package com.vmturbo.action.orchestrator.execution;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow;

/**
 * Action specification with optional workflow.
 */
public class ActionWithWorkflow {
    private final ActionDTO.ActionSpec action;
    private final Optional<Workflow> workflow;

    /**
     * Create new {@code ActionWithWorkflow}.
     *
     * @param action Action specification.
     * @param workflow Workflow {@code Optional}.
     */
    public ActionWithWorkflow(
            @Nonnull final ActionSpec action,
            @Nonnull final Optional<Workflow> workflow) {
        this.action = action;
        this.workflow = workflow;
    }

    @Nonnull
    public ActionSpec getAction() {
        return action;
    }

    @Nonnull
    public Optional<Workflow> getWorkflow() {
        return workflow;
    }

    private long getWorkflowId() {
        return workflow.map(Workflow::getId).orElse(0L);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ActionWithWorkflow that = (ActionWithWorkflow)o;
        return Objects.equals(action.getRecommendationId(), that.action.getRecommendationId())
                && Objects.equals(getWorkflowId(), that.getWorkflowId());
    }

    @Override
    public int hashCode() {
        return Objects.hash(action.getRecommendationId(), getWorkflowId());
    }
}
