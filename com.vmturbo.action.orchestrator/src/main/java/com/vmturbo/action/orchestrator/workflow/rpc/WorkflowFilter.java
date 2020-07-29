package com.vmturbo.action.orchestrator.workflow.rpc;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import jdk.nashorn.internal.ir.annotations.Immutable;

/**
 * A filter to restrict the {@link com.vmturbo.common.protobuf.workflow.WorkflowDTO.Workflow}
 * objects to retrieve from the
 * {@link com.vmturbo.action.orchestrator.workflow.store.WorkflowStore}.
 * Filter properties are applied by AND-ing them together.
 */
@Immutable
public class WorkflowFilter {
    private final List<Long> desiredTargetIds;

    /**
     * Constructor of {@link WorkflowFilter}.
     *
     * @param targetIds desired targets
     */
    public WorkflowFilter(@Nonnull final List<Long> targetIds) {
        this.desiredTargetIds = Objects.requireNonNull(targetIds);
    }

    /**
     * Return desired targets.
     *
     * @return set of desired targets.
     */
    @Nonnull
    public List<Long> getDesiredTargetIds() {
        return desiredTargetIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WorkflowFilter)) {
            return false;
        }
        final WorkflowFilter that = (WorkflowFilter)o;
        return Objects.equals(desiredTargetIds, that.desiredTargetIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(desiredTargetIds);
    }
}
