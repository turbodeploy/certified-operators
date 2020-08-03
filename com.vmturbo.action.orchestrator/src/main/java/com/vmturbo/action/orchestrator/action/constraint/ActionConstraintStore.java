package com.vmturbo.action.orchestrator.action.constraint;

import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;

/**
 * This interface defines an action constraint store for the sake of
 * updating all kinds of action constraint stores at the same time conveniently.
 */
public interface ActionConstraintStore {

    /**
     * Update the action constraint store with the given action constraint info.
     *
     * @param actionConstraintInfo contains the latest action constraint info
     */
    void updateActionConstraintInfo(ActionConstraintInfo actionConstraintInfo);
}
