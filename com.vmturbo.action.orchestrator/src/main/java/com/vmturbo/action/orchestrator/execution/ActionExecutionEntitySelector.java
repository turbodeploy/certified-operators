package com.vmturbo.action.orchestrator.execution;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionEntity;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;

/**
 * Selects a service entity to execute an action against.
 */
public interface ActionExecutionEntitySelector {

    /**
     * Choose an entity whose target will be used to execute an action
     *
     * Note: In the future we anticipate having a list of these attempt to determine the entity in
     *   turns. In this scenario, non-default implementations can return Optional.empty() when they
     *   are unable to determine the target entity to select (i.e. none of the special cases that
     *   they describe apply).
     *
     * @param action the action to be executed
     * @return the entity whose target will be used to execute the action against
     */
    Optional<ActionEntity> getEntity(@Nonnull ActionDTO.Action action)
            throws UnsupportedActionException;
}
