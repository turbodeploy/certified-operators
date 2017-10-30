package com.vmturbo.action.orchestrator.execution;

import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.action.ActionDTO;

/**
 * Interface for resolving conflicts when entity of action is discovered by multiple targets
 * and we should choose which target should execute the action.
 */
public interface ActionTargetResolver {

    /**
     * Resolves which target from provided targets should actually execute the action.
     *
     * @param action action to be executed.
     * @param targets targets which discover some entity of action.
     * @return id of target which should execute the action.
     * @throws TargetResolutionException when action is null or provided targets set is null or
     * empty
     */
    long resolveExecutantTarget(@Nonnull ActionDTO.Action action, @Nonnull Set<Long> targets)
            throws TargetResolutionException;
}
