package com.vmturbo.plan.orchestrator.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;

/**
 * Listener for plan-related events.
 */
public interface PlanListener {
    void onPlanStatusChanged(@Nonnull PlanInstance planInstance);
}
