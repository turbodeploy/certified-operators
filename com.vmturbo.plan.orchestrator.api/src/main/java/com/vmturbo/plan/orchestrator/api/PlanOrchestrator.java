package com.vmturbo.plan.orchestrator.api;

import javax.annotation.Nonnull;

/**
 * Plan orchestrator remote client.
 */
public interface PlanOrchestrator extends AutoCloseable {

    void addPlanListener(@Nonnull PlanListener planListener);
}
