package com.vmturbo.cost.component.plan;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;

/**
 * Defines the business methods for interacting with the plan service.
 */
public interface PlanService {
    /**
     * Returns the plan instance for the specified plan ID.
     *
     * @param planId The ID of the plan for which to retrieve its PlanInstance
     * @return A PlanInstance
     */
    PlanInstance getPlan(Long planId);
}
