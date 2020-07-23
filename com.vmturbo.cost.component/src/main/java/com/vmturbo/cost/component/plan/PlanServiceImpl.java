package com.vmturbo.cost.component.plan;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;

/**
 * A Spring Service that interacts with the plan service using gRPC calls.
 */
public class PlanServiceImpl implements PlanService {
    /**
     * The PlanServiceBlockingStub to use to make gRPC calls to the plan service.
     */
    private PlanServiceGrpc.PlanServiceBlockingStub planRpcService;

    /**
     * Creates a new PlanServiceImpl.
     *
     * @param planRpcService    The PlanServiceBlockingStub to use to make gRPC calls to the plan service
     */
    public PlanServiceImpl(PlanServiceGrpc.PlanServiceBlockingStub planRpcService) {
        this.planRpcService = planRpcService;
    }

    /**
     * Returns the plan instance for the specified plan ID.
     *
     * @param planId The ID of the plan for which to retrieve its PlanInstance
     * @return A PlanInstance
     */
    @Override
    public PlanDTO.PlanInstance getPlan(Long planId) {
        PlanDTO.OptionalPlanInstance optionalPlanInstance = planRpcService.getPlan(PlanDTO.PlanId.newBuilder()
                .setPlanId(planId)
                .build());
        return optionalPlanInstance.getPlanInstance();
    }
}
