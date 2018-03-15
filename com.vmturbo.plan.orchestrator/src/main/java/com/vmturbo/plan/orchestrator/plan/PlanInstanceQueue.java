package com.vmturbo.plan.orchestrator.plan;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;

public class PlanInstanceQueue {
    private final Logger logger = LogManager.getLogger();

    private final PlanDao planDao;
    private final PlanRpcService planService;

    /**
     * Constructor.
     *
     * @param planDao Plan DAO for interacting with the Plan database
     * @param planService Plan service
     */
    public PlanInstanceQueue(PlanDao planDao, PlanRpcService planService) {
        this.planDao = planDao;
        this.planService = planService;
    }

    /**
     * Run the next plan instance that is ready for execution.
     */
    public void runNextPlanInstance() {
        planDao.queueNextPlanInstance().ifPresent(planInstance -> {
            logger.info("Run plan instance {}. ", planInstance.getPlanId());

            // Run this plan
            planService.runQueuedPlan(planInstance,
                    new StreamObserver<PlanInstance>() {
                        @Override
                        public void onNext(PlanInstance value) {
                        }

                        @Override
                        public void onError(Throwable t) {
                            logger.error("Error occurred while executing plan instance {}.",
                                    planInstance.getPlanId());
                        }

                        @Override
                        public void onCompleted() {
                        }
                    });
        });
    }
}
