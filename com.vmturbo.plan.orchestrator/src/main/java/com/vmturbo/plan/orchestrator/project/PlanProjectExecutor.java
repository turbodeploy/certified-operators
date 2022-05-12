package com.vmturbo.plan.orchestrator.project;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.turbonomic.cpucapacity.CPUCapacityEstimator;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.project.headroom.ClusterHeadroomPlanProjectExecutor;
import com.vmturbo.plan.orchestrator.project.migration.MigrationPlanProjectExecutor;
import com.vmturbo.plan.orchestrator.reservation.ReservationManager;
import com.vmturbo.plan.orchestrator.templates.TemplatesDao;
import com.vmturbo.topology.processor.api.TopologyProcessor;

/**
 * This class executes a plan project.
 */
public class PlanProjectExecutor {
    private final Logger logger = LogManager.getLogger();

    private final ClusterHeadroomPlanProjectExecutor headroomExecutor;

    private final MigrationPlanProjectExecutor migrationExecutor;

    /**
     * Constructor for {@link PlanProjectExecutor}.
     *
     * @param planDao Plan DAO
     * @param planProjectDao DAO for plan project status update.
     * @param groupChannel  Group service channel
     * @param planRpcService Plan RPC Service
     * @param projectPlanPostProcessorRegistry Registry for post processors of plans
     * @param repositoryChannel Repository channel
     * @param templatesDao templates DAO
     * @param historyChannel history channel
     * @param projectNotifier Used to send plan project related notification updates.
     * @param headroomCalculationForAllClusters specifies how to run cluster headroom plan
     * @param headroomPlanRerunDelayInSecond specifies when to rerun headroom plan
     * @param topologyProcessor a REST call to get target info
     * @param cpuCapacityEstimator estimates the scaling factor of a cpu model.
     * @param taskScheduler a taskScheduler used for scheduled plan executions
     * @param reservationManager the reservation manager.
     * @param considerReservedVMsInClusterHeadroomPlan consider reserved VMs in cluster headroom plan or not
     */
    PlanProjectExecutor(@Nonnull final PlanDao planDao,
                        @Nonnull final PlanProjectDao planProjectDao,
                        @Nonnull final Channel groupChannel,
                        @Nonnull final PlanRpcService planRpcService,
                        @Nonnull final ProjectPlanPostProcessorRegistry projectPlanPostProcessorRegistry,
                        @Nonnull final Channel repositoryChannel,
                        @Nonnull final TemplatesDao templatesDao,
                        @Nonnull final Channel historyChannel,
                        @Nonnull final PlanProjectNotificationSender projectNotifier,
                        final boolean headroomCalculationForAllClusters,
                        final long headroomPlanRerunDelayInSecond,
                        @Nonnull final TopologyProcessor topologyProcessor,
                        @Nonnull final CPUCapacityEstimator cpuCapacityEstimator,
                        @Nonnull final ThreadPoolTaskScheduler taskScheduler,
                        @Nonnull final ReservationManager reservationManager,
                        final boolean considerReservedVMsInClusterHeadroomPlan) {

        headroomExecutor = new ClusterHeadroomPlanProjectExecutor(planDao, groupChannel,
                planRpcService, projectPlanPostProcessorRegistry, repositoryChannel, templatesDao, historyChannel,
                headroomCalculationForAllClusters, headroomPlanRerunDelayInSecond,
                topologyProcessor, cpuCapacityEstimator, taskScheduler, reservationManager,
                considerReservedVMsInClusterHeadroomPlan);

        migrationExecutor = new MigrationPlanProjectExecutor(planDao, planProjectDao,
                planRpcService, projectPlanPostProcessorRegistry, projectNotifier);
    }

    /**
     * Returns instance of internal headroom executor, only for existing tests to work,
     * which are calling a bunch of internal methods.
     *
     * @return Instance of ClusterHeadroomPlanProjectExecutor.
     */
    @VisibleForTesting
    ClusterHeadroomPlanProjectExecutor getHeadroomExecutor() {
        return headroomExecutor;
    }

    /**
     * Executes a plan project according to the instructions provided in the plan project.
     * See PlanDTO.proto for detailed documentation of fields in the plan project object and how
     * they will be processed during plan execution.
     *
     * @param planProject a plan project
     * @param handleFailure handle failure or not
     */
    public void executePlan(@Nonnull final PlanProject planProject,
                            final boolean handleFailure) {
        logger.info("Executing plan project: {} (name: {})",
                planProject.getPlanProjectId(), planProject.getPlanProjectInfo().getName());

        PlanProjectType type = planProject.getPlanProjectInfo().getType();
        switch (type) {
            case CLUSTER_HEADROOM:
                headroomExecutor.executePlanProject(planProject, handleFailure);
                break;
            case CLOUD_MIGRATION:
            case CONTAINER_MIGRATION:
                migrationExecutor.executePlanProject(planProject);
                break;
            default:
                logger.error("Unsupported project {} type {}. Cannot execute plan.",
                        planProject.getPlanProjectId(), type);
        }
    }

    /**
     * Calls the plan service to run the plan instance.
     *
     * @param planService Plan service to call.
     * @param planInstance Instance of plan to start running.
     * @param logger Used for logging errors.
     */
    public static void runPlanInstance(@Nonnull final PlanRpcService planService,
                                       @Nonnull final PlanInstance planInstance,
                                       @Nonnull final Logger logger) {
        planService.runPlan(PlanId.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .build(), new StreamObserver<PlanInstance>() {
            @Override
            public void onNext(PlanInstance value) {
            }

            @Override
            public void onError(Throwable t) {
                logger.error("Error executing plan {} of project type {}.",
                        planInstance.getPlanId(), planInstance.getProjectType(), t);
            }

            @Override
            public void onCompleted() {
            }
        });
    }
}
