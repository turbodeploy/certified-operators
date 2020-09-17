package com.vmturbo.plan.orchestrator.project.migration;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;
import com.vmturbo.plan.orchestrator.project.PlanProjectDao;
import com.vmturbo.plan.orchestrator.project.PlanProjectExecutor;
import com.vmturbo.plan.orchestrator.project.PlanProjectNotificationSender;
import com.vmturbo.plan.orchestrator.project.PlanProjectStatusTracker;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessor;
import com.vmturbo.plan.orchestrator.project.ProjectPlanPostProcessorRegistry;

/**
 * This class executes a plan project.
 */
public class CloudMigrationPlanProjectExecutor {
    private final Logger logger = LogManager.getLogger();

    private final PlanDao planDao;

    private final PlanProjectDao planProjectDao;

    private final PlanRpcService planService;

    private final ProjectPlanPostProcessorRegistry processorRegistry;

    private final PlanProjectNotificationSender notificationSender;

    /**
     * Constructor for {@link CloudMigrationPlanProjectExecutor}.
     *
     * @param planDao Plan DAO.
     * @param planProjectDao DAO for plan project status update.
     * @param planRpcService Plan RPC Service.
     * @param registry Registry for post processors of plans.
     * @param sender Project status notification sender.
     */
    public CloudMigrationPlanProjectExecutor(@Nonnull final PlanDao planDao,
                                             @Nonnull final PlanProjectDao planProjectDao,
                                             @Nonnull final PlanRpcService planRpcService,
                                             @Nonnull final ProjectPlanPostProcessorRegistry registry,
                                             @Nonnull final PlanProjectNotificationSender sender) {
        this.planService = Objects.requireNonNull(planRpcService);
        this.processorRegistry = Objects.requireNonNull(registry);
        this.planDao = Objects.requireNonNull(planDao);
        this.planProjectDao = Objects.requireNonNull(planProjectDao);
        this.notificationSender = Objects.requireNonNull(sender);
    }

    /**
     * Run cloud migration including the allocation plan and the on-demand plan.
     *
     * @param planProject the cloud migration plan project.
     */
    public void executePlanProject(PlanProject planProject) {
        long projectId = planProject.getPlanProjectId();
        try {
            PlanProjectStatusTracker projectTracker = new PlanProjectStatusTracker(
                    planProject.getPlanProjectId(), planProjectDao, notificationSender);

            PlanProjectInfo projectInfo = planProject.getPlanProjectInfo();
            List<Long> allPlanIds = new ArrayList<>(projectInfo.getRelatedPlanIdsList());
            allPlanIds.add(projectInfo.getMainPlanId());

            for (Long planId : allPlanIds) {
                Optional<PlanInstance> planOptional = planDao.getPlanInstance(planId);
                if (!planOptional.isPresent()) {
                    throw new IntegrityException("Could not look up plan with id: " + planId);
                }
                PlanInstance planInstance = planOptional.get();
                projectTracker.updateStatus(planInstance.getPlanId(), PlanStatus.READY);
                // Register post process handler to listen for plan instance status
                ProjectPlanPostProcessor projectPlanPostProcessor =
                        new CloudMigrationProjectPlanPostProcessor(planInstance.getPlanId(),
                                projectTracker);
                processorRegistry.registerPlanPostProcessor(projectPlanPostProcessor);
                PlanProjectExecutor.runPlanInstance(planService, planInstance, logger);
                projectTracker.updateStatus(planInstance.getPlanId(), PlanStatus.RUNNING_ANALYSIS);
            }
        } catch (Exception e) {
            logger.error("Could not trigger execution of migration plan project: {}",
                    projectId, e);
        }
    }
}
