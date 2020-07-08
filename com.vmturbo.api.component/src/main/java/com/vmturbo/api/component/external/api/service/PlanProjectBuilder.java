package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.AUTOMATIC;
import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN;
import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN__ALLOCATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN__CONSUMPTION;
import static com.vmturbo.common.protobuf.utils.StringConstants.DISABLED;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.mapper.UuidMapper.ApiId;
import com.vmturbo.api.exceptions.OperationFailedException;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo.PlanProjectScenario;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.UpdatePlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectServiceGrpc.PlanProjectServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.UpdateScenarioRequest;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.components.common.setting.EntitySettingSpecs;

/**
 * Helper to create a plan project, called from MarketsService. Used to creation a migration
 * plan project, for example.
 */
class PlanProjectBuilder {
    private final Logger logger = LogManager.getLogger();

    private final PlanProjectServiceBlockingStub planProjectRpcService;
    private final ScenarioServiceBlockingStub scenarioServiceClient;
    private final PlanServiceBlockingStub planRpcService;

    /**
     * All resize settings.
     */
    private final Set<EntitySettingSpecs> resizeSettings = ImmutableSet.of(
            EntitySettingSpecs.Resize);

    PlanProjectBuilder(@Nonnull final PlanProjectServiceBlockingStub planProjectRpcService,
                      @Nonnull final ScenarioServiceBlockingStub scenariosService,
                      @Nonnull final PlanServiceBlockingStub planRpcService) {
        this.planProjectRpcService = planProjectRpcService;
        this.scenarioServiceClient = scenariosService;
        this.planRpcService = planRpcService;
    }

    /**
     * Checks if the request is for the creation of a plan project, in which case a new project
     * is created and later run. If the scenario type matches a known project type, then we create
     * a new project.
     *
     * @param sourceMarketId Market Id from UI - if real-time, then a plan project is created.
     *                       When the request is initially received from UI for a new project
     *                       creation, then this market id is the real-time market id.
     * @param scenario Plan scenario containing scenario type to check.
     * @return Whether a plan project (e.g for migration) needs to be created.
     */
    boolean isPlanProjectRequired(@Nonnull final ApiId sourceMarketId,
                                  @Nonnull final Scenario scenario) {
        if (!sourceMarketId.isRealtimeMarket()) {
            return false;
        }
        if (!scenario.hasScenarioInfo()) {
            return false;
        }
        final String scenarioType = scenario.getScenarioInfo().getType();
        PlanProjectType projectType = null;
        try {
            // If the scenario type not one of the known project types (like CLOUD_MIGRATION),
            // then we don't try to create a plan project.
            projectType = PlanProjectType.valueOf(scenarioType);
        } catch (NullPointerException | IllegalArgumentException e) {
            logger.trace("Scenario type {} for market {} is not a plan project: {}",
                    scenarioType, sourceMarketId.uuid(), e.getMessage());
        }
        return projectType != null;
    }

    /**
     * Entry point for creating a plan project from the given user scenario. Called from
     * MarketService when the scenario is being run by user.
     *
     * @param scenario Scenario that user had initially created.
     * @return Instance of newly created and saved project plan.
     * @throws OperationFailedException Thrown on plan project creation errors.
     */
    @Nonnull
    PlanProject createPlanProject(@Nonnull final Scenario scenario)
            throws OperationFailedException {
        final String scenarioType = scenario.getScenarioInfo().getType();
        if (PlanProjectType.CLOUD_MIGRATION.name().equals(scenarioType)) {
            try {
                return createCloudMigrationPlanProject(scenario);
            } catch (StatusRuntimeException sre) {
                final String message = String.format(
                        "Plan project creation failed using scenario type %s. %s",
                        scenarioType, sre.getMessage());
                throw new OperationFailedException(message);
            }
        }
        throw new OperationFailedException(String.format(
                "Unsupported scenario type %s. Cannot create plan project.", scenarioType));
    }

    /**
     * Construct a cloud migration plan project. The project will contain two
     * {@link PlanProjectScenario}, one for changes needed for Allocation (Lift & Shift) plan
     * and the other for changes needed for Consumption (Optimized) plan. Both the plans are
     * created in the DB as well.
     *
     * @param scenario Scenario that was initially created as part of plan project.
     * @return a plan project.
     * @throws StatusRuntimeException Thrown on errors coming from rpc calls, DB update issues.
     */
    private PlanProject createCloudMigrationPlanProject(@Nonnull final Scenario scenario)
            throws StatusRuntimeException {
        // Create a plan project and save to DB.
        PlanProjectInfo planProjectInfo = PlanProjectInfo.newBuilder()
                .setName(CLOUD_MIGRATION_PLAN)
                .setType(PlanProjectType.CLOUD_MIGRATION)
                .build();
        PlanProject planProject = planProjectRpcService.createPlanProject(planProjectInfo);

        // We use the planProject id from the just created project, and use that in the
        // involved plans being created now.
        long planProjectId = planProject.getPlanProjectId();

        // Set the consumption (i.e optimized) plan as the 'main' plan, out of the 2 plans
        // in the project. Allocation plan is the related plan. Create both plans first.
        Long consumptionPlanId = createCloudMigrationPlan(true, planProjectId, scenario);
        Long allocationPlanId = createCloudMigrationPlan(false, planProjectId, scenario);

        List<Long> relatedPlanIds = new ArrayList<>();
        relatedPlanIds.add(allocationPlanId);

        // Update the plan project and set the main plan id and related ids - makes it easier
        // to look up plan ids from project later.
        return planProjectRpcService.updatePlanProject(UpdatePlanProjectRequest.newBuilder()
                .setPlanProjectId(planProjectId)
                .setMainPlanId(consumptionPlanId)
                .addAllRelatedPlanIds(relatedPlanIds)
                .build());
    }

    /**
     * Get a set of ScenarioChange instances corresponding to all resize settings.
     * @param enableResize Whether to enable resize or not.
     * @return Set of ScenarioChange instances.
     */
    private Set<ScenarioChange> getResizeSettings(boolean enableResize) {
        Set<ScenarioChange> allSettings = new HashSet<>();
        resizeSettings.forEach(setting -> {
            allSettings.add(ScenarioChange.newBuilder()
                    .setSettingOverride(SettingOverride.newBuilder()
                            .setSetting(Setting.newBuilder()
                                    .setSettingSpecName(setting.getSettingName())
                                    .setEnumSettingValue(EnumSettingValue.newBuilder()
                                            .setValue(enableResize ? AUTOMATIC : DISABLED)
                                            .build())
                                    .build())
                            .build())
                    .build());
        });
        return allSettings;
    }

    /**
     * Creates and saves a migration plan to DB, belonging to the given plan project.
     *
     * @param enableResize Whether resize in plan is enabled (Consumption) or not (Allocation).
     * @param planProjectId Id of the plan project.
     * @param existingScenario Scenario that was originally created by user.
     * @return Id of the newly created plan.
     * @throws StatusRuntimeException Thrown on errors coming from rpc calls, DB update issues.
     */
    @Nonnull
    private Long createCloudMigrationPlan(boolean enableResize,
                                          @Nonnull final Long planProjectId,
                                          @Nonnull final Scenario existingScenario)
            throws StatusRuntimeException {
        // Make up a name like "Migrate to Public Cloud 1_MIGRATION_ALLOCATION"
        String scenarioName = existingScenario.getScenarioInfo().getName();
        String updatedScenarioName = String.format("%s_%s", scenarioName,
                enableResize ? CLOUD_MIGRATION_PLAN__CONSUMPTION : CLOUD_MIGRATION_PLAN__ALLOCATION);

        // Add these additional resizes related changes to the scenario as well.
        final ScenarioInfo planScenarioInfo = ScenarioInfo.newBuilder(
                existingScenario.getScenarioInfo())
                .addAllChanges(getResizeSettings(enableResize))
                .setName(updatedScenarioName)
                .build();
        Scenario planScenario;
        String planName;
        if (enableResize) {
            // For the main (consumption) plan, we use the original name specified by user as-is.
            planName = scenarioName;
            // Update the existing scenario with the additional change about resize.
            planScenario = scenarioServiceClient.updateScenario(UpdateScenarioRequest.newBuilder()
                    .setScenarioId(existingScenario.getId())
                    .setNewInfo(planScenarioInfo)
                    .build()).getScenario();
        } else {
            planName = updatedScenarioName;
            // Create a new scenario for related (allocation) plan.
            planScenario = scenarioServiceClient.createScenario(planScenarioInfo);
        }
        // Finally use the scenario to create the plan.
        CreatePlanRequest planRequest = CreatePlanRequest.newBuilder()
                .setScenarioId(planScenario.getId())
                .setName(planName)
                .setProjectType(PlanProjectType.CLOUD_MIGRATION)
                .setPlanProjectId(planProjectId)
                .build();
        PlanInstance planInstance = planRpcService.createPlan(planRequest);

        logger.info("Creating a new migration plan {} with scenario {} for project {}.",
                planInstance.getPlanId(), planScenario.getId(), planProjectId);
        return planInstance.getPlanId();
    }
}
