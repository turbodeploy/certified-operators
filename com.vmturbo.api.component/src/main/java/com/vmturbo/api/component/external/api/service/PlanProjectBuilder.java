package com.vmturbo.api.component.external.api.service;

import static com.vmturbo.common.protobuf.utils.StringConstants.AUTOMATIC;
import static com.vmturbo.common.protobuf.utils.StringConstants.CLOUD_MIGRATION_PLAN;
import static com.vmturbo.common.protobuf.utils.StringConstants.DISABLED;
import static com.vmturbo.common.protobuf.utils.StringConstants.MIGRATE_CONTAINER_WORKLOADS_PLAN;
import static com.vmturbo.common.protobuf.utils.StringConstants.MIGRATION_PLAN__ALLOCATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.MIGRATION_PLAN__CONSUMPTION;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
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
import com.vmturbo.components.common.setting.ConfigurableActionSettings;

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
     * This two-level map holds predefined sets of setting overrides for use in the plan projects.
     * The first-level key is the plan scenario type (such as CLOUD_MIGRATION); the second-level
     * key is the plan mode/subtype, either ALLOCATION (lift & shift) or CONSUMPTION (optimized).
     */
    static final Map<String, Map<String, Set<ScenarioChange>>> settingOverridesByPlanType = ImmutableMap.of(
            CLOUD_MIGRATION_PLAN, ImmutableMap.of(
                    MIGRATION_PLAN__ALLOCATION,
                    Collections.singleton(settingOverride(ConfigurableActionSettings.Resize, DISABLED)),
                    MIGRATION_PLAN__CONSUMPTION,
                    Collections.singleton(settingOverride(ConfigurableActionSettings.Resize, AUTOMATIC))),
            MIGRATE_CONTAINER_WORKLOADS_PLAN, ImmutableMap.of(
                    MIGRATION_PLAN__ALLOCATION,
                    ImmutableSet.of(
                            settingOverride(ConfigurableActionSettings.Resize, DISABLED),
                            settingOverride(ConfigurableActionSettings.Suspend, DISABLED)),
                    MIGRATION_PLAN__CONSUMPTION,
                    ImmutableSet.of(
                            settingOverride(ConfigurableActionSettings.Resize, AUTOMATIC),
                            settingOverride(ConfigurableActionSettings.Suspend, AUTOMATIC))));

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
        return getPlanProjectType(scenario) != null;
    }

    @Nullable
    private PlanProjectType getPlanProjectType(@Nonnull final Scenario scenario) {
        final String scenarioType = scenario.getScenarioInfo().getType();
        if (MIGRATE_CONTAINER_WORKLOADS_PLAN.equals(scenarioType)) {
            // Special conversion for this plan.  We store the name in an action db table, which
            // has a limit of 20 characters for that field.  This plan type name is too long, so
            // we can't use the default Enum.valueOf() method as below to convert.
            return PlanProjectType.CONTAINER_MIGRATION;
        }
        try {
            // If the scenario type not one of the known project types (like CLOUD_MIGRATION),
            // then we don't try to create a plan project.
            return PlanProjectType.valueOf(scenarioType);
        } catch (NullPointerException | IllegalArgumentException e) {
            logger.trace("Scenario {} type {}is not a plan project: {}",
                    scenario.getScenarioInfo().getName(), scenarioType, e.getMessage());
        }
        return null;
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
        if (settingOverridesByPlanType.containsKey(scenarioType)) {
            try {
                return createMigrationPlanProject(scenario);
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
     * Construct a migration plan project. The project will contain two {@link PlanProjectScenario},
     * one for changes needed for Allocation (Lift & Shift) plan and the other for changes needed
     * for Consumption (Optimized) plan. Both the plans are created in the DB as well.
     *
     * @param scenario Scenario that was initially created as part of plan project.
     * @return a plan project.
     * @throws StatusRuntimeException Thrown on errors coming from rpc calls, DB update issues.
     */
    private PlanProject createMigrationPlanProject(@Nonnull final Scenario scenario)
            throws StatusRuntimeException {
        // Create a plan project and save to DB.
        final PlanProjectType projectType = getPlanProjectType(scenario);
        PlanProjectInfo planProjectInfo = PlanProjectInfo.newBuilder()
                .setName(scenario.getScenarioInfo().getName())
                .setType(projectType)
                .build();
        PlanProject planProject = planProjectRpcService.createPlanProject(planProjectInfo);

        // We use the planProject id from the just created project, and use that in the
        // involved plans being created now.
        long planProjectId = planProject.getPlanProjectId();

        // Set the consumption (i.e optimized) plan as the 'main' plan, out of the 2 plans
        // in the project. Allocation plan is the related plan. Create both plans first.
        Long allocationPlanId = createMigrationPlan(MIGRATION_PLAN__ALLOCATION, planProjectId, scenario);
        Long consumptionPlanId = createMigrationPlan(MIGRATION_PLAN__CONSUMPTION, planProjectId, scenario);

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
     * Construct a {@link ScenarioChange} with the {@link SettingOverride} given the setting name
     * and the setting value.
     *
     * @param settings the {@link ConfigurableActionSettings}, such as "Resize"
     * @param settingValue the value of the setting, such as "AUTOMATIC"
     * @return the constructed {@link ScenarioChange}
     */
    @Nonnull
    private static ScenarioChange settingOverride(
            @Nonnull final ConfigurableActionSettings settings,
            @Nonnull final String settingValue) {
        final EnumSettingValue enumSettingValue = EnumSettingValue.newBuilder()
                .setValue(Objects.requireNonNull(settingValue)).build();
        final Setting setting = Setting.newBuilder()
                .setSettingSpecName(Objects.requireNonNull(settings).getSettingName())
                .setEnumSettingValue(enumSettingValue).build();
        final SettingOverride settingOverride = SettingOverride.newBuilder()
                .setSetting(setting).build();
        return ScenarioChange.newBuilder().setSettingOverride(settingOverride).build();
    }

    /**
     * Creates and saves a migration plan to DB, belonging to the given plan project.
     *
     * @param planMode plan mode (Consumption or Allocation).
     * @param planProjectId Id of the plan project.
     * @param existingScenario Scenario that was originally created by user.
     * @return Id of the newly created plan.
     * @throws StatusRuntimeException Thrown on errors coming from rpc calls, DB update issues.
     */
    @Nonnull
    private Long createMigrationPlan(@Nonnull String planMode,
                                     @Nonnull final Long planProjectId,
                                     @Nonnull final Scenario existingScenario)
            throws StatusRuntimeException {
        // Make up a name like "Migrate to Public Cloud 1_MIGRATION_ALLOCATION"
        String scenarioName = existingScenario.getScenarioInfo().getName();
        final String planType = existingScenario.getScenarioInfo().getType();
        String updatedScenarioName = String.format("%s_%s", scenarioName, planType + planMode);

        // Add these additional resizes related changes to the scenario as well.
        final Set<ScenarioChange> settingOverrides = settingOverridesByPlanType.get(planType).get(planMode);
        final Set<ScenarioChange> existingNonOverrideChanges = existingScenario.getScenarioInfo()
                .getChangesList().stream()
                .filter(change -> !change.hasSettingOverride())
                .collect(Collectors.toSet());
        final ScenarioInfo planScenarioInfo = ScenarioInfo.newBuilder(
                existingScenario.getScenarioInfo())
                .clearChanges().addAllChanges(existingNonOverrideChanges)
                .addAllChanges(settingOverrides)
                .setName(updatedScenarioName)
                .build();
        final Scenario planScenario;
        final String planName;
        if (MIGRATION_PLAN__CONSUMPTION.equals(planMode)) {
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
        // Finally, use the scenario to create the plan.
        final PlanProjectType planProjectType = getPlanProjectType(existingScenario);
        final CreatePlanRequest planRequest = CreatePlanRequest.newBuilder()
                .setScenarioId(planScenario.getId())
                .setName(planName)
                .setProjectType(planProjectType)
                .setPlanProjectId(planProjectId)
                .build();
        PlanInstance planInstance = planRpcService.createPlan(planRequest);

        logger.info("Creating a new migration plan {} with scenario {} for project {}.",
                planInstance.getPlanId(), planScenario.getId(), planProjectId);
        return planInstance.getPlanId();
    }
}
