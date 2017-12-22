package com.vmturbo.plan.orchestrator.reservation;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Lists;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.InitialPlacementRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.InitialPlacementResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.Scenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.SettingOverride;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceImplBase;
import com.vmturbo.common.protobuf.setting.SettingProto.EnumSettingValue;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.group.api.SettingPolicySetting;
import com.vmturbo.plan.orchestrator.plan.IntegrityException;
import com.vmturbo.plan.orchestrator.plan.PlanDao;
import com.vmturbo.plan.orchestrator.plan.PlanRpcService;

/**
 * Implementation of gRpc service for Reservation.
 */
public class ReservationRpcService extends ReservationServiceImplBase {
    private final Logger logger = LogManager.getLogger();

    private final PlanDao planDao;

    private final PlanRpcService planService;

    private final String DISABLED = "DISABLED";

    public ReservationRpcService(@Nonnull final PlanDao planDao,
                                 @Nonnull final PlanRpcService planRpcService) {
        this.planDao = Objects.requireNonNull(planDao);
        this.planService = Objects.requireNonNull(planRpcService);
    }

    @Override
    public void initialPlacement(InitialPlacementRequest request,
                                 StreamObserver<InitialPlacementResponse> responseObserver) {
        if (!request.hasScenarioInfo()) {
            logger.error("Missing scenario info for initial placement.");
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Initial placement must have an scenario info").asException());
            return;
        }
        final List<ScenarioChange> scenarioChangeList = request.getScenarioInfo().getChangesList();
        final List<ScenarioChange> settingOverrides = createPlacementActionSettingOverride();
        PlanInstance planInstance = null;
        try {
            final Scenario scenario = Scenario.newBuilder()
                    .setScenarioInfo(ScenarioInfo.newBuilder(request.getScenarioInfo())
                            .clearChanges()
                            .addAllChanges(scenarioChangeList)
                            .addAllChanges(settingOverrides))
                    .build();
            planInstance = planDao.createPlanInstance(scenario, PlanProjectType.INITAL_PLACEMENT);
        } catch (IntegrityException e) {
            logger.error("Failed to create a plan instance for initial placement: ", e);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        }
        logger.info("Starting initial placement: {}", planInstance.getPlanId());
        runPlanInstanceForInitialPlacement(planInstance, responseObserver);
        responseObserver.onNext(InitialPlacementResponse.newBuilder()
                .setPlanId(planInstance.getPlanId())
                .build());
        responseObserver.onCompleted();
    }

    /**
     * Send request to run initial placement plan.
     *
     * @param planInstance {@link PlanInstance} represent initial placement plan.
     * @param responseObserver stream observer for initial placement.
     */
    private void runPlanInstanceForInitialPlacement(
            @Nonnull final PlanInstance planInstance,
            @Nonnull final StreamObserver<InitialPlacementResponse> responseObserver) {
        planService.runPlan(
                PlanId.newBuilder()
                        .setPlanId(planInstance.getPlanId())
                        .build(),
                new StreamObserver<PlanInstance>() {
                    @Override
                    public void onNext(PlanInstance value) {
                    }

                    @Override
                    public void onError(Throwable t) {
                        logger.error("Error occurred while executing plan {}.",
                                planInstance.getPlanId());
                        responseObserver.onError(
                                Status.INTERNAL.withDescription(t.getMessage()).asException());
                    }

                    @Override
                    public void onCompleted() {
                    }
                });
    }

    /**
     * Create settingOverride in order to disable move and provision for placed entity and only unplaced
     * entity could move.
     *
     * @return list of {@link ScenarioChange}.
     */
    private List<ScenarioChange> createPlacementActionSettingOverride() {
        ScenarioChange settingOverrideDisablePMMove = ScenarioChange.newBuilder()
                .setSettingOverride(SettingOverride.newBuilder()
                        .setSetting(Setting.newBuilder()
                                .setSettingSpecName(SettingPolicySetting.Move.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(DISABLED))))
                .build();
        ScenarioChange settingOverrideDisableClone = ScenarioChange.newBuilder()
                .setSettingOverride(SettingOverride.newBuilder()
                        .setSetting(Setting.newBuilder()
                                .setSettingSpecName(SettingPolicySetting.Provision.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(DISABLED))))
                .build();
        ScenarioChange settingOverrideDisableSTMove = ScenarioChange.newBuilder()
                .setSettingOverride(SettingOverride.newBuilder()
                        .setSetting(Setting.newBuilder()
                                .setSettingSpecName(SettingPolicySetting.StorageMove.getSettingName())
                                .setEnumSettingValue(EnumSettingValue.newBuilder().setValue(DISABLED))))
                .build();
        return Lists.newArrayList(settingOverrideDisablePMMove, settingOverrideDisableSTMove,
                settingOverrideDisableClone);
    }

}
