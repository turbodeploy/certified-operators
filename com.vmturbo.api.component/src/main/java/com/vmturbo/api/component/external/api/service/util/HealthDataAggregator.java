package com.vmturbo.api.component.external.api.service.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.HealthDataMapper;
import com.vmturbo.api.component.external.api.service.TargetsService.CommunicationError;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.dto.admin.HealthCategoryReponseDTO;
import com.vmturbo.api.enums.healthCheck.HealthCheckCategory;
import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.common.protobuf.setting.SettingProto.GetGlobalSettingResponse;
import com.vmturbo.common.protobuf.setting.SettingProto.GetSingleGlobalSettingRequest;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetDetailsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetailLevel;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc.TargetsServiceBlockingStub;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;

/**
 * Aggregates health data for the consolidated health check endpoint.
 */
public class HealthDataAggregator {
    private final TargetsServiceBlockingStub targetsService;
    private final SettingServiceBlockingStub settingService;

    /**
     * Constructor.
     * @param targetsService to get targets health info.
     * @param settingService to get health info settings.
     */
    public HealthDataAggregator(@Nonnull final TargetsServiceBlockingStub targetsService,
        SettingServiceBlockingStub settingService) {
        this.targetsService = targetsService;
        this.settingService = settingService;
    }

    /**
     * The principal method that aggregates the health check data by category.
     * @param healthCheckCategory the desired health check category or null (for default: get data for all categories)
     * @return health check data grouped by health categories
     */
    public List<HealthCategoryReponseDTO> getAggregatedHealth(@Nullable HealthCheckCategory healthCheckCategory) {
        try {
            List<HealthCategoryReponseDTO> result = new ArrayList<>();

            //Only the targets health check for the moment.
            if (healthCheckCategory == null || HealthCheckCategory.TARGET.equals(healthCheckCategory)) {
                final Map<Long, TargetDetails> targetDetails = targetsService.getTargetDetails(
                    GetTargetDetailsRequest.newBuilder()
                        .setReturnAll(true)
                        .setDetailLevel(TargetDetailLevel.HEALTH_ONLY)
                        .build()).getTargetDetailsMap();
                final GetGlobalSettingResponse failedDiscoveryCountResponse = settingService.getGlobalSetting(
                    GetSingleGlobalSettingRequest.newBuilder()
                        .setSettingSpecName(GlobalSettingSpecs.FailedDiscoveryCountThreshold
                            .getSettingName())
                        .build());
                int failedDiscoveryCountThreshold = (int)failedDiscoveryCountResponse.getSetting().getNumericSettingValue().getValue();
                List<AggregatedHealthResponseDTO> responseItems = HealthDataMapper
                    .aggregateTargetHealthInfoToDTO(targetDetails, failedDiscoveryCountThreshold);
                HealthCategoryReponseDTO targetsHealth = new HealthCategoryReponseDTO();
                targetsHealth.setHealthCheckCategory(HealthCheckCategory.TARGET);
                targetsHealth.setCategoryDisplayName("Targets");
                targetsHealth.setCategoryHealthState(getWorstHealthState(responseItems.stream()
                    .map(AggregatedHealthResponseDTO::getHealthState).collect(Collectors.toList())));
                targetsHealth.addResponseItems(responseItems);

                result.add(targetsHealth);
            }

            return result;
        } catch (StatusRuntimeException e) {
            throw new CommunicationError(ExceptionMapper.translateStatusException(e));
        }
    }

    /**
     * Returns the worst health state out of the states passed.
     *
     * @param healthStates the health states.
     * @return the worst health state out of the states passed.
     */
    public static HealthState getWorstHealthState(List<HealthState> healthStates) {
        HealthState state = HealthState.NORMAL;
        for (HealthState healthState : healthStates) {
            if (HealthState.CRITICAL.equals(healthState)) {
                return HealthState.CRITICAL;
            }
            if (HealthState.MAJOR.equals(healthState)) {
                state = HealthState.MAJOR;
            } else if (HealthState.NORMAL.equals(state) && HealthState.MINOR.equals(healthState)) {
                state = HealthState.MINOR;
            }
        }
        return state;
    }
}
