package com.vmturbo.api.component.external.api.service.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Collections2;

import io.grpc.StatusRuntimeException;

import com.vmturbo.api.component.external.api.mapper.ExceptionMapper;
import com.vmturbo.api.component.external.api.mapper.HealthDataMapper;
import com.vmturbo.api.component.external.api.service.TargetsService.CommunicationError;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.dto.admin.HealthCategoryReponseDTO;
import com.vmturbo.api.enums.healthCheck.HealthCheckCategory;
import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetDetailsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetailLevel;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc.TargetsServiceBlockingStub;

/**
 * Aggregates health data for the consolidated health check endpoint.
 */
public class HealthDataAggregator {
    private final TargetsServiceBlockingStub targetsService;

    /**
     * Constructor.
     * @param targetsService to get targets health info.
     */
    public HealthDataAggregator(@Nonnull final TargetsServiceBlockingStub targetsService) {
        this.targetsService = targetsService;
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
                List<AggregatedHealthResponseDTO> responseItems = HealthDataMapper
                                .aggregateTargetHealthInfoToDTO(Collections2.transform(targetDetails.values(), TargetDetails::getHealthDetails));

                HealthCategoryReponseDTO targetsHealth = new HealthCategoryReponseDTO();
                targetsHealth.setHealthCheckCategory(HealthCheckCategory.TARGET);
                targetsHealth.setCategoryDisplayName("Targets");
                targetsHealth.setCategoryHealthState(getWorstHealthState(responseItems));
                targetsHealth.addResponseItems(responseItems);

                result.add(targetsHealth);
            }

            return result;
        } catch (StatusRuntimeException e) {
            throw new CommunicationError(ExceptionMapper.translateStatusException(e));
        }
    }

    private HealthState getWorstHealthState(List<AggregatedHealthResponseDTO> responseItems) {
        HealthState state = HealthState.NORMAL;
        for (AggregatedHealthResponseDTO item : responseItems) {
            if (HealthState.CRITICAL.equals(item.getHealthState())) {
                return HealthState.CRITICAL;
            }
            if (HealthState.MAJOR.equals(item.getHealthState())) {
                state = HealthState.MAJOR;
            } else if (HealthState.NORMAL.equals(state) && HealthState.MINOR.equals(item.getHealthState())) {
                state = HealthState.MINOR;
            }
        }
        return state;
    }
}
