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
import com.vmturbo.api.dto.admin.HealthCategoryResponseDTO;
import com.vmturbo.api.enums.health.ActionStatusSubcategory;
import com.vmturbo.api.enums.health.HealthCategory;
import com.vmturbo.api.enums.health.HealthState;
import com.vmturbo.common.protobuf.market.AnalysisStateServiceGrpc.AnalysisStateServiceBlockingStub;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.market.MarketNotification.GetAnalysisStateRequest;
import com.vmturbo.common.protobuf.market.MarketNotification.GetAnalysisStateResponse;
import com.vmturbo.common.protobuf.target.TargetDTO.GetTargetDetailsRequest;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetailLevel;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetsServiceGrpc.TargetsServiceBlockingStub;

/**
 * Aggregates health data for the consolidated health check endpoint.
 */
public class HealthDataAggregator {
    private final TargetsServiceBlockingStub targetsService;
    private final AnalysisStateServiceBlockingStub analysisStub;
    private final long realtimeTopologyContextId;
    private static final String ACTIONS = "Actions";
    private static final String TARGETS = "Targets";

    /**
     * Constructor.
     * @param targetsService to get targets health info.
     * @param analysisStub the stub to query analysis status.
     * @param realtimeTopologyContextId  real time topology context id.
     */
    public HealthDataAggregator(@Nonnull final TargetsServiceBlockingStub targetsService,
            @Nonnull final AnalysisStateServiceBlockingStub analysisStub,
            final long realtimeTopologyContextId) {
        this.targetsService = targetsService;
        this.analysisStub = analysisStub;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * The principal method that aggregates the health check data by category.
     * @param healthCategory the desired health category or null (for default: get data for all categories)
     * @return health check data grouped by health categories
     */
    public List<HealthCategoryResponseDTO> getAggregatedHealth(@Nullable HealthCategory healthCategory) {
        try {
            List<HealthCategoryResponseDTO> result = new ArrayList<>();

            //Only the targets health check for the moment.
            if (healthCategory == null || HealthCategory.TARGET.equals(healthCategory)) {
                final Map<Long, TargetDetails> targetsDetails = targetsService.getTargetDetails(
                                GetTargetDetailsRequest.newBuilder()
                                    .setReturnAll(true)
                                    .setDetailLevel(TargetDetailLevel.HEALTH_ONLY)
                                    .build())
                        .getTargetDetailsMap();
                List<AggregatedHealthResponseDTO> responseItems = HealthDataMapper
                                .aggregateTargetHealthInfoToDTO(targetsDetails);
                HealthCategoryResponseDTO targetsHealth = new HealthCategoryResponseDTO();
                targetsHealth.setHealthCategory(HealthCategory.TARGET);
                targetsHealth.setCategoryDisplayName(TARGETS);
                targetsHealth.setCategoryHealthState(getWorstHealthState(responseItems.stream()
                                .map(AggregatedHealthResponseDTO::getHealthState)
                                .collect(Collectors.toList())));
                targetsHealth.addResponseItems(responseItems);
                result.add(targetsHealth);
            }
            if (healthCategory == null || HealthCategory.ACTION.equals(healthCategory)) {
                long timeout = checkAnalysisTimeOut();
                HealthCategoryResponseDTO actionHealthResponse = new HealthCategoryResponseDTO();
                actionHealthResponse.setHealthCategory(HealthCategory.ACTION);
                actionHealthResponse.setCategoryDisplayName(ACTIONS);
                // For now only one subcategory exists under Action health check.
                // We can build a meta data map and passed into aggregateActionHealthDTO if there
                // are more subcategories built under action.
                List<AggregatedHealthResponseDTO> responseItems = new ArrayList<>();
                if (timeout != -1) {
                    responseItems.addAll(HealthDataMapper.aggregateAnalysisHealthDTO(timeout));
                } else {
                    // When there is no timeout, a normal health state response is generated because
                    // the API endpoint is querying for the health overall which suggests both healthy
                    // and unhealthy states should be returned.
                    AggregatedHealthResponseDTO aggregatedHealthResponseDTO =
                            new AggregatedHealthResponseDTO(ActionStatusSubcategory.ANALYSIS.name(),
                                    HealthState.NORMAL, 0);
                    responseItems.add(aggregatedHealthResponseDTO);
                }
                actionHealthResponse.setCategoryHealthState(getWorstHealthState(
                        responseItems.stream().map(AggregatedHealthResponseDTO::getHealthState).collect(Collectors.toList())));
                actionHealthResponse.addResponseItems(responseItems);
                result.add(actionHealthResponse);
            }
            return result;
        } catch (StatusRuntimeException e) {
            throw new CommunicationError(ExceptionMapper.translateStatusException(e));
        }
    }

    /**
     * Check whether the latest analysis has been timed out in market component.
     * @return -1 if analysis was not timeout, or a positive value that indicates the
     * time duration of the timeout check.
     */
    private long checkAnalysisTimeOut() {
        GetAnalysisStateResponse response = analysisStub.getAnalysisState(
                GetAnalysisStateRequest.newBuilder()
                        .setTopologyContextId(realtimeTopologyContextId)
                        .build());
        if (response.hasAnalysisState() && response.getAnalysisState().equals(AnalysisState.FAILED)) {
            return response.getTimeOutSetting();
        } else {
            return -1;
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
