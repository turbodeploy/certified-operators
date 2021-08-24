package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO.Recommendation;
import com.vmturbo.api.dto.target.TargetHealthApiDTO;
import com.vmturbo.api.enums.healthCheck.HealthState;
import com.vmturbo.api.enums.healthCheck.TargetCheckSubcategory;
import com.vmturbo.api.enums.healthCheck.TargetErrorType;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.commons.Pair;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;

/**
 * Maps health data from topology-processor to API DTOs.
 */
public class HealthDataMapper {
    private HealthDataMapper() {}

    private static final Map<ErrorType, TargetErrorType> ERROR_TYPE_CONVERTER = initializeErrorTypeConverter();

    private static Map<ErrorType, TargetErrorType> initializeErrorTypeConverter() {
        final Map<ErrorType, TargetErrorType> result = new EnumMap<>(ErrorType.class);
        result.put(ErrorType.CONNECTION_TIMEOUT, TargetErrorType.CONNECTIVITY_ERROR);
        result.put(ErrorType.UNAUTHENTICATED, TargetErrorType.UNAUTHENTICATED);
        result.put(ErrorType.UNAUTHORIZED, TargetErrorType.UNAUTHENTICATED);
        result.put(ErrorType.TOKEN_UNAVAILABLE, TargetErrorType.TOKEN_UNAVAILABLE);
        result.put(ErrorType.TOKEN_EXPIRED, TargetErrorType.TOKEN_UNAVAILABLE);
        result.put(ErrorType.VERSION_NOT_SUPPORTED, TargetErrorType.VERSION_NOT_SUPPORTED);
        result.put(ErrorType.DATA_IS_MISSING, TargetErrorType.DATA_ACCESS_ERROR);
        result.put(ErrorType.PROBE_PARSING_ERROR, TargetErrorType.DATA_ACCESS_ERROR);
        result.put(ErrorType.OTHER, TargetErrorType.INTERNAL_PROBE_ERROR);
        result.put(ErrorType.INTERNAL_PROBE_ERROR, TargetErrorType.INTERNAL_PROBE_ERROR);
        result.put(ErrorType.DUPLICATION, TargetErrorType.DUPLICATION);
        return result;
    }

    private static final Map<TargetErrorType, String> TARGET_ERROR_TYPE_RECOMMENDATIONS =
                    initializeTargetErrorTypeRecommendations();

    private static Map<TargetErrorType, String> initializeTargetErrorTypeRecommendations() {
        final Map<TargetErrorType, String> result = new EnumMap<>(TargetErrorType.class);
        result.put(TargetErrorType.CONNECTIVITY_ERROR, "Check the connectivity of the targets.");
        result.put(TargetErrorType.UNAUTHENTICATED, "Some targets are not authenticated. Check the credentials.");
        result.put(TargetErrorType.TOKEN_UNAVAILABLE, "Token is unavailable. Get (a new) one.");
        result.put(TargetErrorType.VERSION_NOT_SUPPORTED, "Target version is not supported. Choose a different target.");
        result.put(TargetErrorType.DATA_ACCESS_ERROR, "Wrong data received from target. Check what is being sent.");
        result.put(TargetErrorType.INTERNAL_PROBE_ERROR,
                "Check the Target Configuration page for more information. Please contact support if the problem persists.");
        result.put(TargetErrorType.DUPLICATION, "There're duplicate targets present in the system."
                        + " Check the Target Configuration page for more information.");
        return result;
    }

    private static final Map<TargetHealthSubCategory, TargetCheckSubcategory> TARGET_CHECK_SUBCATEGORIES_CONVERTER =
                    initializeTargetCheckSubcategories();

    private static Map<TargetHealthSubCategory, TargetCheckSubcategory> initializeTargetCheckSubcategories() {
        final Map<TargetHealthSubCategory, TargetCheckSubcategory> result = new EnumMap<>(
                        TargetHealthSubCategory.class);
        result.put(TargetHealthSubCategory.DISCOVERY, TargetCheckSubcategory.DISCOVERY);
        result.put(TargetHealthSubCategory.VALIDATION, TargetCheckSubcategory.VALIDATION);
        result.put(TargetHealthSubCategory.DUPLICATION, TargetCheckSubcategory.DUPLICATION);
        return result;
    }

    /**
     * Convert target health report from inner representation to external API DTO.
     * @param targetId The id of the target.
     * @param healthInfo a container to transmit info internally between the components.
     * @return target health API DTO
     */
    public static TargetHealthApiDTO mapTargetHealthInfoToDTO(final long targetId, @Nonnull final TargetHealth healthInfo) {
        TargetHealthApiDTO result = new TargetHealthApiDTO();
        result.setUuid(Long.toString(targetId));
        result.setTargetName(healthInfo.getTargetName());
        switch (healthInfo.getSubcategory()) {
            case DISCOVERY:
                result.setCheckSubcategory(TargetCheckSubcategory.DISCOVERY);
                break;
            case VALIDATION:
                result.setCheckSubcategory(TargetCheckSubcategory.VALIDATION);
                break;
            case DUPLICATION:
                result.setCheckSubcategory(TargetCheckSubcategory.DUPLICATION);
                break;
        }

        String errorText = healthInfo.getErrorText();
        if (errorText.isEmpty()) {
            result.setHealthState(HealthState.NORMAL);
        } else if (!healthInfo.hasErrorType()) {
            result.setHealthState(HealthState.MINOR);
            result.setErrorText(errorText);
        } else {
            result.setHealthState(HealthState.CRITICAL);
            result.setErrorType(ERROR_TYPE_CONVERTER.get(healthInfo.getErrorType()));
            result.setErrorText(errorText);
            result.setTimeOfFirstFailure(healthInfo.getTimeOfFirstFailure());
            result.setNumberOfConsecutiveFailures(healthInfo.getConsecutiveFailureCount());
        }

        // set the last successful discovery if we have information for it
        if (healthInfo.hasLastSuccessfulDiscovery()) {
            result.setLastSuccessfulDiscovery(DateTimeUtil.toString(healthInfo.getLastSuccessfulDiscovery()));
        }
        return result;
    }

    /**
     * Aggregate targets health info into a list of {@link AggregatedHealthResponseDTO} items.
     * @param targetDetails target details of targets received from topology-processor
     * @param failedDiscoveryCountThreshold failed discovery count threshold.
     * @return {@link AggregatedHealthResponseDTO} items
     */
    public static @Nonnull List<AggregatedHealthResponseDTO> aggregateTargetHealthInfoToDTO(
            @Nonnull Map<Long, TargetDetails> targetDetails, int failedDiscoveryCountThreshold) {
        Map<TargetHealthSubCategory, Map<HealthState, Integer>> subcategoryStatesCounters =
                        new EnumMap<>(TargetHealthSubCategory.class);
        Map<TargetHealthSubCategory, Map<TargetErrorType, String>> subcategoryRecommendations =
                        new EnumMap<>(TargetHealthSubCategory.class);

        targetDetails.values().stream().forEach(targetDetail -> {
            if (!targetDetail.getHidden()) {
                // Do category analysis in update states.
                updateStates(subcategoryStatesCounters, subcategoryRecommendations, targetDetail,
                    failedDiscoveryCountThreshold, targetDetails);
            }
        });

        List<AggregatedHealthResponseDTO> result = new ArrayList<>(2);
        for (Map.Entry<TargetHealthSubCategory, Map<HealthState, Integer>> entry
                        : subcategoryStatesCounters.entrySet()) {
            Map<TargetErrorType, String> recommendations = subcategoryRecommendations.get(entry.getKey());
            AggregatedHealthResponseDTO responseItem = makeResponseItem(
                            TARGET_CHECK_SUBCATEGORIES_CONVERTER.get(entry.getKey()),
                            entry.getValue(), recommendations);
            result.add(responseItem);
        }

        return result;
    }

    private static void updateStates(@Nonnull Map<TargetHealthSubCategory, Map<HealthState, Integer>> subcategoryStatesCounters,
            @Nonnull Map<TargetHealthSubCategory, Map<TargetErrorType, String>> subcategoryRecommendations,
            @Nonnull TargetDetails targetDetails, int failedDiscoveryCountThreshold,
            @Nonnull Map<Long, TargetDetails> targetDetailsMap) {
        List<TargetHealth> allMembers = Lists.newArrayList(targetDetails.getHealthDetails());
        allMembers.addAll(targetDetails.getDerivedList().stream()
            .map(id -> targetDetailsMap.get(id)).filter(Objects::nonNull)
            .filter(targetDetail -> targetDetail.getHidden())
            .map(TargetDetails::getHealthDetails).collect(Collectors.toList()));
        Pair<HealthState, TargetHealthSubCategory> categoryAndHealth =
            determineSubCategoryAndHealthState(targetDetails, allMembers, failedDiscoveryCountThreshold);
        Map<HealthState, Integer> statesCounter = subcategoryStatesCounters.computeIfAbsent(
                categoryAndHealth.second, k -> new EnumMap<>(HealthState.class));
        Map<TargetErrorType, String> recommendations = subcategoryRecommendations.computeIfAbsent(
                categoryAndHealth.second, k -> new EnumMap<>(TargetErrorType.class));
        for (TargetHealth member : allMembers) {
            if (hasError(member, failedDiscoveryCountThreshold)) {
                if (member.hasErrorType()) {
                    TargetErrorType errorType = ERROR_TYPE_CONVERTER.get(member.getErrorType());
                    recommendations.putIfAbsent(errorType, TARGET_ERROR_TYPE_RECOMMENDATIONS.get(errorType)
                        + (member != targetDetails.getHealthDetails() ? " (Hidden Target)" : ""));
                } else if (member.getErrorText().contains("Validation pending")) {
                    recommendations.putIfAbsent(TargetErrorType.INTERNAL_PROBE_ERROR,
                        TARGET_ERROR_TYPE_RECOMMENDATIONS.get(TargetErrorType.INTERNAL_PROBE_ERROR)
                        + (member != targetDetails.getHealthDetails() ? " (Hidden Target)" : ""));
                }
            }
        }
        int counter = statesCounter.getOrDefault(categoryAndHealth.first, 0) + 1;
        statesCounter.put(categoryAndHealth.first, counter);
    }

    private static @Nonnull HealthState getHealthState(@Nonnull TargetHealth targetHealth,
            int failedDiscoveryCountThreshold) {
        if (hasError(targetHealth, failedDiscoveryCountThreshold)) {
            return targetHealth.hasErrorType() ? HealthState.CRITICAL : HealthState.MINOR;
        } else {
            return HealthState.NORMAL;
        }
    }

    private static @Nonnull Pair<HealthState, TargetHealthSubCategory> determineSubCategoryAndHealthState(
            @Nonnull TargetDetails parentDetails, @Nonnull List<TargetHealth> allMembers,
            int failedDiscoveryCountThreshold) {
        HealthState parentHealth = getHealthState(parentDetails.getHealthDetails(), failedDiscoveryCountThreshold);
        if (allMembers.size() == 1 || parentHealth != HealthState.NORMAL) {
            return new Pair<>(parentHealth, parentDetails.getHealthDetails().getSubcategory());
        }
        return getWorstTargetHealth(allMembers, failedDiscoveryCountThreshold);
    }

    private static boolean hasError(@Nonnull TargetHealth healthInfo, int failedDiscoveryCountThreshold) {
        return !Strings.isNullOrEmpty(healthInfo.getErrorText())
            && !ignoreFailedDiscovery(healthInfo, failedDiscoveryCountThreshold);
    }

    private static boolean ignoreFailedDiscovery(TargetHealth healthInfo,
            int failedDiscoveryCountThreshold) {
        return healthInfo.getSubcategory() == TargetHealthSubCategory.DISCOVERY
            && healthInfo.hasErrorType()
            && healthInfo.getConsecutiveFailureCount() < failedDiscoveryCountThreshold;
    }

    private static AggregatedHealthResponseDTO makeResponseItem(TargetCheckSubcategory subcategory,
                    Map<HealthState, Integer> statesCounter, Map<TargetErrorType, String> recommendations) {
        HealthState state = HealthState.NORMAL;
        if (statesCounter.containsKey(HealthState.CRITICAL)) {
            state = HealthState.CRITICAL;
        } else if (statesCounter.containsKey(HealthState.MINOR)) {
            state = HealthState.MINOR;
        }
        int numberOfTargets = statesCounter.getOrDefault(state, 0);

        AggregatedHealthResponseDTO response = new AggregatedHealthResponseDTO(
                subcategory.toString(), state, numberOfTargets);
        for (Map.Entry<TargetErrorType, String> entry : recommendations.entrySet()) {
            Recommendation recommendation = new Recommendation(entry.getKey().toString(), entry.getValue());
            response.addRecommendation(recommendation);
        }
        return response;
    }

    private static @Nonnull Pair<HealthState, TargetHealthSubCategory> getWorstTargetHealth(
            @Nonnull List<TargetHealth> targetHealths, int failedDiscoveryCountThreshold) {
        HealthState worstHealthState = getHealthState(targetHealths.get(0), failedDiscoveryCountThreshold);
        TargetHealthSubCategory worstCategory = targetHealths.get(0).getSubcategory();
        for (TargetHealth newTargetHealth : targetHealths) {
            // Return early.
            if (worstHealthState == HealthState.CRITICAL && worstCategory == TargetHealthSubCategory.VALIDATION) {
                return new Pair<>(HealthState.CRITICAL, TargetHealthSubCategory.VALIDATION);
            }
            HealthState newHealthState = getHealthState(newTargetHealth, failedDiscoveryCountThreshold);
            TargetHealthSubCategory newCategory = newTargetHealth.getSubcategory();
            if (newHealthState.ordinal() < worstHealthState.ordinal()) {
                worstHealthState = newHealthState;
                worstCategory = newTargetHealth.getSubcategory();
            } else if (newHealthState.ordinal() == worstHealthState.ordinal()) {
                if (worstCategory == TargetHealthSubCategory.VALIDATION) {
                    continue;
                } else if (newCategory == TargetHealthSubCategory.VALIDATION) {
                    worstCategory = newCategory;
                } else if (worstCategory == TargetHealthSubCategory.DISCOVERY) {
                    continue;
                } else if (newCategory == TargetHealthSubCategory.DISCOVERY) {
                    worstCategory = newCategory;
                }
            }
        }
        return new Pair<>(worstHealthState, worstCategory);
    }
}
