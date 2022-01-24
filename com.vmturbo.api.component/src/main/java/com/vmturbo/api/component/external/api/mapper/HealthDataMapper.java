package com.vmturbo.api.component.external.api.mapper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO;
import com.vmturbo.api.dto.admin.AggregatedHealthResponseDTO.Recommendation;
import com.vmturbo.api.dto.target.BaseTargetErrorDetailsApiDTO;
import com.vmturbo.api.dto.target.DelayedDataInfoApiDTO;
import com.vmturbo.api.dto.target.DiscoveryInfoApiDTO;
import com.vmturbo.api.dto.target.TargetErrorDetailsApiDTO;
import com.vmturbo.api.dto.target.TargetHealthApiDTO;
import com.vmturbo.api.dto.target.TargetThirdPartyErrorDetailsApiDTO;
import com.vmturbo.api.enums.health.HealthState;
import com.vmturbo.api.enums.health.TargetErrorType;
import com.vmturbo.api.enums.health.TargetStatusSubcategory;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetDetails;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.commons.Pair;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.ErrorTypeInfoCase;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.ThirdPartyApiFailureErrorType;

import common.HealthCheck;

/**
 * Maps health data from topology-processor to API DTOs.
 */
public class HealthDataMapper {
    private HealthDataMapper() {}

    /**
     * A suffix appended to the message (error) text for hidden targets.
     */
    public static final String HIDDEN_TARGET_MESSAGE_SUFFIX = " (Hidden Target)";

    private static final Map<ErrorType, TargetErrorType> ERROR_TYPE_CONVERTER = initializeErrorTypeConverter();

    private static final Map<ErrorTypeInfoCase, TargetErrorType> ERROR_TYPE_INFO_CONVERTER = Maps.newHashMap();

    static {
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.CONNECTION_TIME_OUT_ERROR_TYPE,
                TargetErrorType.CONNECTIVITY_ERROR);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.UNAUTHENTICATED_ERROR_TYPE, TargetErrorType.UNAUTHENTICATED);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.UNAUTHORIZED_ERROR_TYPE, TargetErrorType.UNAUTHENTICATED);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.TOKEN_UNAVAILABLE_ERROR_TYPE, TargetErrorType.TOKEN_UNAVAILABLE);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.TOKEN_EXPIRED_ERROR_TYPE, TargetErrorType.TOKEN_UNAVAILABLE);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.VERSION_NOT_SUPPORTED_ERROR_TYPE, TargetErrorType.VERSION_NOT_SUPPORTED);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.DATA_IS_MISSING_ERROR_TYPE, TargetErrorType.DATA_ACCESS_ERROR);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.PROBE_PARSING_ERROR_TYPE, TargetErrorType.DATA_ACCESS_ERROR);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.OTHER_ERROR_TYPE, TargetErrorType.INTERNAL_PROBE_ERROR);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.INTERNAL_PROBE_ERROR_TYPE, TargetErrorType.INTERNAL_PROBE_ERROR);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.DUPLICATION_ERROR_TYPE, TargetErrorType.DUPLICATION);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.DELAYED_DATA_ERROR_TYPE, TargetErrorType.DELAYED_DATA);
        ERROR_TYPE_INFO_CONVERTER.put(ErrorTypeInfoCase.THIRD_PARTY_API_FAILURE_ERROR_TYPE,
                TargetErrorType.THIRD_PARTY_FAILURE);
    }

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
        result.put(ErrorType.DELAYED_DATA, TargetErrorType.DELAYED_DATA);
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
        result.put(TargetErrorType.DELAYED_DATA, "Discovered data is too old. Check the pod states,"
                + " target servers, and Target Configuration page for more information.");
        return result;
    }

    private static final Map<TargetHealthSubCategory, TargetStatusSubcategory> TARGET_CHECK_SUBCATEGORIES_CONVERTER =
                    initializeTargetCheckSubcategories();

    private static Map<TargetHealthSubCategory, TargetStatusSubcategory> initializeTargetCheckSubcategories() {
        final Map<TargetHealthSubCategory, TargetStatusSubcategory> result = new EnumMap<>(
                        TargetHealthSubCategory.class);
        result.put(TargetHealthSubCategory.DISCOVERY, TargetStatusSubcategory.DISCOVERY);
        result.put(TargetHealthSubCategory.VALIDATION, TargetStatusSubcategory.VALIDATION);
        result.put(TargetHealthSubCategory.DUPLICATION, TargetStatusSubcategory.DUPLICATION);
        result.put(TargetHealthSubCategory.DELAYED_DATA, TargetStatusSubcategory.DELAYED_DATA);
        return result;
    }

    /**
     * A utility map to convert a topology processor HealthState to an API HealthState.
     */
    public static final Map<HealthCheck.HealthState, HealthState> HEALTH_STATE_CONVERTER =
                    initializeHealthStateConverter();

    private static Map<HealthCheck.HealthState, HealthState> initializeHealthStateConverter() {
        final Map<HealthCheck.HealthState, HealthState> result = new EnumMap<>(HealthCheck.HealthState.class);
        result.put(HealthCheck.HealthState.CRITICAL, HealthState.CRITICAL);
        result.put(HealthCheck.HealthState.MAJOR, HealthState.MAJOR);
        result.put(HealthCheck.HealthState.MINOR, HealthState.MINOR);
        result.put(HealthCheck.HealthState.NORMAL, HealthState.NORMAL);
        return result;
    }

    /**
     * Convert target health report from inner representation to external API DTO.
     * @param targetId The id of the target.
     * @param healthInfo a container to transmit info internally between the components.
     * @return target health API DTO
     */
    public static TargetHealthApiDTO mapTargetHealthInfoToDTO(final long targetId,
                    @Nonnull final TargetHealth healthInfo) {
        TargetHealthApiDTO result = new TargetHealthApiDTO();
        result.setUuid(Long.toString(targetId));
        result.setTargetName(healthInfo.getTargetName());

        result.setTargetStatusSubcategory(TARGET_CHECK_SUBCATEGORIES_CONVERTER.get(healthInfo.getSubcategory()));
        result.setHealthState(HEALTH_STATE_CONVERTER.get(healthInfo.getHealthState()));

        if (healthInfo.hasMessageText() && !healthInfo.getMessageText().isEmpty()) {
            result.setErrorText(healthInfo.getMessageText());
        }
        if (healthInfo.hasTimeOfFirstFailure()) {
            result.setTimeOfFirstFailure(DateTimeUtil.toString(healthInfo.getTimeOfFirstFailure()));
        }
        if (healthInfo.hasLastSuccessfulDiscoveryCompletionTime()) {
            result.setTimeOfLastSuccessfulDiscovery(DateTimeUtil.toString(
                            healthInfo.getLastSuccessfulDiscoveryCompletionTime()));
        }
        result.setTargetErrorDetails(addTargetErrorDetails(healthInfo));

        return result;
    }

    private static Collection<TargetErrorDetailsApiDTO> addTargetErrorDetails(
                    @Nonnull final TargetHealth healthInfo) {
        final Collection<TargetErrorDetailsApiDTO> targetErrorDetailsApiDTOS = new ArrayList<>();
        healthInfo.getErrorTypeInfoList().forEach(errorTypeInfo -> {
            final TargetErrorDetailsApiDTO targetErrorDetailsApiDTO;
            final TargetErrorType targetErrorType = getErrorTypeInfoConverter().get(
                    errorTypeInfo.getErrorTypeInfoCase());
            switch (errorTypeInfo.getErrorTypeInfoCase()) {
                case THIRD_PARTY_API_FAILURE_ERROR_TYPE:
                    ThirdPartyApiFailureErrorType thirdPartyError = errorTypeInfo.getThirdPartyApiFailureErrorType();
                    targetErrorDetailsApiDTO =
                            new TargetThirdPartyErrorDetailsApiDTO(targetErrorType,
                                    thirdPartyError.getErrorCode(),
                                    thirdPartyError.getEndPoint());

                    break;
                case DELAYED_DATA_ERROR_TYPE:
                    targetErrorDetailsApiDTO = new DelayedDataInfoApiDTO();
                    ((DelayedDataInfoApiDTO)targetErrorDetailsApiDTO).setTimeOfCheck(
                                    DateTimeUtil.toString(healthInfo.getTimeOfCheck()));
                    ((DelayedDataInfoApiDTO)targetErrorDetailsApiDTO).setTimeOfLastSuccessfulDiscoveryStart(
                            DateTimeUtil.toString(healthInfo.getLastSuccessfulDiscoveryStartTime()));
                    if (healthInfo.hasLastSuccessfulIncrementalDiscoveryStartTime()) {
                        ((DelayedDataInfoApiDTO)targetErrorDetailsApiDTO)
                            .setTimeOfLastSuccessfulIncrementalDiscoveryStart(
                                DateTimeUtil.toString(healthInfo.getLastSuccessfulIncrementalDiscoveryStartTime()));
                    }
                    if (healthInfo.hasLastSuccessfulIncrementalDiscoveryCompletionTime()) {
                        ((DelayedDataInfoApiDTO)targetErrorDetailsApiDTO)
                            .setTimeOfLastSuccessfulIncrementalDiscoveryFinish(
                                DateTimeUtil.toString(healthInfo.getLastSuccessfulIncrementalDiscoveryCompletionTime()));
                    }
                    break;
                default:
                    if (TargetHealthSubCategory.DISCOVERY.equals(healthInfo.getSubcategory())) {
                        targetErrorDetailsApiDTO = new DiscoveryInfoApiDTO(targetErrorType);
                        ((DiscoveryInfoApiDTO)targetErrorDetailsApiDTO).setNumberOfConsecutiveFailures(
                                        healthInfo.getConsecutiveFailureCount());
                    } else {
                        targetErrorDetailsApiDTO = new BaseTargetErrorDetailsApiDTO(targetErrorType);
                    }
            }
            targetErrorDetailsApiDTOS.add(targetErrorDetailsApiDTO);
        });
        return targetErrorDetailsApiDTOS;
    }

    /**
     * Aggregate targets health info into a list of {@link AggregatedHealthResponseDTO} items.
     * @param targetsDetails target details of targets received from topology-processor
     * @return {@link AggregatedHealthResponseDTO} items
     */
    public static @Nonnull List<AggregatedHealthResponseDTO> aggregateTargetHealthInfoToDTO(
            @Nonnull Map<Long, TargetDetails> targetsDetails) {
        Map<TargetHealthSubCategory, Map<HealthCheck.HealthState, Integer>> subcategoryStatesCounters =
                        new EnumMap<>(TargetHealthSubCategory.class);
        Map<TargetHealthSubCategory, Map<ErrorTypeInfoCase, Boolean>> subcategoryErrorSets =
                        new EnumMap<>(TargetHealthSubCategory.class);

        for (TargetDetails targetDetail : targetsDetails.values()) {
            if (!targetDetail.getHidden()) {
                // Do category analysis in update states.
                updateStates(subcategoryStatesCounters, subcategoryErrorSets, targetDetail, targetsDetails);
            }
        }

        List<AggregatedHealthResponseDTO> result = new ArrayList<>(2);
        for (Map.Entry<TargetHealthSubCategory, Map<HealthCheck.HealthState, Integer>> entry
                        : subcategoryStatesCounters.entrySet()) {
            AggregatedHealthResponseDTO responseItem = makeResponseItem(entry.getKey(),
                            entry.getValue(), subcategoryErrorSets.get(entry.getKey()));
            if (HealthState.NORMAL != responseItem.getHealthState()) {
                result.add(responseItem);
            }
        }

        return result;
    }

    private static void updateStates(
                    @Nonnull Map<TargetHealthSubCategory, Map<HealthCheck.HealthState, Integer>> subcategoryStatesCounters,
                    @Nonnull Map<TargetHealthSubCategory, Map<ErrorTypeInfoCase, Boolean>> subcategoryErrorSets,
                    @Nonnull TargetDetails targetDetails,
                    @Nonnull Map<Long, TargetDetails> targetDetailsMap) {
        TargetHealth parentTargetHealth = targetDetails.getHealthDetails();
        List<TargetHealth> allMembers = Lists.newArrayList(parentTargetHealth);
        allMembers.addAll(targetDetails.getDerivedList().stream()
                        .map(targetDetailsMap::get)
                        .filter(Objects::nonNull)
                        .filter(TargetDetails::getHidden)
                        .map(TargetDetails::getHealthDetails)
                        .collect(Collectors.toList()));

        Pair<HealthCheck.HealthState, TargetHealthSubCategory> healthAndSubcategory =
                        siftHealthStateAndSubcategory(parentTargetHealth, allMembers);

        Map<HealthCheck.HealthState, Integer> statesCounter = subcategoryStatesCounters.computeIfAbsent(
                healthAndSubcategory.second, k -> new EnumMap<>(HealthCheck.HealthState.class));
        Map<ErrorTypeInfoCase, Boolean> errorsSet = subcategoryErrorSets.computeIfAbsent(
                healthAndSubcategory.second, k -> new EnumMap<>(ErrorTypeInfoCase.class));
        for (TargetHealth member : allMembers) {
            if (member.getHealthState() != HealthCheck.HealthState.NORMAL) {
                if (!member.getErrorTypeInfoList().isEmpty()) {
                    // TODO : Roop we are only considering the first error type info at this point.
                    ErrorTypeInfoCase errorTypeInfoCase = member.getErrorTypeInfoList().get(0).getErrorTypeInfoCase();
                    errorsSet.putIfAbsent(errorTypeInfoCase, member != parentTargetHealth);
                } else if (member.getMessageText().contains("Validation pending")) {
                    errorsSet.putIfAbsent(ErrorTypeInfoCase.INTERNAL_PROBE_ERROR_TYPE,
                            member != parentTargetHealth);
                }
            }
        }
        int counter = statesCounter.getOrDefault(healthAndSubcategory.first, 0) + 1;
        statesCounter.put(healthAndSubcategory.first, counter);
    }


    private static @Nonnull Pair<HealthCheck.HealthState, TargetHealthSubCategory> siftHealthStateAndSubcategory(
            @Nonnull TargetHealth parentHealth, @Nonnull List<TargetHealth> allMembers) {
        HealthCheck.HealthState parentHealthState = parentHealth.getHealthState();
        if (allMembers.size() == 1 || parentHealthState != HealthCheck.HealthState.NORMAL) {
            return new Pair<>(parentHealthState, parentHealth.getSubcategory());
        }

        HealthCheck.HealthState worstHealthState = parentHealthState;
        TargetHealthSubCategory worstCategory = parentHealth.getSubcategory();
        for (TargetHealth newTargetHealth : allMembers) {
            // Return early.
            if (worstHealthState == HealthCheck.HealthState.CRITICAL
                            && worstCategory == TargetHealthSubCategory.VALIDATION) {
                return new Pair<>(HealthCheck.HealthState.CRITICAL, TargetHealthSubCategory.VALIDATION);
            }
            HealthCheck.HealthState newHealthState = newTargetHealth.getHealthState();
            TargetHealthSubCategory newCategory = newTargetHealth.getSubcategory();
            if (newHealthState.ordinal() < worstHealthState.ordinal()) {
                worstHealthState = newHealthState;
                worstCategory = newCategory;
            } else if (newHealthState.ordinal() == worstHealthState.ordinal()
                            && newCategory.ordinal() < worstCategory.ordinal()) {
                worstCategory = newCategory;
            }
        }
        return new Pair<>(worstHealthState, worstCategory);
    }

    private static AggregatedHealthResponseDTO makeResponseItem(TargetHealthSubCategory subcategory,
                    Map<HealthCheck.HealthState, Integer> statesCounter,
                    Map<ErrorTypeInfoCase, Boolean> errorsSet) {
        HealthCheck.HealthState state = HealthCheck.HealthState.NORMAL;
        if (statesCounter.containsKey(HealthCheck.HealthState.CRITICAL)) {
            state = HealthCheck.HealthState.CRITICAL;
        } else if (statesCounter.containsKey(HealthCheck.HealthState.MAJOR)) {
            state = HealthCheck.HealthState.MAJOR;
        } else if (statesCounter.containsKey(HealthCheck.HealthState.MINOR)) {
            state = HealthCheck.HealthState.MINOR;
        }
        int numberOfTargets = statesCounter.getOrDefault(state, 0);

        AggregatedHealthResponseDTO response = new AggregatedHealthResponseDTO(
                        TARGET_CHECK_SUBCATEGORIES_CONVERTER.get(subcategory).toString(),
                        HEALTH_STATE_CONVERTER.get(state),
                        numberOfTargets);
        for (Map.Entry<ErrorTypeInfoCase, Boolean> entry : errorsSet.entrySet()) {
            TargetErrorType errorType = getErrorTypeInfoConverter().get(entry.getKey());
            boolean reportForHidden = entry.getValue();
            String recommendationText = TARGET_ERROR_TYPE_RECOMMENDATIONS.get(errorType)
                            + (reportForHidden ? HIDDEN_TARGET_MESSAGE_SUFFIX : "");
            Recommendation recommendation = new Recommendation(errorType.toString(),
                            recommendationText);
            response.addRecommendation(recommendation);
        }
        return response;
    }

    private static Map<ErrorTypeInfoCase, TargetErrorType> getErrorTypeInfoConverter() {
        return Collections.unmodifiableMap(ERROR_TYPE_INFO_CONVERTER);
    }
}
