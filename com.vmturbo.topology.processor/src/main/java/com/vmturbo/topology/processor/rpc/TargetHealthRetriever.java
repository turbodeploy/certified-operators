package com.vmturbo.topology.processor.rpc;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.targets.status.TargetStatusTrackerImpl.DiscoveryFailure;

import common.HealthCheck.HealthState;

/**
 * Retrieves any relevant information to determine the health of a target, and returns the
 * information as a {@link TargetHealth} object.
 *
 * <p/>Separated from the {@link TargetStatusTracker} for easier testability, and to encapsulate
 * all the special cases for the interplay between latest validations and discoveries.
 */
public class TargetHealthRetriever {
    private final IOperationManager operationManager;
    private final TargetStatusTracker targetStatusTracker;
    private final TargetStore targetStore;
    private final ProbeStore probeStore;
    private final Clock clock;
    private final SettingServiceBlockingStub settingServiceClient;

    private int failedDiscoveriesCountThreshold = (int)GlobalSettingSpecs.FailedDiscoveryCountThreshold
                    .createSettingSpec().getNumericSettingValueType().getDefault();
    private long delayedDataThresholdMultiplier = (long)GlobalSettingSpecs.DelayedDataThresholdMultiplier
                    .createSettingSpec().getNumericSettingValueType().getDefault();

    TargetHealthRetriever(@Nonnull final IOperationManager operationManager,
            @Nonnull final TargetStatusTracker targetStatusTracker,
            @Nonnull final TargetStore targetStore, @Nonnull final ProbeStore probeStore,
            @Nonnull final Clock clock,
            @Nonnull final SettingServiceBlockingStub settingServiceClient) {
        this.operationManager = operationManager;
        this.targetStatusTracker = targetStatusTracker;
        this.targetStore = targetStore;
        this.probeStore = probeStore;
        this.clock = clock;
        this.settingServiceClient = settingServiceClient;
    }

    /**
     * Get the health of a set of targets.
     * @param targetIds The input target ids.
     * @param requestAll If true, an empty "targetIds" input means "request all". This is to prevent
     *                   accidentally requesting all while still allowing it.
     * @return {@link TargetHealth} for each valid target id.
     */
    @Nonnull
    public Map<Long, TargetHealth> getTargetHealth(@Nonnull final Set<Long> targetIds,
            final boolean requestAll) {
        if (targetIds.isEmpty() && !requestAll) {
            return Collections.emptyMap();
        }
        final List<Target> matchingTargets;
        if (targetIds.isEmpty()) {
            matchingTargets = targetStore.getAll();
        } else {
            matchingTargets = targetStore.getTargets(targetIds);
        }

        requestSettings();

        return matchingTargets.stream().collect(
                        Collectors.toMap(Target::getId, this::computeTargetHealthInfo));
    }

    private void requestSettings() {
        GetMultipleGlobalSettingsRequest multipleSettingsRequest = GetMultipleGlobalSettingsRequest.newBuilder()
                        .addSettingSpecName(GlobalSettingSpecs.FailedDiscoveryCountThreshold.getSettingName())
                        .addSettingSpecName(GlobalSettingSpecs.DelayedDataThresholdMultiplier.getSettingName())
                        .build();
        Iterator<Setting> settings = settingServiceClient.getMultipleGlobalSettings(multipleSettingsRequest);
        while (settings.hasNext()) {
            Setting setting = settings.next();
            if (GlobalSettingSpecs.FailedDiscoveryCountThreshold.getSettingName()
                            .equals(setting.getSettingSpecName())) {
                failedDiscoveriesCountThreshold = (int)setting.getNumericSettingValue().getValue();
            } else if (GlobalSettingSpecs.DelayedDataThresholdMultiplier.getSettingName()
                            .equals(setting.getSettingSpecName())) {
                delayedDataThresholdMultiplier = (long)setting.getNumericSettingValue().getValue();
            }
        }
    }

    @Nonnull
    private TargetHealth computeTargetHealthInfo(@Nonnull final Target target) {
        TargetHealth.Builder targetHealthBuilder = TargetHealth.newBuilder();
        targetHealthBuilder.setTargetName(target.getDisplayName());

        long targetId = target.getId();
        //Set lastSuccessfulDiscoveryTime if time is present/known.
        Pair<Long, Long> lastSuccessfulDiscoveryTime = targetStatusTracker.getLastSuccessfulDiscoveryTime(targetId);
        if (lastSuccessfulDiscoveryTime != null) {
            targetHealthBuilder.setLastSuccessfulDiscoveryCompletionTime(
                            lastSuccessfulDiscoveryTime.getSecond());
        }

        Optional<Validation> lastValidation = operationManager.getLastValidationForTarget(targetId);
        Optional<Discovery> lastDiscovery = operationManager.getLastDiscoveryForTarget(targetId,
                DiscoveryType.FULL);

        //Check if we have info about validation.
        if (!lastValidation.isPresent()) {
            if (!lastDiscovery.isPresent()) {
                return reportNoDiscoveryData(targetHealthBuilder, target, false);
            } else {
                return verifyDiscovery(targetHealthBuilder, target, lastDiscovery.get());
            }
        }

        Validation validation = lastValidation.get();
        //Check if the validation has passed fine.
        if (validation.getStatus() == Status.SUCCESS) {
            if (!lastDiscovery.isPresent()) {
                return reportNoDiscoveryData(targetHealthBuilder, target, true);
            } else {
                return verifyDiscovery(targetHealthBuilder, target, lastDiscovery.get());
            }
        }

        //Validation was not Ok, but check the last discovery.
        if (lastDiscovery.isPresent()) {
            //Check if there's a discovery that has happened later and passed fine.
            LocalDateTime validationFinishTime = validation.getCompletionTime();
            if (lastSuccessfulDiscoveryTime != null && lastSuccessfulDiscoveryTime.getSecond()
                            .compareTo(TimeUtil.localTimeToMillis(validationFinishTime, clock)) >= 0) {
                //There has been a successful discovery after the last validation which means
                //that validation problem probably got fixed and now discovery problems should
                //be reported if any. (Or discovery success.)
                return verifyDiscovery(targetHealthBuilder, target, lastDiscovery.get());
            }

            TargetHealth duplicateTargetHealth = reportTargetDuplication(targetHealthBuilder,
                            lastDiscovery.get());
            if (duplicateTargetHealth != null) {
                //We have a duplicate target.
                return duplicateTargetHealth;
            }
        }

        //Report the failed validation.
        return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                .setSubcategory(TargetHealthSubCategory.VALIDATION)
                .setErrorType(validation.getErrorTypes().get(0))
                .setMessageText(validation.getErrors().get(0))
                .setTimeOfFirstFailure(
                        TimeUtil.localTimeToMillis(validation.getCompletionTime(), clock))
                .build();
    }

    /**
     * Prepare the TargetHealth report for the case when discovery data is not available.
     * @param targetHealthBuilder TargetHealth builder
     * @param target the checked target
     * @param successfulValidation whether we have info about a successful validation
     * @return TargetHealth object
     */
    private TargetHealth reportNoDiscoveryData(TargetHealth.Builder targetHealthBuilder,
                    Target target, boolean successfulValidation) {
        //Check if the probe has lost connection.
        if (!probeStore.isProbeConnected(target.getProbeId())) {
            return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                    .setSubcategory(successfulValidation ? TargetHealthSubCategory.DISCOVERY
                                            : TargetHealthSubCategory.VALIDATION)
                    .setErrorType(ErrorType.OTHER)
                    .setMessageText("The probe for '" + target.getDisplayName() + "' is not connected.")
                    .build();
        }

        //Check whether there's some persisting discovered data in the system that is too old.
        if (checkForDelayedData(target)) {
            return reportDelayedData(targetHealthBuilder, target.getId());
        }

        targetHealthBuilder.setSubcategory(TargetHealthSubCategory.VALIDATION);
        if (successfulValidation) {
            targetHealthBuilder.setHealthState(HealthState.NORMAL)
                .setMessageText("Validation passed. Waiting for discovery.");
        } else {
            targetHealthBuilder.setHealthState(HealthState.MINOR)
                .setMessageText("Validation pending.");
        }
        return targetHealthBuilder.build();
    }

    /**
     * Checks if the DISCOVERY subcategory is considered to be in NORMAL health state.
     * If it is then also checks for whether the discovered data is too old.
     * @param targetHealthBuilder TargetHealth builder
     * @param target the checked target
     * @param lastDiscovery the discovery info for the checked target
     * @return TargetHealth if everything is fine or if the data is too old; null if the discovery failed
     */
    @Nullable
    private TargetHealth reportWhenSuccessfulDiscovery(TargetHealth.Builder targetHealthBuilder,
                    Target target, Discovery lastDiscovery) {
        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = targetStatusTracker.getFailedDiscoveries();
        DiscoveryFailure discoveryFailure = targetToFailedDiscoveries.get(target.getId());
        if (lastDiscovery.getStatus() != Status.SUCCESS && discoveryFailure != null
                        && discoveryFailure.getFailsCount() >= failedDiscoveriesCountThreshold) {
            return null;
        }

        //The discovery was ok or the number of consecutive failures is below the threshold.
        if (checkForDelayedData(target)) {
            //The data is too old.
            return reportDelayedData(targetHealthBuilder, target.getId());
        }
        return targetHealthBuilder.setHealthState(HealthState.NORMAL)
                .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                .setConsecutiveFailureCount(discoveryFailure == null ? 0 : discoveryFailure.getFailsCount())
                .build();
    }

    /**
     * Check if the target was discovered to be a duplicate one and report a problem if it was.
     * @param targetHealthBuilder TargetHealth builder
     * @param lastDiscovery info about the last discovery of the checked target
     * @return a report on CRITICAL target health if it is a duplication or null otherwise.
     */
    @Nullable
    private TargetHealth reportTargetDuplication(TargetHealth.Builder targetHealthBuilder,
                    Discovery lastDiscovery) {
        for (ErrorType errorType : lastDiscovery.getErrorTypes()) {
            if (errorType == ErrorType.DUPLICATION) {
                //We have the case of duplicate targets.
                return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                        .setSubcategory(TargetHealthSubCategory.DUPLICATION)
                        .setErrorType(ErrorType.DUPLICATION)
                        .setMessageText("Duplicate targets.")
                        .setTimeOfFirstFailure(
                                TimeUtil.localTimeToMillis(lastDiscovery.getCompletionTime(), clock))
                        .build();
            }
        }

        //No targets duplication detected.
        return null;
    }

    private TargetHealth verifyDiscovery(TargetHealth.Builder targetHealthBuilder,
                    Target target, Discovery lastDiscovery) {
        TargetHealth targetHealth = reportWhenSuccessfulDiscovery(targetHealthBuilder,
                        target, lastDiscovery);
        if (targetHealth != null) {
            return targetHealth;
        }

        targetHealth = reportTargetDuplication(targetHealthBuilder, lastDiscovery);
        if (targetHealth != null) {
            return targetHealth;
        }

        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = targetStatusTracker.getFailedDiscoveries();
        DiscoveryFailure discoveryFailure = targetToFailedDiscoveries.get(target.getId());
        if (discoveryFailure != null) {
            //There was a discovery failure.
            return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setErrorType(discoveryFailure.getErrorType())
                    .setMessageText(discoveryFailure.getErrorText())
                    .setConsecutiveFailureCount(discoveryFailure.getFailsCount())
                    .setTimeOfFirstFailure(
                            TimeUtil.localTimeToMillis(discoveryFailure.getFailTime(), clock))
                    .build();
        } else if (!probeStore.isProbeConnected(target.getProbeId())) {
            //The probe got disconnected. Report it.
            return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setErrorType(ErrorType.OTHER)
                    .setMessageText("The probe for '" + target.getDisplayName() + "' is not connected.")
                    .build();
        } else if (checkForDelayedData(target)) {
            //The discovered data is too old, report it.
            return reportDelayedData(targetHealthBuilder, target.getId());
        } else {
            return targetHealthBuilder.setHealthState(HealthState.MINOR)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setMessageText("Discovery pending.")
                    .build();
        }
    }

    private TargetHealth reportDelayedData(TargetHealth.Builder targetHealthBuilder, long targetId) {
        Pair<Long, Long> lastSuccessfulDiscoveryTime = targetStatusTracker
                        .getLastSuccessfulDiscoveryTime(targetId);
        if (lastSuccessfulDiscoveryTime != null) {
            targetHealthBuilder.setLastSuccessfulDiscoveryStartTime(lastSuccessfulDiscoveryTime.getFirst());
        }
        Pair<Long, Long> lastSuccessfulIncrementalDiscoveryTime = targetStatusTracker
                        .getLastSuccessfulIncrementalDiscoveryTime(targetId);
        if (lastSuccessfulIncrementalDiscoveryTime != null) {
            targetHealthBuilder.setLastSuccessfulIncrementalDiscoveryStartTime(
                            lastSuccessfulIncrementalDiscoveryTime.getFirst())
                    .setLastSuccessfulIncrementalDiscoveryCompletionTime(
                            lastSuccessfulIncrementalDiscoveryTime.getSecond());
        }
        return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                .setSubcategory(TargetHealthSubCategory.DELAYED_DATA)
                .setErrorType(ErrorType.DELAYED_DATA)
                .setMessageText("The data is too old.")
                .setTimeOfCheck(System.currentTimeMillis())
                .build();
    }

    /**
     * Check if the data for the target is delayed (too old, the discovery happened too long ago
     * or ran too long itself).
     * @param target is the target whose data is checked.
     * @return true if the data is too old based on the info about discoveries.
     */
    private boolean checkForDelayedData(Target target) {
        long fullRediscoveryThreshold = delayedDataThresholdMultiplier
                        * target.getProbeInfo().getFullRediscoveryIntervalSeconds() * 1000;
        long incrementalRediscoveryThreshold = delayedDataThresholdMultiplier
                        * target.getProbeInfo().getIncrementalRediscoveryIntervalSeconds() * 1000;
        long currentInstant = System.currentTimeMillis();

        Pair<Long, Long> lastSuccessfulDiscoveryTime = targetStatusTracker
                        .getLastSuccessfulDiscoveryTime(target.getId());
        Pair<Long, Long> lastSuccessfulIncrementalDiscoveryTime = targetStatusTracker
                        .getLastSuccessfulIncrementalDiscoveryTime(target.getId());

        return checkLastRediscoveryTime(lastSuccessfulDiscoveryTime, currentInstant, fullRediscoveryThreshold)
                        || checkLastRediscoveryTime(lastSuccessfulIncrementalDiscoveryTime,
                                        currentInstant, incrementalRediscoveryThreshold);
    }

    /**
     * Check if the last discovery was too long ago or took too long.
     * @param lastSuccessfulDiscoveryTime the start and end time of the checked discovery.
     * @param currentInstant the current check time.
     * @param rediscoveryPeriodThreshold the threshold to check against.
     * @return true if the discovery was too long ago or took too long;
     *      false otherwise or if there was no successful last discovery time record.
     */
    private boolean checkLastRediscoveryTime(Pair<Long, Long> lastSuccessfulDiscoveryTime,
                    long currentInstant, long rediscoveryPeriodThreshold) {
        if (lastSuccessfulDiscoveryTime == null) {
            return false;
        }
        long lengthOfLastRediscovery = lastSuccessfulDiscoveryTime.getSecond() - lastSuccessfulDiscoveryTime.getFirst();
        return currentInstant - lastSuccessfulDiscoveryTime.getSecond() > rediscoveryPeriodThreshold
                        || lengthOfLastRediscovery > rediscoveryPeriodThreshold;
    }
}
