package com.vmturbo.topology.processor.rpc;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.StatusRuntimeException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.GetMultipleGlobalSettingsRequest;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DelayedDataErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.DuplicationErrorType;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.ErrorTypeInfoCase;
import com.vmturbo.platform.common.dto.Discovery.ErrorTypeInfo.OtherErrorType;
import com.vmturbo.platform.sdk.common.util.Pair;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.api.impl.ProbeRegistrationRESTApi.ProbeRegistrationDescription;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.scheduling.Scheduler;
import com.vmturbo.topology.processor.scheduling.TargetDiscoverySchedule;
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
    private static final Logger logger = LogManager.getLogger();

    private final IOperationManager operationManager;
    private final TargetStatusTracker targetStatusTracker;
    private final TargetStore targetStore;
    private final ProbeStore probeStore;
    private final Clock clock;
    private final SettingServiceBlockingStub settingServiceClient;
    private Scheduler scheduler;
    private Map<Long, TargetHealth> healthFromDiags;

    /**
     * Discovery timing status.
     */
    enum DiscoveryTimingStatus {
        /**
         * FULL discovery delayed.
         */
        FULL_DISCOVERY_DELAYED,

        /**
         * INCREMENTAL discovery delayed.
         */
        INCREMENTAL_DISCOVERY_DELAYED,

        /**
         * Discovery not delayed.
         */
        DISCOVERY_NOT_DELAYED
    }


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
        this.healthFromDiags = new HashMap<>();
    }

    /**
     * We use this to inject the scheduler after the instance has been created. We cannot pass in the
     * schedule during instantiation due to cyclic dependency involving the TargetHealthRetriever and
     * the Scheduler.
     *
     * @param scheduler The scheduler
     */
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
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
        Map<Long, TargetHealth> result = new HashMap<>();
        List<Target> liveTargets = new ArrayList<>();
        for (Target target: matchingTargets) {
            Long targetId = target.getId();
            //For all the restored targets, the health state will be unhealthy because of connectivity.
            //To better reproduce loaded environment, we use the restored target health from diags, if exists.
            if (healthFromDiags.containsKey(targetId)) {
                result.put(targetId, healthFromDiags.get(targetId));
            } else {
                liveTargets.add(target);
            }
        }
        if (!liveTargets.isEmpty()) {
            requestSettings();
            for (Target target: liveTargets) {
                result.put(target.getId(), computeTargetHealthInfo(target));
            }
        }
        return result;
    }

    /**
     * Set target health data from diags.
     * If exists, we'll use this data as target health, instead of computing it.
     * @param healthFromDiags The target health data to set into retriever.
     */
    public void setHealthFromDiags(@Nonnull final Map<Long, TargetHealth> healthFromDiags) {
        this.healthFromDiags = healthFromDiags;
    }

    private void requestSettings() {
        GetMultipleGlobalSettingsRequest multipleSettingsRequest = GetMultipleGlobalSettingsRequest.newBuilder()
                        .addSettingSpecName(GlobalSettingSpecs.FailedDiscoveryCountThreshold.getSettingName())
                        .addSettingSpecName(GlobalSettingSpecs.DelayedDataThresholdMultiplier.getSettingName())
                        .build();
        try {
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
        } catch (StatusRuntimeException e) {
            logger.error("Failed to query global settings for target health, assuming defaults", e);
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
                .addAllErrorTypeInfo(validation.getErrorTypeInfos())
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
        if (!probeStore.isAnyTransportConnectedForTarget(target)) {
            return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                    .setSubcategory(successfulValidation ? TargetHealthSubCategory.DISCOVERY
                            : TargetHealthSubCategory.VALIDATION)
                    .addErrorTypeInfo(ErrorTypeInfo.newBuilder().setOtherErrorType(
                            OtherErrorType.getDefaultInstance()).build())
                    .setMessageText("The probe for '" + target.getDisplayName() + "' is not connected.")
                    .build();
        }

        //Check whether there's some persisting discovered data in the system that is too old.
        DiscoveryTimingStatus discoveryTimingStatus = checkForDelayedData(target);
        if (discoveryTimingStatus != DiscoveryTimingStatus.DISCOVERY_NOT_DELAYED) {
            return reportDelayedData(targetHealthBuilder, target.getId(), discoveryTimingStatus);
        }

        if (!successfulValidation) {
            return targetHealthBuilder.setHealthState(HealthState.MINOR)
                    .setSubcategory(TargetHealthSubCategory.VALIDATION)
                    .setMessageText("Validation pending.").build();
        }

        final Pair<HealthState, String> probeStatus = getMostSevereProbeHealth(target);
        if (HealthState.NORMAL != probeStatus.getFirst()) {
            return targetHealthBuilder.setHealthState(probeStatus.getFirst())
                    .setSubcategory(TargetHealthSubCategory.VALIDATION)
                    .setMessageText(probeStatus.getSecond()).build();
        }

        return targetHealthBuilder.setHealthState(HealthState.NORMAL)
                .setSubcategory(TargetHealthSubCategory.VALIDATION)
                .setMessageText("Validation passed. Waiting for discovery.").build();
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
        DiscoveryTimingStatus discoveryTimingStatus = checkForDelayedData(target);
        if (discoveryTimingStatus != DiscoveryTimingStatus.DISCOVERY_NOT_DELAYED) {
            return reportDelayedData(targetHealthBuilder, target.getId(), discoveryTimingStatus);
        }

        final Pair<HealthState, String> probeStatus = getMostSevereProbeHealth(target);
        if (HealthState.NORMAL != probeStatus.getFirst()) {
            return targetHealthBuilder.setHealthState(probeStatus.getFirst())
                    .setSubcategory(TargetHealthSubCategory.VALIDATION)
                    .setMessageText(probeStatus.getSecond()).build();
        }

        if (discoveryFailure == null) {
            targetHealthBuilder.setConsecutiveFailureCount(0);
        } else {
            targetHealthBuilder.addAllErrorTypeInfo(discoveryFailure.getErrorTypeInfos())
                    .setConsecutiveFailureCount(discoveryFailure.getFailsCount());
        }

        Optional<com.vmturbo.platform.common.dto.Discovery.ErrorDTO> dto = lastDiscovery.getErrorsDTOList().stream()
                .filter(errorDTO -> errorDTO.getSeverity() == com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorSeverity.WARNING)
                .findFirst();

        if (dto.isPresent()) {
            return targetHealthBuilder.setHealthState(HealthState.MAJOR)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .addErrorTypeInfo(ErrorTypeInfo.newBuilder().setOtherErrorType(ErrorTypeInfo.OtherErrorType.newBuilder().build()))
                    .setMessageText("Discovery partially successful: MAJOR: " + dto.get().getDescription())
                    .setTimeOfCheck(System.currentTimeMillis())
                    .build();
        } else {
            return targetHealthBuilder.setHealthState(HealthState.NORMAL)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .build();
        }
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
        for (ErrorTypeInfo errorTypeInfo : lastDiscovery.getErrorTypeInfos()) {
            if (errorTypeInfo.getErrorTypeInfoCase() == ErrorTypeInfoCase.DUPLICATION_ERROR_TYPE) {
                //We have the case of duplicate targets.
                return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                        .setSubcategory(TargetHealthSubCategory.DUPLICATION)
                        .addErrorTypeInfo(ErrorTypeInfo.newBuilder()
                                .setDuplicationErrorType(DuplicationErrorType.getDefaultInstance())
                                .build())
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
        TargetHealth targetHealth = reportTargetDuplication(targetHealthBuilder, lastDiscovery);
        if (targetHealth != null) {
            return targetHealth;
        }

        targetHealth = reportWhenSuccessfulDiscovery(targetHealthBuilder, target, lastDiscovery);
        if (targetHealth != null) {
            return targetHealth;
        }

        Map<Long, DiscoveryFailure> targetToFailedDiscoveries = targetStatusTracker.getFailedDiscoveries();
        DiscoveryFailure discoveryFailure = targetToFailedDiscoveries.get(target.getId());
        if (discoveryFailure != null) {
            //There was a discovery failure.
            return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setMessageText(discoveryFailure.getErrorText())
                    .addAllErrorTypeInfo(discoveryFailure.getErrorTypeInfos())
                    .setConsecutiveFailureCount(discoveryFailure.getFailsCount())
                    .setTimeOfFirstFailure(
                            TimeUtil.localTimeToMillis(discoveryFailure.getFailTime(), clock))
                    .build();
        } else if (!probeStore.isAnyTransportConnectedForTarget(target)) {
            //The probe got disconnected. Report it.
            return targetHealthBuilder.setHealthState(HealthState.CRITICAL)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .addErrorTypeInfo(ErrorTypeInfo.newBuilder().setOtherErrorType(
                            OtherErrorType.getDefaultInstance()).build())
                    .setMessageText("The probe for '" + target.getDisplayName() + "' is not connected.")
                    .build();
        }

        DiscoveryTimingStatus discoveryTimingStatus = checkForDelayedData(target);
        if (discoveryTimingStatus != DiscoveryTimingStatus.DISCOVERY_NOT_DELAYED) {
            //The discovered data is too old, report it.
            return reportDelayedData(targetHealthBuilder, target.getId(), discoveryTimingStatus);
        } else {
            return targetHealthBuilder.setHealthState(HealthState.MINOR)
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setMessageText("Discovery pending.")
                    .build();
        }
    }

    private TargetHealth reportDelayedData(TargetHealth.Builder targetHealthBuilder, long targetId, DiscoveryTimingStatus discoveryTimingStatus) {
        Pair<Long, Long> lastSuccessfulDiscoveryTime = targetStatusTracker
                        .getLastSuccessfulDiscoveryTime(targetId);
        if (lastSuccessfulDiscoveryTime != null) {
            targetHealthBuilder.setLastSuccessfulDiscoveryStartTime(lastSuccessfulDiscoveryTime.getFirst());
        }
        StringBuilder delayedDataMsg = new StringBuilder("The observed data is delayed ");
        if (discoveryTimingStatus == DiscoveryTimingStatus.FULL_DISCOVERY_DELAYED) {
            delayedDataMsg.append("[Full Discovery].");
        } else {
            delayedDataMsg.append("[Incremental Discovery].");
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
                .addErrorTypeInfo(ErrorTypeInfo.newBuilder().setDelayedDataErrorType(
                        DelayedDataErrorType.getDefaultInstance()).build())
                .setMessageText(delayedDataMsg.toString())
                .setTimeOfCheck(System.currentTimeMillis())
                .build();
    }

    /**
     * Check if the data for the target is delayed (too old, the discovery happened too long ago
     * or ran too long itself).
     * @param target is the target whose data is checked.
     * @return Discovery timing status.  FULL_DISCOVERY_DELAYED if full discovery is delayed.
     * INCREMENTAL_DISCOVERY_DELAYED if incremental discovery is delayed.
     * DISCOVERY_NOT_DELAYED if last discovery is not delayed or there is no successful last discovery.
     */
    private DiscoveryTimingStatus checkForDelayedData(Target target) {
        long fullRediscoveryThreshold = delayedDataThresholdMultiplier
                        * getRediscoveryInterval(target, DiscoveryType.FULL) * 1000;
        long currentInstant = System.currentTimeMillis();

        Pair<Long, Long> lastSuccessfulDiscoveryTime = targetStatusTracker
                        .getLastSuccessfulDiscoveryTime(target.getId());
        if (checkLastRediscoveryTime(lastSuccessfulDiscoveryTime, currentInstant, fullRediscoveryThreshold)) {
            return DiscoveryTimingStatus.FULL_DISCOVERY_DELAYED;
        }
        //TODO: optimize incremental delayed data logic
        return DiscoveryTimingStatus.DISCOVERY_NOT_DELAYED;
    }

    /**
     * Get the rediscovery interval for the interval in seconds using the scheduler. If the scheduler
     * does not have this info, we resort to using the probes default value 600s.
     *
     * @param target The target in question
     * @param discoveryType FULL or INCREMENTAL
     * @return The rediscovery interval in seconds
     */
    private long getRediscoveryInterval(@Nonnull Target target, @Nonnull DiscoveryType discoveryType) {
        // Check if the target has its own discovery interval, otherwise we fall back to the default
        // probes value
        long schedulerRediscoveryInterval = schedulerRediscoveryInterval(target.getId(), discoveryType);
        if (schedulerRediscoveryInterval != -1L) {
            return schedulerRediscoveryInterval;
        }

        return discoveryType.equals(DiscoveryType.FULL)
                ? target.getProbeInfo().getFullRediscoveryIntervalSeconds()
                : target.getProbeInfo().getIncrementalRediscoveryIntervalSeconds();
    }

    /**
     * Get the rediscovery interval (seconds) defined in the scheduler, if no such entry exists,
     * return -1.
     *
     * @param targetId The id of the target
     * @param discoveryType FULL or INCREMENTAL
     * @return The rediscovery interval defined in the scheduler
     */
    private long schedulerRediscoveryInterval(long targetId, @Nonnull DiscoveryType discoveryType) {
        final Optional<TargetDiscoverySchedule> discoverySchedule =
                this.scheduler.getDiscoverySchedule(targetId, discoveryType);

        return discoverySchedule.map(
                targetDiscoverySchedule ->
                        targetDiscoverySchedule.getScheduleInterval(TimeUnit.SECONDS))
                .orElse(-1L);
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

    /**
     * Fetch all probe registrations associated with the given target and select the one with the
     * most severe health state and return the health state and the status string.
     *
     * <p/> If there are no probe registrations, then a critical health state and the associated
     * message will be returned.
     *
     * @param target the target which associated probe registrations to be examined
     * @return a pair of health state and the status string
     */
    private Pair<HealthState, String> getMostSevereProbeHealth(final Target target) {
        final Collection<ProbeRegistrationDescription> probeRegistrations =
                probeStore.getProbeRegistrationsForTarget(target);
        if (probeRegistrations.isEmpty()) {
            return Pair.create(HealthState.CRITICAL, ProbeStore.NO_TRANSPORTS_MESSAGE);
        }
        final ProbeRegistrationDescription probeRegistration = probeRegistrations.stream()
                .reduce(new ProbeRegistrationDescription(), (r1, r2) -> r1.getHealthState().compareTo(r2.getHealthState()) <= 0 ? r1 : r2);
        return Pair.create(probeRegistration.getHealthState(), probeRegistration.getStatus());
    }
}
