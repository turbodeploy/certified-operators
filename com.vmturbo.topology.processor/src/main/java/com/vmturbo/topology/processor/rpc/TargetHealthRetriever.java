package com.vmturbo.topology.processor.rpc;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealth;
import com.vmturbo.common.protobuf.target.TargetDTO.TargetHealthSubCategory;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.operation.IOperationManager;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.status.TargetStatusTracker;
import com.vmturbo.topology.processor.targets.status.TargetStatusTrackerImpl.DiscoveryFailure;

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
    private final Clock clock;

    TargetHealthRetriever(@Nonnull final IOperationManager operationManager,
            @Nonnull final TargetStatusTracker targetStatusTracker,
            @Nonnull final TargetStore targetStore, @Nonnull final Clock clock) {
        this.operationManager = operationManager;
        this.targetStatusTracker = targetStatusTracker;
        this.targetStore = targetStore;
        this.clock = clock;
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

        return matchingTargets.stream()
            .collect(Collectors.toMap(Target::getId, this::targetToTargetHealthInfo));
    }

    @Nonnull
    private TargetHealth targetToTargetHealthInfo(@Nonnull final Target target) {
        long targetId = target.getId();
        String targetName = target.getDisplayName();

        Optional<Validation> lastValidation = operationManager.getLastValidationForTarget(targetId);
        Optional<Discovery> lastDiscovery = operationManager.getLastDiscoveryForTarget(targetId,
                DiscoveryType.FULL);

        //Check if we have info about validation.
        if (!lastValidation.isPresent()) {
            if (!lastDiscovery.isPresent()) {
                return TargetHealth.newBuilder()
                        .setSubcategory(TargetHealthSubCategory.VALIDATION)
                        .setTargetName(targetName)
                        .setErrorText("Validation pending.")
                        .build();
            } else {
                return verifyDiscovery(targetId, targetName, lastDiscovery.get());
            }
        }

        Validation validation = lastValidation.get();
        //Check if the validation has passed fine.
        if (validation.getStatus() == Status.SUCCESS) {
            if (!lastDiscovery.isPresent()) {
                return TargetHealth.newBuilder()
                        .setSubcategory(TargetHealthSubCategory.VALIDATION)
                        .setTargetName(targetName)
                        .build();
            } else {
                return verifyDiscovery(targetId, targetName, lastDiscovery.get());
            }
        }

        //Validation was not Ok, but check the last discovery.
        if (lastDiscovery.isPresent()) {
            LocalDateTime validationCompletionTime = validation.getCompletionTime();
            LocalDateTime discoveryCompletionTime = lastDiscovery.get().getCompletionTime();

            //Check if there's a discovery that has happened later and passed fine.
            if (discoveryCompletionTime.compareTo(validationCompletionTime) >= 0
                    && lastDiscovery.get().getStatus() == Status.SUCCESS) {
                //All is good!
                return TargetHealth.newBuilder()
                        .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                        .setTargetName(targetName)
                        .build();
            }

            if (checkTargetDuplication(lastDiscovery.get())) {
                //We have the case of duplicate targets.
                return TargetHealth.newBuilder()
                        .setSubcategory(TargetHealthSubCategory.DUPLICATION)
                        .setErrorType(ErrorType.DUPLICATION)
                        .setErrorText("Duplicate targets.")
                        .setConsecutiveFailureCount(1)
                        .setTimeOfFirstFailure(
                                TimeUtil.localTimeToMillis(discoveryCompletionTime, clock))
                        .setTargetName(targetName)
                        .build();
            }
        }

        //Report the failed validation.
        return TargetHealth.newBuilder()
                .setSubcategory(TargetHealthSubCategory.VALIDATION)
                .setTargetName(targetName)
                .setErrorType(validation.getErrorTypes().get(0))
                .setErrorText(validation.getErrors().get(0))
                .setConsecutiveFailureCount(1)
                .setTimeOfFirstFailure(
                        TimeUtil.localTimeToMillis(validation.getCompletionTime(), clock))
                .build();
    }

    private boolean checkTargetDuplication(Discovery lastDiscovery) {
        for (ErrorType errorType : lastDiscovery.getErrorTypes()) {
            if (errorType == ErrorType.DUPLICATION) {
                return true;
            }
        }
        return false;
    }

    private TargetHealth verifyDiscovery(long targetId, String targetName,
            Discovery lastDiscovery) {
        if (lastDiscovery.getStatus() == Status.SUCCESS) {
            //The discovery was ok.
            return TargetHealth.newBuilder()
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setTargetName(targetName)
                    .build();
        }

        if (checkTargetDuplication(lastDiscovery)) {
            //We have the case of duplicate targets.
            return TargetHealth.newBuilder()
                    .setSubcategory(TargetHealthSubCategory.DUPLICATION)
                    .setErrorType(ErrorType.DUPLICATION)
                    .setErrorText("Duplicate targets.")
                    .setConsecutiveFailureCount(1)
                    .setTimeOfFirstFailure(
                            TimeUtil.localTimeToMillis(lastDiscovery.getCompletionTime(), clock))
                    .setTargetName(targetName)
                    .build();
        }

        Map<Long, DiscoveryFailure> targetToFailedDiscoveries =
                targetStatusTracker.getFailedDiscoveries();
        DiscoveryFailure discoveryFailure = targetToFailedDiscoveries.get(targetId);
        if (discoveryFailure != null) {
            //There was a discovery failure.
            return TargetHealth.newBuilder()
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setTargetName(targetName)
                    .setErrorType(discoveryFailure.getErrorType())
                    .setErrorText(discoveryFailure.getErrorText())
                    .setConsecutiveFailureCount(discoveryFailure.getFailsCount())
                    .setTimeOfFirstFailure(
                            TimeUtil.localTimeToMillis(discoveryFailure.getFailTime(), clock))
                    .build();
        } else {
            //The last discovery was probably attempted while there was no probe registered for it
            //(e.g. after the topology-processor restart).
            return TargetHealth.newBuilder()
                    .setSubcategory(TargetHealthSubCategory.DISCOVERY)
                    .setTargetName(targetName)
                    .setErrorText(
                            "No finished discovery. May be because of an unregistered probe during the last attempt.")
                    .build();
        }
    }
}
