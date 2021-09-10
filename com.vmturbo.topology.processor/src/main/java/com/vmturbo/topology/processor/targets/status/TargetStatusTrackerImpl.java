package com.vmturbo.topology.processor.targets.status;

import java.time.Clock;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.FormattedString;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.utils.TimeUtil;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.operation.Operation;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.operation.validation.Validation;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStatusOuterClass.TargetStatus;
import com.vmturbo.topology.processor.targets.TargetStore;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

/**
 * Class responsible for tracking statuses of the targets.
 */
public class TargetStatusTrackerImpl implements TargetStatusTracker, TargetStoreListener {
    private static final Logger LOGGER = LogManager.getLogger();
    private final Map<Long, DiscoveryFailure> targetToFailedDiscoveries = Collections.synchronizedMap(new HashMap<>());
    private final Map<Long, TargetStatus> targetStatusCache;
    private final Map<Long, Long> lastSuccessfulDiscoveryTimeByTargetId = new HashMap<>();
    private final TargetStore targetStore;
    private final ProbeStore probeStore;
    private final Clock clock;

    /**
     * Constructor.
     *
     * @param targetStore the target store
     * @param probeStore the probe store
     * @param clock to interpret discovery/validation completion times.
     */
    public TargetStatusTrackerImpl(@Nonnull TargetStore targetStore, @Nonnull ProbeStore probeStore,
            @Nonnull final Clock clock) {
        this.targetStore = Objects.requireNonNull(targetStore);
        this.probeStore = Objects.requireNonNull(probeStore);
        this.clock = clock;
        targetStatusCache = Collections.synchronizedMap(new HashMap<>());
    }

    @Nonnull
    @Override
    public Map<Long, TargetStatus> getTargetsStatuses(@Nonnull final Set<Long> targetIds, final boolean returnAll) {
        if (returnAll && targetIds.isEmpty()) {
            return Collections.unmodifiableMap(targetStatusCache);
        } else {
            return targetIds.stream()
                    .map(targetStatusCache::get)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toMap(TargetStatus::getTargetId, Function.identity()));
        }
    }

    @Override
    public void onTargetRemoved(@Nonnull final Target target) {
        final long removedTargetId = target.getId();
        targetStatusCache.remove(removedTargetId);
        removeFailedDiscovery(removedTargetId);
        // do not report last successful discovery time on targets that no longer exist
        lastSuccessfulDiscoveryTimeByTargetId.remove(removedTargetId);
    }


    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender)
            throws DiagnosticsException {
        appender.appendString("==== Target Status ====");
        Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
        synchronized (targetStatusCache) {
            targetStatusCache.forEach((targetId, targetStatus) -> {
                try {
                    appender.appendString(printer.print(targetStatus));
                } catch (DiagnosticsException | InvalidProtocolBufferException e) {
                    LOGGER.error("Failed to add target status for target {} to diags.", targetId, e);
                }
            });
        }

        appender.appendString("==== Discovery Failures ====");
        synchronized (targetToFailedDiscoveries) {
            targetToFailedDiscoveries.forEach((targetId, failedDiscovery) -> {
                try {
                    appender.appendString(FormattedString.format("Target {} - {}", targetId, failedDiscovery));
                } catch (DiagnosticsException e) {
                    LOGGER.error("Failed to add discovery failure for target {} to diags.", targetId, e);
                }
            });
        }
    }

    @Nonnull
    @Override
    public String getFileName() {
        return "TargetsStatuses";
    }

    @Override
    public void notifyOperationState(@Nonnull final Operation operation) {
        final Status operationStatus = operation.getStatus();
        if (operation.getClass() == Discovery.class && operationStatus != Status.IN_PROGRESS) {
            final Discovery discovery = (Discovery)operation;
            if (discovery.getDiscoveryType() == DiscoveryType.FULL) {
                setTargetStatusInternal(discovery.getProbeId(), TargetStatus.newBuilder()
                        .setTargetId(discovery.getTargetId())
                        .addAllStageDetails(discovery.getStagesReports())
                        .setOperationCompletionTime(TimeUtil.localTimeToMillis(discovery.getCompletionTime(), clock))
                        .build());

                if (discovery.getStatus() == Status.FAILED && isOperationActual(discovery.getProbeId(), discovery.getTargetId())) {
                    storeFailedDiscovery(discovery.getTargetId(), discovery);
                } else if (discovery.getStatus() == Status.SUCCESS) {
                    removeFailedDiscovery(discovery.getTargetId());
                    lastSuccessfulDiscoveryTimeByTargetId.put(discovery.getTargetId(),
                            TimeUtil.localTimeToMillis(discovery.getCompletionTime(), clock));
                }
            }
        } else if (operation.getClass() == Validation.class
                && operationStatus != Status.IN_PROGRESS) {
            final Validation validation = (Validation)operation;
            setTargetStatusInternal(validation.getProbeId(), TargetStatus.newBuilder()
                    .setTargetId(validation.getTargetId())
                    .addAllStageDetails(validation.getStagesReports())
                    .setOperationCompletionTime(TimeUtil.localTimeToMillis(validation.getCompletionTime(), clock))
                    .build());
        }
    }

    @Override
    public void notifyOperationsCleared() {
        targetToFailedDiscoveries.clear();
    }

    /**
     * Gets failed discoveries.
     *
     * @return Map of target Id to failed discoveries info
     */
    @Nonnull
    public Map<Long, DiscoveryFailure> getFailedDiscoveries() {
        return Collections.unmodifiableMap(targetToFailedDiscoveries);
    }

    /**
     * Returns last successful discovery time of target.
     *
     * @param targetId id of target.
     * @return time of last successful discovery.
     */
    @Nullable
    @Override
    public Long getLastSuccessfulDiscoveryTime(long targetId) {
        return lastSuccessfulDiscoveryTimeByTargetId.get(targetId);
    }

    private void setTargetStatusInternal(final long probeId, @Nonnull TargetStatus targetStatus) {
        if (targetStatus.getStageDetailsList().isEmpty()) {
            LOGGER.debug("Not persisting target status for target {} because there are no details.",
                    targetStatus.getTargetId());
            return;
        }

        final long targetId = targetStatus.getTargetId();
        if (isOperationActual(probeId, targetId)) {
            targetStatusCache.put(targetId, targetStatus);
        } else {
            LOGGER.warn("Status of the {} target wasn't updated, because the target was deleted or the probe is not connected.",
                    targetId);
        }
    }

    private boolean isOperationActual(final long probeId, final long targetId) {
        return targetStore.getTarget(targetId)
                .map(target -> probeStore.isAnyTransportConnectedForTarget(target))
                .orElse(false);
    }

    /**
     * Store first failed discovery and fails count.
     *
     * @param targetId target Id
     * @param discovery failed discovery to store
     */
    private void storeFailedDiscovery(final long targetId, final Discovery discovery) {
        final DiscoveryFailure discoveryFailure = targetToFailedDiscoveries
                .computeIfAbsent(targetId, k -> new DiscoveryFailure(discovery));
        discoveryFailure.incrementFailsCount();
        LOGGER.debug("Target {} discovery was failed in {} with {} fails", targetId,
                discoveryFailure.getFailTime(), discoveryFailure.getFailsCount());
    }

    /**
     * Remove failed discovery.
     *
     * @param targetId target Id to remove errors.
     */
    private void removeFailedDiscovery(final long targetId) {
        targetToFailedDiscoveries.remove(targetId);
        LOGGER.debug("Failed discovery information was removed for target {}", targetId);
    }

    /**
     * Discovery failure information.
     */
    public static class DiscoveryFailure {
        private final LocalDateTime failTime;
        private int failsCount;
        private ErrorType firstFailedDiscoveryErrorType;
        private String firstFailedDiscoveryErrorText;

        /**
         * Creates {@link DiscoveryFailure} instance.
         * @param discovery for which the failure record is created
         */
        public DiscoveryFailure(Discovery discovery) {
            failTime = discovery.getCompletionTime();
            failsCount = 0;
            List<ErrorType> errorTypes = discovery.getErrorTypes();
            if (!errorTypes.isEmpty())  {
                firstFailedDiscoveryErrorType = errorTypes.get(0);
            } else {
                LOGGER.warn("Had a discovery failure for target with id {} but "
                        + "the error type is not set", discovery.getTargetId());
            }
            List<String> errorTexts = discovery.getErrors();
            if (!errorTexts.isEmpty())    {
                firstFailedDiscoveryErrorText = errorTexts.get(0);
            } else {
                LOGGER.warn("Have had a discovery failure for target with id {} "
                        + "but the error text is not set", discovery.getTargetId());
            }
        }

        /**
         * Increment fails count.
         */
        public void incrementFailsCount() {
            this.failsCount += 1;
        }

        /**
         * Getter for failTime.
         *
         * @return values of the failTime
         */
        public LocalDateTime getFailTime() {
            return failTime;
        }

        /**
         * Getter for failsCount.
         *
         * @return values of the failsCount
         */
        public int getFailsCount() {
            return failsCount;
        }

        /**
         * Get first failed discovery error type.
         * @return {@link ErrorType} object
         */
        public ErrorType getErrorType() {
            return firstFailedDiscoveryErrorType;
        }

        /**
         * Get first failed discovery error message.
         * @return error text
         */
        public String getErrorText()    {
            return firstFailedDiscoveryErrorText;
        }

        @Override
        public String toString() {
            return FormattedString.format("Failed {} consecutive times starting at {}. First failure: {} - {}",
                    failsCount, failTime, firstFailedDiscoveryErrorType, firstFailedDiscoveryErrorText);
        }
    }

}
