package com.vmturbo.topology.processor.operation;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.platform.common.dto.Discovery.ErrorDTO.ErrorType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.probes.ProbeStore;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

/**
 *  Failed target discoveries tracker.
 */
public class FailedDiscoveryTracker implements OperationListener, TargetStoreListener {

    private Map<Long, DiscoveryFailure> targetToFailedDiscoveries = new HashMap<>();

    private ProbeStore probeStore;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Failed discovery tracker constructor.
     * @param probeStore a probe store
     */
    public FailedDiscoveryTracker(@Nonnull ProbeStore probeStore) {
        this.probeStore = probeStore;
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
        logger.debug("Target {} discovery was failed in {} with {} fails", targetId,
                     discoveryFailure.getFailTime(), discoveryFailure.getFailsCount());
    }

    /**
     * Remove failed discovery.
     *
     * @param targetId target Id to remove errors.
     */
    private void removeFailedDiscovery(final long targetId) {
        targetToFailedDiscoveries.remove(targetId);
        logger.debug("Failed discovery information was removed for target {}", targetId);
    }

    /**
     * Gets failed discoveries.
     *
     * @return Map of target Id to failed discoveries info
     */
    @Nonnull
    public Map<Long, DiscoveryFailure> getFailedDiscoveries() {
        final Map<Long, DiscoveryFailure> result = new HashMap<>();
        result.putAll(targetToFailedDiscoveries);
        return Collections.unmodifiableMap(result);
    }

    @Override
    public void notifyOperationState(Operation operation) {
        if (operation.getClass() == Discovery.class
                        && operation.getStatus() != Status.IN_PROGRESS) {
            final Discovery discovery = (Discovery)operation;
            if (discovery.getDiscoveryType() == DiscoveryType.FULL) {
                final long targetId = discovery.getTargetId();
                if (discovery.getStatus() == Status.FAILED
                            && probeStore.isProbeConnected(discovery.getProbeId())) {
                    storeFailedDiscovery(targetId, discovery);
                } else if (discovery.getStatus() == Status.SUCCESS) {
                    removeFailedDiscovery(targetId);
                }
            }
        }
    }

    @Override
    public void notifyOperationsCleared() {
        targetToFailedDiscoveries.clear();
    }

    @Override
    public void onTargetRemoved(@Nonnull final Target target) {
        final long targetId = target.getId();
        removeFailedDiscovery(targetId);
    }

    /**
     * Discovery failure information.
     */
    public static class DiscoveryFailure {
        private LocalDateTime failTime;
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
                logger.warn("Had a discovery failure for target with id {} but "
                                + "the error type is not set", discovery.getTargetId());
            }
            List<String> errorTexts = discovery.getErrors();
            if (!errorTexts.isEmpty())    {
                firstFailedDiscoveryErrorText = errorTexts.get(0);
            } else {
                logger.warn("Have had a discovery failure for target with id {} "
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
    }
}
