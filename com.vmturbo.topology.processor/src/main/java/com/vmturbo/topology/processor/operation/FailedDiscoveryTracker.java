package com.vmturbo.topology.processor.operation;

import java.time.LocalDateTime;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryType;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.OperationStatus.Status;
import com.vmturbo.topology.processor.operation.discovery.Discovery;
import com.vmturbo.topology.processor.targets.Target;
import com.vmturbo.topology.processor.targets.TargetStoreListener;

/**
 *  Failed target discoveries tracker.
 */
public class FailedDiscoveryTracker implements OperationListener, TargetStoreListener {

    private Map<Long, DiscoveryFailure> targetToFailedDiscoveries = new HashMap<>();

    private static final Logger logger = LogManager.getLogger();

    /**
     * Store first failed discovery and fails count.
     *
     * @param targetId target Id
     * @param discovery failed discovery to store
     */
    private void storeFailedDiscovery(final long targetId, final Discovery discovery) {
        final DiscoveryFailure discoveryFailure = targetToFailedDiscoveries
                        .computeIfAbsent(targetId, k -> new DiscoveryFailure(discovery
                                        .getCompletionTime()));
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
                if (discovery.getStatus() == Status.FAILED) {
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

        /**
         * Creates {@link DiscoveryFailure} instance.
         *
         * @param failTime time of ferst fail
         */
        DiscoveryFailure(LocalDateTime failTime) {
            this.failTime = failTime;
            this.failsCount = 0;
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
    }

}
