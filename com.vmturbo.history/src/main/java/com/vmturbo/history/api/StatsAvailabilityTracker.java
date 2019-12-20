package com.vmturbo.history.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;

/**
 * Tracks the availability of statistics in history for topology contexts.
 *
 * When stats for a context become available, the tracker forgets the information for that context.
 *
 * TODO: (DavidBlinn 5/31/17 - Support handling of concurrent live topologies - that is, handle the case
 * when a second live topology comes before the first one is fully available).
 */
@ThreadSafe
public class StatsAvailabilityTracker {
    private final Logger logger = LogManager.getLogger();

    @GuardedBy("statsAvailabilityLock")
    private final Map<Long, AvailabilityInfo> statsAvailabilityMap = new HashMap<>();

    private final Object statsAvailabilityLock = new Object();

    /**
     * The possible topology context types.
     */
    public enum TopologyContextType {
        /**
         * A live topology context. Associated with the live operation of the system.
         */
        LIVE,

        /**
         * A plan topology context. Associated with a user or system-initiated plan analysis.
         */
        PLAN
    }

    /**
     * Describes if statistics are available for a particular topology context.
     * Note that for a plan, the plan identifier and topology context are the same.
     */
    public enum StatsAvailabilityStatus {
        /**
         * Statistics are available.
         */
        UNAVAILABLE,

        /**
         * Statistics are unavailable.
         */
        AVAILABLE
    }

    private final HistoryNotificationSender notificationSender;

    public StatsAvailabilityTracker(@Nonnull final HistoryNotificationSender notificationSender) {
        this.notificationSender = Objects.requireNonNull(notificationSender);
    }

    public StatsAvailabilityStatus topologyAvailable(final long topologyContextId,
                                                     TopologyContextType contextType,
                                                     boolean isAvailable)
            throws CommunicationException, InterruptedException {
        return statsPartAvailable(topologyContextId, contextType,
                ai -> ai.markTopologyAvailable(isAvailable));
    }

    public StatsAvailabilityStatus projectedTopologyAvailable(final long topologyContextId,
                                                              TopologyContextType contextType,
                                                              boolean isAvailable)
            throws CommunicationException, InterruptedException {
        return statsPartAvailable(topologyContextId, contextType,
                ai -> ai.markProjectedTopologyAvailable(isAvailable));
    }

    public boolean isTracking(final long topologyContextId) {
        synchronized (statsAvailabilityLock) {
            return statsAvailabilityMap.containsKey(topologyContextId);
        }
    }

    private StatsAvailabilityStatus statsPartAvailable(final long topologyContextId,
                                                       final TopologyContextType contextType,
                                                       @Nonnull final Consumer<AvailabilityInfo>
                                                               availabilityInfoConsumer) {
        Objects.requireNonNull(availabilityInfoConsumer);
        boolean allAvailable;
        boolean isUnavailable;

        synchronized (statsAvailabilityLock) {
            AvailabilityInfo availabilityInfo = statsAvailabilityMap.get(topologyContextId);
            if (availabilityInfo == null) {
                availabilityInfo = new AvailabilityInfo(contextType);
                statsAvailabilityMap.put(topologyContextId, availabilityInfo);
            } else if (availabilityInfo.getContextType() != contextType) {
                logger.error("Mismatched context type for context {}. Existing value: {}, new value: {}",
                    topologyContextId, availabilityInfo.getContextType(), contextType);
            }
            availabilityInfoConsumer.accept(availabilityInfo);
            allAvailable = availabilityInfo.areAllAvailable();
            isUnavailable = availabilityInfo.anyUnavailable();
            if (availabilityInfo.allAvailabilityKnown()) {
                statsAvailabilityMap.remove(topologyContextId);
            }
        }

        if (allAvailable) {
            notificationSender.statsAvailable(topologyContextId);
        } else if (isUnavailable) {
            notificationSender.statsFailure(topologyContextId);
        }

        return allAvailable ? StatsAvailabilityStatus.AVAILABLE : StatsAvailabilityStatus.UNAVAILABLE;
    }

    /**
     * Utility class to track availability of all components of plan stats.
     * <p>
     * Plan statistics are considered available when the history component has
     * received, processed, and recorded the price index, original topology, and
     * projected topology.
     */
    private static class AvailabilityInfo {
        private Boolean topology = null;
        private Boolean projectedTopology = null;
        private TopologyContextType contextType;

        private AvailabilityInfo(final TopologyContextType contextType) {
            this.contextType = contextType;
        }

        void markTopologyAvailable(boolean isAvailable) {
            topology = isAvailable;
        }

        void markProjectedTopologyAvailable(boolean isAvailable) {
            projectedTopology = isAvailable;
        }

        boolean areAllAvailable() {
            return topology != null && topology && projectedTopology != null && projectedTopology;
        }

        boolean anyUnavailable() {
            return (topology != null && !topology) ||
                    (projectedTopology != null && !projectedTopology);
        }

        boolean allAvailabilityKnown() {
            return topology != null && projectedTopology != null;
        }

        public TopologyContextType getContextType() {
            return contextType;
        }
    }
}
