package com.vmturbo.stitching;

import java.util.Optional;

/**
 * Information describing when the entity was last updated and by which target(s) this entity was discovered.
 */
public class DiscoveryInformation {
    private final long targetId;
    private final long lastUpdatedTime;

    private DiscoveryInformation(final long targetId,
                                 final long lastUpdatedTime) {
        this.targetId = targetId;
        this.lastUpdatedTime = lastUpdatedTime;
    }

    /**
     * Get the time that the data for this entity was last updated.
     * If the entity was discovered by multiple targets, this time is the time at which the most recent update
     * across all those targets provided new information for this entity.
     *
     * Important note: This is the time that TopologyProcessor received this data from the probe, not the actual
     * time that the probe retrieved the information from the target.
     *
     * This field may be used as a heuristic for the recency of the data in the absence of better information.
     * The time is in "computer time" and not necessarily UTC, however, times on {@link TopologyEntity}s
     * are comparable. See {@link System#currentTimeMillis()} for further details.
     *
     * @return The time that the data for this entity was last updated.
     */
    public long getLastUpdatedTime() {
        return lastUpdatedTime;
    }

    /**
     * Get the id of the target that discovered this entity. If the entity was never discovered (ie it is the
     * product of a clone or replace-by-template operation to add demand for a plan), it will return
     * {@link Optional#empty()}.
     *
     * TODO: Support for multiple targets discovering the entity.
     *
     * @return The ID of the target that discovered this entity.
     */
    public long getTargetId() {
        return targetId;
    }

    /**
     * A builder for {@link DiscoveryInformation} that contains only
     * the targetId indicating which target discovered an entity.
     */
    public static class DiscoveredByInformationBuilder {
        private final long targetId;

        private DiscoveredByInformationBuilder(final long targetId) {
            this.targetId = targetId;
        }

        public DiscoveryInformation lastUpdatedAt(final long lastUpdateTime) {
            return new DiscoveryInformation(targetId, lastUpdateTime);
        }
    }

    public static DiscoveredByInformationBuilder discoveredBy(final long targetId) {
        return new DiscoveredByInformationBuilder(targetId);
    }
}
