package com.vmturbo.stitching;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;

/**
 * Builder for information describing when the entity was last updated and by which target(s)
 * this entity was discovered.
 */
public class DiscoveryOriginBuilder {
    private final long targetId;
    private Stream<Long> mergeFromTargetIds;

    /**
     * Builder for information describing when the entity was last updated and by which target(s)
     * this entity was discovered.
     *
     * @param targetId The id of the target that originally discovered the entity.
     */
    private DiscoveryOriginBuilder(final long targetId) {
        this.targetId = targetId;
        this.mergeFromTargetIds = Stream.empty();
    }

    /**
     * Set the targetIds for targets that discovered entities that were merged onto
     * this entity during stitching.
     *
     * @return a reference to {@link this} for method chaining.
     */
    public DiscoveryOriginBuilder withMergeFromTargetIds(@Nonnull final Stream<Long> mergeFromTargetIds) {
        this.mergeFromTargetIds = Objects.requireNonNull(mergeFromTargetIds);

        return this;
    }

    /**
     * Set the targetIds for targets that discovered entities that were merged onto
     * this entity during stitching.
     *
     * @return a reference to {@link this} for method chaining.
     */
    public DiscoveryOriginBuilder withMergeFromTargetIds(@Nonnull final List<Long> mergeFromTargetIds) {
        return withMergeFromTargetIds(mergeFromTargetIds.stream());
    }

    /**
     * Set the targetIds for targets that discovered entities that were merged onto
     * this entity during stitching.
     *
     * @return a reference to {@link this} for method chaining.
     */
    public DiscoveryOriginBuilder withMergeFromTargetIds(
        @Nonnull final Long... mergeFromTargetIds) {
        return withMergeFromTargetIds(Arrays.asList(mergeFromTargetIds));
    }

    /**
     * Set the time that the data for this entity was last updated and build the {@Link DiscoveryOrigin}.
     *
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
     * @return The {@Link DiscoveryOrigin} information.
     */
    public DiscoveryOrigin lastUpdatedAt(final long lastUpdateTime) {
        final DiscoveryOrigin.Builder builder = DiscoveryOrigin.newBuilder()
            .setLastUpdatedTime(lastUpdateTime);

        Stream.concat(Stream.of(targetId), mergeFromTargetIds)
            .distinct()
            .forEach(builder::addDiscoveringTargetIds);

        return builder.build();
    }

    public static DiscoveryOriginBuilder discoveredBy(final long targetId) {
        return new DiscoveryOriginBuilder(targetId);
    }
}
