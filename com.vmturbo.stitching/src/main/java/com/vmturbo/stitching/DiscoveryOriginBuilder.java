package com.vmturbo.stitching;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;

/**
 * Builder for information describing when the entity was last updated and by which target(s)
 * this entity was discovered.
 */
public class DiscoveryOriginBuilder {
    private final long targetId;
    private Stream<StitchingMergeInformation> mergeFromTargets;
    private String vendorId;

    /**
     * Builder for information describing when the entity was last updated and by which target(s)
     * this entity was discovered.
     *
     * @param targetId The id of the target that originally discovered the entity.
     * @param vendorId external identity as seen by the target
     */
    private DiscoveryOriginBuilder(final long targetId, @Nullable String vendorId) {
        this.targetId = targetId;
        this.mergeFromTargets = Stream.empty();
        this.vendorId = vendorId;
    }

    /**
     * Set the targets information for targets that discovered entities that were merged onto
     * this entity during stitching.
     *
     * @param mergeFromTargets targets being merged
     * @return a reference to {@link this} for method chaining.
     */
    public DiscoveryOriginBuilder withMerge(@Nonnull final Stream<StitchingMergeInformation> mergeFromTargets) {
        this.mergeFromTargets = Objects.requireNonNull(mergeFromTargets);
        return this;
    }

    /**
     * Set the targets information for targets that discovered entities that were merged onto
     * this entity during stitching.
     *
     * @param mergeFromTargets targets being merged
     * @return a reference to {@link this} for method chaining.
     */
    public DiscoveryOriginBuilder withMerge(@Nonnull final List<StitchingMergeInformation> mergeFromTargets) {
        return withMerge(mergeFromTargets.stream());
    }

    /**
     * Set the targets information for targets that discovered entities that were merged onto
     * this entity during stitching.
     *
     * @param mergeFromTargets targets being merged
     * @return a reference to {@link this} for method chaining.
     */
    public DiscoveryOriginBuilder withMerge(@Nonnull final StitchingMergeInformation... mergeFromTargets) {
        return withMerge(Arrays.asList(mergeFromTargets));
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

        mergeFromTargets.forEach(smi -> addPerTargetInformation(builder, smi.getTargetId(), smi.getVendorId()));
        addPerTargetInformation(builder, targetId, vendorId);

        return builder.build();
    }

    private static void addPerTargetInformation(DiscoveryOrigin.Builder builder, long targetId, String vendorId) {
        PerTargetEntityInformation info = StringUtils.isEmpty(vendorId)
                        ? PerTargetEntityInformation.getDefaultInstance()
                        : PerTargetEntityInformation.newBuilder().setVendorId(vendorId).build();
        builder.putDiscoveredTargetData(targetId, info);
    }

    /**
     * Add the information about target discovering this entity.
     *
     * @param targetId target identifier
     * @param localName vendor id
     * @return this for chaining
     */
    public static DiscoveryOriginBuilder discoveredBy(final long targetId, String localName) {
        return new DiscoveryOriginBuilder(targetId, localName);
    }

    /**
     * Add the information about target discovering this entity.
     * With unset local name for the target.
     *
     * @param targetId target identifier
     * @return this for chaining
     */
    public static DiscoveryOriginBuilder discoveredBy(final long targetId) {
        return discoveredBy(targetId, null);
    }
}
