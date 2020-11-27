package com.vmturbo.stitching;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.lang3.StringUtils;

import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PerTargetEntityInformation.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.DiscoveryOrigin;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityOrigin;

/**
 * Builder for information describing when the entity was last updated and by which target(s)
 * this entity was discovered.
 */
public class DiscoveryOriginBuilder {
    private final long targetId;
    private Stream<StitchingMergeInformation> mergeFromTargets;
    private final String vendorId;
    private final EntityOrigin origin;

    /**
     * Builder for information describing when the entity was last updated and by which target(s)
     * this entity was discovered.
     *
     * @param targetId The id of the target that originally discovered the entity.
     * @param vendorId external identity as seen by the target
     * @param origin the origin type.
     */
    private DiscoveryOriginBuilder(final long targetId, @Nullable String vendorId,
            @Nonnull EntityOrigin origin) {
        this.targetId = targetId;
        this.mergeFromTargets = Stream.empty();
        this.vendorId = vendorId;
        this.origin = origin;
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

        mergeFromTargets.forEach(smi -> addPerTargetInformation(builder, smi.getTargetId(), smi.getVendorId(), smi.getOrigin()));
        addPerTargetInformation(builder, targetId, vendorId, origin);

        return builder.build();
    }

    private static void addPerTargetInformation(@Nonnull DiscoveryOrigin.Builder builder,
            long targetId, @Nullable String vendorId, @Nonnull EntityOrigin origin) {
        final Builder infoBuilder = PerTargetEntityInformation.newBuilder();
        if (!StringUtils.isEmpty(vendorId)) {
            infoBuilder.setVendorId(vendorId);
        }
        infoBuilder.setOrigin(origin);
        builder.putDiscoveredTargetData(targetId, infoBuilder.build());
    }

    /**
     * Add the information about target discovering this entity.
     *
     * @param targetId target identifier
     * @param localName vendor id
     * @param origin the origin type.
     * @return this for chaining
     */
    public static DiscoveryOriginBuilder discoveredBy(final long targetId, String localName, EntityOrigin origin) {
        return new DiscoveryOriginBuilder(targetId, localName, origin);
    }

    /**
     * Add the information about target discovering this entity.
     * With unset local name for the target.
     *
     * @param targetId target identifier
     * @return this for chaining
     */
    public static DiscoveryOriginBuilder discoveredBy(final long targetId) {
        return discoveredBy(targetId, null, EntityOrigin.DISCOVERED);
    }
}
