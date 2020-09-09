package com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.selection;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.demand.ScopedCloudTierDemand;
import com.vmturbo.cloud.commitment.analysis.demand.TimeInterval;
import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.classification.DemandClassification;

/**
 * A data class representing a single (entity, cloud tier) tuple. Each instance of this class
 *  will be indicate through {@link #isRecommendationCandidate()} whether the contained demand
 *  can be considered in justifying a purchase recommendation.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface ClassifiedEntitySelection extends ScopedCloudTierDemand {

    /**
     * The entity OID.
     * @return The entity OID.
     */
    long entityOid();

    /**
     * The demand classification.
     * @return The demand classification.
     */
    @Nonnull
    DemandClassification classification();

    /**
     * The demand timeline for the {@link #cloudTierDemand()} for this entity.
     * @return The demand timeline.
     */
    @Nonnull
    TimeSeries<TimeInterval> demandTimeline();

    /**
     * Whether this (entity, classification) tuple can be considered in justifying a purchase recommendation.
     * If false, this demand will only be considered for uncovered demand calculations.
     * @return True, if this demand is a recommendation candidate.
     */
    boolean isRecommendationCandidate();

    /**
     * Whether this entity is currently suspended.
     * @return True, if this entity is currently suspended.
     */
    boolean isSuspended();

    /**
     * Whether this entity is currently terminated.
     * @return True, if this entity is currently terminated.
     */
    boolean isTerminated();

    /**
     * Creates and returns a new builder instance.
     * @return The newly created builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link ClassifiedEntitySelection} instances.
     */
    class Builder extends ImmutableClassifiedEntitySelection.Builder {}
}
