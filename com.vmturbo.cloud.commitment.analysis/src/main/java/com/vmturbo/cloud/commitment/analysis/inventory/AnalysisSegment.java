package com.vmturbo.cloud.commitment.analysis.inventory;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.demand.TimeSeriesData;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;

/**
 * Interface representing an analysis segment.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface AnalysisSegment extends TimeSeriesData {
    /**
     * The set of demand which is accumulated in this segment.
     *
     * @return The aggregated demand set.
     */
    Set<AggregateCloudTierDemand> aggregateCloudTierDemandSet();

    /**
     * Returns A map of cloud commitment oid to cloud commitment used in the run of CCA.
     *
     * @return A map of cloud commitment oid to cloud commitment
     */
    Map<Long, CloudCommitmentCapacity> cloudCommitmentByOid();

    /**
     * A builder for this interface.
     *
     * @return The builder.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link AnalysisSegment}.
     */
    class Builder extends ImmutableAnalysisSegment.Builder {}
}
