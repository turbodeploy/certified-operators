package com.vmturbo.cloud.commitment.analysis.runtime.data;

import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.TimeSeriesData;
import com.vmturbo.cloud.commitment.analysis.inventory.CloudCommitmentCapacity;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.transformation.AggregateCloudTierDemand;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;

/**
 * A segment (e.g. 1 hour block) of the analysis topology, representing both the demand and cloud
 * commitments available to this analysis block.
 */
@HiddenImmutableImplementation
@Immutable
public interface AnalysisTopologySegment extends TimeSeriesData {

    /**
     * The aggregate demand contained within this analysis segment.
     * @return The aggregate demand contained within this analysis segment.
     */
    @Nonnull
    Set<AggregateCloudTierDemand> aggregateCloudTierDemandSet();

    /**
     * The capacity for each cloud commitment within the analysis topology. The capacity may vary
     * across demand segments, depending on how the capacity is determined (e.g. if it's determined
     * based on historical utilization of the commitment).
     * @return The capacity for each cloud commitment within the analysis topology.
     */
    @Nonnull
    Map<Long, CloudCommitmentCapacity> cloudCommitmentByOid();

    /**
     * Converts this {@link AnalysisTopologySegment} to a {@link Builder} instance.
     * @return This topology segment, converted to a {@link Builder}.
     */
    @Nonnull
    default Builder toBuilder() {
        return AnalysisTopologySegment.builder().from(this);
    }

    /**
     * Constructs and returns a new {@link Builder} instance.
     * @return The newly constructd builder instance.
     */
    @Nonnull
    static Builder builder() {
        return new Builder();
    }

    /**
     * A builder class for {@link AnalysisTopologySegment}.
     */
    class Builder extends ImmutableAnalysisTopologySegment.Builder {}
}
