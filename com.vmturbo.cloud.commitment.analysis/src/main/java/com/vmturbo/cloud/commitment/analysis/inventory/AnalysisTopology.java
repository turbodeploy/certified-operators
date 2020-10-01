package com.vmturbo.cloud.commitment.analysis.inventory;

import java.util.Map;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Style;
import org.immutables.value.Value.Style.ImplementationVisibility;

import com.vmturbo.cloud.commitment.analysis.demand.TimeSeries;
import com.vmturbo.cloud.common.commitment.CloudCommitmentData;

/**
 * The analysis topology represents the time series breakdown of demand segments along with the cloud
 * commitment available for the run of CCA.
 */
@Style(visibility = ImplementationVisibility.PACKAGE, overshadowImplementation = true)
@Immutable
public interface AnalysisTopology {
    /**
     * A map of cloud commitment oid to cloud commitment data.
     *
     * @return A map of cloud commitment oid to cloud commitment data.
     */
    Map<Long, CloudCommitmentData> cloudCommitmentsByOid();

    /**
     * Analysis segments broken down by time intervals.
     *
     * @return Time series breakdown of analysis segments.
     */
    TimeSeries<AnalysisSegment> segment();

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
     * A builder class for {@link AnalysisTopology}.
     */
    class Builder extends ImmutableAnalysisTopology.Builder {}
}
