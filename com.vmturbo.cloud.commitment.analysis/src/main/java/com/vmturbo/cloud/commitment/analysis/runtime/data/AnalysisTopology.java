package com.vmturbo.cloud.commitment.analysis.runtime.data;

import java.util.Map;

import javax.annotation.Nonnull;

import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.common.commitment.CloudCommitmentData;
import com.vmturbo.cloud.common.data.TimeSeries;
import com.vmturbo.cloud.common.immutable.HiddenImmutableImplementation;
import com.vmturbo.cloud.common.immutable.TimeSeriesEncodingEnabled;

/**
 * The analysis topology, represented all aggregated demand considered for coverage analysis and
 * recommendations, along with the cloud commitments in scope of the analysis. The analysis demand
 * is broken down into time segments (e.g. 1 hour blocks) through the {@link AnalysisTopologySegment}
 * instances.
 */
@TimeSeriesEncodingEnabled
@HiddenImmutableImplementation
@Immutable
public interface AnalysisTopology {

    /**
     * The cloud commitments in scope of the analysis, indexed by their OIDs.
     * @return The cloud commitments in scope of the analysis, indexed by their OIDs.
     */
    @Nonnull
    Map<Long, CloudCommitmentData> cloudCommitmentsByOid();

    /**
     * The {@link AnalysisTopologySegment} instances as a time series.
     * @return The {@link AnalysisTopologySegment} instances as a time series.
     */
    @Nonnull
    TimeSeries<AnalysisTopologySegment> segments();

    /**
     * Converts this {@link AnalysisTopology} instance to a {@link Builder}.
     * @return This instance, converted to a {@link Builder}.
     */
    @Nonnull
    default Builder toBuilder() {
        return AnalysisTopology.builder().from(this);
    }

    /**
     * Constructs and creates a new {@link Builder} instance.
     * @return The newly constructed builder instance.
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
