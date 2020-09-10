package com.vmturbo.cloud.commitment.analysis.runtime;

import java.time.temporal.ChronoUnit;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

import com.vmturbo.cloud.commitment.analysis.demand.BoundedDuration;

/**
 * A configuration for {@link CloudCommitmentAnalysis}, containing static attributes that do not
 * change across invocations.
 */
@Immutable
public abstract class StaticAnalysisConfig {

    /**
     * The set of supported time units for {@link #analysisBucket()} ()}.
     */
    public static final Set<ChronoUnit> SUPPORTED_ANALYSIS_BUCKET_UNITS = ImmutableSet.of(
            ChronoUnit.SECONDS,
            ChronoUnit.MINUTES,
            ChronoUnit.HOURS,
            ChronoUnit.DAYS);

    /**
     * The analysis bucket interval to use for the analysis. Demand will be group and analyzed in buckets,
     * based on the demand start/end time.
     * @return The analysis bucket interval.
     */
    public abstract BoundedDuration analysisBucket();

    @Check
    protected void validate() {
        Preconditions.checkState(
                SUPPORTED_ANALYSIS_BUCKET_UNITS.contains(analysisBucket().unit()),
                "Analysis bucket unit must be in SUPPORTED_ANALYSIS_BUCKET_UNITS");
    }
}
