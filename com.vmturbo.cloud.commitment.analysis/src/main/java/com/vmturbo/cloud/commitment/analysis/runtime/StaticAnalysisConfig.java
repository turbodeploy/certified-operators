package com.vmturbo.cloud.commitment.analysis.runtime;

import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import org.immutables.value.Value.Check;
import org.immutables.value.Value.Immutable;

/**
 * A configuration for {@link CloudCommitmentAnalysis}, containing static attributes that do not
 * change across invocations.
 */
@Immutable
public abstract class StaticAnalysisConfig {

    /**
     * The set of supported time units for {@link #analysisSegmentUnit()}.
     */
    public static final Set<ChronoUnit> SUPPORTED_ANALYSIS_SEGMENT_UNITS = ImmutableSet.of(
            ChronoUnit.SECONDS,
            ChronoUnit.MINUTES,
            ChronoUnit.HOURS,
            ChronoUnit.DAYS);

    /**
     * THe interval/amount of the analysis segment. This will be combined with the {@link #analysisSegmentUnit()}
     * to create the analysis segment. For example, the interval may be set to 6 and the unit may be set
     * to hours. Therefore, the analysis will analyze demand in 6 hour blocks.
     *
     * @return The interval/amount of the analysis segment.
     */
    public abstract long analysisSegmentInterval();

    /**
     * The unit of the analysis segment. See {@link #analysisSegmentInterval()} for an example on how
     * this may be used.
     *
     * @return The unit of the analysis segment.
     */
    @Nonnull
    public abstract TemporalUnit analysisSegmentUnit();

    @Check
    protected void validate() {
        Preconditions.checkState(
                SUPPORTED_ANALYSIS_SEGMENT_UNITS.contains(analysisSegmentUnit()),
                "Analysis segment unit must be in SUPPORTED_ANALYSIS_SEGMENT_UNITS");
    }
}
