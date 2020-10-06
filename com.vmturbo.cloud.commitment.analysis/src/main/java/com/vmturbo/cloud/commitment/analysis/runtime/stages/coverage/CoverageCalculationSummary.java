package com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage;

import java.util.DoubleSummaryStatistics;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import com.google.common.math.DoubleMath;

import org.apache.commons.lang3.mutable.MutableLong;
import org.stringtemplate.v4.ST;

import com.vmturbo.cloud.commitment.analysis.runtime.data.AnalysisTopology;
import com.vmturbo.cloud.commitment.analysis.runtime.data.DurationStatistics;
import com.vmturbo.cloud.commitment.analysis.runtime.stages.coverage.CoverageCalculationTask.CoverageCalculationInfo;

/**
 * Represents an execution summary for {@link CoverageCalculationStage}.
 */
public class CoverageCalculationSummary {

    private static final String COVERAGE_SUMMARY =
            "Calculation Task Duration: <calculationTaskDuration>\n"
                    + "Coverage Amount: <coverageAmount>\n"
                    + "Coverage Percentage: <coveragePercentage>%\n"
                    + "Demand Amount: <demandAmount>\n"
                    + "Empty Demand Segments: <emptySegmentCount>\n"
                    + "Commitment Capacity: <commitmentCapacity>\n"
                    + "Commitment Utilization: <commitmentUtilization>%";

    private final AnalysisTopology analysisTopology;

    private final List<CoverageCalculationInfo> coverageCalculationList;

    private CoverageCalculationSummary(@Nonnull AnalysisTopology analysisTopology,
                                       @Nonnull List<CoverageCalculationInfo> coverageCalculationList) {

        this.analysisTopology = Objects.requireNonNull(analysisTopology);
        this.coverageCalculationList = ImmutableList.copyOf(Objects.requireNonNull(coverageCalculationList));
    }

    /**
     * Provides a human-readable summary of the execution of a {@link CoverageCalculationStage} instance.
     * @return The summary string.
     */
    @Override
    public String toString() {

        final DurationStatistics.Collector taskDurationCollector = DurationStatistics.collector();
        final DoubleSummaryStatistics coverageStats = new DoubleSummaryStatistics();
        final DoubleSummaryStatistics demandStats = new DoubleSummaryStatistics();
        final DoubleSummaryStatistics commitmentStats = new DoubleSummaryStatistics();
        final MutableLong emptySegmentCount = new MutableLong(0);

        coverageCalculationList.forEach(calculationInfo -> {
            final CoverageCalculationTask.CoverageCalculationSummary calculationSummary = calculationInfo.summary();

            taskDurationCollector.collect(calculationSummary.calculationDuration());
            coverageStats.accept(calculationSummary.coveredDemand());
            demandStats.accept(calculationSummary.aggregateDemand());
            commitmentStats.accept(calculationSummary.commitmentCapacity());

            if (DoubleMath.fuzzyCompare(calculationSummary.aggregateDemand(), 0.0, .01) == 0) {
                emptySegmentCount.increment();
            }
        });

        final ST template = new ST(COVERAGE_SUMMARY);

        template.add("calculationTaskDuration", taskDurationCollector.toStatistics());
        template.add("coverageAmount", coverageStats);
        template.add("coveragePercentage",
                demandStats.getSum() > 0
                        ? coverageStats.getSum() / demandStats.getSum() * 100
                        : 0);
        template.add("demandAmount", demandStats);
        template.add("emptySegmentCount", emptySegmentCount.getValue());
        template.add("commitmentCapacity", commitmentStats);
        template.add("commitmentUtilization",
                commitmentStats.getSum() > 0
                        ? coverageStats.getSum() / commitmentStats.getSum() * 100
                        : 0);

        return template.render();
    }

    /**
     * A factory class for creating {@link CoverageCalculationSummary} instances.
     */
    public static class CoverageCalculationSummaryFactory {

        /**
         * Constucts a new {@link CoverageCalculationSummary} instance.
         * @param analysisTopology The {@link AnalysisTopology} analyzed.
         * @param coverageCalculationList The {@link CoverageCalculationInfo} list, representing the results
         *                                for each analysis segment contained within {@code analysisTopology}.
         * @return A new {@link CoverageCalculationSummary}, representing a human-readable summary of the
         * coverage calculation results across the {@code analysisTopology}.
         */
        @Nonnull
        public CoverageCalculationSummary newSummary(@Nonnull AnalysisTopology analysisTopology,
                                                     @Nonnull List<CoverageCalculationInfo> coverageCalculationList) {
            return new CoverageCalculationSummary(analysisTopology, coverageCalculationList);
        }
    }
}
