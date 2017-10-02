package com.vmturbo.components.test.utilities.alert;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.stringtemplate.v4.ST;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.components.test.utilities.alert.MetricMeasurement.AlertStatus;
import com.vmturbo.components.test.utilities.alert.MetricNameSuggestor.Suggestion;

/**
 * The {@link AlertRule} defines the criteria for triggering an alert.
 * <p>
 * An alert should be triggered if either:
 *
 * 1. A metric with an SLA_VIOLATION is in violation of its SLA_VIOLATION threshold.
 * 2. the average of the metric over a certain number of time is more than some number
 * of standard deviations greater than or less than the average of the metric over a
 * baseline time period.
 */
public class AlertRule {

    /**
     * Template for converting the alert rule to a string in the case when the alert was triggered
     * by a deviation from the baseline average.
     */
    private static final String DEVIATION_FROM_BASELINE_TEMPLATE =
            "Over the past <comparisonTime>, the metric \"<metricName>\" " +
                    "<if(isIncrease)>increased<else>decreased<endif> by more than " +
                    "<stdDivs> standard deviations from it's average over the past <baselineTime>!";

    /**
     * Template for converting the alert rule to a string in the case when the alert was triggered
     * by a violation of a metric SLA_VIOLATION.
     */
    private static final String SLA_VIOLATION_TEMPLATE =
        "The metric \"<metricName>\" is in violation of its promised SLA of <slaThreshold>!";

    @VisibleForTesting
    static final long DEFAULT_COMPARISON_DAYS = 1;

    @VisibleForTesting
    static final long DEFAULT_BASELINE_DAYS = 14;

    @VisibleForTesting
    static final double DEFAULT_STD_DEVIATIONS = 1.5;

    private final Logger logger = LogManager.getLogger();

    private final Time comparisonTime;

    private final Time baselineTime;

    private final double standardDeviations;

    public static Builder newBuilder() {
        return new Builder();
    }

    private AlertRule(final Time comparisonTime,
                      final Time baselineTime,
                      final double standardDeviations) {
        if (comparisonTime.millis() > baselineTime.millis()) {
            throw new IllegalArgumentException("Baseline must be higher than comparison.");
        }
        this.comparisonTime = comparisonTime;
        this.standardDeviations = standardDeviations;
        this.baselineTime = baselineTime;
    }

    public Time getComparisonTime() {
        return comparisonTime;
    }

    public double getStandardDeviations() {
        return standardDeviations;
    }

    public Time getBaselineTime() {
        return baselineTime;
    }

    @Nonnull
    public Optional<MetricEvaluationResult> evaluate(@Nonnull final AlertMetricId metric,
                                               @Nonnull final String testName,
                                               @Nonnull final String testClassName,
                                               @Nonnull final MetricsStore metricsStore) {
        try {
            final Optional<Double> stdDev = metricsStore.getStandardDeviation(
                    testName, metric.getMetricName(), metric.getLabels(), baselineTime.millis());
            final double baselineAvg = metricsStore.getAvg(
                    testName, metric.getMetricName(), metric.getLabels(), baselineTime.millis());
            final double recentAvg = metricsStore.getAvg(
                    testName, metric.getMetricName(), metric.getLabels(), comparisonTime.millis());
            final double latestSample = metricsStore.getLatestSample(
                    testName, metric.getMetricName(), metric.getLabels(), comparisonTime.millis());

            final MetricMeasurement measurement = new MetricMeasurement(
                recentAvg, baselineAvg, stdDev, latestSample, metric, testName, testClassName);

            // SLA_VIOLATION violations take precedence over deviations from the baseline.
            Optional<TriggeredAlert> alert =
                evaluateSlaThreshold(measurement, recentAvg, metric.getSlaThreshold());
            if (!alert.isPresent()) {
                alert = evaluateDeviationFromBaseline(measurement, recentAvg, baselineAvg);
            }

            return Optional.of(new MetricEvaluationResult(measurement, alert));
        } catch (RuntimeException e) {
            final List<String> suggestions =
                findSuggestionsForMistypedName(metric, testName, metricsStore);
            if (suggestions.isEmpty()) {
                logger.error("Error evaluating alert rule for test " + testName + " and metric " + metric + "!", e);
            } else {
                logger.error("Error evaluating alert rule for test {} and metric {}. Did you mean?{}",
                    testName, metric, suggestions.stream()
                        .map(suggestion -> "\t" + suggestion)
                        .collect(Collectors.joining("\n", "\n", "\n")));
            }
            return Optional.empty();
        }
    }

    @Nonnull
    String describe(@Nonnull final AlertMetricId metric, final AlertStatus alertStatus) {
        return (alertStatus == AlertStatus.SLA_VIOLATION) ?
            describeSlaViolation(metric) :
            describeDeviationFromBaseline(metric, alertStatus);
    }

    private Optional<TriggeredAlert> evaluateSlaThreshold(@Nonnull final MetricMeasurement measurement,
                                                          final double recentAvg,
                                                          @Nonnull final Optional<SlaThreshold> slaThreshold) {
        final Optional<TriggeredAlert> alert = slaThreshold
            .flatMap(sla -> recentAvg > sla.getRawValue() ?
                Optional.of(new TriggeredAlert(this, measurement)) :
                Optional.empty());
        alert.ifPresent(triggeredAlert -> measurement.setAlertStatus(AlertStatus.SLA_VIOLATION));

        return alert;
    }

    private Optional<TriggeredAlert> evaluateDeviationFromBaseline(@Nonnull final MetricMeasurement measurement,
                                                                   final double recentAvg,
                                                                   final double baselineAvg) {
        final Optional<TriggeredAlert> alert = measurement.getNumberOfDeviations()
            .flatMap(numDeviations -> numDeviations > standardDeviations ?
                Optional.of(new TriggeredAlert(this, measurement)) :
                Optional.empty());
        alert.ifPresent(triggeredAlert -> measurement
            .setAlertStatus((recentAvg > baselineAvg) ? AlertStatus.REGRESSION : AlertStatus.IMPROVEMENT));

        return alert;
    }

    @Nonnull
    private String describeSlaViolation(@Nonnull final AlertMetricId metric) {
        return new ST(SLA_VIOLATION_TEMPLATE)
            .add("metricName", metric)
            .add("slaThreshold", metric.getSlaThreshold().get().getDescription())
            .render();
    }

    @Nonnull
    private String describeDeviationFromBaseline(@Nonnull final AlertMetricId metric,
                                                 @Nonnull final AlertStatus alertStatus) {
        return new ST(DEVIATION_FROM_BASELINE_TEMPLATE)
            .add("metricName", metric)
            .add("isIncrease", alertStatus == AlertStatus.REGRESSION)
            .add("comparisonTime", comparisonTime)
            .add("baselineTime", baselineTime)
            .add("stdDivs", standardDeviations)
            .render();
    }

    /**
     * Find suggested metric names for the actual metric name. It is fairly common to create an alert for a metric
     * such as "repo_global_supply_chain_duration_seconds" and to forget the "_sum" part that has to be added
     * for things to work. This is designed to remind users of alerts if they mistype the name as above.
     *
     * It is designed to be similar in helpfulness to language features such as Ruby's "did you mean"
     * https://github.com/yuki24/did_you_mean
     *
     * It should not provide suggestions if the metric name is actually correct (ie it perfectly matches a metric
     * known to the metric store) because the problem is due to something other than a mistyped name in this case.
     *
     * @param metric The metric to offer suggestions for.
     * @param testName The name of the test from which to draw the metric name suggestion pool.
     * @param metricsStore The store containing metric data.
     * @return Suggestions for what the alert may have been intended to be.
     */
    private List<String> findSuggestionsForMistypedName(@Nonnull final AlertMetricId metric,
                                                        @Nonnull final String testName,
                                                        @Nonnull final MetricsStore metricsStore) {
        return findSuggestionsForMistypedName(metric,
            metricsStore.getAllMetricNamesForTest(testName),
            new MetricNameSuggestor());
    }

    @VisibleForTesting
    @Nonnull
    List<String> findSuggestionsForMistypedName(@Nonnull final AlertMetricId metric,
                                                        @Nonnull final Collection<String> allMetricNames,
                                                        @Nonnull final MetricNameSuggestor suggestor) {
        List<Suggestion> suggestions = suggestor.computeSuggestions(metric.getMetricName(), allMetricNames);
        if (suggestions.stream().anyMatch(Suggestion::isPerfectMatch)) {
            // The error was not caused by a mistyped name. Don't offer suggestions.
            return Collections.emptyList();
        }

        return suggestions.stream()
            .map(Suggestion::getSuggestedMetricName)
            .collect(Collectors.toList());
    }


    /**
     * A utility class to make passing TimeUnit-based times
     * less verbose.
     */
    public static class Time {

        private final long amount;

        private final TimeUnit timeUnit;

        public Time(final long amount, final TimeUnit timeUnit) {
            this.amount = amount;
            this.timeUnit = timeUnit;
        }

        public long millis() {
            return timeUnit.toMillis(amount);
        }

        @Override
        public String toString() {
            return Long.toString(amount) + " " + pluralizedUnits();
        }

        public String singularForm() {
            return Long.toString(amount) + " " + singularUnits(unitsString());
        }

        private String unitsString() {
            return timeUnit.toString().toLowerCase();
        }

        /**
         * Strip off the trailing 's' on the units string if it is not plural.
         *
         * @return The units string, in singular or plural form as appropriate.
         */
        private String pluralizedUnits() {
            return amount > 1 ? unitsString() : singularUnits(unitsString());
        }

        private String singularUnits(@Nonnull final String unitsString) {
            return unitsString.substring(0, unitsString.length() - 1);
        }
    }

    /**
     * The result of evaluating a particular metric in a particular alert.
     */
    public static class MetricEvaluationResult {
        @Nonnull
        private final MetricMeasurement measurement;

        @Nonnull
        private final Optional<TriggeredAlert> alert;

        @VisibleForTesting
        MetricEvaluationResult(@Nonnull MetricMeasurement measurement, @Nonnull Optional<TriggeredAlert> alert) {
            this.measurement = measurement;
            this.alert = alert;
        }

        public MetricMeasurement getMeasurement() {
            return measurement;
        }

        public Optional<TriggeredAlert> getAlert() {
            return alert;
        }
    }

    public static class Builder {
        private Time comparisonTime = new Time(
            Long.getLong("alert.rule.comparison.days", DEFAULT_COMPARISON_DAYS), TimeUnit.DAYS);
        private Time baselineTime = new Time(
            Long.getLong("alert.rule.baseline.days", DEFAULT_BASELINE_DAYS), TimeUnit.DAYS);
        private double standardDeviations = Double.parseDouble(
            System.getProperty("alert.rule.std.deviations", Double.toString(DEFAULT_STD_DEVIATIONS)));

        private Builder() {}

        @Nonnull
        public Builder setComparisonTime(final long comparison, final TimeUnit timeUnit) {
            if (comparison <= 0) {
                throw new IllegalArgumentException("Comparison time must be positive!");
            }
            this.comparisonTime = new Time(comparison, timeUnit);
            return this;
        }

        @Nonnull
        public Builder setBaselineTime(final long baseline, final TimeUnit timeUnit) {
            if (baseline <= 0) {
                throw new IllegalArgumentException("Baseline time must be positive!");
            }
            this.baselineTime = new Time(baseline, timeUnit);
            return this;
        }

        @Nonnull
        public Builder setStandardDeviations(final double standardDeviations) {
            if (standardDeviations <= 0) {
                throw new IllegalArgumentException("Standard deviations must be positive!");
            }
            this.standardDeviations = standardDeviations;
            return this;
        }

        @Nonnull
        public AlertRule build() {
            return new AlertRule(comparisonTime, baselineTime, standardDeviations);
        }
    }
}
