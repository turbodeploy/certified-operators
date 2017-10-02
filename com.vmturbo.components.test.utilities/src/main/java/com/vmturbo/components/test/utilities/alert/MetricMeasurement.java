package com.vmturbo.components.test.utilities.alert;

import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * Captures a measurement for a given metric.
 */
public class MetricMeasurement {
    private final double recentAverage;
    private final double baselineAverage;
    private final Optional<Double> standardDeviation;
    private final double latestValue;

    /**
     * The class name (simple) within which the test is defined.
     */
    private final String testClassName;

    /**
     * The name of the test that the measurement applies to.
     */
    private final String testName;

    /**
     * The ID of the metric that the measurement applies to.
     */
    private final AlertMetricId metric;

    /**
     * The status of the metric with respect to the alert it is related to.
     */
    public enum AlertStatus {
        /**
         * The metric is within the normal bounds of its baseline performance.
         */
        NORMAL("normal"),

        /**
         * The metric experienced a performance regression (recent performance is worse than the baseline).
         */
        REGRESSION("regression"),

        /**
         * The metric performance has improved (recent performance is superior to the baseline).
         */
        IMPROVEMENT("improvement"),

        /**
         * The metric is currently in violation of an SLA_VIOLATION (service level agreement) established for it.
         * SLA_VIOLATION violation status takes precedence over all others. That is, even if a metric is experiencing
         * a regression or improvement with respect to its baseline but is failing to meet its SLA, its
         * status should be SLA_VIOLATION rather than REGRESSION or IMPROVEMENT.
         */
        SLA_VIOLATION("sla violatn");

        private final String description;

        AlertStatus(@Nonnull final String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    /**
     * The status of the this metric with respect to the alert it is related to.
     */
    public AlertStatus alertStatus;

    public MetricMeasurement(final double recentAverage,
                             final double baselineAverage,
                             final double standardDeviation,
                             final double latestValue,
                             @Nonnull final AlertMetricId metricId,
                             @Nonnull final String testName,
                             @Nonnull final String testClassName) {
        this(recentAverage,
            baselineAverage,
            Optional.of(standardDeviation),
            latestValue,
            metricId,
            testName,
            testClassName
        );
    }

    public MetricMeasurement(final double recentAverage,
                             final double baselineAverage,
                             final Optional<Double> standardDeviation,
                             final double latestValue,
                             @Nonnull final AlertMetricId metricId,
                             @Nonnull final String testName,
                             @Nonnull final String testClassName) {
        this.standardDeviation = standardDeviation;
        this.baselineAverage = baselineAverage;
        this.recentAverage = recentAverage;
        this.latestValue = latestValue;

        this.testClassName = testClassName;
        this.testName = testName;
        this.metric = metricId;
        this.alertStatus = AlertStatus.NORMAL;
    }

    public double getRecentAverage() {
        return recentAverage;
    }

    public double getBaselineAverage() {
        return baselineAverage;
    }

    public Optional<Double> getStandardDeviation() {
        return standardDeviation;
    }

    public double getLatestValue() {
        return latestValue;
    }

    public String getTestClassName() {
        return testClassName;
    }

    public String getTestName() {
        return testName;
    }

    public AlertMetricId getMetric() {
        return metric;
    }

    /**
     * Mark the measurement as being responsible for triggering an alert.
     */
    public void setAlertStatus(@Nonnull final AlertStatus alertStatus) {
        this.alertStatus = alertStatus;
    }

    /**
     * Get the status of the alert that may have been triggered by this metric.
     *
     * @return The status of the alert that may have been triggered by this metric.
     */
    public AlertStatus getAlertStatus() {
        return alertStatus;
    }

    /**
     * Get the number of standard deviations above or below the baseline that the recent
     * average was from the baseline average. This number is always positive.
     *
     * If the measurement has no standard deviation, this value is undefined and will return empty.
     * If the standard deviation is 0, the value is also undefined and will return empty.
     *
     * @return The number of standard deviations above or below the baseline.
     */
    public Optional<Double> getNumberOfDeviations() {
        return standardDeviation
            .flatMap(stddev -> stddev == 0 ?
                Optional.empty() :
                Optional.of(Math.abs(recentAverage - baselineAverage) / stddev)
            );
    }
}
