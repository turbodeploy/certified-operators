package com.vmturbo.components.test.utilities.alert;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

/**
 * This store manages access to the backend that contains all the metrics. It's purpose is to
 * provide methods to serve as a foundation for alerting calculations.
 */
public interface MetricsStore {

    /**
     * Check if the metrics store is available. A metrics store may be unavailable if, for
     * example, no connection to the store can be established.
     *
     * @return True if the store is available, false otherwise.
     */
    boolean isAvailable();

    /**
     * Get the standard of deviation for the metric values in a particular (test, metric) tuple
     * between now and some number of days ago.
     *
     * The standard deviation cannot be calculated if only a single sample exists. In this case,
     * returns {@link Optional#empty()}.
     *
     * @param testName The name of the test.
     * @param metricName The name of the metric.
     * @param labels The extra labels to filter by.
     * @param lookbackMs The time to look back at. The time range is always [lookbackMs, now].
     * @return The standard of deviation.
     */
    Optional<Double> getStandardDeviation(@Nonnull final String testName,
                                @Nonnull final String metricName,
                                @Nonnull final Map<String, String> labels,
                                final long lookbackMs);

    /**
     * Get the average (mean) for the metric values in a particular (test, metric) tuple
     * between now and some number of days ago.
     *
     * @param testName The name of the test.
     * @param metricName The name of the metric.
     * @param labels The extra labels to filter by.
     * @param lookbackMs The time to look back at. The time range is always [lookbackMs, now].
     * @return The average.
     */
    double getAvg(@Nonnull final String testName,
                  @Nonnull final String metricName,
                  @Nonnull final Map<String, String> labels,
                  final long lookbackMs);

    /**
     * Get the latest (most recent in terms of time sample) for the metric in a particular (test, metric) tuple
     * between now and some number of days ago.
     *
     * @param testName The name of the test.
     * @param metricName The name of the metric.
     * @param labels The extra labels to filter by.
     * @param lookbackMs The time to look back at. The time range is always [lookbackMs, now].
     * @return The value of the latest sample.
     */
    double getLatestSample(@Nonnull final String testName,
                           @Nonnull final String metricName,
                           @Nonnull final Map<String, String> labels,
                           final long lookbackMs);

    /**
     * Get a collection of all metric names known to the store
     * that are associated with the given testName.
     *
     * @param testClassName the name of the test class that the test method belongs to.
     * @return All metric names known to the store.
     */
    Collection<String> getAllMetricNamesForTest(@Nonnull final String testClassName);
}
