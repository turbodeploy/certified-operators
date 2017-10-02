package com.vmturbo.proactivesupport;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.prometheus.client.Histogram;

/**
 * The DataMetricHistogram implements the histogram, whereby the values are sorted into the buckets
 * depending on the value.
 */
@ThreadSafe
public class DataMetricHistogram extends DataMetricBase<DataMetricHistogram,
        DataMetricHistogram.HistogramData,
        DataMetricHistogram.HistogramObserver> {

    private final Logger logger = LogManager.getLogger(DataMetricHistogram.class);

    /**
     * The buckets.
     */
    private double[] buckets_;

    // a Prometheus metric object that will do all the heavy lifting for us
    private final Histogram histogram;

    /**
     * Constructs the histogram metric.
     *
     * @param name     The metric name.
     * @param labelNames   The metric labels.
     * @param help     The metric help.
     * @param severity The metric severity.
     * @param urgent   The urgent flag.
     */
    protected DataMetricHistogram(final @Nonnull String name,
                                  final @Nonnull String[] labelNames,
                                  final @Nonnull String help,
                                  final @Nonnull Severity severity,
                                  final @Nonnull double[] buckets,
                                  final boolean urgent) {
        super(MetricType.HISTOGRAM, name, labelNames, help, severity, urgent);
        buckets_ = Objects.requireNonNull(buckets);
        histogram = createPrometheusHistogram();
    }

    /**
     * No-args constructor for GSON.
     */
    private DataMetricHistogram() {
        super();
        histogram = createPrometheusHistogram();
    }

    /**
     * Create the prometheus metric that holds the data for this instance
     * @return the promethean "slave"
     */
    private Histogram createPrometheusHistogram() {
        Histogram.Builder builder = Histogram.build()
                .name(getName())
                .help(getHelp())
                .labelNames(getLabelNames())
                .buckets(getBucketBounds() );
        try {
            return builder.register(); // register with prometheus
        }
        catch(IllegalArgumentException iae) {
            // illegal argument is thrown if the metric name was already taken. This happens often
            // during our unit tests, and is a by-product of us not clearing out the Prometheus
            // defaultRegistry singleton. We will assume it's a test-related error and log a warning
            // about it, but not stop execution.
            logger.warn("Metric "+ getName() +" already registered with prometheus. Will not re-register.");
            return builder.create();
        }
    }

    /**
     * Returns the builder.
     *
     * @return The builder.
     */
    public static @Nonnull Builder builder() {
        return new Builder();
    }

    /**
     * Observes a value with no labels.
     *
     * @param value The value.
     */
    public synchronized void observe(final @Nonnull Double value) {
        labels().observe(value);
    }

    /**
     * Returns the non-labels sum.
     *
     * @return The sum.
     */
    public synchronized double getSum() {
        return labels().getSum();
    }

    /**
     * Returns the non-labels data slotted into buckets.
     *
     * @return The data slotted into buckets.
     */
    public synchronized Map<Double, Long> getBuckets() {
        return labels().getBuckets();
    }

    /**
     * Return the array of bucket upper-bound values.
     *
     * @return The array of bucket upper-bound values.
     */
    public double[] getBucketBounds() {
        return buckets_;
    }

    /**
     * Constructs the instance of the metric with all the default values.
     *
     * @return The instance of the metric with all the default values.
     */
    protected HistogramData constructDefault( String[] labels) {
        return new HistogramData(this,labels);
    }

    /**
     * The scalar type builder.
     */
    public static class Builder extends DataMetricBuilder<Builder> {
        /**
         * The buckets.
         */
        private double[] buckets_;

        /**
         * Sets the scalar metric.
         *
         * @param buckets The buckets.
         * @return The metric.
         */
        public Builder withBuckets(final double... buckets) {
            buckets_ = buckets;
            return this;
        }

        /**
         * Constructs the metric.
         *
         * @return The metric.
         */
        public DataMetricHistogram build() {
            return new DataMetricHistogram(name_, labelNames_, help_, severity_, buckets_, urgent_);
        }
    }

    /**
     * HistogramData stores histogram data, which includes a set of counter values distributed
     * into the buckets defined in the DataMetricHistogram class instance that owns the data, as
     * well as the total sum of values in all of the buckets.
     */
    static public class HistogramData extends DataMetricBase.LabeledMetricData<DataMetricHistogram> {

        /**
         * Constructs a HistogramData instance belonging to the specified metric, and labelled with
         * the specified set of label values.
         *
         * @param metric  The metric that owns this data
         * @param labels  The list of label values associated with this data. This should correspond
         *                to the list of label names defined in the metric owner.
         */
        public HistogramData(DataMetricHistogram metric, String[] labels) {
            super(metric, labels);
        }

        /**
         * Observes the value.
         *
         * @param value The value.
         */
        public synchronized void observe(final @Nonnull Double value) {
            metric.histogram.labels(getLabels()).observe(value);

            // notify the observers
            metric.getListeners().forEach(observer->observer.histogramObserved(this, value));
        }

        /**
         * Returns the sum.
         *
         * @return The sum.
         */
        public synchronized double getSum() {
            return metric.histogram.labels(getLabels()).get().sum;
        }

        /**
         * Returns the bucket values.
         *
         * @return The bucket values.
         */
        public synchronized Map<Double, Long> getBuckets() {
            double[] values = metric.histogram.labels(getLabels()).get().buckets;
            double[] buckets = metric.getBucketBounds();
            Map<Double, Long> map = new HashMap<>();
            for (int i = 0; i < buckets.length; i++) {
                map.put(buckets[i], (long)values[i]);
            }
            return map;
        }

    }

    /**
     * Observer interface for monitoring changes to histogram metrics
     */
    public interface HistogramObserver {

        /**
         * Fired when the DataMetricSummary has recieved a new observation
         *
         * @param data  the labels histogram data instance that recieved the observation
         * @param observation  the value of the observation
         */
        void histogramObserved(final @Nonnull HistogramData data, final double observation);
    }
}
