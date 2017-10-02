package com.vmturbo.proactivesupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.prometheus.client.Gauge;

import com.vmturbo.proactivesupport.DataMetricGauge.GaugeData;

/**
 * The DataMetricGauge supports a simple scalar value.
 */
@ThreadSafe
public class DataMetricGauge extends DataMetricBase<DataMetricGauge,
        GaugeData,
        DataMetricGauge.MultiGaugeListener> {

    private final Logger logger = LogManager.getLogger(DataMetricGauge.class);

    // a Prometheus metric object that will do all the heavy lifting for us
    private final Gauge gauge;

    /**
     * Constructs the base data metric.
     *
     * @param name     The metric name.
     * @param labelNames   The metric labels.
     * @param help     The metric help.
     * @param severity The metric severity.
     * @param urgent   The urgent flag.
     */
    private DataMetricGauge(final @Nonnull String name,
                            final @Nonnull String[] labelNames,
                            final @Nonnull String help,
                            final @Nonnull Severity severity,
                            final boolean urgent) {
        super(MetricType.GAUGE, name, labelNames, help, severity, urgent);
        gauge = createPrometheusGauge();
    }

    /**
     * No-args constructor for GSON.
     */
    private DataMetricGauge() {
        super();
        gauge = createPrometheusGauge();
    }

    /**
     * Create the prometheus metric that holds the data for this instance
     * @return the promethean "slave"
     */
    private Gauge createPrometheusGauge() {
        Gauge.Builder builder = Gauge.build()
                .name(getName())
                .help(getHelp())
                .labelNames(getLabelNames());
        try {
            return builder.register();
        }
        catch(IllegalArgumentException iae){
            // the metric name was already registered. This generally happens during unit testing,
            // but we'll log a warning when it does.
            logger.warn("Metric " + getName() + " already registered with prometheus. Will not re-register.");
            return builder.create();
        }
    }

    /**
     * Increments the unlabeled value associated with the tags.
     * In case value does not exist, initializes it to 1.
     */
    public void increment() {
        labels().increment();
    }

    /**
     * Decrements the unlabeled value associated with the specified tags.
     * In case value does not exist, initializes it to -1.
     */
    public void decrement() {
        labels().decrement();
    }

    /**
     * Returns the value associated with the tags.
     *
     * @return The value associated with the tags.
     */
    public double getData() {
        return labels().getData();
    }

    /**
     * Sets the value associated with the tags.
     *
     * @param value The value.
     */
    public void setData(final @Nonnull Double value) {
        labels().setData(value);
    }

    /**
     * Constructs the instance of the metric with all the default values.
     *
     * @return The instance of the metric with all the default values.
     */
    protected GaugeData constructDefault(String[] labels) {
        return new GaugeData(this,labels);
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
     * The scalar type builder.
     */
    public static class Builder extends DataMetricBuilder<Builder> {
        /**
         * Constructs the metric.
         *
         * @return The metric.
         */
        public DataMetricGauge build() {
            return new DataMetricGauge(name_, labelNames_, help_, severity_, urgent_);
        }
    }

    /**
     * GaugeData contains gauge data values optionally associated with a set of label values.
     */
    static public class GaugeData extends DataMetricBase.LabeledMetricData<DataMetricGauge> {

        public GaugeData() {
            super();
        }

        /**
         * Constructs a GaugeData instance belonging to the specified metric, and labels with
         * the specified set of label values.
         *
         * @param owner  The metric that owns this data
         * @param labels  The list of label values associated with this data. This should correspond
         *                to the list of label names defined in the metric owner.
         */
        public GaugeData(DataMetricGauge owner, String[] labels) {
            super(owner, labels);
        }

        /**
         * Increments the value.
         * In case value does not exist, initializes it to 1.
         */
        public void increment() {
            metric.gauge.labels(getLabels()).inc();

            // notify the observers
            metric.getListeners().forEach(observer -> observer.gaugeIncremented(this, 1.0d));
        }

        /**
         * Decrements the value.
         * In case value does not exist, initializes it to -1.
         */
        public void decrement() {
            metric.gauge.labels(getLabels()).dec();
            // notify the observers
            metric.getListeners().forEach(observer -> observer.gaugeDecremented(this, 1.0d));
        }

        /**
         * Returns the value of the gauge.
         * @return The value for the gauge.
         */
        public double getData() {
            return metric.gauge.labels(getLabels()).get();
        }

        /**
         * Sets the value for the label.
         *
         * @param value  The value.
         */
        public void setData(final @Nonnull Double value) {
            metric.gauge.labels(getLabels()).set(value);
            // notify the observers
            metric.getListeners().forEach(observer -> observer.gaugeSet(this, value));
        }
    }
    /**
     * Observer interface for monitoring changes to this metric
     */
    public interface MultiGaugeListener {

        /**
         * Fired when a Gauge has been incremented
         *
         * @param data the labels data instance that was incremented
         * @param amount the amount the gauge was incremented by
         */
        void gaugeIncremented(final @Nonnull GaugeData data, final double amount);

        /**
         * Fired when a Gauge has been decremented
         *
         * @param data the labels data instance that was decremented
         * @param amount the amount the gauge was decremented by
         */
        void gaugeDecremented(final @Nonnull GaugeData data, final double amount);

        /**
         * Fired when a Gauge has been explicitly set, rather than being incremented or decremented
         *
         * @param data the labels data instance that was decremented
         * @param newValue the new value the gauge was set to
         */
        void gaugeSet(final @Nonnull GaugeData data, final double newValue);

    }
}
