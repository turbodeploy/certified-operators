package com.vmturbo.proactivesupport;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.prometheus.client.Counter;

import com.vmturbo.proactivesupport.DataMetricCounter.CounterData;

/**
 * The DataMetricCounter supports a simple Counter metric.
 *
 */
@ThreadSafe
public class DataMetricCounter extends DataMetricBase<DataMetricCounter,
        CounterData,
        DataMetricCounter.MultiCounterListener> {

    private final Logger logger = LogManager.getLogger(DataMetricCounter.class);

    // a Prometheus metric object that will do all the heavy lifting for us
    private final Counter counter;

    /**
     * Constructs the base data metric.
     *
     * @param name     The metric name.
     * @param labelNames   The metric labels.
     * @param help     The metric help.
     * @param severity The metric severity.
     * @param urgent   The urgent flag.
     */
    private DataMetricCounter(final @Nonnull String name,
                              final @Nonnull String[] labelNames,
                              final @Nonnull String help,
                              final @Nonnull Severity severity,
                              final boolean urgent) {
        super(MetricType.COUNTER, name, labelNames, help, severity, urgent);
        counter = createPrometheusCounter();
    }

    /**
     * No-args constructor for GSON.
     */
    private DataMetricCounter() {
        super();
        counter = createPrometheusCounter();
    }

    /**
     * Create the prometheus metric that holds the data for this instance
     * @return the promethean "slave"
     */
    private Counter createPrometheusCounter() {
        Counter.Builder builder = Counter.build()
                .name(getName())
                .help(getHelp())
                .labelNames(getLabelNames());
        try {
            return builder.register();
        }
        catch(IllegalArgumentException iae) {
            // the metric name was already registered. This generally happens during unit testing,
            // but we'll log a warning when it does.
            logger.warn("Metric " + getName() + " already registered with prometheus. Will not re-register.");
            return builder.create();

        }
    }

    /**
     * Increments the value of an unlabeled counter.
     * In case value does not exist, initializes it to 1.
     */
    public void increment() {
        labels().increment();
    }

    /**
     * Increment the non-labeled counter by an amount other than 1.
     *
     * @param value The value.
     */
    public void increment(final @Nonnull Double value) {
        labels().increment(value);
    }

    /**
     * Returns the value associated with an unlabeled counter.
     *
     * @return The value of the unlabeled counter.
     */
    public double getData() {
        return labels().getData();
    }

    /**
     * Constructs the instance of the metric with all the default values.
     *
     * @return The instance of the metric with all the default values.
     */
    protected CounterData constructDefault(String[] labels) {
        return new CounterData(this,labels);
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
        public DataMetricCounter build() {
            return new DataMetricCounter(name_, labelNames_, help_, severity_, urgent_);
        }
    }

    /**
     * Stores labels counter data with the ability to further segregate data by tags.
     */
    static public class CounterData extends DataMetricBase.LabeledMetricData<DataMetricCounter> {

        /**
         * Constructs a CounterData instance belonging to the specified metric, and labelled with
         * the specified set of label values.
         *
         * @param owner  The MetricCounter owner of this data
         * @param labels  The list of label values associated with this data. This should correspond
         *                to the list of label names defined in the metric owner.
         */
        public CounterData(DataMetricCounter owner, String[] labels) {
            super(owner, labels);
        }

        /**
         * Increments the value by 1.
         * In case value does not exist, initializes it to 1.
         */
        public void increment() {
            metric.counter.labels(getLabels()).inc();
            // call the observers
            metric.getListeners().forEach(observer -> observer.counterIncremented(this, 1.0D));
        }

        /**
         * Increment the counter by an amount other than 1.
         *
         * @param delta The amount to increment by. Must be > 0.
         */
        public void increment(final @Nonnull Double delta) {
            if ( delta <= 0 ) {
                throw new IllegalArgumentException("Cannot increment by less than 0");
            }
            metric.counter.labels(getLabels()).inc(delta);
            metric.getListeners().forEach(observer -> observer.counterIncremented(this, delta));
        }

        /**
         * Returns the value for the counter.
         *
         * @return The value of this counter.
         */
        public double getData() {
            return metric.counter.labels(getLabels()).get();
        }

        /**
         * Observes the value.
         *
         * @param value The value.
         */
        public synchronized void observe(final @Nonnull Double value) {
            metric.counter.labels(getLabels()).inc(value);
            // notify observers of this new observation
            metric.getListeners().forEach(observer -> observer.counterIncremented(this, value ));
        }

        /**
         * Start a timer that will record an observation on this metric when it stops.
         *
         * @return a new Timer instance associated with this metric.
         */
        public DataMetricTimer startTimer() {
            return new DataMetricTimer(this::observe);
        }


    }

    /**
     * Observer interface for monitoring changes to this metric
     */
    public interface MultiCounterListener {

        /**
         * Fired when a MultiCounter has been incremented
         *
         * @param data the labels counter data instance being incremented.
         * @param amount the amount the counter has been incremented by
         */
        void counterIncremented(final @Nonnull CounterData data, final double amount);
    }
}
