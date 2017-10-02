package com.vmturbo.proactivesupport;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.prometheus.client.Summary;

/**
 * The DataMetricSummary implements the summary, whereby the values are sorted into the buckets
 * depending on the value.
 * TODO(MB): The quantiles over a sliding window will be implemented in the future.
 */
@ThreadSafe
public class DataMetricSummary extends DataMetricBase<DataMetricSummary,
        DataMetricSummary.SummaryData,
        DataMetricSummary.SummaryListener> {

    private final Logger logger = LogManager.getLogger(DataMetricSummary.class);

    static private final long DEFAULT_MAX_AGE_SECS = 600; // 600 seconds = 10 mins

    static private final int DEFAULT_AGE_BUCKETS = 5;

    // we are storing simple quantile configuration data just for the purposes of being able to pass through to another
    // metrics client, e.g. Prometheus
    final private List<Quantile> quantiles;

    final private long maxAgeSeconds;

    final private int ageBuckets;

    // a Prometheus metric object that will do all the heavy lifting for us
    final private Summary prometheusSummary;

    /**
     * Constructs the base data metric.
     *
     * @param name     The metric name.
     * @param labelNames   The metric labels.
     * @param help     The metric help.
     * @param severity The metric severity.
     * @param urgent   The urgent flag.
     */
    protected DataMetricSummary(final @Nonnull String name,
                                final @Nonnull String[] labelNames,
                                final @Nonnull String help,
                                final @Nonnull Severity severity,
                                final boolean urgent,
                                final List<Quantile> quantiles,
                                final long maxAgeSeconds,
                                final int ageBuckets ) {
        super(MetricType.SUMMARY, name, labelNames, help, severity, urgent);
        this.quantiles = quantiles;
        this.maxAgeSeconds = maxAgeSeconds;
        this.ageBuckets = ageBuckets;
        prometheusSummary = createPrometheusSummary();
    }

    /**
     * No-args constructor for GSON.
     */
    private DataMetricSummary() {
        super();
        quantiles = new ArrayList<Quantile>();
        maxAgeSeconds = DEFAULT_MAX_AGE_SECS;
        ageBuckets = DEFAULT_AGE_BUCKETS;
        prometheusSummary = createPrometheusSummary();
    }

    /**
     * Create the prometheus metric that holds the data for this instance
     * @return the promethean "slave"
     */
    private Summary createPrometheusSummary() {
        Summary.Builder builder = Summary.build()
                .name(getName())
                .help(getHelp())
                .labelNames(getLabelNames())
                .ageBuckets(getAgeBuckets())
                .maxAgeSeconds(getMaxAgeSeconds());
        // copy the quantile configuration
        List<DataMetricSummary.Quantile> quantiles = getQuantiles();
        quantiles.forEach( quantile -> builder.quantile(quantile.getQuantile(),quantile.getError()));

        Summary summary = null;
        try {
            summary = builder.register(); // register with prometheus
        }
        catch(IllegalArgumentException iae) {
            // the metric name was already registered. This generally happens during unit testing,
            // but we'll log a warning when it does.
            logger.warn("Metric "+ getName() +" already registered with prometheus. Will not re-register.");
            summary = builder.create();
        }
        return summary;
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
     * Retrieve the list of Quantiles this metric is configured with.
     * @return the list of Quantiles this summary is configured with
     */
    public List<Quantile> getQuantiles() {
        return quantiles;
    }

    /**
     * Return the max age, in seconds, of the observations that will be incorporated into the summary
     * @return the maxAgeSeconds value currently in force.
     */
    public long getMaxAgeSeconds() {
        return maxAgeSeconds;
    }

    /**
     * Return the # of age buckets used to calculate quantiles over the max age span. These two settings determine
     * how fine-grained your buckets are. # of age buckets defaults to 5.
     * @return the # of age buckets configured.
     */
    public int getAgeBuckets() {
        return ageBuckets;
    }

    /**
     * Constructs the instance of the metric with all the default values.
     *
     * @return The instance of the metric with all the default values.
     */
    protected SummaryData constructDefault(String[] labels) {
        return new SummaryData(this,labels);
    }

    /**
     * Observes an unlabeled value.
     *
     * @param value The value.
     */
    public synchronized void observe(final @Nonnull Double value) {
        labels().observe(value);
    }

    /**
     * Returns the sum of the unlabeled value
     *
     * @return The sum.
     */
    public synchronized double getSum() {
        return labels().getSum();
    }

    /**
     * Returns the count of observations on the unlabeled value.
     *
     * @return The count of observed values.
     */
    public synchronized long getCounter() {
        return labels().getCounter();
    }

    /**
     * Return a DataMetricTimer for making timing observations on this metric. Times captured in
     * seconds!
     *
     * @return the new DataMetricTimer instance to use
     */
    public synchronized DataMetricTimer startTimer() { return labels().startTimer(); }

    /**
     * The scalar type builder.
     */
    public static class Builder extends DataMetricBuilder<Builder> {

        // simple quantile configuration support just for the purposes of being able to pass through to another metrics
        // client, like Prometheus
        private List<Quantile> quantiles = new ArrayList<Quantile>();
        private long maxAgeSeconds = DEFAULT_MAX_AGE_SECS;
        private int ageBuckets = DEFAULT_AGE_BUCKETS;

        /**
         * Adds a quantile configuration to the Summary builder.
         *
         * @param quantile the quantile setting to configure
         * @param error the error tolerance for this quantile
         * @return the Builder instance
         */
        public Builder withQuantile(double quantile, double error) {
            // PJS: these value bounds checks are right out of the prometheus Summary Builder implementation
            if (quantile < 0.0 || quantile > 1.0) {
                throw new IllegalArgumentException("Quantile " + quantile + " invalid: Expected number between 0.0 and 1.0.");
            }
            if (error < 0.0 || error > 1.0) {
                throw new IllegalArgumentException("Error " + error + " invalid: Expected number between 0.0 and 1.0.");
            }
            quantiles.add(new Quantile(quantile, error));
            return this;
        }

        /**
         * Set the maximum age a summary observation is used. The default is 10 mins (or 600 seconds).
         *
         * @param newMaxAgeSeconds  the new max age setting (in seconds)
         * @return the Builder instance
         */
        public Builder withMaxAgeSeconds(long newMaxAgeSeconds) {
            if ( newMaxAgeSeconds <= 0 ) {
                throw new IllegalArgumentException("maxAgeSeconds must be a postive number.");
            }
            this.maxAgeSeconds = newMaxAgeSeconds;
            return this;
        }

        /**
         * Set the # of age buckets to use. The default is 5.
         * @param newValue  the new value to use
         * @return the Builder instance
         */
        public Builder withAgeBuckets( int newValue ) {
            if ( newValue <= 0 ) {
                throw new IllegalArgumentException("The # of age buckets can't be less than 1");
            }
            this.ageBuckets = newValue;
            return this;
        }

        /**
         * Constructs the metric.
         *
         * @return The metric.
         */
        public DataMetricSummary build() {
            return new DataMetricSummary(name_, labelNames_, help_, severity_, urgent_, quantiles, maxAgeSeconds, ageBuckets);
        }
    }

    /**
     * Simple Quantile definition class -- this is a placeholder for quantile configuration data
     * until we truly support quantile calculation
     */
    static public class Quantile {
        private final double quantile;
        private final double error;

        /**
         * Constructs a Quantile with the specified quantile and error tolerance parameters.
         *
         * @param quantile  the quantile definition
         * @param error  the error tolerance level
         */
        public Quantile(double quantile, double error ) {
            this.quantile = quantile;
            this.error = error;
        }

        /**
         * Get the value of the quantile property.
         *
         * @return the current quantile value
         */
        public double getQuantile() { return quantile; }

        /**
         * Get the value of the error tolerance property.
         *
         * @return the error tolerance value
         */
        public double getError() { return error; }
    }

    /**
     * The Summary metric value holder class
     */
    static public class SummaryData extends DataMetricBase.LabeledMetricData<DataMetricSummary> {

        /**
         * Constructs a SummaryData instance belonging to the specified metric, and labelled with
         * the specified set of label values.
         *
         * @param owner  The metric that owns this data
         * @param labels  The list of label values associated with this data. This should correspond
         *                to the list of label names defined in the metric owner.
         *
         */
        public SummaryData(DataMetricSummary owner, String[] labels ) {
            super(owner, labels);
        }

        /**
         * Observes the value.
         *
         * @param value The value.
         */
        public synchronized void observe(final @Nonnull Double value) {
            metric.prometheusSummary.labels(getLabels()).observe(value);
            // notify observers of this new observation
            metric.getListeners().forEach(observer -> observer.summaryObserved(this, value ));
        }

        /**
         * Start a timer that will record an observation on this metric when it stops.
         *
         * @return a new Timer instance associated with this metric.
         */
        public DataMetricTimer startTimer() {
            return new DataMetricTimer(this::observe);
        }

        /**
         * Returns the sum.
         *
         * @return The sum.
         */
        public double getSum() {
            return metric.prometheusSummary.labels(getLabels()).get().sum;
        }

        /**
         * Returns the count of observed values.
         *
         * @return The count of observed values.
         */
        public long getCounter() {
            return (long)metric.prometheusSummary.labels(getLabels()).get().count;
        }


    }

    /**
     * Observer interface for monitoring changes to summary metrics
     */
    public interface SummaryListener {
        /**
         * Fired when the DataMetricSummary has recieved a new observation
         *
         * @param data  the labels SummaryData instance recieving the new observation
         * @param observation  the value of the new observation
         */
        void summaryObserved(final @Nonnull SummaryData data, final @Nonnull Double observation);
    }
}
