package com.vmturbo.ml.datastore.influx;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.influxdb.BatchOptions;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;

/**
 * Factory for generating connections to influxdb.
 *
 * Includes numerous configurations for the influx connection.
 */
public class InfluxMetricsWriterFactory {
    private final String influxDbDatabase;
    private final String influxConnectionUrl;
    private final String influxConnectionUser;
    private final String influxConnectionPassword;
    private final String shardDuration;
    private final String retentionPeriod;
    private final String retentionPolicyName;
    public final int influxBatchSize;
    private final boolean gzipToInflux;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Create a new InfluxDBConnectionFactory. Not publicly visible. Prefer use of the
     * associated {@link Builder}.
     *
     * @param influxDbDatabase The name of the influx database to connect and write to.
     * @param influxConnectionUrl The url to use to connect to the influx database.
     * @param influxConnectionUser The user to use when connecting to influx.
     * @param influxConnectionPassword The password to use when connecting to influx.
     * @param shardDuration The period of time that a shard should be kept before rolling
     *                      over into a new shard. For more details on how shards and retention
     *                      policies interact, see:
     *                      https://www.influxdata.com/blog/influxdb-shards-retention-policies/
     *                      Format examples: 1w, 3d, 600s, etc.
     * @param retentionPeriod The length of time to retain data.
     *                        Format examples: 1w, 3d, 600s, etc.
     * @param retentionPolicyName The influx retention policy to use.
     * @param influxBatchSize The batch size to use when batching data points sent to influx.
     * @param gzipToInflux Whether to gzip data sent to influx.
     */
    private InfluxMetricsWriterFactory(@Nonnull final String influxDbDatabase,
                                       @Nonnull final String influxConnectionUrl,
                                       @Nonnull final String influxConnectionUser,
                                       @Nonnull final String influxConnectionPassword,
                                       @Nonnull final String shardDuration,
                                       @Nonnull final String retentionPeriod,
                                       @Nonnull final String retentionPolicyName,
                                       final int influxBatchSize,
                                       final boolean gzipToInflux) {
        this.influxDbDatabase = Objects.requireNonNull(influxDbDatabase);
        this.influxConnectionUrl = Objects.requireNonNull(influxConnectionUrl);
        this.influxConnectionUser = Objects.requireNonNull(influxConnectionUser);
        this.influxConnectionPassword = Objects.requireNonNull(influxConnectionPassword);
        this.shardDuration = Objects.requireNonNull(shardDuration);
        this.retentionPeriod = Objects.requireNonNull(retentionPeriod);
        this.retentionPolicyName = Objects.requireNonNull(retentionPolicyName);
        this.influxBatchSize = influxBatchSize;
        this.gzipToInflux = gzipToInflux;
    }

    /**
     * Get the name of the influx database to use.
     *
     * @return the name of the influx database to use.
     */
    @Nonnull
    public String getDatabase() {
        return influxDbDatabase;
    }

    /**
     * Get the name of the influx retention policy to use.
     *
     * @return the name of the influx database to use.
     */
    @Nonnull
    public String getRetentionPolicyName() {
        return retentionPolicyName;
    }

    /**
     * Create a new metricsWriter to write metrics to influx.
     *
     * @param whitelist The whitelist of commodities and metrics to write.
     * @param metricJitter The jitter to apply to metrics.
     * @param obfuscator An obfuscator to obfuscate sensitive customer information.
     * @return a new metricsWriter to write metrics to influx.
     * @throws InfluxUnavailableException If no connection to influx can be established.
     */
    @Nonnull
    public InfluxTopologyMetricsWriter createTopologyMetricsWriter(@Nonnull final MetricsStoreWhitelist whitelist,
                                                   @Nonnull final MetricJitter metricJitter,
                                                   @Nonnull final Obfuscator obfuscator)
        throws InfluxUnavailableException {

            return new InfluxTopologyMetricsWriter(createInfluxConnection(), getDatabase(),
                    getRetentionPolicyName(), whitelist, metricJitter, obfuscator);
    }

    /**
     * Create a new actionMetricsWriter to write metrics to influx.
     *
     * @param whitelist The whitelist of commodities and metrics to write.
     * @return a new metricsWriter to write metrics to influx.
     * @throws InfluxUnavailableException If no connection to influx can be established.
     */
    @Nonnull
    public InfluxActionsWriter createActionMetricsWriter(@Nonnull final MetricsStoreWhitelist whitelist)
            throws InfluxUnavailableException {

            return new InfluxActionsWriter(createInfluxConnection(), getDatabase(),
                    getRetentionPolicyName(), whitelist);
    }

    /**
     * Create a new connection to InfluxDB via this factory.
     *
     * @return A new connection to influx, configured
     * @throws InfluxUnavailableException if no connection to influx can be established.
     */
    @Nonnull
    private InfluxDB createInfluxConnection() throws InfluxUnavailableException {
        try {
            final InfluxDB influxDB = InfluxDBFactory.connect(influxConnectionUrl,
                influxConnectionUser, influxConnectionPassword);

            if (gzipToInflux) {
                influxDB.enableGzip();
            }

            // Test that the connection is valid at the time of creation by pinging.
            influxDB.ping();

            // Enable batching.
            influxDB.enableBatch(BatchOptions.DEFAULTS.actions(influxBatchSize));

            // Even though this API is deprecated it is still necessary and the
            // documentation still says to call it.
            influxDB.createDatabase(getDatabase());
            influxDB.setDatabase(getDatabase());
            logger.info("Created connection to influxDB version {}, database name = {}",
                influxDB.version(), getDatabase());

            // Even though this API is deprecated it is still usable and the
            // documentation still says to call it.
            influxDB.createRetentionPolicy(retentionPolicyName, influxDbDatabase,
                retentionPeriod, shardDuration, 1, true);
            logger.info("Created retention policy {} with retention period {} and shard duration {}",
                retentionPolicyName, retentionPeriod, shardDuration);

            return influxDB;
        } catch (RuntimeException e) {
            throw new InfluxUnavailableException(e);
        }
    }

    /**
     * A builder for creating a new influx connection. Use to declaratively supply configuration
     * for the connection.
     */
    public static class Builder {
        private String influxDbDatabase;
        private String influxConnectionUrl;
        private String influxConnectionUser;
        private String influxConnectionPassword;
        private String shardDuration;
        private String retentionPeriod;
        private String retentionPolicyName;
        private int influxBatchSize;
        private boolean gzipToInflux;

        public Builder setInfluxDbDatabase(String influxDbDatabase) {
            this.influxDbDatabase = influxDbDatabase;
            return this;
        }

        public Builder setInfluxConnectionUrl(String influxConnectionUrl) {
            this.influxConnectionUrl = influxConnectionUrl;
            return this;
        }

        public Builder setInfluxConnectionUser(String influxConnectionUser) {
            this.influxConnectionUser = influxConnectionUser;
            return this;
        }

        public Builder setInfluxConnectionPassword(String influxConnectionPassword) {
            this.influxConnectionPassword = influxConnectionPassword;
            return this;
        }

        public Builder setRetentionPolicyName(String retentionPolicyName) {
            this.retentionPolicyName = retentionPolicyName;
            return this;
        }

        public Builder setInfluxBatchSize(int influxBatchSize) {
            this.influxBatchSize = influxBatchSize;
            return this;
        }

        public Builder setGzipToInflux(boolean gzipToInflux) {
            this.gzipToInflux = gzipToInflux;
            return this;
        }

        public Builder setShardDuration(String shardDuration) {
            this.shardDuration = shardDuration;
            return this;
        }

        public Builder setRetentionPeriod(String retentionPeriod) {
            this.retentionPeriod = retentionPeriod;
            return this;
        }

        public InfluxMetricsWriterFactory build() {
            return new InfluxMetricsWriterFactory(influxDbDatabase,
                influxConnectionUrl,
                influxConnectionUser,
                influxConnectionPassword,
                shardDuration,
                retentionPeriod,
                retentionPolicyName,
                influxBatchSize,
                gzipToInflux);
        }
    }

    /**
     * Exception thrown when the factory cannot establish a connection to the influxDB database.
     */
    public static class InfluxUnavailableException extends RuntimeException {
        private InfluxUnavailableException(@Nonnull final Throwable cause) {
            super("Unable to reach InfluxDB.", cause);
        }
    }
}
