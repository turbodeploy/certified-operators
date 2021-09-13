package com.vmturbo.sql.utils.pool;

import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Sampling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * This class periodically logs information about state of the connection pool and also reports
 * it to prometheus. This uses the metrics listed in HikariCP wiki
 * {@see <a href="https://github.com/brettwooldridge/HikariCP/wiki/Dropwizard-Metrics">HikariCP Dropwizard Metrics</a>}
 * to report the state of connection pool.
 */
@ThreadSafe
public class HikariPoolMonitor {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Category for mean wait time to get a connection from pool.
     */
    private static final String POOL_WAIT_TIME_CATEGORY = "wait_time";
    /**
     * Category for mean time a connection is out of the pool.
     */
    private static final String POOL_CONNECTION_IN_USE_TIME_CATEGORY = "in_use_time";
    /**
     * Category for total connections in the pool.
     */
    private static final String POOL_TOTAL_CONNECTIONS_CATEGORY = "total_connections";
    /**
     * Category for idle connections in the pool.
     */
    private static final String POOL_IDLE_CONNECTIONS_CATEGORY = "idle_connections";
    /**
     * Category for active connections in the pool.
     */
    private static final String POOL_ACTIVE_CONNECTIONS_CATEGORY = "active_connections";
    /**
     * Category for pending connections requests of the pool.
     */
    private static final String POOL_PENDING_CONNECTIONS_CATEGORY = "pending_connections";

    /**
     * Prometheus metric to capture mean connection wait time.
     */
    private static final DataMetricGauge POOL_TIME_GAUGE = DataMetricGauge.builder()
            .withName("pool_wait_time")
            .withHelp("Mean time for pool(ms)")
            .withLabelNames("schema", "category")
            .build()
            .register();

    /**
     * Prometheus metric to capture connection counts collected by {@link HikariPoolMonitor}.
     */
    private static final DataMetricGauge CONNECTION_COUNTS_GAUGE = DataMetricGauge.builder()
            .withName("pool_connections")
            .withHelp("Connection counts reported by connection pool")
            .withLabelNames("schema", "category")
            .build()
            .register();

    /**
     * The period that the pool state must be logged even if there has been no change.
     */
    private static final long LOGGING_MAXIMUM_INTERVAL = TimeUnit.MINUTES.toMillis(30);

    private final MetricRegistry metricRegistry;
    private final String poolName;
    private final String dbSchemaName;
    private final ScheduledExecutorService executorService;
    private Reading previousReading;
    private Long lastLoggingTimestamp;

    /**
     * Creates an instance of pool monitor.
     *
     * @param poolName        the name of the connection pool. This is required to access metrics in the
     *                          metrics registry.
     * @param intervalSec     the interval to reports state of the pool in logs.
     * @param dbSchemaName    the db schema that this pool works with. Used for report to prometheus.
     * @param executorService the executor service for scheduling the polling.
     */
    public HikariPoolMonitor(@Nonnull final String poolName,
                             final int intervalSec,
                             @Nonnull final String dbSchemaName,
                             @Nonnull final ScheduledExecutorService executorService) {
        this.metricRegistry = new MetricRegistry();
        this.poolName = Objects.requireNonNull(poolName);
        this.dbSchemaName = Objects.requireNonNull(dbSchemaName);
        this.executorService = executorService;
        executorService.scheduleWithFixedDelay(this::runOnePass, 0, intervalSec,
                TimeUnit.SECONDS);
    }

    /**
     * Gets the metric registry for the pool.
     *
     * @return the metric registry for the pool.
     */
    @Nonnull
    public MetricRegistry getMetricRegistry() {
        return metricRegistry;
    }

    /**
     * Runs one pass of monitoring process.
     */
    private synchronized void runOnePass() {
        Reading reading = new Reading(metricRegistry, poolName);
        reportMetrics(reading);
        logPoolMetrics(reading);
    }

    private void logPoolMetrics(final Reading reading) {
        if (!reading.equals(previousReading)
                || System.currentTimeMillis() - lastLoggingTimestamp > LOGGING_MAXIMUM_INTERVAL) {
            lastLoggingTimestamp = System.currentTimeMillis();
            logger.info("POOL HEALTH REPORT: "
                    + "Pool Name: " + poolName + " "
                    + reading);
        }
        previousReading = reading;
    }

    private void reportMetrics(Reading reading) {
        reportMetric(POOL_TIME_GAUGE, dbSchemaName, POOL_WAIT_TIME_CATEGORY,
                reading.waitTimeInMillis);
        reportMetric(POOL_TIME_GAUGE, dbSchemaName, POOL_CONNECTION_IN_USE_TIME_CATEGORY,
                reading.outsidePoolTimeInMillis);
        reportMetric(CONNECTION_COUNTS_GAUGE, dbSchemaName, POOL_TOTAL_CONNECTIONS_CATEGORY,
                reading.totalConnections);
        reportMetric(CONNECTION_COUNTS_GAUGE, dbSchemaName, POOL_ACTIVE_CONNECTIONS_CATEGORY,
                reading.activeConnections);
        reportMetric(CONNECTION_COUNTS_GAUGE, dbSchemaName, POOL_IDLE_CONNECTIONS_CATEGORY,
                reading.idleConnections);
        reportMetric(CONNECTION_COUNTS_GAUGE, dbSchemaName, POOL_PENDING_CONNECTIONS_CATEGORY,
                reading.pendingConnections);
    }

    private void reportMetric(@Nonnull DataMetricGauge gauge,
            @Nonnull String dbSchemaName,
            @Nonnull String label,
            @Nullable Number reading) {
        if (reading != null) {
            gauge.labels(dbSchemaName, label).setData(reading.doubleValue());
        }
    }

    /**
     * A reading of the pool metrics.
     */
    @Immutable
    private static class Reading {
        /**
         * According to Hikari wiki:
         *
         * <p>A Timer instance collecting how long requesting threads to getConnection() are
         * waiting for a connection (or timeout exception) from the pool. </p>
         */
        private static final String POOL_WAIT_TIME = ".pool.Wait";

        /**
         * According Hikari wiki:
         *
         * <p>A Histogram instance collecting how long each connection is used before
         * being returned to the pool. This is the "out of pool" or "in-use" time.</p>
         */
        private static final String POOL_CONNECTION_IN_USE_TIME = ".pool.Usage";

        /**
         * According to Hikari wiki:
         *
         * <p>A CachedGauge, refreshed on demand at 1 second resolution, indicating the total
         * number of connections in the pool.</p>
         */
        private static final String POOL_TOTAL_CONNECTIONS = ".pool.TotalConnections";

        /**
         * According to Hikari wiki:
         *
         * <p>A CachedGauge, refreshed on demand at 1 second resolution, indicating the
         * number of idle connections in the pool.</p>
         */
        private static final String POOL_IDLE_CONNECTIONS = ".pool.IdleConnections";

        /**
         * According to Hikari wiki:
         *
         * <p>A CachedGauge, refreshed on demand at 1 second resolution, indicating
         * the number of active (in-use) connections in the pool.</p>
         */
        private static final String POOL_ACTIVE_CONNECTIONS = ".pool.ActiveConnections";

        /**
         * According to Hikari wiki:
         *
         * <p>A CachedGauge, refreshed on demand at 1 second resolution, indicating the number
         * of threads awaiting
         * connections from the pool.</p>
         */
        private static final String POOL_PENDING_CONNECTIONS = ".pool.PendingConnections";


        private final Long waitTimeInMillis;
        private final Long outsidePoolTimeInMillis;
        private final Integer totalConnections;
        private final Integer activeConnections;
        private final Integer idleConnections;
        private final Integer pendingConnections;

        private Reading(@Nonnull MetricRegistry metricRegistry, @Nonnull String poolName) {
            Objects.requireNonNull(metricRegistry);
            Objects.requireNonNull(poolName);
            waitTimeInMillis = getSamplingMean(metricRegistry, poolName, POOL_WAIT_TIME);
            outsidePoolTimeInMillis = getSamplingMean(metricRegistry, poolName,
                    POOL_CONNECTION_IN_USE_TIME);
            totalConnections = getGaugeValue(metricRegistry, poolName, POOL_TOTAL_CONNECTIONS);
            idleConnections = getGaugeValue(metricRegistry, poolName, POOL_IDLE_CONNECTIONS);
            activeConnections = getGaugeValue(metricRegistry, poolName, POOL_ACTIVE_CONNECTIONS);
            pendingConnections = getGaugeValue(metricRegistry, poolName, POOL_PENDING_CONNECTIONS);
        }

        @Nullable
        private Long getSamplingMean(@Nonnull MetricRegistry metricRegistry,
                                  @Nonnull String poolName, @Nonnull String name) {
            Metric timer = metricRegistry.getMetrics().get(poolName + name);
            if (timer instanceof Sampling) {
                return TimeUnit.NANOSECONDS
                        .toMillis((long)((Sampling)timer).getSnapshot().getMean());
            } else {
                logger.trace("The sampling value for {} for pool {} is not present.", name, poolName);
                return null;
            }
        }

        @Nullable
        private Integer getGaugeValue(@Nonnull MetricRegistry metricRegistry,
                                      @Nonnull String poolName, @Nonnull String name) {
            Metric gauge = metricRegistry.getMetrics().get(poolName + name);
            if (gauge instanceof Gauge) {
                logger.trace("The gauge value for {} for pool {} is not present.", name, poolName);
                return ((Gauge<Integer>)gauge).getValue();
            } else {
                return null;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(waitTimeInMillis, outsidePoolTimeInMillis, totalConnections,
                    idleConnections, activeConnections, pendingConnections);
        }

        @Override
        public boolean equals(final Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }

            Reading other = (Reading)obj;
            return Objects.equals(waitTimeInMillis, other.waitTimeInMillis)
                    && Objects.equals(outsidePoolTimeInMillis, other.outsidePoolTimeInMillis)
                    && Objects.equals(totalConnections, other.totalConnections)
                    && Objects.equals(idleConnections, other.idleConnections)
                    && Objects.equals(activeConnections, other.activeConnections)
                    && Objects.equals(pendingConnections, other.pendingConnections);
        }

        @Override
        public String toString() {
            return "Mean wait time for pool(ms): " + waitTimeInMillis + ", "
                + "Mean time for a connection to be outside pool(ms): "
                    + outsidePoolTimeInMillis + ", "
                + "Total connections: " + totalConnections + ", "
                + "Active connections: " + activeConnections + ", "
                + "Idle connections: " + idleConnections + ", "
                + "Pending connection requests: " + pendingConnections + " ";
        }
    }
}
