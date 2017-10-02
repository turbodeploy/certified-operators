package com.vmturbo.components.test.utilities.metric.writer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import com.google.common.annotations.VisibleForTesting;

import io.prometheus.client.Collector.Type;

import com.vmturbo.components.test.utilities.InfluxConnectionProperties;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouseVisitor;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricFamilyKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricMetadata;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.TimestampedSample;

/**
 * A visitor that stores metrics in InfluxDB.
 * <p>
 * The metric format fits pretty nicely into InfluxDB.
 *    Metric Family -> (ignore, but keep as tag)
 *    Metric -> InfluxDB Measurement
 *       Labels -> InfluxDB tags
 *    Timestamped Sample -> InfluxDB point (sample value is the field value)
 *
 * Additional information (e.g. the test name, harvester names) is also added as tags.
 *
 * The visitor manages the InfluxDB connection internally. It looks for system environment variables
 * to point it at the right InfluxDB instance (+ credentials). If these are
 * absent it defaults to out-of-the-box values to connect to a local InfluxDB instance
 */
public class InfluxMetricsWarehouseVisitor implements MetricsWarehouseVisitor {

    private String curHarvester;

    private MetricFamilyKey curFamilyKey;

    private MetricKey curKey;

    private final InfluxDB influxConnection;

    private final String database;

    private final InfluxPointFactory pointFactory;

    private BatchPoints points;

    private InfluxConnectionProperties props = new InfluxConnectionProperties();

    public InfluxMetricsWarehouseVisitor() {
        this(new DefaultInfluxPointFactory());
    }

    @VisibleForTesting
    InfluxMetricsWarehouseVisitor(@Nonnull final InfluxPointFactory pointFactory) {
        this.influxConnection = props.newInfluxConnection();
        this.database = props.getDatabase();
        this.pointFactory = pointFactory;
        try {
            influxConnection.ping();
            influxConnection.createDatabase(database);
        } catch (RuntimeException e) {
            throw new InfluxUnavailableException(e);
        }
    }

    @Override
    public void onStartWarehouse(@Nonnull final String testName) {
        this.points = BatchPoints.database(database)
            .retentionPolicy("autogen")
            .consistency(ConsistencyLevel.ALL)
            .tag("testName", testName)
            .build();
    }

    @Override
    public void onEndWarehouse() {
        influxConnection.write(Objects.requireNonNull(this.points));
    }

    @Override
    public void onStartHarvester(@Nonnull final String harvesterName) {
        curHarvester = harvesterName;
    }

    @Override
    public void onEndHarvester() {
        curHarvester = null;
    }

    @Override
    public void onStartMetricFamily(@Nonnull final MetricFamilyKey key) {
        curFamilyKey = key;
    }

    @Override
    public void onEndMetricFamily() {
        curFamilyKey = null;
    }

    @Override
    public void onStartMetric(@Nonnull final MetricKey key,
                              @Nonnull final MetricMetadata metadata) {
        // Set key first, because addPoint reads things from the current key.
        curKey = key;

        // We want to track maximum values for gauges. We want to graph the maximum of some
        // gauges (e.g. memory usage) across test runs, and relying on InfluxDB aggregation
        // of samples means our aggregated points will depend on the time the test ran. If the test
        // run crossed the aggregation boundary (e.g. it ran from 9:58 to 10:03, with hourly
        // aggregation) there will be two sample points instead of just one. That leads to
        // charts that are hard to read.
        if (curFamilyKey.getType().equals(Type.GAUGE)) {
            points.point(pointFactory.createPoint(curKey.getName() + "_max",
                    curHarvester, curFamilyKey, curKey,
                    metadata.getLatestSampleTimeMs(),
                    metadata.getMaxValue()));
        } else if (curFamilyKey.getType().equals(Type.COUNTER)) {
            // Counters are, by definition, non-decreasing, so the maximum value of the counter
            // is just the total number of increments for the counter over the course of the test
            // run. Save that into a single sample that can represent the test run.
            points.point(pointFactory.createPoint(curKey.getName() + "_total",
                    curHarvester, curFamilyKey, curKey,
                    metadata.getLatestSampleTimeMs(),
                    metadata.getMaxValue()));
        }
    }

    @Override
    public void onEndMetric() {
        curKey = null;
    }

    @Override
    public void onSample(@Nonnull final TimestampedSample sample) {
        points.point(pointFactory.createPoint(
                curKey.getName(), curHarvester, curFamilyKey, curKey, sample.timeMs, sample.value));
    }

    /**
     * Exception thrown when the visitor fails to connect to the influxDB instance.
     */
    public static class InfluxUnavailableException extends RuntimeException {
        private InfluxUnavailableException(@Nonnull final Throwable cause) {
            super("Unable to reach InfluxDB.", cause);
        }
    }

    public static class DefaultInfluxPointFactory implements InfluxPointFactory {

        @Nonnull
        @Override
        public Point createPoint(@Nonnull final String measurement,
                                 @Nonnull final String source,
                                 @Nonnull final MetricFamilyKey familyKey,
                                 @Nonnull final MetricKey metricKey,
                                 final long timeMs,
                                 final double value) {
            return Point.measurement(measurement)
                    .tag("source", Objects.requireNonNull(source))
                    .tag("family", Objects.requireNonNull(familyKey).getType().toString())
                    .tag(metricKey.getLabels())
                    .time(timeMs, TimeUnit.MILLISECONDS)
                    .addField("value", value)
                    .build();
        }
    }

}
