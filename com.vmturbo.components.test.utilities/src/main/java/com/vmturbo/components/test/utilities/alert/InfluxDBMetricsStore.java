package com.vmturbo.components.test.utilities.alert;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.stringtemplate.v4.ST;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.components.test.utilities.InfluxConnectionProperties;
import com.vmturbo.components.test.utilities.metric.writer.InfluxMetricsWarehouseVisitor;

/**
 * A {@link MetricsStore} that looks up values in InfluxDB.
 * <p>
 * It assumes that the metrics in InfluxDB are stored in the format
 * produced by {@link InfluxMetricsWarehouseVisitor}.
 */
public class InfluxDBMetricsStore implements MetricsStore {

    private static final String STD_DEV_QUERY =
            "SELECT stddev(\"value\") FROM \"<metricName>\" WHERE \"testName\" = \'<testName>\' AND time > now() - <lookbackMs>ms <tags>";

    private static final String AVG_QUERY =
            "SELECT mean(\"value\") FROM \"<metricName>\" WHERE \"testName\" = \'<testName>\' AND TIME > now() - <lookbackMs>ms <tags>";

    private static final String LATEST_SAMPLE_QUERY =
            "SELECT last(\"value\") FROM \"<metricName>\" WHERE \"testName\" = \'<testName>\' AND TIME > now() - <lookbackMs>ms <tags>";

    private static final String SHOW_SERIES_QUERY =
            "SHOW SERIES WHERE testName=\'<testName>\'";

    private final InfluxConnectionProperties props = new InfluxConnectionProperties();

    private final InfluxDB influxDB;

    public static final String INFLUX_DATABASE_NAME = "test";

    public InfluxDBMetricsStore() {
        this.influxDB = props.newInfluxConnection();
    }

    @VisibleForTesting
    InfluxDBMetricsStore(@Nonnull final InfluxDB influxDB) {
        this.influxDB = influxDB;
    }

    @Nonnull
    @VisibleForTesting
    static String buildTagString(@Nonnull final Map<String, String> tags) {
        if (!tags.isEmpty()) {
            return "AND " + tags.entrySet().stream()
                    .map(entry -> "\"" + entry.getKey() + "\" = \'" + entry.getValue() + "\'")
                    .collect(Collectors.joining(" AND "));
        } else {
            return "";
        }
    }

    @Override
    public boolean isAvailable() {
        try {
            influxDB.ping();
            return true; // Connection is available.
        } catch (RuntimeException e) {
            return false; // Connection is unavailable.
        }
    }

    @Override
    public Optional<Double> getStandardDeviation(@Nonnull final String testName,
                                       @Nonnull final String metricName,
                                       @Nonnull final Map<String, String> labels,
                                       final long lookbackMs) {
        final String queryStr = new ST(STD_DEV_QUERY)
            .add("metricName", metricName)
            .add("testName", testName)
            .add("lookbackMs", lookbackMs)
            .add("tags", buildTagString(labels))
            .render();

        return Optional.ofNullable(executeSingleResultQuery(queryStr));
    }

    @Override
    public double getAvg(@Nonnull final String testName,
                         @Nonnull final String metricName,
                         @Nonnull final Map<String, String> labels,
                         final long lookbackMs) {
        final String queryStr = new ST(AVG_QUERY)
                .add("metricName", metricName)
                .add("testName", testName)
                .add("lookbackMs", lookbackMs)
                .add("tags", buildTagString(labels))
                .render();

        return executeSingleResultQuery(queryStr);
    }

    @Override
    public double getLatestSample(@Nonnull final String testName,
                                  @Nonnull final String metricName,
                                  @Nonnull final Map<String, String> labels,
                                  final long lookbackMs) {
        final String queryStr = new ST(LATEST_SAMPLE_QUERY)
            .add("metricName", metricName)
            .add("testName", testName)
            .add("lookbackMs", lookbackMs)
            .add("tags", buildTagString(labels))
            .render();

        return executeSingleResultQuery(queryStr);
    }

    @Override
    public Collection<String> getAllMetricNamesForTest(@Nonnull final String testName) {
        final String queryStr = new ST(SHOW_SERIES_QUERY)
            .add("testName", testName)
            .render();

        final Query query = new Query(queryStr, INFLUX_DATABASE_NAME);
        final QueryResult result = influxDB.query(query);

        if (result.hasError()) {
            throw new MetricsLookupException(result.getError());
        }

        return metricNamesFromSeries(result.getResults().get(0)
            .getSeries().get(0)
            .getValues().stream());
    }

    /**
     * Transform the stream of series into a collection of metric names.
     * A series is formatted in the following way:
     * [mkt_process_result_count,family=SUMMARY,source=market,testName=test50kTopology]
     *
     * @param listsOfSeries The lists of series to transform into a collection of metric names.
     * @return The series to transform.
     */
    @VisibleForTesting
    static Collection<String> metricNamesFromSeries(@Nonnull final Stream<List<Object>> listsOfSeries) {
        return listsOfSeries
            .map(list -> list.get(0).toString())
            .map(individualSeries -> individualSeries.split(",")[0])
            .distinct()
            .collect(Collectors.toList());
    }

    private Double executeSingleResultQuery(@Nonnull final String queryStr) {
        final Query query = new Query(queryStr, INFLUX_DATABASE_NAME);
        final QueryResult result = influxDB.query(query);

        if (result.hasError()) {
            throw new MetricsLookupException(result.getError());
        }

        return (Double)result.getResults().get(0)
            .getSeries().get(0)
            .getValues().get(0).get(1);
    }

    /**
     * An error that occurs when looking up metrics in InfluxDB.
     */
    public static class MetricsLookupException extends RuntimeException {
        public MetricsLookupException(@Nonnull final String errorMessage) {
            super(Objects.requireNonNull(errorMessage));
        }
    }
}
