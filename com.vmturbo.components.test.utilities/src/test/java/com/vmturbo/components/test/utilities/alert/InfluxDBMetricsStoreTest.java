package com.vmturbo.components.test.utilities.alert;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Pong;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Result;
import org.influxdb.dto.QueryResult.Series;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.components.test.utilities.alert.InfluxDBMetricsStore.MetricsLookupException;

public class InfluxDBMetricsStoreTest {
    private InfluxDB influxDB = mock(InfluxDB.class);

    private InfluxDBMetricsStore statsStore = new InfluxDBMetricsStore(influxDB);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testStdDeviation() {
        when(influxDB.query(any())).thenReturn(makeResult(10.0));

        assertEquals(10,
                statsStore.getStandardDeviation("test", "metric", Collections.emptyMap(), 1).get(),
                0);
    }

    @Test
    public void testStdDeviationNoValue() {
        when(influxDB.query(any())).thenReturn(makeResult(null));

        assertEquals(Optional.empty(),
                statsStore.getStandardDeviation("test", "metric", Collections.emptyMap(), 1));
    }

    @Test
    public void testAvg() {
        when(influxDB.query(any())).thenReturn(makeResult(10.0));

        assertEquals(10,
                statsStore.getAvg("test", "metric", Collections.emptyMap(), 1),
                0);
    }

    @Test
    public void testLatestSample() {
        when(influxDB.query(any())).thenReturn(makeResult(10.0));

        assertEquals(10,
            statsStore.getLatestSample("test", "metric", Collections.emptyMap(), 1),
            0);
    }

    @Test
    public void testEmptyTagString() {
        assertEquals("", InfluxDBMetricsStore.buildTagString(Collections.emptyMap()));
    }

    @Test
    public void testSingleTagString() {
        assertEquals("AND \"tag\" = \'value\'",
                InfluxDBMetricsStore.buildTagString(ImmutableMap.of("tag", "value")));
    }

    @Test
    public void testTwoTagsString() {
        assertEquals("AND \"tag1\" = \'value1\' AND \"tag2\" = \'value2\'",
            InfluxDBMetricsStore.buildTagString(ImmutableMap.of(
                "tag1", "value1",
                "tag2", "value2")));
    }

    @Test
    public void testSampleWithError() {
        QueryResult queryResult = new QueryResult();
        queryResult.setError("Failed");

        when(influxDB.query(any())).thenReturn(queryResult);
        expectedException.expect(MetricsLookupException.class);

        statsStore.getLatestSample("test", "metric", Collections.emptyMap(), 1);
    }

    @Test
    public void testMetricNamesFromSeries() {
        final Collection<String> metricNames = InfluxDBMetricsStore.metricNamesFromSeries(
            Stream.of(
                Collections.singletonList("mkt_process_result_count,family=SUMMARY,source=market,testName=test50kTopology"),
                Collections.singletonList("mkt_economy_build,family=SUMMARY,quantile=0.9,source=market,testName=test50kTopology"),
                Collections.singletonList("mkt_economy_build,family=SUMMARY,quantile=0.5,source=market,testName=test50kTopology"),
                Collections.singletonList("process_cpu_seconds_total,family=COUNTER,source=market,testName=test50kTopology")
            )
        );

        assertThat(metricNames, contains("mkt_process_result_count", "mkt_economy_build", "process_cpu_seconds_total"));
    }

    @Test
    public void testAvailable() {
        when(influxDB.ping()).thenReturn(mock(Pong.class));
        assertTrue(statsStore.isAvailable());
    }

    @Test
    public void testUnavailable() {
        when(influxDB.ping()).thenThrow(new RuntimeException("Influx unavailable"));
        assertFalse(statsStore.isAvailable());
    }

    private QueryResult makeResult(@Nullable Double value) {
        QueryResult queryResult = new QueryResult();
        Result result = new Result();
        Series series = new Series();

        final long timestamp = 10;
        List<Object> values = Arrays.asList(timestamp, value);
        series.setValues(Collections.singletonList(values));
        result.setSeries(Collections.singletonList(series));
        queryResult.setResults(Collections.singletonList(result));
        return queryResult;
    }
}
