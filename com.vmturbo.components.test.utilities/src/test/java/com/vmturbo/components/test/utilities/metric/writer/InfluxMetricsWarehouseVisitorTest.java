package com.vmturbo.components.test.utilities.metric.writer;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.prometheus.client.Collector.Type;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDB.ConsistencyLevel;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;

import com.vmturbo.components.test.utilities.metric.MetricTestUtil;
import com.vmturbo.components.test.utilities.metric.MetricTestUtil.TestMetricsScraper;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouse;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricFamilyKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricMetadata;
import com.vmturbo.components.test.utilities.metric.writer.InfluxMetricsWarehouseVisitor.InfluxUnavailableException;

//@RunWith(PowerMockRunner.class)
//@PrepareForTest(InfluxDBFactory.class)
//// Prevent linkage error:
//// http://stackoverflow.com/questions/16520699/mockito-powermock-linkageerror-while-mocking-system-class
//@PowerMockIgnore("javax.management.*")
@Ignore("This unit test fails on Java11. Need to be fixed later")
public class InfluxMetricsWarehouseVisitorTest {

    private final InfluxDB influxConnection = Mockito.mock(InfluxDB.class);

    @Before
    public void setup() {
        PowerMockito.mockStatic(InfluxDBFactory.class);
        Mockito.when(InfluxDBFactory.connect(
                Mockito.anyString(), Mockito.anyString(), Mockito.anyString()))
                .thenReturn(influxConnection);
    }

    @Test
    public void testInfluxWriter() {

        final Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(1000L);

        final TestMetricsScraper testHarvester = new TestMetricsScraper(
                Collections.singletonList(MetricTestUtil.createSimpleFamilyWithSamples(
                        "family",
                        MetricTestUtil.createOneLabelSample(10),
                        MetricTestUtil.createNoLabelSample(10))), clock);

        final MetricsWarehouse farm = MetricsWarehouse.newBuilder()
                .addScraper(testHarvester)
                .build();
        testHarvester.scrape(true);

        final ArgumentCaptor<BatchPoints> pointsCaptor = ArgumentCaptor.forClass(BatchPoints.class);

        final InfluxMetricsWarehouseVisitor writer = new InfluxMetricsWarehouseVisitor();

        farm.visit("testInfluxWriter", writer);

        final InOrder inOrder = Mockito.inOrder(influxConnection);
        inOrder.verify(influxConnection).createDatabase(Mockito.eq("test"));
        inOrder.verify(influxConnection).write(pointsCaptor.capture());

        final BatchPoints writtenPoints = pointsCaptor.getValue();
        Assert.assertEquals(ConsistencyLevel.ALL, writtenPoints.getConsistency());
        Assert.assertEquals("test", writtenPoints.getDatabase());
        Assert.assertEquals("autogen", writtenPoints.getRetentionPolicy());

        final List<Point> points = writtenPoints.getPoints();
        // Each counter has 2 points - the sample, and the total.
        Assert.assertEquals(4, points.size());

        final List<String> gotPoints = points.stream()
            .map(Point::toString)
            .collect(Collectors.toList());

        // This is kind  of ugly, and makes for a brittle test, but the Point class' fields are all
        // private with no getters, and no equal override, so not sure what the alternative is.
        final List<String> expectedStrings = Arrays.asList(
                "Point [name=sample, time=1000, tags={family=COUNTER, source=test," +
                        " testName=testInfluxWriter}, precision=MILLISECONDS, fields={value=10.0}]",
                "Point [name=sample, time=1000, tags={family=COUNTER, name=value, source=test," +
                        " testName=testInfluxWriter}, precision=MILLISECONDS, fields={value=10.0}]",
                "Point [name=sample_total, time=1000, tags={family=COUNTER, source=test," +
                        " testName=testInfluxWriter}, precision=MILLISECONDS, fields={value=10.0}]",
                "Point [name=sample_total, time=1000, tags={family=COUNTER, name=value, source=test," +
                        " testName=testInfluxWriter}, precision=MILLISECONDS, fields={value=10.0}]");

        // For some reason using hamcrest fails:
        //    Assert.assertThat(gotPoints, Matchers.containsInAnyOrder(expectedStrings))
        // Comparing the size and asserting each element is found should be equivalent.
        Assert.assertEquals(expectedStrings.size(), gotPoints.size());
        for (String expectedStr : expectedStrings) {
            Assert.assertTrue(expectedStr, gotPoints.contains(expectedStr));
        }
    }

    @Test
    public void testMaxGauge() {
        final InfluxPointFactory pointFactory = Mockito.mock(InfluxPointFactory.class);
        Mockito.when(pointFactory.createPoint(Mockito.anyString(),
                    Mockito.anyString(), Mockito.any(), Mockito.any(),
                    Mockito.anyLong(), Mockito.anyDouble()))
               .thenReturn(Point.measurement("foo")
                       .addField("field", 0L)
                       .build());

        final InfluxMetricsWarehouseVisitor writer = new InfluxMetricsWarehouseVisitor(pointFactory);

        final MetricFamilyKey familyKey = Mockito.mock(MetricFamilyKey.class);
        Mockito.when(familyKey.getType()).thenReturn(Type.GAUGE);

        final MetricKey metricKey = Mockito.mock(MetricKey.class);
        Mockito.when(metricKey.getName()).thenReturn("test");

        final MetricMetadata metricMetadata = Mockito.mock(MetricMetadata.class);
        Mockito.when(metricMetadata.getMaxValue()).thenReturn(10.0);
        Mockito.when(metricMetadata.getLatestSampleTimeMs()).thenReturn(1000L);

        writer.onStartWarehouse("testMaxGauge");
        writer.onStartHarvester("testHarvester");
        writer.onStartMetricFamily(familyKey);
        writer.onStartMetric(metricKey, metricMetadata);

        Mockito.verify(pointFactory).createPoint(Mockito.eq("test_max"),
                Mockito.eq("testHarvester"),
                Mockito.any(), Mockito.any(),
                Mockito.eq(1000L),
                Mockito.eq(10.0));
    }

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testInfluxPingFails() {
        Mockito.when(influxConnection.ping()).thenThrow(new RuntimeException("BAD"));

        expectedException.expect(InfluxUnavailableException.class);
        new InfluxMetricsWarehouseVisitor();
    }

    @Test
    public void testInfluxCreateDbFails() {
        Mockito.doThrow(new RuntimeException("BAD!"))
               .when(influxConnection).createDatabase(Mockito.anyString());

        expectedException.expect(InfluxUnavailableException.class);
        new InfluxMetricsWarehouseVisitor();
    }

}
