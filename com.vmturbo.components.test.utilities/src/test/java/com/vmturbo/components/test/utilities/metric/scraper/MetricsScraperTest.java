package com.vmturbo.components.test.utilities.metric.scraper;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.Collector.Type;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;

import com.vmturbo.components.test.utilities.metric.MetricTestUtil;
import com.vmturbo.components.test.utilities.metric.MetricTestUtil.TestMetricsScraper;
import com.vmturbo.components.test.utilities.metric.MetricTestUtil.TestWarehouseVisitor;
import com.vmturbo.components.test.utilities.metric.scraper.LocalMetricsScraper;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricFamilyKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricMetadata;

public class MetricsScraperTest {

    private final Clock clock = Mockito.mock(Clock.class);

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setup() {
        Mockito.when(clock.millis()).thenReturn(1000L);
    }

    @Test
    public void testOneMetric() throws Exception {
        final String familyName = "simple";

        MetricFamilySamples initialSample = MetricTestUtil.createSimpleFamily(familyName, 10);
        MetricFamilySamples secondSample = MetricTestUtil.createSimpleFamily(familyName, 11);

        final TestMetricsScraper testHarvester = new TestMetricsScraper(
                Collections.singletonList(initialSample), clock);
        testHarvester.scrape(true);
        testHarvester.overrideSamples(Collections.singletonList(secondSample));
        testHarvester.scrape(true);

        TestWarehouseVisitor visitor = new TestWarehouseVisitor();
        testHarvester.visit(visitor);
        visitor.checkCollectedMetrics(Arrays.asList(initialSample, secondSample), clock);
    }

    @Test
    public void testTwoFamilies() throws Exception {
        List<MetricFamilySamples> familySamples = Arrays.asList(
                MetricTestUtil.createSimpleFamily("foo", 10),
                MetricTestUtil.createSimpleFamily("bar", 11));
        final TestMetricsScraper testHarvester = new TestMetricsScraper(familySamples, clock);
        testHarvester.scrape(true);

        final TestWarehouseVisitor visitor = new TestWarehouseVisitor();
        testHarvester.visit(visitor);
        visitor.checkCollectedMetrics(familySamples, clock);
    }

    @Test
    public void testTwoMetrics() throws Exception {
        final String familyName = "twoMetrics";
        final MetricFamilySamples samples = MetricTestUtil.createSimpleFamilyWithSamples(
                familyName, MetricTestUtil.createOneLabelSample(10), MetricTestUtil.createNoLabelSample(11));

        final TestMetricsScraper testHarvester = new TestMetricsScraper(Collections.singletonList(samples), clock);
        testHarvester.scrape(false);

        final TestWarehouseVisitor visitor = new TestWarehouseVisitor();
        testHarvester.visit(visitor);
        visitor.checkCollectedMetrics(Collections.singletonList(samples), clock);
    }

    @Test
    public void testMetadata() throws Exception {
        Clock clock = Mockito.mock(Clock.class);
        Mockito.when(clock.millis()).thenReturn(100L);

        final CollectorRegistry registry = new CollectorRegistry();

        final LocalMetricsScraper localMetricsScraper = new LocalMetricsScraper(registry, clock);
        Gauge gauge = Gauge.build("name", "help").register(registry);

        gauge.set(5);

        localMetricsScraper.scrape(true);

        Mockito.when(clock.millis()).thenReturn(200L);

        gauge.set(1);

        localMetricsScraper.scrape(true);
        final TestWarehouseVisitor visitor = new TestWarehouseVisitor();
        localMetricsScraper.visit(visitor);

        // We expect exactly one metadata entry, so we can expedite the retrieval process via
        // hacky-looking iteration.
        MetricMetadata metadata = visitor.getMetadata()
                // Get the map of metric key to metadata.
                .entrySet().iterator().next().getValue()
                // Get the metadata from the map of metric key to metadata.
                .entrySet().iterator().next().getValue();
        Assert.assertEquals(200L, metadata.getLatestSampleTimeMs());
        Assert.assertEquals(5.0, metadata.getMaxValue(), 0);
    }

    @Test
    public void testHarvestAll() throws Exception {
        final MetricFamilySamples histogram = new MetricFamilySamples("histogram",
                Type.HISTOGRAM, "help", Collections.emptyList());
        final MetricFamilySamples summary = new MetricFamilySamples("summary",
                Type.SUMMARY, "help", Collections.emptyList());
        final TestMetricsScraper harvester = new TestMetricsScraper(ImmutableList.of(histogram, summary), clock);
        harvester.scrape(false);

        final TestWarehouseVisitor visitor = new TestWarehouseVisitor();
        harvester.visit(visitor);
        visitor.checkCollectedMetrics(Collections.emptyList(), clock);

        harvester.scrape(true);

        harvester.visit(visitor);
        visitor.checkCollectedMetrics(Arrays.asList(histogram, summary), clock);
    }

    @Test
    public void testBadSample() throws Exception {
        final Sample badSample = new Sample("sample",
                Collections.singletonList("name"),
                // Labels without values are illegal.
                Collections.emptyList(),
                10);

        final TestMetricsScraper testHarvester =
                new TestMetricsScraper(
                    Collections.singletonList(
                        MetricTestUtil.createSimpleFamilyWithSamples("bad", badSample)),
                    clock);
        expectedException.expect(IllegalArgumentException.class);
        testHarvester.scrape(true);
    }

    @Test
    public void testMetricsKeyEq() throws Exception {
        final Sample sample = MetricTestUtil.createOneLabelSample(11);
        final MetricKey key = new MetricKey(sample);
        final MetricKey sameKey = new MetricKey(sample);
        final MetricKey diffKey = new MetricKey(MetricTestUtil.createNoLabelSample(11));

        Assert.assertEquals(key, key);
        Assert.assertEquals(key.hashCode(), key.hashCode());
        Assert.assertEquals(key, sameKey);
        Assert.assertEquals(key.hashCode(), sameKey.hashCode());
        Assert.assertNotEquals(key, diffKey);
        Assert.assertNotEquals(key, null);
        Assert.assertNotEquals(key, sample);
    }

    @Test
    public void testMetricFamilyKeyEq() throws Exception {
        final MetricFamilyKey key = new MetricFamilyKey("foo", Type.COUNTER, "help");
        final MetricFamilyKey sameKey = new MetricFamilyKey("foo", Type.COUNTER, "help");
        final MetricFamilyKey sameKeyNoDoc = new MetricFamilyKey("foo", Type.COUNTER, "");
        final MetricFamilyKey diffKeyType = new MetricFamilyKey("foo", Type.GAUGE, "");
        final MetricFamilyKey diffKeyName = new MetricFamilyKey("bar", Type.COUNTER, "");

        Assert.assertEquals(key, key);
        Assert.assertEquals(key.hashCode(), key.hashCode());
        Assert.assertEquals(key, sameKey);
        Assert.assertEquals(key.hashCode(), sameKey.hashCode());
        Assert.assertEquals(key, sameKeyNoDoc);
        Assert.assertEquals(key.hashCode(), sameKeyNoDoc.hashCode());
        Assert.assertNotEquals(key, diffKeyType);
        Assert.assertNotEquals(key, diffKeyName);
        Assert.assertNotEquals(key, null);
        Assert.assertNotEquals(key, "otherObject");
    }
}
