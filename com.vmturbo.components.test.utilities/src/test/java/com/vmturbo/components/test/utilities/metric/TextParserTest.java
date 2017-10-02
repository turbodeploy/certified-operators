package com.vmturbo.components.test.utilities.metric;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.Collector.Type;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import io.prometheus.client.Summary;
import io.prometheus.client.exporter.common.TextFormat;
import io.prometheus.client.hotspot.DefaultExports;

import com.vmturbo.components.test.utilities.metric.TextParser.TextParseException;


/**
 * Unit tests for the {@link TextParser}.
 */
public class TextParserTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testSimpleMetric() throws TextParseException {
        final String simpleMetric =
            "# HELP metric Doc\n" +
            "# TYPE metric counter\n" +
            "metric 140.0";
        Collection<MetricFamilySamples> samples = TextParser.parse004(new StringReader(simpleMetric));
        Assert.assertEquals(1, samples.size());
        MetricFamilySamples sampleFamily = samples.iterator().next();
        Assert.assertEquals("metric", sampleFamily.name);
        Assert.assertEquals("Doc", sampleFamily.help);
        Assert.assertEquals(Type.COUNTER, sampleFamily.type);
        Assert.assertEquals(1, sampleFamily.samples.size());
        Sample sample = sampleFamily.samples.get(0);
        Assert.assertEquals("metric", sample.name);
        Assert.assertEquals(140.0, sample.value, 0);
    }

    @Test
    public void testNoDocString() throws TextParseException {
        final String simpleMetric =
            "# HELP metric\n" +
            "# TYPE metric counter\n" +
            "metric 140.0";
        List<MetricFamilySamples> samples = TextParser.parse004(new StringReader(simpleMetric));
        Assert.assertEquals(1, samples.size());
        MetricFamilySamples sampleFamily = samples.get(0);
        Assert.assertEquals("metric", sampleFamily.name);
        Assert.assertEquals("", sampleFamily.help);
        Assert.assertEquals(Type.COUNTER, sampleFamily.type);
        Assert.assertEquals(1, sampleFamily.samples.size());
        Sample sample = sampleFamily.samples.get(0);
        Assert.assertEquals("metric", sample.name);
        Assert.assertEquals(140.0, sample.value, 0);
    }

    @Test
    public void testTwoLabels() throws TextParseException {
        final String labelMetric =
            "# HELP label_metric Doc\n" +
            "# TYPE label_metric gauge\n" +
            "label_metric{label=\"foo\",other=\"bar\",} 0\n";

        final List<MetricFamilySamples> samples = TextParser.parse004(new StringReader(labelMetric));
        Assert.assertEquals(1, samples.size());

        final MetricFamilySamples sampleFamily = samples.get(0);
        Assert.assertEquals("label_metric", sampleFamily.name);
        Assert.assertEquals("Doc", sampleFamily.help);
        Assert.assertEquals(Type.GAUGE, sampleFamily.type);
        Assert.assertEquals(1, sampleFamily.samples.size());

        final Sample fooSample = sampleFamily.samples.get(0);
        Assert.assertEquals("label_metric", fooSample.name);
        Assert.assertEquals(0, fooSample.value, 0);
        Assert.assertEquals(2, fooSample.labelNames.size());
        Assert.assertEquals(2, fooSample.labelValues.size());
        Assert.assertEquals("label", fooSample.labelNames.get(0));
        Assert.assertEquals("foo", fooSample.labelValues.get(0));
        Assert.assertEquals("other", fooSample.labelNames.get(1));
        Assert.assertEquals("bar", fooSample.labelValues.get(1));
    }

    @Test
    public void testLabelMetric() throws TextParseException {
        final String labelMetric =
            "# HELP label_metric Doc\n" +
            "# TYPE label_metric gauge\n" +
            // One metric without a trailing comma.
            "label_metric{label=\"foo\"} 0\n" +
            // One metric with a trailing comma.
            "label_metric{label=\"bar\",} 1";

        final List<MetricFamilySamples> samples = TextParser.parse004(new StringReader(labelMetric));
        Assert.assertEquals(1, samples.size());

        final MetricFamilySamples sampleFamily = samples.get(0);
        Assert.assertEquals("label_metric", sampleFamily.name);
        Assert.assertEquals("Doc", sampleFamily.help);
        Assert.assertEquals(Type.GAUGE, sampleFamily.type);
        Assert.assertEquals(2, sampleFamily.samples.size());

        final Sample fooSample = sampleFamily.samples.get(0);
        Assert.assertEquals("label_metric", fooSample.name);
        Assert.assertEquals(0, fooSample.value, 0);
        Assert.assertEquals(1, fooSample.labelNames.size());
        Assert.assertEquals(1, fooSample.labelValues.size());
        Assert.assertEquals("label", fooSample.labelNames.get(0));
        Assert.assertEquals("foo", fooSample.labelValues.get(0));

        final Sample barSample = sampleFamily.samples.get(1);
        Assert.assertEquals("label_metric", barSample.name);
        Assert.assertEquals(1, barSample.value, 0);
        Assert.assertEquals(1, barSample.labelNames.size());
        Assert.assertEquals(1, barSample.labelValues.size());
        Assert.assertEquals("label", barSample.labelNames.get(0));
        Assert.assertEquals("bar", barSample.labelValues.get(0));
    }

    @Test
    public void testTwoMetrics() throws TextParseException {
        final String twoMetrics =
            "# HELP test_1 Doc\n" +
            "# TYPE test_1 counter\n" +
            "test_1 140.0\n" +
            "\n" +
            "# HELP test_2 Doc 2\n" +
            "# TYPE test_2 counter\n" +
            "test_2 140.0";

        final List<MetricFamilySamples> samples = TextParser.parse004(new StringReader(twoMetrics));
        Assert.assertEquals(2, samples.size());

        final MetricFamilySamples sampleFamily = samples.get(0);
        Assert.assertEquals("test_1", sampleFamily.name);
        Assert.assertEquals("Doc", sampleFamily.help);
        Assert.assertEquals(Type.COUNTER, sampleFamily.type);
        Assert.assertEquals(1, sampleFamily.samples.size());

        final Sample sample = sampleFamily.samples.get(0);
        Assert.assertEquals(140.0, sample.value, 0);

        final MetricFamilySamples family2 = samples.get(1);
        Assert.assertEquals("test_2", family2.name);
        Assert.assertEquals("Doc 2", family2.help);
        Assert.assertEquals(Type.COUNTER, family2.type);
        Assert.assertEquals(1, family2.samples.size());

        final Sample sample2 = family2.samples.get(0);
        Assert.assertEquals("test_2", sample2.name);
        Assert.assertEquals(140.0, sample2.value, 0);

    }

    @Test
    public void testEmpty() throws TextParseException {
        final List<MetricFamilySamples> samples = TextParser.parse004(new StringReader(""));
        Assert.assertTrue(samples.isEmpty());
    }

    @Test
    public void testBadInput() throws TextParseException {
        final String badLabelMetric =
                "# HELP label_metric Doc\n" +
                "# TYPE label_metric gauge\n" +
                "label_metric{noValue=} 0";
        expectedException.expect(TextParseException.class);
        TextParser.parse004(new StringReader(badLabelMetric));
    }

    @Test
    public void testBadLabel() throws TextParseException {
        expectedException.expect(TextParseException.class);
        TextParser.parse004(new StringReader("oteanrsotienars"));
    }

    @Test
    public void testMissingHelpLine() throws TextParseException {
        final String noTypeMetric =
                "# TYPE metric counter\n" +
                "metric 140.0";
        expectedException.expect(TextParseException.class);
        TextParser.parse004(new StringReader(noTypeMetric));
    }

    @Test
    public void roundTripTest() throws IOException, TextParseException {
        // Initialize the basic JVM metrics.
        DefaultExports.initialize();

        // Basic JVM metrics don't create a histogram or a summary, so add two of those.
        final Histogram histogram = Histogram.build()
            .name("testHist")
            .help("The histogram")
            .register();
        histogram.observe(10);

        final Summary summary = Summary.build()
            .name("testSummary")
            .help("The summary")
            .register();
        summary.observe(10);

        // Save the collected samples into a list, because we'll need to go
        // through it twice - to print, and to compare to parsed results.
        final List<MetricFamilySamples> originalList =
                Collections.list(CollectorRegistry.defaultRegistry.metricFamilySamples());

        final StringWriter stringWriter = new StringWriter();

        TextFormat.write004(stringWriter, Collections.enumeration(originalList));

        final List<MetricFamilySamples> parsedSamples =
                TextParser.parse004(new StringReader(stringWriter.toString()));

        Assert.assertEquals(originalList.size(), parsedSamples.size());
        // It's safe to expect that they will be printed in the order they were provided,
        // and that they will be parsed in the order they were printed.
        for (int i = 0; i < parsedSamples.size(); ++i) {
            Assert.assertEquals(parsedSamples.get(i), originalList.get(i));
        }
    }
}
