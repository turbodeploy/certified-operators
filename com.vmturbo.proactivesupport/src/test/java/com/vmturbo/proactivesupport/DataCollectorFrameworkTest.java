package com.vmturbo.proactivesupport;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import com.vmturbo.proactivesupport.DataMetricGauge.GaugeData;

/**
 * The com.vmturbo.proactivesupport.DataCollectorFrameworkTest tests the DataCollectorFramework.
 */
public class DataCollectorFrameworkTest {
    /**
     * The test data.
     */
    private static final String TESTDATA = "The string to be tested.";

    @Before
    public void setup() {
        DataCollectorFramework.instance_ = null;
    }

    /**
     * Test the initial state.
     */
    @Test
    public void testInitial() {
        DataCollectorFramework.instance().setAggregatorBridge(new TestAggregatorBridge());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().analyzers_.size());
    }

    /**
     * Test the adding a single data collector.
     */
    @Test
    public void testAddOneCollector() {
        DataCollectorFramework.instance().setAggregatorBridge(new TestAggregatorBridge());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        DataMetricGauge metric = DataMetricGauge.builder().withName("name").withHelp("Help")
                        .withSeverity(
                                DataMetric.Severity.INFO).withUrgent().build()
                        .register();
        metric.setData(1.d);
        Assert.assertEquals(0, DataCollectorFramework.instance().analyzers_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
    }

    /**
     * Test the adding a single data analyzer.
     */
    @Test
    public void testAddOneAnalyzer() {
        DataCollectorFramework.instance().setAggregatorBridge(new TestAggregatorBridge());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        DataCollectorFramework.instance().addAnalyzer(new TestDataAnalyzer());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().analyzers_.size());
    }

    /**
     * Tests a single collection cycle with analyzers.
     */
    @Test
    public void testCollectionCycleWithAnalyzers() throws Exception {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setKeyValueCollector(() -> {return true;});
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        DataMetricLOB.builder()
                     .withName("heap")
                     .withHelp("Help")
                     .withSeverity(DataMetric.Severity.INFO)
                     .withData(new ByteArrayInputStream(TESTDATA.getBytes()))
                     .withUrgent()
                     .build().register();

        TestDataAnalyzer analyzer = new TestDataAnalyzer();
        DataCollectorFramework.instance().addAnalyzer(analyzer);
        Assert.assertEquals(1, DataCollectorFramework.instance().analyzers_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        // Test the data and severity
        DataCollectorFramework.instance().start(60000L, 0L);
        // Wait for it. Will throw InterruptedException in case of a timeout.
        long start = System.currentTimeMillis();
        while (aggregatorBridge.messages_ == null) {
            Thread.sleep(10L);
            // Make sure we don't hang.
            // 10 minutes is way too long. So, if 10 minutes expire - we've got a problem.
            if (System.currentTimeMillis() - start > 60000L) {
                break;
            }
        }
        Assert.assertEquals(TESTDATA, analyzer.testData);
        DataCollectorFramework.instance().stop();
        DataCollectorFramework.instance().runningThread.join();
        // Test the end results availability
        Assert.assertEquals(2, aggregatorBridge.messages_.size());
    }

    /**
     * Tests a single collection cycle with analyzers.
     * The data analyzers don't have any associated data collectors.
     */
    @Test
    public void testCollectionCycleWithAnalyzersNoIdMatch() throws Exception {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setKeyValueCollector(() -> {return true;});
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        DataMetricGauge metric = DataMetricGauge.builder().withName("name").withHelp("Help")
                        .withSeverity(DataMetric.Severity.INFO).withUrgent().build()
                        .register();
        metric.setData(1.d);

        TestDataAnalyzer analyzer = new TestDataAnalyzer();
        DataCollectorFramework.instance().addAnalyzer(analyzer);
        Assert.assertEquals(1, DataCollectorFramework.instance().analyzers_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        // Test the data and severity
        DataCollectorFramework.instance().start(60000L, 0L);
        // Wait for it. Will throw InterruptedException in case of a timeout.
        Assert.assertNull(analyzer.testData);
        DataCollectorFramework.instance().stop();
        DataCollectorFramework.instance().runningThread.join();
        // Test the end results availability
        Assert.assertEquals(1, aggregatorBridge.messages_.size());
    }

    /**
     * Tests an urgent collection cycle with analyzers.
     * We simulate an error reading the data.
     */
    @Test
    public void testUrgentCollectionErrorReading() throws Exception {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setKeyValueCollector(() -> {return true;});
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        DataMetricLOB.builder()
                     .withName("name")
                     .withHelp("Help")
                     .withSeverity(DataMetric.Severity.INFO)
                     .withData(new InputStream() {
                         @Override public int read() throws IOException {
                             throw new IOException();
                         }
                     })
                     .build().register();

        Assert.assertEquals(0, DataCollectorFramework.instance().analyzers_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().collectorsOffline_.size());
        // Test the data and severity
        DataCollectorFramework.instance().start(60000L, 0L);
        DataCollectorFramework.instance().stop();
        DataCollectorFramework.instance().runningThread.join();
        // Test the end results availability
        Assert.assertEquals(0, aggregatorBridge.messages_.size());
    }

    /**
     * Tests an urgent collection cycle with analyzers.
     * We simulate an error reading the data.
     * The analyzers have an error handling for the case when the LOB data collector
     * fails to read the data from its input stream.
     */
    @Test
    public void testUrgentCollectionErrorReadingWithAnalyzers() throws Exception {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        DataMetricLOB.builder()
                     .withName("name")
                     .withHelp("Help")
                     .withSeverity(DataMetric.Severity.INFO)
                     .withData(new InputStream() {
                         @Override public int read() throws IOException {
                             throw new IOException();
                         }
                     })
                     .build().register();
        DataMetricGauge gauge = DataMetricGauge.builder().withName("name").withSeverity(
                DataMetric.Severity.INFO).withUrgent().withHelp("Help").build()
                        .register();
        gauge.setData(1.d);
        DataCollectorFramework.instance().addAnalyzer(new TestDataAnalyzer());

        Assert.assertEquals(1, DataCollectorFramework.instance().analyzers_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().collectorsOffline_.size());
        // Test the data and severity
        long start = System.currentTimeMillis();
        DataCollectorFramework.instance().start(1000L, 0L);
        while (aggregatorBridge.messages_ == null) {
            if (System.currentTimeMillis() - start > 2000L) {
                break;
            }
            Thread.sleep(10L);
        }
        DataCollectorFramework.instance().stop();
        DataCollectorFramework.instance().runningThread.join();
        // Test the end results availability
        Assert.assertEquals(1, aggregatorBridge.messages_.size());
    }

    /**
     * Tests an offline collection cycle with analyzers.
     * We simulate an error reading the data.
     */
    @Test
    public void testOfflineCollectionErrorReading() throws Exception {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        DataMetricGauge gauge = DataMetricGauge.builder().withName("name").withHelp("Help")
                        .withSeverity(
                                DataMetric.Severity.INFO).build().register();

        Assert.assertEquals(0, DataCollectorFramework.instance().analyzers_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().collectorsOffline_.size());
        // Test the data and severity
        long start = System.currentTimeMillis();
        DataCollectorFramework.instance().start(1000L, 0L);
        while (aggregatorBridge.messagesOffline_ == null) {
            if (System.currentTimeMillis() - start > 2000L) {
                break;
            }
            Thread.sleep(10L);
        }
        DataCollectorFramework.instance().stop();
        DataCollectorFramework.instance().runningThread.join();
        // Test the end results availability
        Assert.assertEquals(0, aggregatorBridge.messages_.size());
    }

    /**
     * Tests a single collection cycle with offline data.
     */
    @Test
    public void testCollectionCycleOfflineData() throws Exception {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsOffline_.size());
        DataMetricLOB.builder()
                     .withName("name")
                     .withHelp("Help")
                     .withSeverity(DataMetric.Severity.INFO)
                     .withData(new ByteArrayInputStream("Hello".getBytes()))
                     .build().register();

        Assert.assertEquals(0, DataCollectorFramework.instance().collectorsUrgent_.size());
        Assert.assertEquals(1, DataCollectorFramework.instance().collectorsOffline_.size());
        // Test the data and severity
        long start = System.currentTimeMillis();
        DataCollectorFramework.instance().start(10L, 2000L);
        Assert.assertNull(aggregatorBridge.messagesOffline_);
        while (aggregatorBridge.messagesOffline_ == null) {
            if (System.currentTimeMillis() - start > 4000L) {
                break;
            }
            Thread.sleep(10L);
        }
        DataCollectorFramework.instance().stop();
        DataCollectorFramework.instance().runningThread.join();
        Assert.assertNotNull(aggregatorBridge.messagesOffline_);
        long duration = System.currentTimeMillis() - start;
        // Test the end results availability
        Assert.assertTrue(duration >= 2000L);
        Assert.assertEquals(1, aggregatorBridge.messagesOffline_.size());
    }

    /**
     * Tests the start and observe of the LDCF.
     */
    @Test
    public void testStartStop() {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        DataCollectorFramework.instance().start(60000L, 0L);
        DataCollectorFramework.instance().stop();
        Assert.assertFalse(DataCollectorFramework.instance().runningThread.isAlive());
    }

    /**
     * Tests the observe without the start.
     */
    @Test
    public void testInactiveStop() {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        DataCollectorFramework.instance().stop();
        Assert.assertNull(DataCollectorFramework.instance().runningThread);
    }

    /**
     * Tests the double observe.
     */
    @Test
    public void testDoubleStop() {
        TestAggregatorBridge aggregatorBridge = new TestAggregatorBridge();
        DataCollectorFramework.instance().setAggregatorBridge(aggregatorBridge);
        DataCollectorFramework.instance().start(60000L, 0L);
        DataCollectorFramework.instance().stop();
        Assert.assertFalse(DataCollectorFramework.instance().runningThread.isAlive());
        DataCollectorFramework.instance().stop();
        Assert.assertFalse(DataCollectorFramework.instance().runningThread.isAlive());
    }

    /**
     * The test data analyzer.
     */
    private static class TestDataAnalyzer implements DataAnalyzer {
        String testData = null;

        @Nonnull @Override public Iterable<String> getCollectorTypes() {
            return ImmutableList.of("heap");
        }

        @Nonnull @Override
        public DataMetric analyze(final @Nonnull Map<String, DataMetric> data)
                throws IOException {
            DataMetricLOB metric = (DataMetricLOB)data.values().iterator().next();
            InputStream in = metric.getData();
            // Make sure we try to read something, so that we can trigger IOException in some of the
            // test cases.
            final byte[] b = new byte[Math.max(1, in.available())];
            Assert.assertEquals(b.length, in.read(b));
            testData = new String(b);
            return DataMetricLOB.builder()
                                .withName(data.values().iterator().next().getName())
                                .withHelp("Help")
                                .withSeverity(DataMetric.Severity.INFO)
                                .withData(new ByteArrayInputStream(b)).build();
        }
    }

    @Test
    public void testBuildCounterMetric() {
        DataMetricCounter metric = DataMetricCounter.builder()
                                                              .withName("name")
                                                              .withHelp("Help")
                                                              .withSeverity(
                                                                      DataMetric.Severity.FATAL)
                                                              .build();
        Assert.assertEquals(DataMetricCounter.class, metric.getClass());
        Assert.assertEquals("name", metric.getName());
        Assert.assertEquals(DataMetric.MetricType.COUNTER, metric.getType());
        Assert.assertEquals(DataMetric.Severity.FATAL, metric.getSeverity());
        metric.increment();
        metric.increment(10.);
        Assert.assertEquals(11, metric.getData(),0.0);
    }

    @Test
    public void testBuildCounterMetricEdgeCases() {
        DataMetric metric = DataMetricCounter.builder()
                                                  .withName("name")
                                                  .withHelp("Help")
                                                  .withSeverity(DataMetric.Severity.FATAL)
                                                  .build();
        Assert.assertEquals(DataMetricCounter.class, metric.getClass());
        DataMetricCounter collection = (DataMetricCounter)metric;
        Assert.assertEquals("name", metric.getName());
        Assert.assertEquals(DataMetric.MetricType.COUNTER, metric.getType());
        Assert.assertEquals(DataMetric.Severity.FATAL, metric.getSeverity());
    }

    @Test
    public void testBuildHistogramMetric() {
        DataMetricHistogram metric = DataMetricHistogram.builder()
                                                        .withName("name")
                                                        .withHelp("Help")
                                                        .withSeverity(DataMetric.Severity.FATAL)
                                                        .withBuckets(1., 2., 10.)
                                                        .build()
                                                        .register();
        Assert.assertEquals(DataMetricHistogram.class, metric.getClass());
        Assert.assertEquals("name", metric.getName());
        Assert.assertEquals(DataMetric.MetricType.HISTOGRAM, metric.getType());
        Assert.assertEquals(DataMetric.Severity.FATAL, metric.getSeverity());

        // Calculate sum
        metric.observe(0.08);
        metric.observe(0.0491);
        metric.observe(0.3218);
        metric.observe(0.0113);
        metric.observe(1.1);
        metric.observe(2.1);
        Assert.assertEquals(3.6622, metric.getSum(), 0.0001);

        // Get the buckets
        Map<Double, Long> counters = metric.getBuckets();
        Assert.assertEquals(4, counters.get(1.).longValue());
        Assert.assertEquals(5, counters.get(2.).longValue());
        Assert.assertEquals(6, counters.get(10.).longValue());
    }

    @Test
    public void testBuildSummaryMetric() {
        DataMetric metric = DataMetricSummary.builder()
                                             .withName("name")
                                             .withHelp("Help")
                                             .withSeverity(DataMetric.Severity.FATAL)
                                             .build();
        Assert.assertEquals(DataMetricSummary.class, metric.getClass());
        DataMetricSummary collection = (DataMetricSummary)metric;
        Assert.assertEquals("name", metric.getName());
        Assert.assertEquals("Help", metric.getHelp());
        Assert.assertEquals(DataMetric.MetricType.SUMMARY, metric.getType());
        Assert.assertEquals(DataMetric.Severity.FATAL, metric.getSeverity());

        // Calculate sum
        collection.observe(0.08);
        collection.observe(0.0491);
        collection.observe(0.3218);
        collection.observe(0.0113);
        collection.observe(1.1);
        collection.observe(2.1);
        Assert.assertEquals(3.6622, collection.getSum(), 0.0001);
        Assert.assertEquals(6, collection.getCounter());
    }

    @Test
    public void testMetricSeverity() {
        Assert.assertTrue(
                DataMetric.Severity.WARN.severity() > DataMetric.Severity.INFO.severity());
        Assert.assertTrue(
                DataMetric.Severity.ERROR.severity() > DataMetric.Severity.WARN.severity());
        Assert.assertTrue(
                DataMetric.Severity.FATAL.severity() > DataMetric.Severity.ERROR.severity());
    }

    @Test
    public void testBuildGaugeMetric() {
        DataMetricGauge metric = DataMetricGauge.builder()
                                                          .withName("name")
                                                          .withHelp("Help")
                                                          .withSeverity(DataMetric.Severity.FATAL)
                                                          .build();
        Assert.assertEquals(DataMetricGauge.class, metric.getClass());
        Assert.assertEquals("name", metric.getName());
        Assert.assertEquals(DataMetric.MetricType.GAUGE, metric.getType());
        Assert.assertEquals(DataMetric.Severity.FATAL, metric.getSeverity());
        metric.increment();
        metric.setData(10.);
        Assert.assertEquals(10, metric.getData(),0.001);
        // Decrement back
        metric.decrement();
        Assert.assertEquals(9, metric.getData(),0.0);
    }

    /**
     * The test aggregator bridge.
     */
    private static class TestAggregatorBridge implements AggregatorBridge {
        Collection<DataMetric> messages_;
        Collection<DataMetric> messagesOffline_;

        @Override public void sendUrgent(@Nonnull Collection<DataMetric> messages) {
            messages_ = messages;
        }

        @Override public void sendOffline(@Nonnull Collection<DataMetric> messages) {
            messagesOffline_ = messages;
        }
    }

    /**
     * Tests a single collection cycle with analyzers.
     */
    @Test
    public void testLOBNoData() throws Exception {
        DataMetricLOB lob = DataMetricLOB.builder()
                                         .withName("heap")
                                         .withHelp("Help")
                                         .withSeverity(DataMetric.Severity.INFO)
                                         .withData(null)
                                         .withUrgent()
                                         .build().register();
        Assert.assertEquals(DataMetricLOB.NullInputStream.class, lob.getData().getClass());
        Assert.assertEquals(-1, lob.getData().read());
    }

    /**
     * Test labels summary
     */
    @Test
    public void testLabeledSummary() throws Exception {
        DataMetricSummary metric = DataMetricSummary.builder()
                                                    .withName("name")
                                                    .withLabelNames("label")
                                                    .withHelp("Help")
                                                    .withSeverity(DataMetric.Severity.ERROR)
                                                    .build();
        metric.labels("sublabel").observe(1.);
        metric.labels("sublabel").observe(2.);
        Assert.assertEquals(3., metric.labels("sublabel").getSum(), 0.01);
        Assert.assertEquals(2, metric.labels("sublabel").getCounter());
        Assert.assertEquals(0., metric.labels("anotherlabel").getSum(), 0.000001);
        Assert.assertEquals(0, metric.labels("anotherlabel").getCounter());
    }

    /**
     * Test labels histogram
     */
    @Test
    public void testLabeledHistogram() throws Exception {
        DataMetricHistogram metric = DataMetricHistogram.builder()
                                                        .withName("name")
                                                        .withLabelNames("label")
                                                        .withHelp("Help")
                                                        .withSeverity(DataMetric.Severity.ERROR)
                                                        .withBuckets(1., 2.)
                                                        .build();
        metric.labels("sublabel").observe(0.5);
        metric.labels("sublabel").observe(1.5);
        Assert.assertEquals(2.0, metric.labels("sublabel").getSum(), 0.01);
        Assert.assertEquals(0., metric.labels("anotherlabel").getSum(), 0.000001);
    }

    /**
     * Test a simple, unlabeled and untagged counter.
     */
    @Test
    public void testSimpleCounter() throws Exception {
        DataMetricCounter metric = DataMetricCounter.builder()
                .withName("name")
                .withHelp("Help")
                .withSeverity(DataMetric.Severity.ERROR)
                .build();
        metric.increment();
        Assert.assertEquals(1.0, metric.getData(), 0.00001);
        metric.increment(5.);
        Assert.assertEquals(6.0, metric.getData(), 0.00001);
    }

    /**
     * Test labels counter.
     */
    @Test
    public void testLabeledCounter() throws Exception {
        DataMetricCounter metric = DataMetricCounter.builder()
                                                              .withName("name")
                                                              .withLabelNames("label")
                                                              .withHelp("Help")
                                                              .withSeverity(
                                                                      DataMetric.Severity.ERROR)
                                                              .build();
        metric.labels("sublabel").increment();
        metric.labels("sublabel").increment();
        Assert.assertEquals(2.0, metric.labels("sublabel").getData(), 0.00001);
    }

    /**
     * Test labels gauge.
     */
    @Test
    public void testLabeledGauge() throws Exception {
        DataMetricGauge metric = DataMetricGauge.builder()
                .withName("name")
                .withLabelNames("label")
                .withHelp("Help")
                .withSeverity(
                        DataMetric.Severity.ERROR)
                .build();
        metric.labels("sublabel").setData(5.);
        metric.labels("sublabel").increment();
        metric.labels("sublabel").increment();
        metric.labels("sublabel").decrement();
        Assert.assertEquals(6.0, metric.labels("sublabel").getData(), 0.00001);
        Assert.assertEquals(0.0,metric.labels("anotherlabel").getData(),0.0);
    }

    /**
     * Test a simple, unlabeled and untagged gauge.
     */
    @Test
    public void testSimpleGauge() throws Exception {
        DataMetricGauge metric = DataMetricGauge.builder()
                .withName("name")
                .withHelp("Help")
                .withSeverity(DataMetric.Severity.ERROR)
                .build();
        metric.setData(5.);
        metric.increment();
        metric.increment();
        metric.decrement();
        Assert.assertEquals(6.0, metric.getData(), 0.00001);
    }

    /**
     * Test multi-labels gauge.
     */
    @Test
    public void testMultiLabeledGauge() throws Exception {
        DataMetricGauge metric = DataMetricGauge.builder()
                                                          .withName("name")
                                                          .withLabelNames("label1","label2")
                                                          .withHelp("Help")
                                                          .withSeverity(DataMetric.Severity.ERROR)
                                                          .build();
        metric.labels("sublabel", "sublabel2").setData(5.);
        metric.labels("sublabel", "sublabel2").increment();
        metric.labels("sublabel", "sublabel2").increment();
        metric.labels("sublabel", "sublabel3").decrement();
        Assert.assertEquals(7.0, metric.labels("sublabel", "sublabel2").getData(),
                            0.00001);

        Map<List<String>, GaugeData> labeled = metric.getLabeledMetrics();
        Assert.assertEquals(2, labeled.size());
    }

    /**
     * Test labels LOB.
     */
    @Test
    public void testLabeledLOB() throws Exception {
        DataMetricLOB metric = DataMetricLOB.builder()
                                            .withName("name")
                                            .withLabelNames("label")
                                            .withHelp("Help")
                                            .withSeverity(DataMetric.Severity.ERROR)
                                            .build();
        metric.labels("label2").setData(new ByteArrayInputStream(TESTDATA.getBytes()));
        Assert.assertEquals(DataMetricLOB.NullInputStream.class,
                            metric.labels("sublabel").getData().getClass());
        byte[] data = new byte[metric.labels("label2").getData().available()];
        metric.labels("label2").getData().read(data);
        Assert.assertEquals(TESTDATA, new String(data));
    }
}
