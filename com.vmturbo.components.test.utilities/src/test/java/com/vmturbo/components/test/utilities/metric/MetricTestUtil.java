package com.vmturbo.components.test.utilities.metric;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.Collector.Type;

import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricFamilyKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricKey;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.MetricMetadata;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper.TimestampedSample;

public class MetricTestUtil {
    public static final String HARVESTER_NAME = "test";
    public static final String SIMPLE_SAMPLE_NAME = "sample";

    public static final String LABEL_NAME = "name";
    public static final String LABEL_VALUE = "value";
    public static final Map<String, String> LABEL_MAP = ImmutableMap.of(LABEL_NAME, LABEL_VALUE);

    public static final String HELP = "help";


    public static Sample createNoLabelSample(final double value) {
        return new Sample(SIMPLE_SAMPLE_NAME,
                Collections.emptyList(),
                Collections.emptyList(),
                value);
    }

    public static Sample createOneLabelSample(final double value) {
        return new Sample(SIMPLE_SAMPLE_NAME,
                Collections.singletonList(LABEL_NAME),
                Collections.singletonList(LABEL_VALUE),
                value);
    }

    public static MetricFamilySamples createSimpleFamily(String metricFamilyName, double value) {
        return createSimpleFamilyWithSamples(metricFamilyName, createOneLabelSample(value));
    }

    public static MetricFamilySamples createSimpleFamilyWithSamples(@Nonnull final String metricFamilyName,
                                                                    Sample... samples) {
        return new MetricFamilySamples(metricFamilyName, Type.COUNTER,
                HELP, Arrays.asList(samples));
    }

    public static class TestMetricsScraper extends MetricsScraper {

        private List<MetricFamilySamples> samples;

        public TestMetricsScraper(@Nonnull final List<MetricFamilySamples> samples,
                                  @Nonnull final Clock clock) {
            super(HARVESTER_NAME, clock);
            overrideSamples(samples);
        }

        @Nonnull
        @Override
        protected List<MetricFamilySamples> sampleMetrics() {
            return samples;
        }

        @Override
        public void initialize(@Nonnull final ComponentCluster componentCluster) {
        }

        public void overrideSamples(@Nonnull final List<MetricFamilySamples> newSamples) {
            this.samples = newSamples;
        }

    }

    public static class TestWarehouseVisitor implements MetricsWarehouseVisitor {
        private final Map<MetricFamilyKey, Map<MetricKey, List<TimestampedSample>>> visitedSamples
                = new HashMap<>();

        /**
         * Aggregate the metadata in a separate map because we don't want it
         * to affect the equality comparison on visited samples.
         */
        private final Map<MetricFamilyKey, Map<MetricKey, MetricMetadata>> metadata
                = new HashMap<>();

        private Map<MetricKey, List<TimestampedSample>> curFamily;

        private Map<MetricKey, MetricMetadata> curMetadata;

        private List<TimestampedSample> curMetricSamples;

        public void checkCollectedMetrics(@Nonnull final List<MetricFamilySamples> expectedSamples,
                                   @Nonnull Clock clock) {
            // Use the input to build a map with the same structure as the one built during
            // visiting, and compare the maps.
            final Map<MetricFamilyKey, Map<MetricKey, List<TimestampedSample>>> expectedMap =
                    new HashMap<>();

            expectedSamples.forEach(metricFamilySamples -> {
                final Map<MetricKey, List<TimestampedSample>> metricMap =
                        expectedMap.computeIfAbsent(new MetricFamilyKey(metricFamilySamples),
                                k -> new HashMap<>());
                metricFamilySamples.samples.forEach(sample ->
                        metricMap.computeIfAbsent(new MetricKey(sample), k -> new LinkedList<>())
                                .add(new TimestampedSample(sample.value, clock.millis())));
            });

            // Make sure we visited all the same metric families.
            assertThat(expectedMap.keySet(), is(visitedSamples.keySet()));
            expectedMap.forEach((key, expectedSampledMetrics) -> {
                final Map<MetricKey, List<TimestampedSample>> sampledMetrics = visitedSamples.get(key);
                // Make sure we visited all the same metrics.
                assertThat(expectedSampledMetrics.keySet(), is(sampledMetrics.keySet()));

                expectedSampledMetrics.forEach((metricKey, expectedTimestampedSamples) -> {
                    // For visited samples we can't do a straight-up comparison.
                    // Due to timing errors in slow environments (e.g. jenkins) we may have more
                    // visited samples than expected. However, since we should be using fixed,
                    // manually-incremented clocks, and manually incrementing the sample values,
                    // the de-duped samples should be the same.
                    final Set<TimestampedSample> expectedTimestampedSamplesSet = new HashSet<>(expectedTimestampedSamples);
                    final Set<TimestampedSample> timestampedSamplesSet = new HashSet<>(sampledMetrics.get(metricKey));
                    assertThat(expectedTimestampedSamplesSet, is(timestampedSamplesSet));
                });
            });
        }

        public Map<MetricFamilyKey, Map<MetricKey, MetricMetadata>> getMetadata() {
            return Collections.unmodifiableMap(metadata);
        }

        @Override
        public void onStartMetricFamily(@Nonnull final MetricFamilyKey key) {
            this.curFamily = visitedSamples.computeIfAbsent(key, k -> new HashMap<>());
            this.curMetadata = metadata.computeIfAbsent(key, k -> new HashMap<>());
        }

        @Override
        public void onEndMetricFamily() {
            this.curFamily = null;
        }

        @Override
        public void onStartMetric(@Nonnull final MetricKey key,
                                  @Nonnull final MetricMetadata metadata) {
            this.curMetricSamples = curFamily.computeIfAbsent(key, k -> new LinkedList<>());
            this.curMetadata.put(key, metadata);
        }

        @Override
        public void onEndMetric() {
            this.curMetricSamples = null;
        }

        @Override
        public void onSample(@Nonnull final TimestampedSample sample) {
            this.curMetricSamples.add(sample);
        }

        @Override
        public void onStartWarehouse(@Nonnull final String testName) {
        }

        @Override
        public void onEndWarehouse() {
        }

        @Override
        public void onStartHarvester(@Nonnull final String harvesterName) {
        }

        @Override
        public void onEndHarvester() {

        }
    }
}
