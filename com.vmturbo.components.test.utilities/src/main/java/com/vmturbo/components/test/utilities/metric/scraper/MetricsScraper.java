package com.vmturbo.components.test.utilities.metric.scraper;

import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.Collector.Type;

import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouseVisitor;

/**
 * The {@link MetricsScraper} collects metrics from metrics endpoints and converts them
 * to an internal data model. While there is no explicit dependency, the internal data model
 * does map very easily to the way Prometheus (and the Prometheus Java Client
 * Library) represents their time series.
 * <p>
 * The data model is as follows:
 * <p>
 * Metric Family (identified by MetricFamilyKey) contains a number of related metrics. For example,
 *    a histogram creates one metric for each bucket, plus a _count and a _sum metric. All of these
 *    would be in the same family. Also, different label combinations will create multiple metrics,
 *    all of which would be in the same family.
 * <p>
 * Metric (identified by MetricKey) is an individual metric within a Metric Family (see above for
 *    examples of this relationship).
 * <p>
 * Timestamped Sample is a sample collected from a (Metric Family, Metric) tuple at a particular
 *    time.
 * <p>
 * The logical organization is:
 *    Metric Family -> list of (Metric -> list of Timestamped Sample)
 * <p>
 * Users of the scraper can get all collected samples (organized by Metric Family and Metric)
 * by implementing a {@link MetricsWarehouseVisitor} and calling
 * {@link MetricsScraper#visit(MetricsWarehouseVisitor)}.
 */
@ThreadSafe
public abstract class MetricsScraper {

    /**
     * These are the types that should be sampled at every call to
     * {@link MetricsScraper#scrape(boolean)}.
     * <p>
     * Due to the particular requirements of performance tests, we usually don't need to sample
     * histograms and summaries throughout the test runtime. We only sample them at the end
     * so that there is exactly one datapoint that's easy to visualize and graph. Histograms
     * and summaries are also cumulative, so we don't lose any important data (other than rates
     * of increase/decrease).
     */
    private static final Set<Type> ALWAYS_SAMPLE_TYPES = ImmutableSet.of(Type.COUNTER, Type.GAUGE);

    /**
     * The name of this scraper. Each scraper should have a unique name.
     */
    private final String name;

    /**
     * The clock used to stamp collected samples with the time they were collected at.
     * Injected to allow mocking in tests.
     */
    private final Clock clock;

    /**
     * The aggregate metrics collected over all calls to scrape.
     */
    private final Map<MetricFamilyKey, MetricFamilyInfo> scrapedMetrics =
            Collections.synchronizedMap(new HashMap<>());

    protected MetricsScraper(@Nonnull final String name, @Nonnull final Clock clock) {
        this.name = name;
        this.clock = clock;
    }

    /**
     * Get the current value of the metrics this scraper is observing.
     * Subclasses are responsible for interacting with whatever endpoints they are observing
     * in order to implement this method.
     *
     * @return The list of metric family samples at this point in time.
     */
    @Nonnull
    protected abstract List<MetricFamilySamples> sampleMetrics();

    /**
     * An initialization method for scrapers that need to get some information from the
     * test's {@link ComponentCluster}.
     *
     * The default implementation exists so that subclasses that don't need the initialize
     * method don't need to have an empty one.
     *
     * @param componentCluster The test's component cluster.
     */
    public void initialize(@Nonnull final ComponentCluster componentCluster) {
    }

    /**
     * Trigger the sampling of the metrics this scraper is observing.
     * The scraper will save the metrics internally. Users of the scraper
     * should get the metrics by calling {@link MetricsScraper#visit}.
     *
     * @param scrapeAll If true, sample all types of metrics. Otherwise it will only sample
     *                   a subset of the types.
     */
    public void scrape(final boolean scrapeAll) {
        final List<MetricFamilySamples> samples = sampleMetrics();
        final long timeMs = clock.millis();
        samples.stream()
            .filter(metricFamily -> scrapeAll || ALWAYS_SAMPLE_TYPES.contains(metricFamily.type))
            .forEach(metricFamilySample -> {
                final MetricFamilyKey key = new MetricFamilyKey(metricFamilySample);
                MetricFamilyInfo curInfo = scrapedMetrics.computeIfAbsent(key, x -> new MetricFamilyInfo());
                metricFamilySample.samples.forEach(sample -> curInfo.addSample(sample, timeMs));
            });
    }

    public String getName() {
        return name;
    }

    public void visit(final MetricsWarehouseVisitor visitor) {
        visitor.onStartHarvester(getName());
        scrapedMetrics.forEach((familyKey, familyInfo) -> {
            visitor.onStartMetricFamily(familyKey);
            familyInfo.visit(visitor);
            visitor.onEndMetricFamily();
        });
        visitor.onEndHarvester();
    }

    /**
     * The {@link MetricFamilyKey} identifies a Metric Family. See the {@link MetricsScraper}
     * documentation for the overall data model.
     */
    @Immutable
    public static class MetricFamilyKey {
        private final String name;
        private final Type type;
        private final String help;

        public MetricFamilyKey(@Nonnull final MetricFamilySamples samples) {
            this.name = Objects.requireNonNull(samples.name);
            this.type = Objects.requireNonNull(samples.type);
            this.help = Objects.requireNonNull(samples.help);
        }

        @VisibleForTesting
        MetricFamilyKey(@Nonnull final String name,
                        @Nonnull final Type type,
                        @Nonnull final String help) {
            this.name = name;
            this.type = type;
            this.help = help;
        }

        @Nonnull
        public String getName() {
            return name;
        }

        @Nonnull
        public Type getType() {
            return type;
        }

        @Nonnull
        public String getHelp() {
            return help;
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof MetricFamilyKey) {
                MetricFamilyKey otherKey = (MetricFamilyKey)other;
                // Don't count the documentation in the equality comparison, since
                // documentation shouldn't affect the unique ID of the metric family.
                return name.equals(otherKey.name) && type.equals(otherKey.type);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, type);
        }

        @Override
        public String toString() {
            return "Name: " + name + " Type: " + type + " Help: " + help;
        }
    }

    /**
     * The {@link MetricFamilyInfo} is a utility class to represent the metrics and samples
     * associated with a {@link MetricFamilyKey}.
     */
    @VisibleForTesting
    static class MetricFamilyInfo {
        private Map<MetricKey, MetricData> metricsByLabelSet = new HashMap<>();

        void addSample(@Nonnull final Sample metricSample, final long timeMs) {
            final MetricData metricData =
                    metricsByLabelSet.computeIfAbsent(new MetricKey(metricSample),
                            x -> new MetricData());
            metricData.addSample(metricSample, timeMs);
        }

        @VisibleForTesting
        Map<MetricKey, MetricData> getMetrics() {
            return Collections.unmodifiableMap(metricsByLabelSet);
        }

        private void visit(final MetricsWarehouseVisitor visitor) {
            metricsByLabelSet.forEach((metricKey, data) -> {
                visitor.onStartMetric(metricKey, data.getMetadata());
                data.getSamples().forEach(visitor::onSample);
                visitor.onEndMetric();
            });
        }
    }

    /**
     * Utility class to represent the data associated with a particular metric.
     */
    static class MetricData {
        private List<TimestampedSample> samples = new LinkedList<>();
        private MetricMetadata metadata = new MetricMetadata();

        void addSample(@Nonnull final Sample metricSample, final long timeMs) {
            samples.add(new TimestampedSample(metricSample.value, timeMs));
            metadata.processSample(metricSample, timeMs);
        }

        List<TimestampedSample> getSamples() {
            return Collections.unmodifiableList(samples);
        }

        MetricMetadata getMetadata() {
            return metadata;
        }
    }

    /**
     * Metadata about a particular metric gathered over the course of the test run.
     * This is the place for extra per-metric information we want to save explicitly in
     * addition to the timestamped sample values.
     */
    public static class MetricMetadata {
        /**
         * The maximum value for this metric encountered so far.
         */
        private double maxValue = Double.MIN_VALUE;

        private long latestSampleTimeMs = 0;

        private void processSample(@Nonnull final Sample sample, final long timeMs) {
            this.maxValue = Math.max(this.maxValue, sample.value);
            this.latestSampleTimeMs = Math.max(latestSampleTimeMs, timeMs);
        }

        public double getMaxValue() {
            return maxValue;
        }

        public long getLatestSampleTimeMs() {
            return latestSampleTimeMs;
        }
    }

    /**
     * The {@link MetricKey} identifies a Metric within a Metric Family. See the
     * {@link MetricsScraper} documentation for the overall data model.
     */
    @Immutable
    public static class MetricKey {
        private final String name;

        private final Map<String, String> labels = new HashMap<>();

        public MetricKey(@Nonnull final Sample sample) {
            this.name = sample.name;
            if (sample.labelNames.size() != sample.labelValues.size()) {
                throw new IllegalArgumentException(
                        "Input sample should have equal sizes for label names and values.");
            }
            for (int i = 0; i < sample.labelNames.size(); ++i) {
                labels.put(sample.labelNames.get(i), sample.labelValues.get(i));
            }
        }

        @VisibleForTesting
        MetricKey(@Nonnull final String name, @Nonnull final Map<String, String> labels) {
            this.name = name;
            this.labels.putAll(labels);
        }

        public String getName() {
            return name;
        }

        public Map<String, String> getLabels() {
            return Collections.unmodifiableMap(labels);
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof MetricKey) {
                MetricKey otherKey = (MetricKey)other;
                return otherKey.name.equals(name) && otherKey.labels.equals(labels);
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, labels);
        }

        @Override
        public String toString() {
            return "Name: " + name + " Labels: " + labels;
        }
    }


    /**
     * The {@link TimestampedSample} is a collected data point. See the
     * {@link MetricsScraper} documentation for the overall data model.
     */
    @Immutable
    public static class TimestampedSample {
        public final long timeMs;
        public final double value;

        @VisibleForTesting
        public TimestampedSample(final double value, final long timeMs) {
            this.timeMs = timeMs;
            this.value = value;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timeMs, value);
        }

        @Override
        public boolean equals(Object other) {
            if (other instanceof TimestampedSample) {
                final TimestampedSample otherSample = (TimestampedSample)other;
                return otherSample.timeMs == timeMs && otherSample.value == value;
            } else {
                return false;
            }
        }

        @Override
        public String toString() {
            return "Time (ms): " + timeMs + " Value: " + value;
        }
    }

}
