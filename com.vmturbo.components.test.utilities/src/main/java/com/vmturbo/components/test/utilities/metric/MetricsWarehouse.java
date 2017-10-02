package com.vmturbo.components.test.utilities.metric;

import java.time.Clock;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import io.prometheus.client.CollectorRegistry;

import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.metric.scraper.ComponentMetricsScraper;
import com.vmturbo.components.test.utilities.metric.scraper.LocalMetricsScraper;
import com.vmturbo.components.test.utilities.metric.scraper.MetricsScraper;

/**
 * The {@link MetricsWarehouse} is the central storage for metrics collected during the run
 * of a particular test. It is responsible for collecting metrics using
 * {@link MetricsScraper}s provided at creation time, and will send the metrics to
 * {@link MetricsWarehouseVisitor}s (also provided at creation time) on shutdown.
 * <p>
 * The intended lifecycle is as follows:
 *   - The warehouse is created and configured as part of the {@link ComponentTestRule}.
 *   - At the start of the test, the warehouse begins to collect metrics from all available
 *     {@link MetricsScraper}s.
 *   - After the test completes, the configured {@link MetricsWarehouseVisitor}s visit the metrics.
 */
public class MetricsWarehouse implements AutoCloseable {

    public static final long DEFAULT_COLLECTION_INTERVAL_MS = 1000;

    /**
     * Harvesters that we the warehouse uses to actually collect metrics.
     */
    private final ImmutableList<MetricsScraper> metricsScrapers;

    /**
     * Visitors that should visit the warehouse after the scraping is complete (i.e. at
     * the end of the test).
     */
    private final ImmutableList<MetricsWarehouseVisitor> registeredVisitors;

    private final ScheduledExecutorService executorService;

    private final long collectionIntervalMs;

    /**
     * Use {@link MetricsWarehouse#newBuilder()} instead.
     */
    private MetricsWarehouse(final ImmutableList<MetricsScraper> metricsScrapers,
                            final ImmutableList<MetricsWarehouseVisitor> registeredVisitors,
                            final long collectionIntervalMs,
                            @Nullable ScheduledExecutorService executorService) {
        this.metricsScrapers = metricsScrapers;
        this.registeredVisitors = registeredVisitors;
        this.executorService = executorService != null ?
                executorService :
                Executors.newSingleThreadScheduledExecutor();
        this.collectionIntervalMs = collectionIntervalMs;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public void close() throws Exception {
        this.executorService.shutdownNow();
        this.executorService.awaitTermination(10, TimeUnit.SECONDS);

        // Once the regular samples are not running, sample everything one last time.
        // Force this scrape to collect all metric types.
        scrape(true);
    }

    private final Logger logger = LogManager.getLogger();

    private final Object iterationMarker = new Object();

    /**
     * This method exists so that tests can wait for the next collection
     * without race conditions or hard-coded timeouts.
     *
     * @throws InterruptedException If the wait is interrupted.
     */
    @VisibleForTesting
    public void waitForHarvest() throws InterruptedException {
        synchronized (iterationMarker) {
            iterationMarker.wait();
        }
    }

    @VisibleForTesting
    public void visit(@Nonnull final String testName,
                      @Nonnull final MetricsWarehouseVisitor visitor) {
        visitor.onStartWarehouse(testName);
        metricsScrapers.forEach(scraper -> {
            try {
                scraper.visit(visitor);
            } catch (RuntimeException e) {
                logger.error("Encountered error when visiting scraper: " + scraper.getName(), e);
            }
        });
        visitor.onEndWarehouse();
    }

    @VisibleForTesting
    public List<MetricsScraper> getMetricsScrapers() {
        return metricsScrapers;
    }

    private void scrape(final boolean scrapeAll) {
        metricsScrapers.forEach(scraper -> {
            try {
                scraper.scrape(scrapeAll);
            } catch (RuntimeException e) {
                logger.error("Encountered error when scraping: " + scraper.getName(), e);
            }
        });
        synchronized (iterationMarker) {
            iterationMarker.notifyAll();
        }
    }

    public void initialize(@Nonnull final ComponentCluster componentCluster) {
        // Start collecting metric.
        metricsScrapers.forEach(scraper -> scraper.initialize(componentCluster));
        this.executorService.scheduleAtFixedRate(() -> {
            scrape(false);
        }, collectionIntervalMs, collectionIntervalMs, TimeUnit.MILLISECONDS);
    }

    public void stop(@Nonnull final String testName) {
        try {
            close();
        } catch (Exception e) {
            throw new IllegalStateException("Failed to close!", e);
        }

        registeredVisitors.forEach(visitor -> {
            try {
                visit(testName, visitor);
            } catch (RuntimeException e) {
                logger.error("Failed to visit " + visitor.toString() +
                        ". Continuing...", e);
            }
        });
    }

    public static class Builder {

        private ScheduledExecutorService executorService = null;

        private ImmutableList.Builder<MetricsScraper> scrapers = new ImmutableList.Builder<>();

        private ImmutableList.Builder<MetricsWarehouseVisitor> warehouseVisitors =
                new ImmutableList.Builder<>();

        private long collectionIntervalMs = DEFAULT_COLLECTION_INTERVAL_MS;

        public Builder scrapeLocalRegistry() {
            addScraper(new LocalMetricsScraper(
                    CollectorRegistry.defaultRegistry, Clock.systemUTC()));
            return this;
        }

        /**
         * Scrape metrics from a service in the {@link ComponentCluster} used for the test.
         */
        public Builder scrapeService(@Nonnull final String serviceName) {
            addScraper(new ComponentMetricsScraper(serviceName, Clock.systemUTC()));
            return this;
        }

        /**
         * Add a custom {@link MetricsScraper} implementation. This should be unnecessary for
         * most use cases - use {@link Builder#scrapeLocalRegistry()} or
         * {@link Builder#scrapeService(String)} when possible.
         */
        @VisibleForTesting
        public Builder addScraper(@Nonnull final MetricsScraper metricsScraper) {
            scrapers.add(metricsScraper);
            return this;
        }

        public Builder addVisitor(@Nonnull final MetricsWarehouseVisitor collector) {
            warehouseVisitors.add(Objects.requireNonNull(collector));
            return this;
        }

        public Builder setCollectionInterval(final long interval,
                                             @Nonnull final TimeUnit timeUnit) {
            collectionIntervalMs = timeUnit.toMillis(interval);
            return this;
        }

        public Builder useExecutor(@Nonnull final ScheduledExecutorService executorService) {
            this.executorService = executorService;
            return this;
        }

        public MetricsWarehouse build() {
            return new MetricsWarehouse(scrapers.build(), warehouseVisitors.build(),
                    collectionIntervalMs, executorService);
        }

    }
}
