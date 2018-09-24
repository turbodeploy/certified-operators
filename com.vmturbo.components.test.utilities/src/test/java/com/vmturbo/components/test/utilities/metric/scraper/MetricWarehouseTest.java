package com.vmturbo.components.test.utilities.metric.scraper;


import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.mockito.Mockito;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;

import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.metric.MetricTestUtil.TestWarehouseVisitor;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouse;

public class MetricWarehouseTest {

    private final CollectorRegistry testRegistry = new CollectorRegistry();

    private final Counter counter = Counter.build()
            .name("test")
            .labelNames("label")
            .help("help")
            .register(testRegistry);

    private final MutableFixedClock clock = new MutableFixedClock(100);

    @Test
    public void testMetricWarehouse() throws InterruptedException {
        final TestWarehouseVisitor visitor = Mockito.spy(new TestWarehouseVisitor());
        final MetricsWarehouse warehouse = MetricsWarehouse.newBuilder()
                .addScraper(new LocalMetricsScraper(testRegistry, clock))
                .useExecutor(Executors.newSingleThreadScheduledExecutor())
                .setCollectionInterval(100, TimeUnit.MILLISECONDS)
                .addVisitor(visitor)
                .build();
        checkLocalMetrics(warehouse, visitor);
    }

    @Test
    public void testNaughtyCollectingHarvester() throws Exception {
        final MetricsScraper naughtyHarvester = Mockito.mock(MetricsScraper.class);
        Mockito.when(naughtyHarvester.getName()).thenReturn("me");
        Mockito.doThrow(new RuntimeException("BAH!"))
               .when(naughtyHarvester).scrape(Mockito.anyBoolean());
        Mockito.doThrow(new RuntimeException("BAH!"))
                .when(naughtyHarvester).visit(Mockito.any());

        final TestWarehouseVisitor visitor = Mockito.spy(new TestWarehouseVisitor());
        final MetricsWarehouse warehouse = MetricsWarehouse.newBuilder()
                .addScraper(naughtyHarvester)
                .addScraper(new LocalMetricsScraper(testRegistry, clock))
                .useExecutor(Executors.newSingleThreadScheduledExecutor())
                .setCollectionInterval(100, TimeUnit.MILLISECONDS)
                .addVisitor(visitor)
                .build();

        // The bad harvester shouldn't interrupt local harvester data collection.
        checkLocalMetrics(warehouse, visitor);
    }


    /**
     * Utility method to test that the warehouse is working properly, and recording the
     * expected data into the visitor. The visitor must have been added to the warehouse
     * at build time.
     */
    private void checkLocalMetrics(final MetricsWarehouse warehouse,
                                   final TestWarehouseVisitor visitor)
            throws InterruptedException {
        // Increment the counter before starting collection to make sure we never
        // sample a value of 0.
        counter.labels("value").inc();

        final List<MetricFamilySamples> expectedSamples =
                Collections.list(testRegistry.metricFamilySamples());

        // Start collecting.
        warehouse.initialize(Mockito.mock(ComponentCluster.class));

        // Wait for the first collection to finish.
        warehouse.waitForHarvest();

        // Increment the counter again.
        counter.labels("value").inc();

        // Collect the sample from the incremented counter manually.
        expectedSamples.addAll(Collections.list(testRegistry.metricFamilySamples()));

        // Stop collecting. The farm should collect one more time.
        warehouse.stop("testMetricWarehouse");

        Mockito.verify(visitor).onStartWarehouse(Mockito.eq("testMetricWarehouse"));
        Mockito.verify(visitor).onEndWarehouse();

        Mockito.verify(visitor).onStartHarvester(Mockito.any());
        Mockito.verify(visitor).onEndHarvester();

        // Compare that the samples we collected manually match what the harvester got.
        visitor.checkCollectedMetrics(expectedSamples, clock);
    }
}
