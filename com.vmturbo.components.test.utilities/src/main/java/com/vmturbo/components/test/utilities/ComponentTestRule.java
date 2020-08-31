package com.vmturbo.components.test.utilities;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import com.vmturbo.components.api.server.KafkaMessageProducer;
import com.vmturbo.components.test.utilities.communication.ComponentStubHost;
import com.vmturbo.components.test.utilities.component.ComponentCluster;
import com.vmturbo.components.test.utilities.component.DockerEnvironment;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouse;
import com.vmturbo.components.test.utilities.metric.MetricsWarehouseVisitor;
import com.vmturbo.components.test.utilities.metric.scraper.CAdvisorMetricsScraper;
import com.vmturbo.components.test.utilities.metric.scraper.NodeExporterMetricsScraper;
import com.vmturbo.components.test.utilities.metric.writer.InfluxMetricsWarehouseVisitor;

/**
 * The {@link ComponentTestRule} is the rule used for component tests that need to bring
 * up a set of XL components as dependencies.
 */
public class ComponentTestRule implements TestRule {

    private final Logger logger = LogManager.getLogger();

    private final ComponentCluster componentCluster;

    private final ComponentStubHost componentStubHost;

    private final Optional<MetricsWarehouse> metricsWarehouse;

    private String testName;

    private KafkaMessageProducer kafkaMessageProducer;

    private ComponentTestRule(@Nonnull final ComponentCluster componentCluster,
                             @Nonnull final ComponentStubHost componentStubHost,
                             @Nonnull final Optional<MetricsWarehouse> metricsWarehouse) {
        this.componentCluster = componentCluster;
        this.componentStubHost = componentStubHost;
        this.metricsWarehouse = metricsWarehouse;
    }

    @Nonnull
    public ComponentCluster getCluster() {
        return componentCluster;
    }

    @Override
    public Statement apply(final Statement base, final Description description) {
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    setup(description);
                    base.evaluate();
                } finally {
                    teardown();
                }
            }
        };
    }

    @VisibleForTesting
    Optional<MetricsWarehouse> getMetricsWarehouse() {
        return metricsWarehouse;
    }

    private void setup(final Description description) {
        testName = description.getMethodName();
        logger.info("Starting setup for test {}...", testName);
        // The components in the  cluster will try to initiate connections to the stubs when
        // they starts up, so start the stubs first.
        logger.info("Starting component stub host...");
        componentStubHost.start();

        // Start the component cluster before the metrics, because the metrics scrape the cluster.
        logger.info("Bringing up component cluster...");
        componentCluster.up();

        metricsWarehouse.ifPresent(warehouse -> warehouse.initialize(componentCluster));
        logger.info("Completed setup for test {}!", testName);

        kafkaMessageProducer =
                new KafkaMessageProducer(DockerEnvironment.getKafkaBootstrapServers(),
                    "", 67108864, 126976, 600000, 60000, 600);
    }

    private void teardown() {
        logger.info("Starting teardown for test {}...", testName);
        // Shut down in reverse order of startup.
        final List<String> teardownErrors = new ArrayList<>();
        try {
            metricsWarehouse.ifPresent(warehouse -> warehouse.stop(testName));
        } catch (RuntimeException e) {
            teardownErrors.add("Encountered error stopping metrics warehouse: " + e.getMessage());
        }

        try {
            componentCluster.down();
        } catch (RuntimeException e) {
            teardownErrors.add("Encountered error stopping component cluster: " + e.getMessage());
        }

        try {
            componentStubHost.close();
        } catch (RuntimeException e) {
            teardownErrors.add("Encountered error stopping component stub host: " + e.getMessage());
        }

        if (teardownErrors.isEmpty()) {
            logger.info("Successfully completed teardown for test {}!", testName);
        } else {
            logger.info("Completed teardown for test {} with errors: {}", testName,
                    StringUtils.join(teardownErrors, "\n"));
        }
    }

    public static ComponentRuleSetter newBuilder() {
        return new ComponentRuleSetter();
    }

    public KafkaMessageProducer getKafkaMessageProducer() {
        return kafkaMessageProducer;
    }

    /**
     * Utility builder class to enforce that the user set a {@link ComponentCluster} for this
     * rule at compile-time.
     */
    public static class ComponentRuleSetter {
        private ComponentRuleSetter() {}

        public ComponentStubHostSetter withComponentCluster(
                @Nonnull final ComponentCluster.Builder clusterBuilder) {
            return new ComponentStubHostSetter(clusterBuilder.build());
        }
    }

    public static class ComponentStubHostSetter {
        private final ComponentCluster componentCluster;

        private ComponentStubHostSetter(@Nonnull final ComponentCluster componentCluster) {
            this.componentCluster = componentCluster;
        }

        public MetricsWarehouseSetter withStubs(
                @Nonnull final ComponentStubHost.Builder componentStubHostBuilder) {
            return new MetricsWarehouseSetter(componentCluster, componentStubHostBuilder.build());
        }

        public MetricsWarehouseSetter withoutStubs() {
            return new MetricsWarehouseSetter(componentCluster,
                    ComponentStubHost.newBuilder().build());
        }
    }
    /**
     * Utility builder class to allow the user to add a {@link MetricsWarehouse}.
     */
    public static class MetricsWarehouseSetter {
        private final ComponentCluster componentCluster;
        private final ComponentStubHost componentStubHost;

        private MetricsWarehouseSetter(@Nonnull final ComponentCluster componentCluster,
                                       @Nonnull final ComponentStubHost componentStubHost) {
            this.componentCluster = componentCluster;
            this.componentStubHost = componentStubHost;
        }

        /**
         * Add a custom metrics warehouse to this rule. Use this if you want to override the
         * {@link MetricsWarehouseSetter#scrapeClusterAndLocalMetricsToInflux()} behaviour
         * (for example if you want to save the metrics to an alternative location, change the
         * collection interval, or reduce the number of metrics generated).
         */
        @Nonnull
        public ComponentTestRule withMetricsWarehouse(
                @Nonnull final MetricsWarehouse.Builder metricsWarehouse) {
            return new ComponentTestRule(componentCluster, componentStubHost,
                    Optional.of(metricsWarehouse.build()));
        }

        /**
         * The default metrics collection implementation.
         * <p>
         * Scrapes the local registry and all services in the {@link ComponentCluster}, and saves
         * the results into InfluxDB.
         *
         * Also scrapes CAdvisor and NodeExporter metrics if they are available.
         *
         * @return A ComponentTestRule built with the specified configuration.
         */
        @Nonnull
        public ComponentTestRule scrapeClusterAndLocalMetricsToInflux() {
            return scrapeClusterAndLocalMetrics(new InfluxMetricsWarehouseVisitor());
        }

        /**
         * Scrape specific services by name.
         * <p>
         * Scrapes the local registry and all specified services, and saves
         * the results into InfluxDB.
         *
         * Also scrapes CAdvisor and NodeExporter metrics if they are available.
         *
         * @return A ComponentTestRule built with the specified configuration.
         */
        @Nonnull
        public ComponentTestRule scrapeServicesAndLocalMetricsToInflux(@Nonnull final String... serviceNames) {
            return scrapeServicesAndLocalMetrics(new InfluxMetricsWarehouseVisitor(), serviceNames);
        }

        /**
         * The default metrics collection implementation.
         * <p>
         * Scrapes the local registry and all services in the {@link ComponentCluster}, and saves
         * the results into a visitor.
         *
         * Also scrapes CAdvisor and NodeExporter metrics if they are available.
         *
         * @param visitor The visitor to where data will be scraped.
         * @return A ComponentTestRule built with the specified configuration.
         */
        @VisibleForTesting
        @Nonnull
        public ComponentTestRule scrapeClusterAndLocalMetrics(@Nonnull MetricsWarehouseVisitor visitor) {
            final String[] serviceNames = new String[componentCluster.getComponents().size()];
            componentCluster.getComponents().stream()
                .collect(Collectors.toList())
                .toArray(serviceNames);

            return scrapeServicesAndLocalMetrics(visitor, serviceNames);
        }

        /**
         * Scrape specific services by name.
         * <p>
         * Scrapes the local registry and all specified services, and saves
         * the results into a visitor.
         *
         * Also scrapes CAdvisor and NodeExporter metrics if they are available.
         *
         * @param visitor The visitor to where data will be scraped.
         * @param serviceNames The names of the services to be scraped.
         * @return A ComponentTestRule built with the specified configuration.
         */
        @VisibleForTesting
        @Nonnull
        ComponentTestRule scrapeServicesAndLocalMetrics(@Nonnull MetricsWarehouseVisitor visitor,
                                                        @Nonnull final String... serviceNames) {
            final MetricsWarehouse.Builder warehouseBuilder = MetricsWarehouse.newBuilder()
                .setCollectionInterval(MetricsWarehouse.DEFAULT_COLLECTION_INTERVAL_MS,
                    TimeUnit.MILLISECONDS)
                .scrapeLocalRegistry()
                .addVisitor(visitor);

            for (String serviceName : serviceNames) {
                warehouseBuilder.scrapeService(serviceName);
            }

            // Add scrapers for CAdvisor and NodeExporter.
            warehouseBuilder.addScraper(new CAdvisorMetricsScraper(Clock.systemUTC()));
            warehouseBuilder.addScraper(new NodeExporterMetricsScraper(Clock.systemUTC()));

            return withMetricsWarehouse(warehouseBuilder);
        }

        /**
         * Do not scrape metrics of any sort, and do not store scraped metrics anywhere.
         *
         * @return A ComponentTestRule built with the specified configuration.
         */
        @Nonnull
        public ComponentTestRule noMetricsCollection() {
            return new ComponentTestRule(componentCluster, componentStubHost, Optional.empty());
        }
    }
}
