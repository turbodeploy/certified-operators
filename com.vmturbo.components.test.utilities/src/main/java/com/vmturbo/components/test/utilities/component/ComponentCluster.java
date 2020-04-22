package com.vmturbo.components.test.utilities.component;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.palantir.docker.compose.DockerComposeRule;
import com.palantir.docker.compose.configuration.DockerComposeFiles;
import com.palantir.docker.compose.configuration.ProjectName;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;

import io.grpc.Channel;
import io.grpc.ManagedChannelBuilder;

import org.apache.http.client.utils.URIBuilder;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.io.IoBuilder;

import tec.units.ri.unit.MetricPrefix;

import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.test.utilities.ComponentTestRule;
import com.vmturbo.external.api.TurboApiClient;

/**
 * The {@link ComponentCluster} is a wrapper around {@link DockerComposeRule} to do
 * container-management in a way that's better compatible with the structure of our docker-compose
 * files and our components.
 * <p>
 * Tests should use {@link ComponentTestRule} instead of {@link ComponentCluster} so that
 * all component dependencies can be managed via the @Rule annotation.
 *
 * Components are brought up in the order in which they were added to the cluster, and
 * are brought down in the reverse order to which they were added to the cluster. This
 * provides a deterministic startup and teardown order to help improve test reliability
 * and greater behavioral consistency across test runs. It also guarantees that, if
 * a component B depends on component A in docker-compose, if a test author wishes to
 * configure both A and B, they can bring up A with their desired configuration before
 * it is auto-started by docker-compose if B were started first.
 */
public class ComponentCluster {

    private static final String PERF_TEST_PROJECT_NAME = "perftest";

    public static final int DEFAULT_HEALTH_CHECK_WAIT_MINUTES = 8;

    private static final Logger logger = LogManager.getLogger();

    private final DockerComposeRule dockerComposeRule;

    private final LinkedHashMap<String, Component> components;

    private final ServiceLogger serviceLogger;

    /**
     * Health check used to verify ClusterMgr component readiness.
     */
    private final ComponentHealthCheck healthCheck;

    private ComponentCluster(@Nonnull String projectName,
                             @Nonnull final LinkedHashMap<String, Component> components) {
        this(components, DockerComposeRule.builder()
            .files(DockerComposeFiles.from(DockerEnvironment.getDockerComposeFiles()))
            .machine(DockerEnvironment.newLocalMachine(components).build())
            .projectName(ProjectName.fromString(projectName))
            .build());
    }

    private ComponentCluster(@Nonnull final LinkedHashMap<String, Component> components,
                          @Nonnull final DockerComposeRule dockerComposeRule) {
        this(components, dockerComposeRule, new ServiceLogger(dockerComposeRule), new ComponentHealthCheck());
    }

    @VisibleForTesting
    ComponentCluster(@Nonnull final LinkedHashMap<String, Component> components,
                  @Nonnull final DockerComposeRule dockerComposeRule,
                  @Nonnull final ServiceLogger serviceLogger,
                  @Nonnull final ComponentHealthCheck healthCheck) {
        this.dockerComposeRule = dockerComposeRule;
        this.components = components;
        this.serviceLogger = serviceLogger;
        this.healthCheck = healthCheck;
    }

    /**
     * Get the names of the components in the cluster.
     *
     * @return An immutable set of the names of the components.
     */
    public Set<String> getComponents() {
        return Collections.unmodifiableSet(components.keySet());
    }

    /**
     * Brings up all registered components in the rule.
     *
     * Prior to attempting to bring up the components, cleans up any leftovers from
     * prior runs of the performance framework.
     *
     * Brings up clustermgr and applies all service configurations before bringing
     * up components themselves.
     *
     * Components are brought up in the order in which they were added to the cluster.
     */
    public void up() {
        try {
            // Clean up potential leftover containers from a previous interrupted run.
            dockerComposeRule.after();

            // Start up clustermgr.
            Container clusterMgr = dockerComposeRule.containers().container("clustermgr");
            clusterMgr.up();
            healthCheck.waitUntilReady(clusterMgr, Duration.ofMinutes(DEFAULT_HEALTH_CHECK_WAIT_MINUTES));

            components.values().forEach(component -> {
                    component.up(clusterMgr, dockerComposeRule, serviceLogger);
            });
        } catch (InterruptedException| IOException e) {
            down();
            throw new IllegalStateException("Failed to start up clustermgr.", e);
        } catch (RuntimeException e) {
            down();
            throw e;
        }
    }

    /**
     * Close all registered components and clean up the docker project by deleting
     * all associated volumes.
     *
     * Close components in the reverse order of which they were brought up.
     */
    public void down() {
        try {
            final List<Component> orderedComponents = new ArrayList<>(components.values());
            Collections.reverse(orderedComponents);
            orderedComponents.forEach(Component::close);
        } finally {
            dockerComposeRule.after();
        }
    }

    /**
     * Get the connection configuration to connect to a service using the *.api-provided
     * clients for the particular component. For example, see: {@link com.vmturbo.topology.processor.api.impl.TopologyProcessorClient}.
     *
     * @param service The service name.
     * @return The connection configuration.
     */
    public ComponentApiConnectionConfig getConnectionConfig(@Nonnull final String service) {
        final DockerPort dockerPort = components.get(service).getHttpPort();
        return ComponentApiConnectionConfig.newBuilder()
            .setHostAndPort(dockerPort.getIp(), dockerPort.getExternalPort(), "")
            .build();
    }

    @Nonnull
    public TurboApiClient getExternalApiClient() {
        final Component apiComponent = components.get("api");
        if (apiComponent == null) {
            throw new IllegalStateException("API component not included as part of cluster!" +
                    " Can't initialize external API client.");
        }

        final DockerPort dockerPort = apiComponent.getHttpPort();

        return TurboApiClient.newBuilder()
                .setHost(dockerPort.getIp())
                .setPort(dockerPort.getExternalPort())
                .build();
    }

    /**
     * Create a new gRPC channel connecting to a service.
     *
     * This is a convenience method for {@link ComponentCluster#newGrpcChannelBuilder(String)}
     * followed by {@link ManagedChannelBuilder#build()}.
     *
     * @param service The service name.
     * @return The gRPC channel. The caller can use this to instantiate server stubs.
     */
    @Nonnull
    public Channel newGrpcChannel(@Nonnull final String service) {
        return newGrpcChannelBuilder(service).build();
    }

    /**
     * Create a new customizable gRPC channel builder for connecting to a service.
     * <p>
     * Each invocation of this method will create a new connection. It's the caller's responsibility
     * to close the channel.
     *
     * @param service The service name.
     * @return The gRPC channel builder, preconfigured to connect to the service. The caller can
     *         further customize the channel and build it using
     *         {@link ManagedChannelBuilder#build()}.
     */
    @Nonnull
    public ManagedChannelBuilder newGrpcChannelBuilder(@Nonnull final String service) {
        final DockerPort dockerPort = components.get(service).getGrpcPort();
        return GrpcChannelFactory.newChannelBuilder(dockerPort.getIp(), dockerPort.getExternalPort());
    }

    public URI getMetricsURI(@Nonnull final String service) {
        final DockerPort dockerPort = components.get(service).getHttpPort();
        URIBuilder uriBuilder = new URIBuilder();
        try {
            return uriBuilder.setHost(dockerPort.getIp())
                    .setScheme("http")
                    .setPort(dockerPort.getExternalPort())
                    .setPath("/metrics")
                    .build();
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Failed to construct metrics URI.");
        }
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static Component.Builder newService(String name) {
        return new Component.Builder(name);
    }

    /**
     * Builder class for a {@link ComponentCluster}. Intended to provide a semantically meaningful
     * way to construct a {@link ComponentCluster}.
     *
     * Components are brought up in the order in which they are added to the cluster via
     * the {@link #withService(Component)} call, and are brought down in the reverse order .
     * This provides a deterministic startup and teardown order to help improve test reliability
     * and greater behavioral consistency across test runs. It also guarantees that, if
     * a component B depends on component A in docker-compose, if a test author wishes to
     * configure both A and B, they can bring up A with their desired configuration before
     * it is auto-started by docker-compose if B were started first.
     */
    public static class Builder {

        /**
         * Use a {@link LinkedHashMap} so that components can be iterated in the order in which services were added
         * and can be brought up and down in the same order they are added.
         */
        private final LinkedHashMap<String, Component> componentsByName = new LinkedHashMap<>();
        private String projectName = PERF_TEST_PROJECT_NAME;

        @Nonnull
        public Builder withService(@Nonnull final Component component) {
            Component previousVal = componentsByName.put(component.getName(), component);
            if (previousVal != null) {
                throw new IllegalArgumentException("Service: " + previousVal.getName() +
                        " specified more than once.");
            }
            return this;
        }

        @Nonnull
        public Builder projectName(@Nonnull String projectName) {
            this.projectName = projectName;
            return this;
        }

        @Nonnull
        public Builder withService(@Nonnull final Component.Builder componentBuilder) {
            return withService(componentBuilder.build());
        }

        public ComponentCluster build() {
            return new ComponentCluster(projectName, componentsByName);
        }
    }

    /**
     * Class to contain the configuration of a component started for the purpose of performance
     * testing.
     */
    public static class Component implements AutoCloseable {

        /**
         * The name of the service, as defined in the docker-compose file. For example, "market" or
         * "topology-processor".
         */
        private final String serviceName;

        /**
         * The Spring configuration property overrides defined for this component.
         */
        private final Map<String, String> configurationOverrides;

        private final OutputStream logOutputStream;

        /**
         * If set, the memory limit desired for the component.
         */
        private final Optional<Integer> memoryLimitMb;

        /**
         * "-D" system properties passed to the JVM on component startup.
         */
        private final Map<String, String> systemProperties;

        /**
         * The health check for the service.
         */
        private final ServiceHealthCheck healthCheck;

        /**
         * How long to wait before timing out the health check at component startup.
         */
        private final int healthCheckTimeoutMinutes;

        /**
         * The {@link Container} for this component. This is only set once the component
         * is up, and contains information about how to connect to the running container.
         */
        private Container container;

        @VisibleForTesting
        Component(@Nonnull final String serviceName,
                  @Nonnull final Map<String, String> configurationOverrides,
                  final OutputStream logOutputStream,
                  final Optional<Integer> memoryLimitMb,
                  @Nonnull final ServiceHealthCheck healthCheck,
                  Map<String, String> systemProperties,
                  final int healthCheckTimeoutMinutes) {
            this.serviceName = serviceName;
            this.configurationOverrides = configurationOverrides;
            this.logOutputStream = logOutputStream;
            this.memoryLimitMb = memoryLimitMb;
            this.healthCheck = healthCheck;
            this.systemProperties = Objects.requireNonNull(systemProperties);
            this.healthCheckTimeoutMinutes = healthCheckTimeoutMinutes;
        }

        @VisibleForTesting
        String getName() {
            return serviceName;
        }

        public Optional<Integer> getMemoryLimitMb() {
            return memoryLimitMb;
        }

        public ServiceHealthCheck getHealthCheck() {
            return healthCheck;
        }

        public DockerPort getHttpPort() {
            // This method should only be called after up.
            // TODO (roman, March 20 2017): Reduce interdependency between the methods.
            return Objects.requireNonNull(container).port(ComponentUtils.GLOBAL_HTTP_PORT);
        }

        private DockerPort getGrpcPort() {
            // This method should only be called after up.
            // TODO (roman, March 20 2017): Reduce interdependency between the methods.
            return Objects.requireNonNull(container).port(ComponentUtils.GLOBAL_GRPC_PORT);
        }

        @VisibleForTesting
        void up(@Nonnull final Container clusterMgr,
                @Nonnull final DockerComposeRule dockerComposeRule,
                @Nonnull final ServiceLogger serviceLogger) {
            up(clusterMgr, dockerComposeRule, serviceLogger,
                    ServiceConfiguration.forService(getName(), getName() + "-1"));
        }

        @VisibleForTesting
        void up(@Nonnull final Container clusterMgr,
                @Nonnull final DockerComposeRule dockerComposeRule,
                @Nonnull final ServiceLogger serviceLogger,
                @Nonnull final ServiceConfiguration serviceConfiguration) {
            // Override configuration
            configurationOverrides.forEach(serviceConfiguration::withConfiguration);
            serviceConfiguration.apply(clusterMgr);

            // Start up actual component.
            try {
                container = dockerComposeRule.containers().container(getName());
                container.up();
                if (logOutputStream != null) {
                    serviceLogger.startCollecting(container, logOutputStream);
                }

                healthCheck.waitUntilReady(container, Duration.ofMinutes(healthCheckTimeoutMinutes));
            } catch (InterruptedException| IOException e) {
                throw new IllegalStateException("Component " + getName() + "failed to come up!", e);
            }
        }

        @Override
        public void close() {
            try {
                // Stop and remove the container
                if (container != null) {
                    container.stop();
                }
            } catch (Exception e) {
                logger.error("Failed to stop container for service " + getName(), e);
            }
        }

        public Map<String, String> getSystemProperties() {
            return systemProperties;
        }

        /**
         * Builder class for {@link Component}, intended to provide a semantically meaningful
         * way to construct a component.
         */
        public static class Builder {

            private final String serviceName;

            private OutputStream logOutputStream;

            private Integer memoryLimitMb;

            private final Map<String, String> systemProperties = new HashMap<>();

            private ServiceHealthCheck healthCheck;

            private int healthCheckTimeoutMinutes = DEFAULT_HEALTH_CHECK_WAIT_MINUTES;

            private final ImmutableMap.Builder<String, String> configurationOverrides
                    = new ImmutableMap.Builder<>();

            private Builder(@Nonnull final String serviceName) {
                this.serviceName = serviceName;
                this.healthCheck = new ComponentHealthCheck();
            }

            public Builder withConfiguration(String key, String value) {
                configurationOverrides.put(key, value);
                return this;
            }

            /**
             * Set the memory limit for the component.
             * The -Xmx setting (if relevant) will be set as a proportion of the memory limit.
             * The number will be truncated to the nearest megabyte.
             *
             * @param memoryLimit The amount of memory to use for the memory limit.
             * @param metricPrefix The SI metrics prefix to apply to the memory limit number (ie MEGA, GIGA, etc.)
             * @return The builder, for method chaining.
             */
            public Builder withMemLimit(final double memoryLimit, MetricPrefix metricPrefix) {
                final double baseUnits = metricPrefix.getConverter().convert(memoryLimit);
                return withMemLimitMb((int)(MetricPrefix.MEGA.getConverter().inverse().convert(baseUnits)));
            }

            /**
             * Set the memory limit for the component, in MB.
             * The -Xmx setting (if relevant) will be set as a proportion of the memory limit.
             *
             * @param memoryLimitMb The number of MB to use for the memory limit.
             * @return The builder, for method chaining.
             */
            public Builder withMemLimitMb(final int memoryLimitMb) {
                this.memoryLimitMb = memoryLimitMb;

                return this;
            }

            /**
             * Set system properties for the component at startup.
             *
             * @param properties The properties to pass on the command line at component startup.
             *                   Keys in the map are the system property names and their corresponding
             *                   values are the values for those system properties.
             * @return The builder, for method chaining.
             */
            public Builder withSystemProperties(@Nonnull final Map<String, String> properties) {
                this.systemProperties.putAll(properties);

                return this;
            }

            /**
             * Set the health check used specifically for this service / component.
             * Specifying a health check for a component will use the health check only for this component.
             * Useful for configuring the health check for services that do not support the /health endpoint.
             *
             * If no health check is explicitly set for a service, it will by default be health checked using
             * a {@link ComponentHealthCheck}.
             *
             * @param healthCheck The health check to use.
             * @return The builder, for method chaining.
             */
            public Builder withHealthCheck(@Nonnull final ServiceHealthCheck healthCheck) {
                this.healthCheck = healthCheck;
                return this;
            }

            /**
             * Set the wait timeout for the health check at component startup.
             * If the health check does not succeed before the timeout expires, the component
             * startup will be aborted and marked as failed.
             *
             * @param healthCheckTimeoutMinutes How long to wait for the health check to succeed before timing
             *                                  it out.
             * @return The builder, for method chaining.
             */
            public Builder withHealthCheckTimeoutMinutes(final int healthCheckTimeoutMinutes) {
                this.healthCheckTimeoutMinutes = healthCheckTimeoutMinutes;
                return this;
            }

            public Builder logsToLogger(@Nonnull final Logger logger) {
                this.logOutputStream = IoBuilder.forLogger(logger)
                    .setLevel(Level.INFO)
                    .buildOutputStream();
                return this;
            }

            public Builder logsToFile(@Nonnull final String fileName) {
                final File logFile = new File(fileName);
                try {
                    this.logOutputStream = new FileOutputStream(logFile);
                } catch (FileNotFoundException e) {
                    throw new IllegalStateException("Couldn't create log file: " + fileName);
                }
                return this;
            }

            public Component build() {
                return new Component(serviceName, configurationOverrides.build(), logOutputStream,
                        Optional.ofNullable(memoryLimitMb), healthCheck, systemProperties, healthCheckTimeoutMinutes);
            }
        }

    }
}
