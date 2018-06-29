package com.vmturbo.components.common;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletRegistration;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.clustermgr.api.impl.ClusterMgrClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.common.health.CompositeHealthMonitor;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.components.common.health.SimpleHealthStatus;
import com.vmturbo.components.common.metrics.ScheduledMetrics;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.components.common.utils.EnvironmentUtils;
import com.vmturbo.proactivesupport.DataMetricGauge;

/**
 * Provide common aspects of all VmtComponent types:
 * <ul>
 * <li>name
 * <li>status
 * </ul>
 * TODO: This will likely be the place to inject a state machine for each component to verify state-transitions
 * e.g. is start() legal, and if so what is the next state?
 **/
@Configuration
@EnableWebMvc
@Import({BaseVmtComponentConfig.class})
public abstract class BaseVmtComponent implements IVmtComponent,
        ApplicationListener<ContextRefreshedEvent> {

    public static final String KEY_COMPONENT_VERSION = "component.version";
    /**
     * The number of seconds to wait for gRPC server to shutdown
     * during the shutdown procedure for the component.
     */
    private static final int GRPC_SHUTDOWN_WAIT_S = 10;

    /**
     * The minimum acceptable server-side keepalive rate.
     * <p>
     * In gRPC, the server only accepts keepalives every 5 minutes by default. We want to set it
     * a little lower. The server will reject keepalives coming in at a greater rate.
     */
    private static final int GRPC_MIN_KEEPALIVE_TIME_MIN = 1;

    public static final String PROP_COMPNENT_TYPE = "component_type";
    public static final String PROP_INSTANCE_ID = "instance_id";
    public static final String PROP_SERVER_PORT = "server_port";
    public static final String PROP_SECURE_PORT = "secure_server_port";
    public static final String PROP_KEYSTORE_FILE = "keystore_file";
    public static final String PROP_KEYSTORE_PASS = "keystore_pass";
    public static final String PROP_SECURE_CIPHER_SUITES = "secure_cipher_suites";
    public static final String PROP_KEYSTORE_TYPE = "keystore_type";
    public static final String PROP_KEYSTORE_ALIAS = "keystore_alias";
    public static final String DEFAULT_KEYSTORE_PASS = "jumpy-crazy-experience";
    public static final String DEFAULT_CIPHER_SUITES = "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256,"
                                                    + "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,"
                                                    + "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA384,"
                                                    + "TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384";

    /**
     * The URL at which to expose Prometheus metrics.
     */
    public static final String METRICS_URL = "/metrics";

    private static Logger logger = LogManager.getLogger();

    private ExecutionStatus status = ExecutionStatus.NEW;
    private final AtomicBoolean startFired = new AtomicBoolean(false);

    private static final DataMetricGauge STARTUP_DURATION_METRIC = DataMetricGauge.builder()
            .withName("component_startup_duration_ms")
            .withHelp("Duration in ms from component instantiation to Spring context built.")
            .build()
            .register();

    @Value("${" + PROP_COMPNENT_TYPE + ":}")
    private String componentType;

    private static final int SCHEDULED_METRICS_DELAY_MS=60000;

    @Value("${scheduledMetricsIntervalMs:60000}")
    private int scheduledMetricsIntervalMs;

    @Value("${server.grpcPort}")
    private int grpcPort;

    @GuardedBy("grpcServerLock")
    private Server grpcServer;

    private final Object grpcServerLock = new Object();

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Autowired
    BaseVmtComponentConfig baseVmtComponentConfig;

    @SuppressWarnings("SpringAutowiredFieldsWarningInspection")
    @Autowired(required = false)
    DiagnosticService diagnosticService;

    @Autowired
    private ServletContext servletContext;

    /**
     * Embed a component for monitoring dependency/subcomponent health
     */
    private final CompositeHealthMonitor healthMonitor =
            new CompositeHealthMonitor(componentType + " Component");

    static {
        // Capture the beginning of component execution - begins from when the Java static is loaded.
        startupTime = Instant.now();
    }
    private static Instant startupTime;

    /**
     * Constructor for BaseVmtComponent
     */
    public BaseVmtComponent() {
        // install a component ExecutionStatus-based health check into the monitor.
        // This health check will return true if the component is in the RUNNING state and false for
        // all other states.
        healthMonitor.addHealthCheck(new HealthStatusProvider() {
            private HealthStatus lastStatus;

            @Override
            public String getName() { return "ExecutionStatus"; }

            @Override
            public HealthStatus getHealthStatus() {
                lastStatus = new SimpleHealthStatus(
                        getComponentStatus().equals(ExecutionStatus.RUNNING),
                        getComponentStatus().name(),
                        lastStatus);
                return lastStatus;
            }
        });
    }

    @Override
    public String getComponentName() {
        return componentType;
    }

    @PostConstruct
    public void componentContextConstructed() {
        logger.info("------------ spring context constructed ----------");
        final long msToStartUp = Duration.between(startupTime, Instant.now())
                .toMillis();
        logger.info("time to start: {} ms", msToStartUp);
        STARTUP_DURATION_METRIC.setData((double)msToStartUp);
    }

    @PreDestroy
    public void componentContextClosing() {
        logger.info("------------ spring context closing ----------");
    }

    /**
     * The metrics endpoint that exposes Prometheus metrics on the pre-defined /metrics URL.
     */
    @Bean
    public Servlet metricsServlet() {
        final Servlet servlet = new MetricsServlet(CollectorRegistry.defaultRegistry);
        final ServletRegistration.Dynamic registration =
                servletContext.addServlet("metrics-servlet", servlet);
        registration.setLoadOnStartup(1);
        registration.addMapping(METRICS_URL);
        return servlet;
    }

    /**
     * Status is only to be set by individual VmtComponent subclasses.
     *
     * @param status the current execution status of the component; initially NEW
     */
    protected void setStatus(ExecutionStatus status) {
        logger.info("Component transition from " + this.status.toString() + " to " + status.toString() +
                " for " + getComponentName());
        this.status = status;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ExecutionStatus getComponentStatus() {
        return status;
    }

    /**
     * Retrieve the {@link CompositeHealthMonitor} associated with this component.
     *
     * @return the {@link CompositeHealthMonitor instance} embedded in this component
     */
    @Override
    public CompositeHealthMonitor getHealthMonitor() { return healthMonitor; }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void startComponent() {
        setStatus(ExecutionStatus.STARTING);
        DefaultExports.initialize();
        // start up the default scheduled metrics too
        ScheduledMetrics.initializeScheduledMetrics(scheduledMetricsIntervalMs,SCHEDULED_METRICS_DELAY_MS);

        // add the additional health checks if they aren't already registered
        Map<String,HealthStatusProvider> healthChecks = getHealthMonitor().getDependencies();
        if (!healthChecks.containsKey(baseVmtComponentConfig.memoryMonitor().getName())) {
            logger.info("Adding memory health check.");
            getHealthMonitor().addHealthCheck(baseVmtComponentConfig.memoryMonitor());
        }
        if (!healthChecks.containsKey(baseVmtComponentConfig.deadlockHealthMonitor().getName())) {
            logger.info("Adding deadlock health check");
            getHealthMonitor().addHealthCheck(baseVmtComponentConfig.deadlockHealthMonitor());
        }

        startGrpc();
        setStatus(ExecutionStatus.MIGRATING);
        baseVmtComponentConfig.migrationFramework().startMigrations(getMigrations(),
            false/*don't forceStart failed ones*/);
        onStartComponent();
        setStatus(ExecutionStatus.RUNNING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void stopComponent() {
        setStatus(ExecutionStatus.STOPPING);
        onStopComponent();
        stopGrpc();
        setStatus(ExecutionStatus.TERMINATED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void pauseComponent() {
        onPauseComponent();
        setStatus(ExecutionStatus.PAUSED);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void resumeComponent() {
        onResumeComponent();
        setStatus(ExecutionStatus.RUNNING);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void failedComponent() {
        onFailedComponent();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void configurationPropertiesChanged(@Nonnull final Environment environment,
                                               @Nonnull final Set<String> changedPropertyKeys) {
        logger.info("Configuration Properties changed...");
        changedPropertyKeys.stream().sorted().forEachOrdered(
                propKey -> logger.info("  remote property {} = {}", propKey, environment.getProperty(propKey)));
        onConfigurationPropertiesChanged(environment, changedPropertyKeys);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void dumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        onDumpDiags(diagnosticZip);

        // diagnosticService must be injected by the particular component. Check that happened correctly.
        if (baseVmtComponentConfig.diagnosticService() == null) {
            throw new RuntimeException("DiagnosticService missing");
        }
        baseVmtComponentConfig.diagnosticService().dumpDiags(diagnosticZip);
    }

    // START: Methods to allow component implementations to hook into
    // the component lifecycle. BaseVmtComponent should NOT have any
    // code here. These methods are effectively abstract, but made non-abstract
    // for convenience.
    protected void onStartComponent() {
        publishVersionInformation();
    }

    protected void onStopComponent() {}

    protected void onPauseComponent() {}

    protected void onResumeComponent() {}

    protected void onFailedComponent() {}

    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {}

    protected void onConfigurationPropertiesChanged(@Nonnull final Environment environment,
                                                    @Nonnull Set<String> changedPropertyKeys) {
        logger.info(">>>>>>> config props changed <<<<<<<<<");
            changedPropertyKeys.forEach(propertyKey -> logger.info("  changed property  {} = {}",
                    propertyKey, environment.getProperty(propertyKey)));
    }

    /**
     * Components must override this method if they want to use gRPC, and use the
     * provided builder to create a {@link Server}.
     *
     * @param builder An empty builder for the server.
     * @return An optional containing the gRPC server to use, or an empty optional if
     *         gRPC is not desired for this component.
     */
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        return Optional.empty();
    }

    /**
     * Components must override this method if they want to run data migrations.
     *
     * @return The migrations to be executed as a sorted mapping from
     *  migrationName -> Migrations.
     *
     *  The migrationName should be of the form:
     *  V_${two_digits_zero_padded_3_part_version_number_separate_by_underscore}__${MigrationName}
     *  e.g. V_07_02_00__Migration1
     *       V_07_12_10__Migration2
     */
    @Nonnull
    protected SortedMap<String, Migration> getMigrations() {
        return Collections.emptySortedMap();
    }

    // END: Methods to allow component implementations to hook into
    // the component lifecycle.

    private void startGrpc() {
        synchronized (grpcServerLock) {
            final NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(grpcPort)
                    // Allow keepalives even when there are no existing calls, because we want
                    // to send intermittent keepalives to keep the http2 connections open.
                    .permitKeepAliveWithoutCalls(true)
                    .permitKeepAliveTime(GRPC_MIN_KEEPALIVE_TIME_MIN, TimeUnit.MINUTES);
            final Optional<Server> builtServer = buildGrpcServer(serverBuilder);
            if (builtServer.isPresent()) {
                grpcServer = builtServer.get();
                try {
                    grpcServer.start();
                    logger.info("Initialized gRPC server on port {}.", grpcPort);
                } catch (IOException e) {
                    logger.error("Failed to start gRPC server. gRPC methods will not be available!", e);
                    stopGrpc();
                }
            } else {
                logger.info("Not initializing gRPC server for {}", getComponentName());
            }
        }
    }

    private void stopGrpc() {
        synchronized (grpcServerLock) {
            if (grpcServer != null) {
                grpcServer.shutdownNow();
                try {
                    if (grpcServer.awaitTermination(GRPC_SHUTDOWN_WAIT_S, TimeUnit.SECONDS)) {
                        logger.info("gRPC server successfully stopped.");
                    } else {
                        logger.error("gRPC server failed to stop after {} seconds!",
                                GRPC_SHUTDOWN_WAIT_S);
                    }
                } catch (InterruptedException e) {
                    logger.error("Interrupted while waiting for gRPC server to stop. " +
                                    "gRPC server is: {}",
                            grpcServer.isTerminated() ? "stopped" : "running");
                }
                grpcServer = null;
            }
        }
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (!startFired.getAndSet(true)) {
            startComponent();
        }
    }

    /**
     * Returns environment variable value.
     *
     * @param propertyName environment variable name
     * @return evironment variable value
     * @throws IllegalStateException if there is not such environment property set
     */
    @Nonnull
    protected static String requireEnvProperty(@Nonnull String propertyName) {
        return getOptionalEnvProperty(propertyName)
                .orElseThrow(() -> new IllegalStateException("System or environment property \"" + propertyName + "\" must be set"));
    }

    /**
     * Return the value of a system property or environment value (in that order) with the specified
     * name, if one is defined, otherwise empty.
     *
     * @param propertyName The name of the system or environment property to check.
     * @return An Optional of the system property value or environment variable, if one was found.
     */
    protected static Optional<String> getOptionalEnvProperty(@Nonnull String propertyName) {
        final String sysPropValue = System.getProperty(propertyName);
        if (sysPropValue != null) {
            return Optional.of(sysPropValue);
        }
        final String envPropValue = System.getenv(propertyName);
        if (envPropValue != null) {
            return Optional.of(envPropValue);
        }
        return Optional.empty();
    }

    /**
     * Get an optional environment property as an int value, returning the default value if the
     * property is not found or not parseable.
     *
     * @param propertyName the property name to look for
     * @param defaultValue the default value to use for the property
     * @return the found value, if there is one, otherwise the default value.
     */
    protected static int getOptionalIntEnvProperty(String propertyName, int defaultValue) {
        try {
            return getOptionalEnvProperty(propertyName)
                    .map(Integer::parseInt)
                    .orElse(defaultValue);
        } catch (NumberFormatException nfe) {
            logger.error("NumberFormatException parsing property {}, will use default value of {}",
                    propertyName, defaultValue);
            return defaultValue;
        }
    }

    @Nonnull
    protected static ConfigurableApplicationContext attachSpringContext(
            @Nonnull ServletContextHandler contextServer, @Nonnull Class<?> configurationClass) {
        final AnnotationConfigWebApplicationContext applicationContext =
                new AnnotationConfigWebApplicationContext();
        applicationContext.register(configurationClass);
        final Servlet dispatcherServlet = new DispatcherServlet(applicationContext);
        final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);
        contextServer.addServlet(servletHolder, "/*");
        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(applicationContext);
        contextServer.addEventListener(springListener);
        applicationContext.isActive();
        return applicationContext;
    }

    /**
     * Starts web server with Spring context, initialized from the specified configuration class.
     *
     * @param configurationClass spring context configuration class
     * @return Spring context initialized
     */
    @Nonnull
    protected static ConfigurableApplicationContext startContext(
            @Nonnull Class<?> configurationClass) {
        return startContext(servletContextHolder -> attachSpringContext(servletContextHolder,
                configurationClass));
    }

    /**
     * Starts web server with Spring context, initialized from the specified configuration class.
     *
     * @param contextConfigurer configuration callback to perform some specific
     *         configuration on the servlet context
     * @return Spring context, created as the result of webserver startup.
     */
    @Nonnull
    protected static ConfigurableApplicationContext startContext(
            @Nonnull ContextConfigurer contextConfigurer) {
        fetchConfigurationProperties();
        logger.info("Starting web server with spring context");
        final String serverPort = requireEnvProperty(PROP_SERVER_PORT);
        System.setProperty("org.jooq.no-logo", "true");

        final org.eclipse.jetty.server.Server server =
                new org.eclipse.jetty.server.Server(Integer.valueOf(serverPort));

        // check if we should open a secure port too.
        getOptionalEnvProperty(PROP_SECURE_PORT).ifPresent(securePortProperty ->
                addSecureConnector(server, securePortProperty));

        final ServletContextHandler contextServer =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        final ConfigurableApplicationContext context;
        try {
            server.setHandler(contextServer);
            context = contextConfigurer.configure(contextServer);
            server.start();
            if (!context.isActive()) {
                logger.error("Spring context failed to start. Shutting down.");
                System.exit(1);
            }
            return context;
        } catch (Exception e) {
            logger.error("Web server failed to start. Shutting down.", e);
            System.exit(1);
        }
        // Could should never reach here
        return null;
    }

    /**
     * Add a secure port listener to the jetty server.
     *
     * @param server The Jetty server to add the secure listener to.
     * @param securePortProperty The port number to use.
     */
    static private void addSecureConnector(org.eclipse.jetty.server.Server server, String securePortProperty) {
        // try to configure a secure listener
        try {
            logger.info("Secure port {} defined.", securePortProperty);
            int securePort = Integer.parseInt(securePortProperty);
            SslContextFactory sslContextFactory = new SslContextFactory();

            // keystore file, pass and cipher suites have default values
            final String keyStoreFile = getOptionalEnvProperty(PROP_KEYSTORE_FILE)
                    .orElse("/home/turbonomic/data/keystore.p12");
            logger.info("Keystore file {} will be used.", keyStoreFile);
            sslContextFactory.setKeyStorePath(keyStoreFile);

            sslContextFactory.setKeyStorePassword(getOptionalEnvProperty(PROP_KEYSTORE_PASS)
                    .orElse(DEFAULT_KEYSTORE_PASS));
            String[] cipherSuites = getOptionalEnvProperty(PROP_SECURE_CIPHER_SUITES)
                    .orElse(DEFAULT_CIPHER_SUITES).split(",");
            logger.info("Cipher suites({}): {}", cipherSuites.length, cipherSuites);
            sslContextFactory.setIncludeCipherSuites(cipherSuites);
            // exclude older SSL protocols
            sslContextFactory.addExcludeProtocols("SSLv2Hello","SSLv3","TLSv1","TLSv1.1");

            // cert alias and keystore type are optional
            getOptionalEnvProperty(PROP_KEYSTORE_ALIAS).ifPresent(sslContextFactory::setCertAlias);
            getOptionalEnvProperty(PROP_KEYSTORE_TYPE).ifPresent(sslContextFactory::setKeyStoreType);

            // create the secured ServerConnector
            ServerConnector httpsConnector = new ServerConnector(server, sslContextFactory);
            httpsConnector.setPort(securePort);
            server.addConnector(httpsConnector);
            logger.debug("Secure connector created.");
        } catch (NumberFormatException | NullPointerException e) {
            // one of the required properties was not found -- log an error and exit.
            logger.error("Error configuring secure port. Component will exit.", e);
            System.exit(1);
        }
    }

    /**
     * Fetch the configuration for this component type from ClusterMgr and store them in
     * the System properties. Retry until you succeed - this is a blocking call.
     *
     * This method is intended to be called by each component's main() method before
     * beginning Spring instantiation.
     */
    private static void fetchConfigurationProperties() {
        final int clusterMgrPort = EnvironmentUtils.parseIntegerFromEnv("clustermgr_port");
        final int clusterMgrConnectionRetryDelaySecs =
                EnvironmentUtils.parseIntegerFromEnv("clustermgr_retry_delay_sec");
        final long clusterMgrConnectionRetryDelayMs =
                Duration.ofSeconds(clusterMgrConnectionRetryDelaySecs).toMillis();
        final String componentType = requireEnvProperty(PROP_COMPNENT_TYPE);
        final String clusterMgrHost = requireEnvProperty("clustermgr_host");
        final String instanceId = requireEnvProperty(PROP_INSTANCE_ID);

        logger.info("Fetching configuration from ClusterMgr for '{}' component of type '{}'",
                instanceId, componentType);
        logger.info("clustermgr_host: {}, clustermgr_port: {}", clusterMgrHost, clusterMgrPort);

        // call ClusterMgr to fetch the configuration for this component type
        final IClusterService clusterMgrClient = ClusterMgrClient.createClient(
                ComponentApiConnectionConfig.newBuilder()
                        .setHostAndPort(clusterMgrHost, clusterMgrPort)
                        .build());
        do {
            try {
                ComponentPropertiesDTO componentProperties =
                        clusterMgrClient.getComponentInstanceProperties(componentType, instanceId);
                componentProperties.forEach((configKey, configValue) -> {
                    logger.info("       {} = {}", configKey, configValue);
                    System.setProperty(configKey, configValue);
                });
                break;
            } catch(ResourceAccessException e) {
                logger.error("Error fetching configuration from ClusterMgr: {}", e.getMessage());
                try {
                    logger.info("...trying again...");
                    Thread.sleep(clusterMgrConnectionRetryDelayMs);
                } catch (InterruptedException e2) {
                    logger.warn("Interrupted while waiting for ClusterMgr; continuing to wait.");
                    Thread.currentThread().interrupt();
                }
            }
        } while (true);
        logger.info("configuration initialized");
    }

    /**
     * Publishes version information of this container into a centralized key-value store.
     */
    private void publishVersionInformation() {
        final String specVersion = getClass().getPackage().getSpecificationVersion();
        if (specVersion != null) {
            logger.debug("Component version {} found", specVersion);
            baseVmtComponentConfig.keyValueStore().put(KEY_COMPONENT_VERSION, specVersion);
        } else {
            logger.error("Could not get Specification-Version for component class {}", getClass());
        }
    }

    /**
     * Exception to be thrown if error occurred while configuring servlet context holder.
     */
    public static class ContextConfigurationException extends Exception {
        public ContextConfigurationException(@Nonnull String message, @Nonnull Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Interface (callback) to configure servlet context.
     */
    public interface ContextConfigurer {
        /**
         * Creates application context and registers it with {@code servletContext}. If there are
         * multiple contexts created, the one should be returned, that could be used to check
         * the health status of the container.
         *
         * @param servletContext servlet context handler
         * @return Spring context to track
         * @throws ContextConfigurationException if exception thrown while configuring
         *         servlets
         *         and contexts
         */
        ConfigurableApplicationContext configure(@Nonnull ServletContextHandler servletContext)
                throws ContextConfigurationException;
    }
}
