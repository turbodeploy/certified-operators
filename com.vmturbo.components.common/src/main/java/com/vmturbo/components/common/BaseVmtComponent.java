package com.vmturbo.components.common;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.google.common.collect.Lists;

import io.grpc.BindableService;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.netty.NettyServerBuilder;
import io.opentracing.contrib.grpc.ServerTracingInterceptor;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.clustermgr.api.ClusterMgrClient;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.common.health.CompositeHealthMonitor;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.components.common.health.SimpleHealthStatus;
import com.vmturbo.components.common.metrics.ScheduledMetrics;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.components.api.tracing.Tracing;
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

    public static final String PROP_COMPONENT_TYPE = "component_type";
    public static final String PROP_INSTANCE_ID = "instance_id";
    public static final String PROP_STANDALONE = "standalone";
    public static final String PROP_serverHttpPort = "serverHttpPort";

    public static final String ENV_CLUSTERMGR_HOST = "clustermgr_host";
    public static final String ENV_CLUSTERMGR_PORT = "clustermgr_port";
    public static final String ENV_CLUSTERMGR_RETRY_S = "clustermgr_retry_delay_sec";

    public static final String PROP_SECURE_PORT = "secure_serverHttpPort";
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

    private static final Logger logger = LogManager.getLogger();

    private static String componentType;
    private static String instanceId;
    private static Boolean enableConsulRegistration;
    private static Boolean standalone;
    private static long clusterMgrConnectionRetryDelayMs;

    private static final DataMetricGauge STARTUP_DURATION_METRIC = DataMetricGauge.builder()
            .withName("component_startup_duration_ms")
            .withHelp("Duration in ms from component instantiation to Spring context built.")
            .build()
            .register();
    private static final String COMPONENT_DEFAULT_PATH =
            "config/component_default.properties";

    private ExecutionStatus status = ExecutionStatus.NEW;
    private final AtomicBoolean startFired = new AtomicBoolean(false);

    private static final int SCHEDULED_METRICS_DELAY_MS=60000;

    @Value("${scheduledMetricsIntervalMs:60000}")
    private int scheduledMetricsIntervalMs;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    // the max message size (in bytes) that the GRPC server for this component will accept. Default
    // value is 4194304 bytes (or 4 MB) which is the GRPC default behavior.
    @Value("${server.grpcMaxMessageBytes:4194304}")
    private int grpcMaxMessageBytes;


    @GuardedBy("grpcServerLock")
    private Server grpcServer;

    private static SetOnce<org.eclipse.jetty.server.Server> JETTY_SERVER = new SetOnce<>();

    private final Object grpcServerLock = new Object();

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    BaseVmtComponentConfig baseVmtComponentConfig;

    @Autowired(required = false)
    DiagnosticService diagnosticService;

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
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
        // we are using the component type as the 'name' until we have more than one instance of a
        // given component type
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
    public CompositeHealthMonitor getHealthMonitor() {
        return healthMonitor;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void startComponent() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down {} component.", getComponentName());
                stopComponent();
            }));
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

        setStatus(ExecutionStatus.MIGRATING);
        enableConsulRegistration = EnvironmentUtils.getOptionalEnvProperty(ConsulDiscoveryManualConfig.ENABLE_CONSUL_REGISTRATION)
                .map(Boolean::parseBoolean)
                .orElse(false);
        if (enableConsulRegistration) {
            baseVmtComponentConfig.migrationFramework().startMigrations(getMigrations(), false);
        }
        startGrpc();
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
        JETTY_SERVER.getValue().ifPresent(server -> {
            try {
                server.stop();
            } catch (Exception e) {
                logger.error("Jetty server failed to stop cleanly.", e);
            }
        });
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
     * Component implementations should override this method and return the gRPC services
     * they want to expose externally.
     * <p>
     * Note - we could do this automatically with Spring, but we choose to do it explicitly
     * so it's easy to tell how services get into the gRPC server.
     */
    @Nonnull
    protected List<BindableService> getGrpcServices() {
        return Collections.emptyList();
    }

    /**
     * Component implementations should override this method and return the gRPC interceptors
     * they want to attach to the services.
     * <p>
     * Each of these interceptors will be attached to each of the services returned by
     * {@link BaseVmtComponent#getGrpcServices()}. If the component author wants to attach an
     * interceptor only to a specific service, do so in the {@link BaseVmtComponent#getGrpcServices()}
     * implementation.
     * <p>
     * TODO (roman, Apr 24 2019): The main reason we have this right now is for the JWT interceptor.
     * We can't import the spring security config directly in BaseVmtComponent because that would
     * create a circular dependency between components.common and auth.api. In the future we should
     * move the "common" security stuff to components.common, initialize the JWT interceptor in
     * BaseVmtComponent, and remove this method.
     */
    @Nonnull
    protected List<ServerInterceptor> getServerInterceptors() {
        return Collections.emptyList();
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
            final List<BindableService> services = getGrpcServices();
            if (!services.isEmpty()) {
                final NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(grpcPort)
                        // Allow keepalives even when there are no existing calls, because we want
                        // to send intermittent keepalives to keep the http2 connections open.
                        .permitKeepAliveWithoutCalls(true)
                        .permitKeepAliveTime(GRPC_MIN_KEEPALIVE_TIME_MIN, TimeUnit.MINUTES)
                        .maxMessageSize(grpcMaxMessageBytes);
                final MonitoringServerInterceptor monitoringInterceptor =
                    MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());
                final ServerTracingInterceptor tracingInterceptor =
                    new ServerTracingInterceptor(Tracing.tracer());

                // add a log level configuration service that will be available if the
                // component decides to build a grpc server. (if not, the REST endpoint for it will
                // still be available).
                final List<BindableService> allServices = Lists.newArrayList(services);
                allServices.add(baseVmtComponentConfig.logConfigurationService());
                allServices.add(baseVmtComponentConfig.tracingConfigurationRpcService());

                final List<ServerInterceptor> serverInterceptors = Lists.newArrayList(getServerInterceptors());
                serverInterceptors.add(monitoringInterceptor);
                // Add the tracing interceptor last, so that it gets called first (Matthew 20:16 :P),
                // and the other interceptors get traced too.
                serverInterceptors.add(tracingInterceptor);

                allServices.forEach(service -> {
                    serverBuilder.addService(ServerInterceptors.intercept(service, serverInterceptors));
                });

                try {
                    grpcServer = serverBuilder.build();
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
     * Get an optional environment property as an int value, returning the default value if the
     * property is not found or not parseable.
     *
     * @param propertyName the property name to look for
     * @param defaultValue the default value to use for the property
     * @return the found value, if there is one, otherwise the default value.
     */
    protected static int getOptionalIntEnvProperty(String propertyName, int defaultValue) {
        try {
            return EnvironmentUtils.getOptionalEnvProperty(propertyName)
                    .map(Integer::parseInt)
                    .orElse(defaultValue);
        } catch (NumberFormatException nfe) {
            logger.error("NumberFormatException parsing property {}, will use default value of {}",
                    propertyName, defaultValue);
            return defaultValue;
        }
    }

    @Nonnull
    protected static ConfigurableWebApplicationContext attachSpringContext(
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
    protected static ConfigurableWebApplicationContext startContext(
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
    protected static ConfigurableWebApplicationContext startContext(
            @Nonnull ContextConfigurer contextConfigurer) {
        // fetch the component information from the environment
        componentType = EnvironmentUtils.requireEnvProperty(PROP_COMPONENT_TYPE);
        instanceId = EnvironmentUtils.requireEnvProperty(PROP_INSTANCE_ID);
        standalone = EnvironmentUtils.getOptionalEnvProperty(PROP_STANDALONE)
                .map(Boolean::parseBoolean)
                .orElse(false);
        // load the local configuration properties
        Properties defaultProperties = loadConfigurationProperties();
        if (standalone) {
            // call ClusterMgr to fetch the configuration for this component type - blocking call
            fetchLocalConfigurationProperties(defaultProperties);
        } else {
            // get a pointer to the ClusterMgr client api
            ClusterMgrRestClient clusterMgrClient = getClusterMgrClient();

            // send the default config properties to clustermgr - blocking call
            updateClusterConfigurationProperties(clusterMgrClient, defaultProperties);

            // call ClusterMgr to fetch the configuration for this component type - blocking call
            fetchClusterConfigurationProperties(clusterMgrClient);
        }


        logger.info("Starting web server with spring context");
        final String serverPort = EnvironmentUtils.requireEnvProperty(PROP_serverHttpPort);
        System.setProperty("org.jooq.no-logo", "true");

        org.eclipse.jetty.server.Server server =
                new org.eclipse.jetty.server.Server(Integer.valueOf(serverPort));
        JETTY_SERVER.trySetValue(server);

        final ServletContextHandler contextServer =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        final ConfigurableWebApplicationContext context;
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

    public static ClusterMgrRestClient getClusterMgrClient() {
        final int clusterMgrPort = EnvironmentUtils.parseIntegerFromEnv("clustermgr_port");
        final int clusterMgrConnectionRetryDelaySecs = EnvironmentUtils.parseIntegerFromEnv("clustermgr_retry_delay_sec");
        clusterMgrConnectionRetryDelayMs =
                Duration.ofSeconds(clusterMgrConnectionRetryDelaySecs).toMillis();
        final String clusterMgrHost = EnvironmentUtils.requireEnvProperty("clustermgr_host");

        logger.info("clustermgr_host: {}, clustermgr_port: {}", clusterMgrHost, clusterMgrPort);
        return ClusterMgrClient.createClient(
                ComponentApiConnectionConfig.newBuilder()
                        .setHostAndPort(clusterMgrHost, clusterMgrPort)
                        .build());
    }

    /**
     * Read the default configuration properties for this component from the resource file.
     *
     */
    private static Properties loadConfigurationProperties() {
        logger.info("Sending the default configuration for this component to ClusterMgr");
        // first read the default config from a resource
        final Properties defaultProperties = new Properties();
        try (final InputStream configPropertiesStream = BaseVmtComponent.class.getClassLoader()
                .getResourceAsStream(COMPONENT_DEFAULT_PATH)) {
            if (configPropertiesStream != null) {
                defaultProperties.load(configPropertiesStream);
            } else {
                logger.warn("Cannot find component default properties file: {} for component: {}",
                        COMPONENT_DEFAULT_PATH, componentType);
            }
        } catch (IOException e) {
            // if the component defaults cannot be found we still need to send an empty
            // default properties to ClusterMgr where the global defaults will be used.
            logger.warn("Cannot read default configuration properties file: {} for component: {};" +
                            "- assuming empty configuration.", COMPONENT_DEFAULT_PATH,
                    componentType);
        }
        return defaultProperties;
    }

    /**
     * Read the default configuration properties for this component from the resource file and
     * publish those to the clustermgr configuration port.
     *
     * @param clusterMgrClient the clustermgr api client handle
     */
    private static void updateClusterConfigurationProperties(
            @Nonnull final ClusterMgrRestClient clusterMgrClient, Properties defaultProperties) {
        logger.info("Sending the default configuration for this component to ClusterMgr");
        // now loop forever trying to call the clustermgr client to store those default properties
        final ComponentPropertiesDTO defaultComponentProperties = new ComponentPropertiesDTO();
        defaultProperties.forEach((defaultKey, defaultValue) ->
            defaultComponentProperties.put(defaultKey.toString(), defaultValue.toString()));
        defaultComponentProperties.put(PROP_INSTANCE_ID, instanceId);
        int tryCount = 1;
        do {
            try {
                clusterMgrClient.putComponentDefaultProperties(componentType, defaultComponentProperties);
                logger.info("Default property values for component type '{}' successfully stored:",
                    componentType);
                defaultProperties.forEach((configKey, configValue) ->
                    logger.info("       {} = >{}<", configKey, configValue));
                break;
            } catch (ResourceAccessException e) {
                logger.error("Error in attempt {} to send default configuration from ClusterMgr: {}",
                    tryCount++, e.getMessage());
                sleepWaitingForClusterMgr();
            }
        } while(true);
    }

    /**
     * Fetch the local configuration for this component type and store them in
     * the System properties. Retry until you succeed - this is a blocking call.
     *
     * This method is intended to be called by each component's main() method before
     * beginning Spring instantiation.
     *
     */
    private static void fetchLocalConfigurationProperties(Properties defaultProperties) {
        do {
            try {
                defaultProperties.stringPropertyNames().forEach(configKey -> {
                    logger.info("       {} = >{}<", configKey, defaultProperties.getProperty(configKey));
                    System.setProperty(configKey, defaultProperties.getProperty(configKey));
                });
                break;
            } catch(ResourceAccessException e) {
                logger.error("Error fetching configuration from ClusterMgr: {}", e.getMessage());
                sleepWaitingForClusterMgr();
            }
        } while (true);
        logger.info("configuration initialized");
    }

    /**
     * Fetch the configuration for this component type from ClusterMgr and store them in
     * the System properties. Retry until you succeed - this is a blocking call.
     *
     * This method is intended to be called by each component's main() method before
     * beginning Spring instantiation.
     *
     * @param clusterMgrClient the clustermgr api client handle
     */
    private static void fetchClusterConfigurationProperties(ClusterMgrRestClient clusterMgrClient) {

        logger.info("Fetching configuration from ClusterMgr for '{}' component of type '{}'",
                instanceId, componentType);
        do {
            try {
                ComponentPropertiesDTO componentProperties =
                        clusterMgrClient.getComponentInstanceProperties(componentType, instanceId);
                componentProperties.forEach((configKey, configValue) -> {
                    logger.info("       {} = >{}<", configKey, configValue);
                    System.setProperty(configKey, configValue);
                });
                break;
            } catch(ResourceAccessException e) {
                logger.error("Error fetching configuration from ClusterMgr: {}", e.getMessage());
                sleepWaitingForClusterMgr();
            }
        } while (true);
        logger.info("configuration initialized");
    }

    /**
     * Sleep while wait/looping for ClusterMgr to respond.
     */
    private static void sleepWaitingForClusterMgr() {
        try {
            logger.info("...sleeping for {} ms and then trying again...",
                    clusterMgrConnectionRetryDelayMs);
            Thread.sleep(clusterMgrConnectionRetryDelayMs);
        } catch (InterruptedException e2) {
            logger.warn("Interrupted while waiting for ClusterMgr; continuing to wait.");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Publishes version information of this container into a centralized key-value store.
     */
    private void publishVersionInformation() {
        enableConsulRegistration = EnvironmentUtils.getOptionalEnvProperty(ConsulDiscoveryManualConfig.ENABLE_CONSUL_REGISTRATION)
                .map(Boolean::parseBoolean)
                .orElse(false);
        final String specVersion = getClass().getPackage().getSpecificationVersion();
        if (enableConsulRegistration && specVersion != null) {
            logger.debug("Component version {} found", specVersion);
            baseVmtComponentConfig.keyValueStore().put(KEY_COMPONENT_VERSION, specVersion);
        } else if (specVersion == null) {
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
        ConfigurableWebApplicationContext configure(@Nonnull ServletContextHandler servletContext)
                throws ContextConfigurationException;
    }

}
