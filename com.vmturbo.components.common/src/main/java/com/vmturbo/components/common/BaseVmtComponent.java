package com.vmturbo.components.common;

import static com.vmturbo.clustermgr.api.ClusterMgrClient.COMPONENT_VERSION_KEY;
import static com.vmturbo.components.common.ConsulRegistrationConfig.ENABLE_CONSUL_REGISTRATION;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletRegistration;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
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

import org.apache.commons.lang3.StringUtils;
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

import com.vmturbo.clustermgr.api.ClusterMgrClient;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.clustermgr.api.ComponentProperties;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.tracing.Tracing;
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

    /**
     * The number of seconds to wait for gRPC server to shutdown
     * during the shutdown procedure for the component.
     */
    private static final int GRPC_SHUTDOWN_WAIT_S = 10;

    /**
     * The minimum acceptable server-side keepalive rate.
     *
     * <p>In gRPC, the server only accepts keepalives every 5 minutes by default.
     * We want to set it a little lower. The server will reject keepalives coming
     * in at a greater rate.
     */
    private static final int GRPC_MIN_KEEPALIVE_TIME_MIN = 1;

    /**
     * The environment key for the "type" (or category) for the current component.
     */
    public static final String PROP_COMPONENT_TYPE = "component_type";
    /**
     * The environment key for the unique ID for this instance of the component type.
     */
    public static final String PROP_INSTANCE_ID = "instance_id";
    /**
     * The environment key for the IP for this component instance.
     */
    public static final String PROP_INSTANCE_IP = "instance_ip";
    /**
     * The environment key - should this component only take configuration properties from the
     * environment ("standalone: true") vs. fetch configuration properties from
     * ClusterMgr ("standalone: false").
     */
    public static final String PROP_STANDALONE = "standalone";
    /**
     * The environment key for the port number for the Jetty instance for each component.
     */
    public static final String PROP_serverHttpPort = "serverHttpPort";

    /**
     * The environment key for the hostname to contact for the ClusterMgr API.
     */
    public static final String ENV_CLUSTERMGR_HOST = "clustermgr_host";
    /**
     * The environment key for the port number to contact for the ClusterMgr API.
     */
    public static final String ENV_CLUSTERMGR_PORT = "clustermgr_port";
    /**
     * The environment key for the value to delay when looping trying to contact
     * ClusterMgr.
     */
    public static final String ENV_CLUSTERMGR_RETRY_S = "clustermgr_retry_delay_sec";

    // These keys/values are defined in global_defaults.properties files. During components startup,
    // if keys are in the OVERRIDABLE_ENV_PROPERTIES set and passed in from JVM environment,
    // values will be applied.
    private static final Set<String> OVERRIDABLE_ENV_PROPERTIES =
        ImmutableSet.of("dbPort", "dbUsername", "dbUserPassword", "sqlDialect");

    /**
     * The URL at which to expose Prometheus metrics.
     */
    public static final String METRICS_URL = "/metrics";

    private static final Logger logger = LogManager.getLogger();

    private static String componentType;
    private static String instanceId;
    private static String instanceIp;
    private static Boolean standalone;
    private static long clusterMgrConnectionRetryDelayMs;

    private static final DataMetricGauge STARTUP_DURATION_METRIC = DataMetricGauge.builder()
            .withName("component_startup_duration_ms")
            .withHelp("Duration in ms from component instantiation to Spring context built.")
            .build()
            .register();

    private static final String CONFIG = "config";
    private static final String COMPONENT_DEFAULT_PATH =
                    CONFIG + "/component_default.properties";

    private ExecutionStatus status = ExecutionStatus.NEW;
    private final AtomicBoolean startFired = new AtomicBoolean(false);

    private static final int SCHEDULED_METRICS_DELAY_MS = 60000;

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

    private static final SetOnce<org.eclipse.jetty.server.Server> JETTY_SERVER = new SetOnce<>();

    private final Object grpcServerLock = new Object();

    @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
    @Autowired
    BaseVmtComponentConfig baseVmtComponentConfig;

    @Autowired(required = false)
    DiagnosticService diagnosticService;

    @Autowired
    private ServletContext servletContext;

    @Autowired
    private ConsulRegistrationConfig consulRegistrationConfig;

    /**
     * Embed a component for monitoring dependency/subcomponent health.
     */
    private final CompositeHealthMonitor healthMonitor =
            new CompositeHealthMonitor(componentType + " Component");

    static {
        // Capture the beginning of component execution - begins from when the Java static is loaded.
        startupTime = Instant.now();
    }

    private static Instant startupTime;

    /**
     * Constructor for BaseVmtComponent.
     */
    public BaseVmtComponent() {
        // install a component ExecutionStatus-based health check into the monitor.
        // This health check will return true if the component is in the RUNNING state and false for
        // all other states.
        healthMonitor.addHealthCheck(new HealthStatusProvider() {
            private HealthStatus lastStatus;

            @Override
            public String getName() {
                return "ExecutionStatus";
            }

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
        // return the component type as the 'name' only if the instanceId is not set
        if (StringUtils.isNotBlank(instanceId)) {
            return instanceId;
        }
        return componentType;
    }

    /**
     * After the Spring Environment has been constructed calculate the elapsed time.
     */
    @PostConstruct
    public void componentContextConstructed() {
        logger.info("------------ spring context constructed ----------");
        final long msToStartUp = Duration.between(startupTime, Instant.now())
                .toMillis();
        logger.info("time to start: {} ms", msToStartUp);
        STARTUP_DURATION_METRIC.setData((double)msToStartUp);
    }

    /**
     * Before the Spring Context is closed, log a message.
     */
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
        ScheduledMetrics.initializeScheduledMetrics(
            scheduledMetricsIntervalMs, SCHEDULED_METRICS_DELAY_MS);

        // add the additional health checks if they aren't already registered
        Map<String, HealthStatusProvider> healthChecks = getHealthMonitor().getDependencies();
        if (!healthChecks.containsKey(baseVmtComponentConfig.memoryMonitor().getName())) {
            logger.info("Adding memory health check.");
            getHealthMonitor().addHealthCheck(baseVmtComponentConfig.memoryMonitor());
        }
        if (!healthChecks.containsKey(baseVmtComponentConfig.deadlockHealthMonitor().getName())) {
            logger.info("Adding deadlock health check");
            getHealthMonitor().addHealthCheck(baseVmtComponentConfig.deadlockHealthMonitor());
        }

        setStatus(ExecutionStatus.MIGRATING);
        final Boolean enableConsulRegistration = EnvironmentUtils
            .getOptionalEnvProperty(ENABLE_CONSUL_REGISTRATION)
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
        // Remove the instance IP address from a component that is being stopped
        // TODO: need to implement complete deregistration under OM-48049
        if (standalone != null && !standalone) {
            // get a pointer to the ClusterMgr client api
            ClusterMgrRestClient clusterMgrClient = getClusterMgrClient();

            clusterMgrClient.setComponentInstanceProperty(componentType, instanceId,
                PROP_INSTANCE_IP, "");
        }
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
     *
     * <p>Note - we could do this automatically with Spring, but we choose to do it explicitly
     * so it's easy to tell how services get into the gRPC server.
     *
     * @return exposed gRPC services
     */
    @Nonnull
    protected List<BindableService> getGrpcServices() {
        return Collections.emptyList();
    }

    /**
     * Component implementations should override this method and return the gRPC interceptors
     * they want to attach to the services.
     *
     * <p>Each of these interceptors will be attached to each of the services returned by
     * {@link BaseVmtComponent#getGrpcServices()}. If the component author wants to attach an
     * interceptor only to a specific service, do so in the {@link BaseVmtComponent#getGrpcServices()}
     * implementation.
     *
     * <p>TODO (roman, Apr 24 2019): The main reason we have this right now is for the JWT interceptor.
     * We can't import the spring security config directly in BaseVmtComponent because that would
     * create a circular dependency between components.common and auth.api. In the future we should
     * move the "common" security stuff to components.common, initialize the JWT interceptor in
     * BaseVmtComponent, and remove this method.
     *
     * @return gRPC interceptors
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

                allServices.forEach(service -> serverBuilder
                    .addService(ServerInterceptors.intercept(service, serverInterceptors)));

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
        try {
            instanceIp = EnvironmentUtils.getOptionalEnvProperty(PROP_INSTANCE_IP)
                .orElse(InetAddress.getLocalHost().getHostAddress());
        } catch (UnknownHostException e) {
            logger.error("Cannot fetch localHost().", e);
            System.exit(1);
        }
        logger.info("External configuration: componentType {}; instanceId {}; instanceIp {};",
            componentType, instanceId, instanceIp);
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
                new org.eclipse.jetty.server.Server(Integer.parseInt(serverPort));
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

    /**
     * Create a handle to the ClusterMgr component REST API.
     *
     * @return a new Client for the ClusterMgr REST API
     */
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
     * @return properties loaded from the config resource of the component
     *
     */
    @VisibleForTesting
    static Properties loadConfigurationProperties() {
        logger.info("Sending the default configuration for this component to ClusterMgr");
        final Properties defaultProperties = new Properties();
        try {
            // Handle component defaults first (so other properties can override)
            defaultProperties.putAll(loadDefaultProperties());
            // Handle other resources in config
            defaultProperties.putAll(loadOtherProperties());
        } catch (URISyntaxException | IOException e) {
            logger.error("Could not access the config resources", e);
        }

        return defaultProperties;
    }

    private static Properties loadDefaultProperties() throws IOException {
        // TODO: what if there are other jars with the same resource? Should we
        // forcefully prevent that (not let the component start)? Consider them as override?
        logger.info("Loading component defaults from " + COMPONENT_DEFAULT_PATH);
        return propsFromInputStream(() -> BaseVmtComponent.class.getClassLoader()
                        .getResourceAsStream(COMPONENT_DEFAULT_PATH), COMPONENT_DEFAULT_PATH);
    }

    /**
     * Load properties other than {@link #COMPONENT_DEFAULT_PATH}.
     * Look for files in the "config" resource. Files of type ".properties" are
     * treated as {@link Properties} files. For other file types create a property
     * which name is the file name and value is the content of the file.
     *
     * @return a properties map with all the loaded properties
     * @throws IOException when there is a problem accessing resources
     * @throws URISyntaxException when unable to convert a resource URL to URI
     */
    private static Properties loadOtherProperties() throws URISyntaxException, IOException {
        Properties properties = new Properties();
        Enumeration<URL> configs = BaseVmtComponent.class.getClassLoader().getResources(CONFIG);
        while (configs.hasMoreElements()) {
            URI uri = configs.nextElement().toURI();
            FileSystem fs = fileSystem(uri);
            Path configPath = fs.getPath(path(uri));
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(configPath)) {
                ds.forEach(propPath -> {
                    // Skip COMPONENT_DEFAULT_PATH - it is loaded in loadDefaultProperties()
                    if (!propPath.toString().endsWith(COMPONENT_DEFAULT_PATH)) {
                        String fileName = propPath.getFileName().toString();
                        if (fileName.endsWith(".properties")) {
                            properties.putAll(propsFromInputStream(
                                pathInputStream(propPath), propPath.toString()));
                        } else {
                            logger.info("Looading " + propPath);
                            try {
                                String content = new String(Files.readAllBytes(propPath));
                                properties.put(fileName, content);
                                logger.info("Loaded " + content.length()
                                + " bytes from " + propPath);
                            } catch (IOException e) {
                                logger.warn("Could not load " + propPath);
                            }
                        }
                    }
                });
            } finally {
                try {
                    fs.close();
                } catch (UnsupportedOperationException usoe) {
                    // Happens during testing with "file" scheme. Ignore.
                }
            }
        }
        return properties;
    }

    private static Supplier<InputStream> pathInputStream(Path propPath) {
        return () -> {
            try {
                return Files.newInputStream(propPath);
            } catch (IOException e) {
                return null;
            }
        };
    }

    private static Properties propsFromInputStream(Supplier<InputStream> isSupplier,
                    String pathName) {
        logger.info("Loading properties from " + pathName);
        Properties props = new Properties();
        try (InputStream is = isSupplier.get()) {
            props.load(is);
            int numProps = props.size();
            String propCount = numProps + (numProps == 1 ? " property" : " properties");
            logger.info("Loaded " + propCount + " from " + pathName);
        } catch (IOException ioe) {
            logger.warn("Could not load properties from " + pathName);
        }
        return props;
    }

    private static FileSystem fileSystem(URI uri) throws IOException {
        return "file".equals(uri.getScheme())
            // "file" scheme used in unit tests
            ? FileSystems.getDefault()
            // "jar" scheme expected at runtime
            : FileSystems.newFileSystem(
                URI.create(uri.toString().replaceFirst("!.*", "")), Collections.emptyMap());
    }

    private static String path(URI uri) {
        return "file".equals(uri.getScheme())
            // "file" scheme used in unit tests
            ? uri.getPath()
            // "jar" scheme expected at runtime
            : uri.toString().replaceFirst(".*!", "");
    }

    /**
     * Read the default configuration properties for this component from the resource file and
     * publish those to the clustermgr configuration port.
     *
     * @param clusterMgrClient the clustermgr api client handle
     * @param defaultProperties default properties to be sent to clustermgr
     */
    private static void updateClusterConfigurationProperties(
            @Nonnull final ClusterMgrRestClient clusterMgrClient, Properties defaultProperties) {
        logger.info("Sending the default configuration for this component to ClusterMgr");
        // now loop forever trying to call the clustermgr client to store those default properties
        final ComponentProperties defaultComponentProperties = new ComponentProperties();
        defaultProperties.forEach((defaultKey, defaultValue) ->
            defaultComponentProperties.put(defaultKey.toString(), defaultValue.toString()));
        int tryCount = 1;
        do {
            try {
                clusterMgrClient.putComponentDefaultProperties(componentType, defaultComponentProperties);
                logger.info("Default property values for component type '{}' successfully stored",
                    componentType);
                defaultProperties.forEach(BaseVmtComponent::logProperty);
                break;
            } catch (ResourceAccessException e) {
                logger.error("Error in attempt {} to send default configuration from ClusterMgr: {}",
                    tryCount++, e.getMessage());
                sleepWaitingForClusterMgr();
            }
        } while (true);
    }

    /**
     * Fetch the local configuration for this component type and store them in
     * the System properties. Retry until you succeed - this is a blocking call.
     *
     * This method is intended to be called by each component's main() method before
     * beginning Spring instantiation.
     *
     * @param defaultProperties the set of properties to use as the baseline;
     *                          loaded from a file "global_defaults.properties"
     */
    @VisibleForTesting
    static void fetchLocalConfigurationProperties(Properties defaultProperties) {
        do {
            try {
                defaultProperties.stringPropertyNames().forEach(configKey -> {
                    logProperty(configKey, defaultProperties.getProperty(configKey));
                    System.setProperty(configKey, defaultProperties.getProperty(configKey));
                });
                break;
            } catch (ResourceAccessException e) {
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
                ComponentProperties componentProperties =
                        clusterMgrClient.getEffectiveInstanceProperties(componentType, instanceId);
                componentProperties.forEach((configKey, configValue) -> {
                    logProperty(configKey, configValue);
                    // {@link PropertySourcesPlaceholderConfigurer} is registered in
                    // BaseVmtComponentConfig::configurer(), so environment variable values
                    // will also be injected.
                    if (isOverridden(configKey)) {
                       logger.info("Found overridable environment variable: {}, with value: {}." +
                           " Skip applying instance value: {}", configKey,
                           EnvironmentUtils.requireEnvProperty(configKey), configValue);
                    } else {
                        System.setProperty(configKey, configValue);
                    }
                });
                break;
            } catch (ResourceAccessException e) {
                logger.error("Error fetching configuration from ClusterMgr: {}", e.getMessage());
                sleepWaitingForClusterMgr();
            }
        } while (true);
        logger.info("configuration initialized");
    }

    @VisibleForTesting
    static boolean isOverridden(@Nonnull final String configKey) {
        return OVERRIDABLE_ENV_PROPERTIES.contains(configKey)
            && EnvironmentUtils.getOptionalEnvProperty(configKey).isPresent();
    }

    /**
     * Log a pair of key-value properties. If the value is too long or
     * is multi-lines then print the first few characters followed by ...
     *
     * @param key a property key
     * @param value a property value
     */
    private static void logProperty(Object key, Object value) {
        String str;
        if (value == null) {
            str = null;
        } else {
            str = value.toString();
            int originalLength = str.length();
            boolean truncated = false;
            if (str.length() > 80) {
                truncated = true;
                str = str.substring(0, 76);
            }
            if (str.contains("\n")) {
                truncated = true;
                str = str.substring(0, str.indexOf("\n"));
            }
            if (truncated) {
                str += "... [" + (originalLength - str.length() + " bytes truncated]");
            }
        }
        logger.info("       {} = '{}'", key, str);
    }

    /**
     * Get the value of a single configuration property.
     * This implementation assumes the configuration properties are stored as System
     * properties. This may change when switching to Kubernetes configmaps.
     *
     * @param property the configuration property to get
     * @return the value of the configuration property
     */
    public static String getConfigurationProperty(String property) {
        return System.getProperty(property);
    }

    /**
     * Sleep while wait/looping for ClusterMgr to respond.
     */
    private static void sleepWaitingForClusterMgr() {
        try {
            logger.info("...sleeping for {} ms and then trying again...",
                    clusterMgrConnectionRetryDelayMs);
            Thread.sleep(clusterMgrConnectionRetryDelayMs);
        } catch (InterruptedException ie) {
            logger.warn("Interrupted while waiting for ClusterMgr; continuing to wait.");
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Publishes version information of this container into a centralized key-value store.
     */
    private void publishVersionInformation() {
        if (standalone != null && !standalone) {
            // get a pointer to the ClusterMgr client api
            ClusterMgrRestClient clusterMgrClient = getClusterMgrClient();

            // get the component version
            final String specVersion = getClass().getPackage().getSpecificationVersion();
            if (specVersion != null) {
                logger.info("Component version for {} found: {}", componentType, specVersion);
                baseVmtComponentConfig.keyValueStore().put(COMPONENT_VERSION_KEY, specVersion);
            } else if (specVersion == null) {
                logger.error("Could not get Specification-Version for component class {}", getClass());
            }
            // and the component IP address
            if (StringUtils.isNotBlank(instanceId) && StringUtils.isNotBlank(instanceIp)) {
                clusterMgrClient.setComponentInstanceProperty(componentType, instanceId,
                    PROP_INSTANCE_IP, instanceIp);
            }
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
