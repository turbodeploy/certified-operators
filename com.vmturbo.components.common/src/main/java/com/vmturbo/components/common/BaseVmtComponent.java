package com.vmturbo.components.common;

import static com.vmturbo.clustermgr.api.ClusterMgrClient.COMPONENT_VERSION_KEY;
import static com.vmturbo.components.common.ConsulRegistrationConfig.ENABLE_CONSUL_REGISTRATION;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.servlet.Servlet;
import javax.servlet.ServletContext;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.Environment;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.clustermgr.api.ClusterMgrClient;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.common.config.PropertiesLoader;
import com.vmturbo.components.common.diagnostics.DiagnosticService;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.health.ComponentStatusNotifier;
import com.vmturbo.components.common.health.CompositeHealthMonitor;
import com.vmturbo.components.common.health.ConsulHealthcheckRegistration;
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
     * The environment key for the IP for this component instance.
     */
    public static final String PROP_INSTANCE_ROUTE = "instance_route";
    /**
     * The environment key - should this component only take configuration properties from the
     * environment ("standalone: true") vs. fetch configuration properties from
     * ClusterMgr ("standalone: false").
     */
    public static final String PROP_STANDALONE = "standalone";
    /**
     * The environment key for a flag indicating whether this component should force retry
     * failed migrations on startup.
     */
    public static final String PROP_FORCE_RETRY_MIGRATIONS = "force_retry_migrations";

    /**
     * The path to the "properties.yaml" file used to load external configuration properties
     * prior to Spring configuration.
     */
    public static final String PROP_PROPERTIES_YAML_PATH = "propertiesYamlPath";

    /**
     * The path to the secret file used to load external secret properties
     * prior to Spring configuration.
     */
    public static final String PROP_SECRETS_YAML_PATH = "secretsYamlPath";

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
     * The environment key for the route to contact for the ClusterMgr API.
     */
    public static final String ENV_CLUSTERMGR_ROUTE = "clustermgr_route";
    /**
     * The environment key for the value to delay when looping trying to contact
     * ClusterMgr.
     */
    public static final String ENV_CLUSTERMGR_RETRY_S = "clustermgr_retry_delay_sec";

    // These keys/values are defined in global_defaults.properties files. During components startup,
    // if keys are in the OVERRIDABLE_ENV_PROPERTIES set and passed in from JVM environment,
    // values will be applied.
    private static final Set<String> OVERRIDABLE_ENV_PROPERTIES =
        ImmutableSet.of("dbPort", "dbRootUsername", "dbRootPassword", "sqlDialect");

    /**
     * The URL at which to expose Prometheus metrics.
     */
    public static final String METRICS_URL = "/metrics";

    private static final Logger logger = LogManager.getLogger();

    private static final int DEFAULT_SERVER_HTTP_PORT = 8080;

    @Value("${" + BaseVmtComponent.PROP_COMPONENT_TYPE + '}')
    private String componentType;

    @Value("${" + BaseVmtComponent.PROP_INSTANCE_ID + '}')
    private String instanceId;

    private static SetOnce<String> instanceIp = new SetOnce<>();

    /**
     * Indicate whether this component should contact ClusterMgr for configuration information
     * on startup or shut-down.
     */
    private Boolean standalone = EnvironmentUtils.getOptionalEnvProperty(PROP_STANDALONE)
        .map(Boolean::parseBoolean)
        .orElse(false);

    /**
     * Indicate whether this component should force retry failed migrations on startup.
     */
    private Boolean forceRetryMigrations =
        EnvironmentUtils.getOptionalEnvProperty(PROP_FORCE_RETRY_MIGRATIONS)
            .map(Boolean::parseBoolean)
            .orElse(false);


    private static final DataMetricGauge STARTUP_DURATION_METRIC = DataMetricGauge.builder()
            .withName("component_startup_duration_ms")
            .withHelp("Duration in ms from component instantiation to Spring context built.")
            .build()
            .register();

    private ExecutionStatus status = ExecutionStatus.NEW;
    private final AtomicBoolean startFired = new AtomicBoolean(false);

    /**
     * This property is used to disable consul registration. This is necessary for tests and
     * for components running outside the primary Turbonomic K8s cluster.
     */
    @Value("${" + ENABLE_CONSUL_REGISTRATION + ":true}")
    private Boolean enableConsulRegistration;

    private static final int SCHEDULED_METRICS_DELAY_MS = 60000;

    @Value("${scheduledMetricsIntervalMs:60000}")
    private int scheduledMetricsIntervalMs;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${connRetryIntervalSeconds}")
    private String connRetryIntervalSeconds;

    // the max message size (in bytes) that the GRPC server for this component will accept. Default
    // value is 4194304 bytes (or 4 MB) which is the GRPC default behavior.
    @Value("${server.grpcMaxMessageBytes:4194304}")
    private int grpcMaxMessageBytes;

    @Value("${enableMemoryMonitor:false}")
    private boolean enableMemoryMonitor;

    private static final SetOnce<org.eclipse.jetty.server.Server> JETTY_SERVER = new SetOnce<>();

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
        if (enableMemoryMonitor && !healthChecks.containsKey(baseVmtComponentConfig.memoryMonitor().getName())) {
            logger.info("Adding memory health check.");
            getHealthMonitor().addHealthCheck(baseVmtComponentConfig.memoryMonitor());
        }
        if (!healthChecks.containsKey(baseVmtComponentConfig.deadlockHealthMonitor().getName())) {
            logger.info("Adding deadlock health check");
            getHealthMonitor().addHealthCheck(baseVmtComponentConfig.deadlockHealthMonitor());
        }

        setStatus(ExecutionStatus.MIGRATING);
        if (enableConsulRegistration) {
            baseVmtComponentConfig.migrationFramework()
                .startMigrations(getMigrations(), forceRetryMigrations);
        }

        registerGrpcServices();

        // Run the "onStartComponent", and other methods that require external access, in a separate
        // thread so that the main context // initialization thread is not blocked by any blocking operations in the "startComponent"
        // method.
        final ExecutorService svc = Executors.newSingleThreadExecutor();
        svc.execute(() -> {
            try {
                this.onStartComponent();
                publishVersionInformation();
                consulRegistrationConfig.componentStatusNotifier().ifPresent(ComponentStatusNotifier::notifyComponentStartup);
                setStatus(ExecutionStatus.RUNNING);
            } catch (Exception e) {
                logger.error("Error while trying to finish startup routine. Will shut down.", e);
                System.exit(1);
            }
        });
        svc.shutdown();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public final void stopComponent() {
        setStatus(ExecutionStatus.STOPPING);
        logger.info("Deregistering service: {}", instanceId);
        consulRegistrationConfig.consulHealthcheckRegistration().deregisterService();
        consulRegistrationConfig.componentStatusNotifier().ifPresent(ComponentStatusNotifier::notifyComponentShutdown);
        onStopComponent();
        ComponentGrpcServer.get().stop();
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
    public final void dumpDiags(@Nonnull final ZipOutputStream diagnosticZip) throws DiagnosticsException {
        try {
            // diagnosticService must be injected by the particular component. Check that happened correctly.
            if (baseVmtComponentConfig.diagnosticService() == null) {
                throw new RuntimeException("DiagnosticService missing");
            }
            try {
                baseVmtComponentConfig.diagnosticService().dumpSystemDiags(diagnosticZip);
            } catch (DiagnosticsException e) {
                logger.error("Failed to collect system diagnostics. Will try to get component diagnostics.", e);
            }

            // call the component's dump diags handler override, if any
            onDumpDiags(diagnosticZip);

        } finally {
            try {
                diagnosticZip.finish();
            } catch (IOException e) {
                logger.error("Error finishing diagnostic zip", e);
                throw new DiagnosticsException("Error finishing diagnostic zip", e);
            }
        }
    }

    // START: Methods to allow component implementations to hook into
    // the component lifecycle. BaseVmtComponent should NOT have any
    // code here. These methods are effectively abstract, but made non-abstract
    // for convenience.
    protected void onStartComponent() {}

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

    private void registerGrpcServices() {
        final List<BindableService> services = Lists.newArrayList(getGrpcServices());
        services.add(baseVmtComponentConfig.logConfigurationService());
        services.add(baseVmtComponentConfig.tracingConfigurationRpcService());
        ComponentGrpcServer.get().addServices(services, getServerInterceptors());
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

    private static String getInitializedInstanceIp() {
        try {
            final String ip = EnvironmentUtils.getOptionalEnvProperty(PROP_INSTANCE_IP)
                .orElse(InetAddress.getLocalHost().getHostAddress());
            return instanceIp.ensureSet(() -> ip);
        } catch (UnknownHostException e) {
            logger.error("Cannot fetch localHost().", e);
            System.exit(1);
            throw new IllegalStateException("Cannot fetch localhost.", e);
        }
    }

    /**
     * Initialize a Spring context configured for a Web Application. Register the given
     * Configuration class in the context, load the external configuration properties as
     * property sources, and define a DispatcherServlet in the context. Return the Context
     * as "active".
     *
     * @param contextConfigurer add context configuration specific to a particular component
     * @param configurationClass the main @Configuration class to load into the context
     * @return the ConfigurableWebApplicationContext configured with the given @Configuration class
     * and set to "active"
     * @throws ContextConfigurationException if there's an error reading the external configuration
     * properties
     */
    @Nonnull
    protected static ConfigurableWebApplicationContext attachSpringContext(
            @Nonnull ServletContextHandler contextConfigurer,
            @Nonnull Class<?> configurationClass) throws ContextConfigurationException {
        final AnnotationConfigWebApplicationContext applicationContext =
                new AnnotationConfigWebApplicationContext();
        logger.info("Creating application context for: componentType {}; instanceId {}; instanceIp {};",
            EnvironmentUtils.requireEnvProperty(PROP_COMPONENT_TYPE),
            EnvironmentUtils.requireEnvProperty(PROP_INSTANCE_ID),
            getInitializedInstanceIp());
        PropertiesLoader.addConfigurationPropertySources(applicationContext);

        // Add the main @Configuration class to the context and add servlet dispatcher and holder
        applicationContext.register(configurationClass);
        final Servlet dispatcherServlet = new DispatcherServlet(applicationContext);
        final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);
        contextConfigurer.addServlet(servletHolder, "/*");

        // Setup Spring context event listener / dispatcher
        contextConfigurer.addEventListener(new ContextLoaderListener(applicationContext));
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

    protected static void addMetricsServlet(@Nonnull final ServletContextHandler contextServer) {
        final Servlet servlet = new MetricsServlet(CollectorRegistry.defaultRegistry);
        final ServletHolder servletholder = new ServletHolder(servlet);
        contextServer.addServlet(servletholder, METRICS_URL);
    }

    @Nonnull
    protected static ConfigurableWebApplicationContext startServer(
            @Nonnull ContextConfigurer contextConfigurer) {
        logger.info("Starting web server with spring context");
        final int serverPort = EnvironmentUtils.parseOptionalIntegerFromEnv(PROP_serverHttpPort)
            .orElse(DEFAULT_SERVER_HTTP_PORT);
        System.setProperty("org.jooq.no-logo", "true");

        // This shouldn't be null, because the only way ensureSet returns null is if
        // the inner supplier returns null.
        org.eclipse.jetty.server.Server server = JETTY_SERVER.ensureSet(() ->
            new org.eclipse.jetty.server.Server(serverPort));

        final ServletContextHandler contextServer =
            new ServletContextHandler(ServletContextHandler.SESSIONS);
        final ConfigurableWebApplicationContext context;
        try {
            server.setHandler(contextServer);
            context = contextConfigurer.configure(contextServer);
            addMetricsServlet(contextServer);
            server.start();
            if (!context.isActive()) {
                logger.error("Spring context failed to start. Shutting down.");
                System.exit(1);
            }

            // The starting of the component should add the gRPC services defined in the spring
            // context to the gRPC server.
            ComponentGrpcServer.get().start(context.getEnvironment());
            return context;
        } catch (Exception e) {
            logger.error("Web server failed to start. Shutting down.", e);
            System.exit(1);
        }
        // Could should never reach here
        return null;
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
        return startServer(contextConfigurer);
    }

    public static ClusterMgrRestClient getClusterMgrClient() {
        final String clusterMgrHost = EnvironmentUtils.requireEnvProperty(ENV_CLUSTERMGR_HOST);
        final int clusterMgrPort = EnvironmentUtils.parseIntegerFromEnv(ENV_CLUSTERMGR_PORT);
        final String clusterMgrRoute = EnvironmentUtils.getOptionalEnvProperty(ENV_CLUSTERMGR_ROUTE).orElse("");

        logger.info("clustermgr_host: {}, clustermgr_port: {}", clusterMgrHost, clusterMgrPort);
        return ClusterMgrClient.createClient(
                ComponentApiConnectionConfig.newBuilder()
                        .setHostAndPort(clusterMgrHost, clusterMgrPort, clusterMgrRoute)
                        .build());
    }

    @VisibleForTesting
    static boolean isOverridden(@Nonnull final String configKey) {
        return OVERRIDABLE_ENV_PROPERTIES.contains(configKey)
            && EnvironmentUtils.getOptionalEnvProperty(configKey).isPresent();
    }

    /**
     * Publishes version information of this container into a centralized key-value store.
     */
    private void publishVersionInformation() {
        if (standalone != null && !standalone) {
            // get the component version and record it in the key/value store
            final String specVersion = getClass().getPackage().getSpecificationVersion();
            if (specVersion != null) {
                logger.info("Component version for {} found: {}", componentType, specVersion);
                baseVmtComponentConfig.keyValueStore().put(COMPONENT_VERSION_KEY, specVersion);
            } else {
                logger.error("Could not get Specification-Version for component class {}", getClass());
            }
        }
    }

    /**
     * Get whether this component should force retry failed migrations on startup.
     *
     * @return whether this component should force retry failed migrations on startup.
     */
    public Boolean getForceRetryMigrations() {
        return forceRetryMigrations;
    }

    /**
     * Set whether this component should force retry failed migrations on startup.
     *
     * @param forceRetryMigrations whether this component should force retry failed migrations
     *                             on startup.
     */
    protected void setForceRetryMigrations(final Boolean forceRetryMigrations) {
        this.forceRetryMigrations = forceRetryMigrations;
    }

    /**
     * Exception to be thrown if error occurred while configuring servlet context holder.
     */
    public static class ContextConfigurationException extends Exception {
        /**
         * Create an instance of the ContextConfigurationException initialized with
         * a message and a root cause Throwable.
         *
         * @param message the text message describing this exception
         * @param cause the original cause of this exception
         */
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
            throws ContextConfigurationException, IOException;
    }
}
