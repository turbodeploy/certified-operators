package com.vmturbo.components.common;

import static com.vmturbo.clustermgr.api.ClusterMgrClient.COMPONENT_VERSION_KEY;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
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
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.beans.FatalBeanException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

import com.vmturbo.api.enums.DeploymentMode;
import com.vmturbo.common.api.utils.EnvironmentUtils;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.components.api.ServerStartedNotifier;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.components.common.RequiresDataInitialization.InitializationException;
import com.vmturbo.components.common.config.PropertiesLoader;
import com.vmturbo.components.common.diagnostics.DiagnosticService;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.featureflags.FeatureFlagEnablementStore;
import com.vmturbo.components.common.featureflags.FeatureFlagManager;
import com.vmturbo.components.common.featureflags.PropertiesLoaderFeatureFlagEnablementStore;
import com.vmturbo.components.common.health.ComponentStatusNotifier;
import com.vmturbo.components.common.health.CompositeHealthMonitor;
import com.vmturbo.components.common.health.HealthStatus;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.components.common.health.SimpleHealthStatus;
import com.vmturbo.components.common.logging.HeapDumpRpcService;
import com.vmturbo.components.common.metrics.MemoryMetricsManager;
import com.vmturbo.components.common.metrics.MemoryMetricsManager.ManagedRoot;
import com.vmturbo.components.common.metrics.ScheduledMetrics;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.components.common.utils.BuildProperties;
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
@Import({BaseVmtComponentConfig.class, ComponentStatusNotifierConfig.class})
public abstract class BaseVmtComponent implements IVmtComponent,
        ApplicationListener<ContextRefreshedEvent> {

    private static final String PRODUCT = "Turbonomic Operations Manager";

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
     * The path to the mTLS secret file used to load external secret properties
     * prior to Spring configuration.
     */
    public static final String PROP_TLS_SECRETS_YAML_PATH = "tlsSecretsYamlPath";

    /**
     * The environment key for the port number for the Jetty instance for each component.
     */
    public static final String PROP_serverHttpPort = "serverHttpPort";

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

    /**
     * Deployment configuration used to expose areas of the application front or backend.
     */
    @Value("${deploymentMode:SERVER}")
    private DeploymentMode deploymentMode;

    private static SetOnce<String> instanceIp = new SetOnce<>();

    /**
     * Information about the component version and build.
     */
    private static final DataMetricGauge VERSION_GAUGE = DataMetricGauge.builder()
            .withName(StringConstants.METRICS_TURBO_PREFIX + "version_info")
            .withHelp("Version and build of the component")
            .withLabelNames("version", "build", "product_name", "deployment_mode")
            .build()
            .register();

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

    private static final int SCHEDULED_METRICS_DELAY_MS = 60000;

    @Value("${scheduledMetricsIntervalMs:60000}")
    private int scheduledMetricsIntervalMs;

    @Value("${serverGrpcPort}")
    private int grpcPort;

    @Value("${connRetryIntervalSeconds:10}")
    private String connRetryIntervalSeconds;

    @Value("${enableMemoryMonitor:true}")
    private boolean enableMemoryMonitor;

    @Value("${enableSystemDiags:true}")
    private boolean enableSystemDiags;

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

    @Autowired
    private ComponentStatusNotifierConfig componentStatusNotifierConfig;

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
        logger.info("Component transition from " + this.status.toString()
                + " to " + status.toString() + " for " + getComponentName());
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
    public final void startComponent(@Nonnull final ApplicationContext applicationContext) {
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
        baseVmtComponentConfig.migrationFramework().ifPresent(migrationFramework ->
                migrationFramework.startMigrations(getMigrations(), forceRetryMigrations));

        registerGrpcServices();

        // Run the "onStartComponent", and other methods that require external access, in a separate
        // thread so that the main context // initialization thread is not blocked by any blocking operations in the "startComponent"
        // method.
        final ExecutorService svc = Executors.newSingleThreadExecutor();
        svc.execute(() -> {
            try {
                final Map<String, RequiresDataInitialization> reqInitialization =
                        applicationContext.getBeansOfType(RequiresDataInitialization.class);
                if (!reqInitialization.isEmpty()) {
                    setStatus(ExecutionStatus.INITIALIZING_DATA);
                    logger.info("Starting initializations for {} beans", reqInitialization.size());
                    final Map<String, RequiresDataInitialization> sortedInitialization =
                        new LinkedHashMap<>();
                    // sort the initializations based on priorities
                    reqInitialization.entrySet()
                        .stream()
                        .sorted(Map.Entry.comparingByValue(Comparator.comparing(RequiresDataInitialization::priority).reversed()))
                        .forEachOrdered(entry -> sortedInitialization.put(entry.getKey(),
                            entry.getValue()));
                    logger.info("Begin component bean initialization, component={}, beans:\n - {}",
                            getComponentName(), String.join("\n - ",
                                    sortedInitialization.entrySet().stream()
                                            .map(Entry::getKey)
                                            .collect(Collectors.toList())));
                    for (final Entry<String, RequiresDataInitialization> entry : sortedInitialization.entrySet()) {
                        try {
                            logger.info("Running bean initialization, component={}, bean={}",
                                    getComponentName(), entry.getKey());
                            entry.getValue().initialize();
                        } catch (InitializationException e) {
                            if (!e.isFatal()) {
                                logger.error("Bean {} failed initialization with non-fatal error, component={}",
                                        entry.getKey(), getComponentName(), e);
                            } else {
                                throw e;
                            }
                        }
                    }
                    logger.info("Finished component bean initialization, component={}, beans={}",
                            getComponentName(), sortedInitialization.size());
                }
                logInitialAuditMessage();
                this.onStartComponent();
                publishVersionInformation();
                componentStatusNotifierConfig.componentStatusNotifier().ifPresent(
                        ComponentStatusNotifier::notifyComponentStartup);
                setStatus(ExecutionStatus.RUNNING);
            } catch (Exception e) {
                logger.error("Error while trying to finish startup routine. Will shut down.", e);
                System.exit(1);
            }
        });
        BuildProperties buildProperties = BuildProperties.get();
        VERSION_GAUGE.labels(buildProperties.getVersion(), buildProperties.getBuildNumber(),
            PRODUCT, deploymentMode.toString()).setData(1.0);
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
        componentStatusNotifierConfig.componentStatusNotifier().ifPresent(
                ComponentStatusNotifier::notifyComponentShutdown);
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

            if (enableSystemDiags) {
                try {
                    baseVmtComponentConfig.diagnosticService().dumpSystemDiags(diagnosticZip);
                } catch (DiagnosticsException e) {
                    logger.error("Failed to collect system diagnostics. Will try to get component diagnostics.", e);
                }
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
     * Method for components to log whatever would be relevant and useful to the audit log upon
     * startup.
     */
    protected void logInitialAuditMessage() {}

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
        services.add(baseVmtComponentConfig.memoryMetricsRpcService());
        potentiallyAddHeapDumpService(services);
        ComponentGrpcServer.get().addServices(services, getServerInterceptors());
    }

    private void potentiallyAddHeapDumpService(List<BindableService> services) {
        final Optional<HeapDumpRpcService> heapDumpRpcService = baseVmtComponentConfig.heapDumpRpcService();
        if (heapDumpRpcService.isPresent()) {
            final HeapDumpRpcService heapDumper = heapDumpRpcService.get();
            heapDumper.initialize(baseVmtComponentConfig.heapDumpEnabled(), getComponentName());
            services.add(heapDumper);
        } else {
            HeapDumpRpcService.sendAuditLogMessage("Heap dump service will not be created and is unavailable.",
                getComponentName(), true);
        }
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        if (!startFired.getAndSet(true)) {
            startComponent(event.getApplicationContext());
            configureMemoryManager(event.getApplicationContext());
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
     * Configuration class in the context, install an environment reflecting external configuration
     * and define a DispatcherServlet in the context. Return the Context as "active".
     *
     * @param contextConfigurer add context configuration specific to a particular component
     * @param environment spring Environment for application context
     * @param configurationClass the main @Configuration class to load into the context
     * @return the ConfigurableWebApplicationContext configured with the given @Configuration class
     * and set to "active"
     * @throws ContextConfigurationException if there's an error reading the external configuration
     * properties
     */
    @Nonnull
    protected static ConfigurableWebApplicationContext attachSpringContext(
            @Nonnull ServletContextHandler contextConfigurer,
            @Nonnull ConfigurableEnvironment environment,
            @Nonnull Class<?> configurationClass) throws ContextConfigurationException {
        final AnnotationConfigWebApplicationContext applicationContext =
                new AnnotationConfigWebApplicationContext();
        applicationContext.setEnvironment(environment);
        logger.info(
                "Creating application context for: componentType {}; instanceId {}; instanceIp {};",
                EnvironmentUtils.requireEnvProperty(PROP_COMPONENT_TYPE),
                EnvironmentUtils.requireEnvProperty(PROP_INSTANCE_ID),
                getInitializedInstanceIp());

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
     * Start the component by starting its web server with Spring context, initialized from the
     * given configuration class. Before that's done, feature flags are initialized with the needed
     * {@link FeatureFlagEnablementStore}, so feature flags can be used in code used during context
     * construction.
     *
     * @param configurationClass starting point for Spring context creation
     */
    protected static void runComponent(@Nonnull Class<?> configurationClass) {
        runComponent((servletContextHolder, environment) ->
                attachSpringContext(servletContextHolder, environment, configurationClass));
    }

    /**
     * Start the component by starting its web server with Spring context, initialized from the
     * given configuration class. Before that's done, feature flags are initialized with the needed
     * {@link FeatureFlagEnablementStore}, so feature flags can be used in code used during context
     * construction.
     *
     * @param contextConfigurer configuration callback to perform some specific configuration on the
     *                          servlet context
     */
    protected static void runComponent(ContextConfigurer contextConfigurer) {
        StandardEnvironment environment = new StandardEnvironment();
        try {
            PropertiesLoader.addConfigurationPropertySources(environment);
            configureFeatureFlagStore(environment);
            startContext(contextConfigurer, environment);
        } catch (ContextConfigurationException e) {
            throw new FatalBeanException("Failed to configure a feature flag store", e);
        }
    }

    private static void configureFeatureFlagStore(Environment environment) {
        FeatureFlagManager.setStore(new PropertiesLoaderFeatureFlagEnablementStore(environment));
    }

    protected static void addMetricsServlet(@Nonnull final ServletContextHandler contextServer) {
        final Servlet servlet = new MetricsServlet(CollectorRegistry.defaultRegistry);
        final ServletHolder servletholder = new ServletHolder(servlet);
        contextServer.addServlet(servletholder, METRICS_URL);
    }

    @Nonnull
    protected static ConfigurableWebApplicationContext startServer(
            @Nonnull ContextConfigurer contextConfigurer,
            @Nonnull ConfigurableEnvironment environment)
            throws Exception {
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
        HttpConfiguration httpConfig = new HttpConfiguration();
        httpConfig.setSendServerVersion( false );
        HttpConnectionFactory httpFactory = new HttpConnectionFactory( httpConfig );
        ServerConnector httpConnector = new ServerConnector( server, httpFactory );
        httpConnector.setPort(serverPort);
        server.setConnectors( new Connector[] { httpConnector } );
        server.setHandler(contextServer);
        context = contextConfigurer.configure(contextServer, environment);
        addMetricsServlet(contextServer);
        server.start();
        if (!context.isActive()) {
            throw new IllegalStateException("Spring context failed to start.");
        }

        // The starting of the component should add the gRPC services defined in the spring
        // context to the gRPC server.
        ServerStartedNotifier.get().notifyServerStarted(context);
        return context;
    }

    /**
     * Starts web server with Spring context, initialized from the specified configuration class.
     *
     * @param contextConfigurer configuration callback to perform some specific
     *         configuration on the servlet context
     * @param environment spring environment reflecting external configuration
     * @return Spring context, created as the result of webserver startup.
     */
    @Nonnull
    protected static ConfigurableWebApplicationContext startContext(
            @Nonnull ContextConfigurer contextConfigurer,
            @Nonnull ConfigurableEnvironment environment) {
        try {
            return startServer(contextConfigurer, environment);
        } catch (Exception e) {
            logger.error("Web server failed to start. Shutting down.", e);
            System.exit(1);
            // This shouldn't happen according to System.exit documentation.
            throw new IllegalStateException("System.exit() returned normally");
        }
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
            final String specVersion = getClass().getPackage().getImplementationVersion();
            if (specVersion != null) {
                logger.info("Component version for {} found: {}", componentType, specVersion);
                baseVmtComponentConfig.keyValueStore().put(COMPONENT_VERSION_KEY, specVersion);
            } else {
                logger.error("Could not get Specification-Version for component class {}", getClass());
            }
        }
    }

    /**
     * Configure {@code MemoryMetricsManager} with the objects managed by the given
     * spring application context.
     *
     * @param applicationContext The application context managing spring objects.
     */
    private void configureMemoryManager(ApplicationContext applicationContext) {
        if (applicationContext instanceof AnnotationConfigWebApplicationContext) {
            // Clear out any old configured roots for this component. This will usually be called
            // at component startup, but if called due to a spring context refresh we want
            // to be sure to clear out any old component roots to prevent leaking them.
            final String componentName = getComponentName();
            MemoryMetricsManager.clearComponentRoots(componentName);

            final AnnotationConfigWebApplicationContext context =
                (AnnotationConfigWebApplicationContext)applicationContext;
            final ConfigurableListableBeanFactory clbf = context.getBeanFactory();
            final List<ManagedRoot> springBeans = new ArrayList<>(applicationContext.getBeanDefinitionCount());

            // Make the base spring context object a root object.
            springBeans.add(new ManagedRoot(componentName + "-context", context, componentName));

            // The Spring Context bean factory manages created spring beans. We want the memory
            // manager to be able to reach all objects reachable from the bean factory.
            springBeans.add(new ManagedRoot(componentName + "-bean-factory", clbf, componentName));

            // Lots and lots of code retain references to logger objects, all of which refer
            // back to the static LogManager context. Make this context known to the
            // MemoryMetricsManager too.
            springBeans.add(new ManagedRoot(componentName + "-log-manager-context",
                LogManager.getContext(), getComponentName()));

            // The most important of a components objects tend to be the objects created via
            // Spring beans. Make these beans directly known to the MemoryMetricsManager.
            for (String name : clbf.getSingletonNames()) {
                Object singletonBean = clbf.getSingleton(name);
                if (singletonBean != null) {
                    springBeans.add(new ManagedRoot(name, singletonBean, componentName));
                }
            }

            MemoryMetricsManager.addToManagedRootSet(springBeans);
            logger.info("Added {} managed root objects {} to memory manager. {} total roots under management.",
                springBeans.size(),
                getClass().getSimpleName(),
                MemoryMetricsManager.managedRootCount());
        } else {
            logger.warn("{} is a {} and not an AnnotationConfigWebApplicationContext. "
                + "Skipping its beans while configuring memory manager.");
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
         * @param environment sprint Environment reflecting external configuration
         * @return Spring context to track
         * @throws ContextConfigurationException if exception thrown while configuring
         *         servlets and contexts
         */
        ConfigurableWebApplicationContext configure(@Nonnull ServletContextHandler servletContext,
                @Nonnull ConfigurableEnvironment environment)
                throws ContextConfigurationException;
    }
}
