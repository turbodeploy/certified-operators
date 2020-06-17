package com.vmturbo.voltron;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.servlet.Servlet;
import javax.servlet.ServletException;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.protobuf.services.ProtoReflectionService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertiesPropertySource;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.web.context.ConfigurableWebApplicationContext;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.clustermgr.ClusterMgrService;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.config.ConfigMapPropertiesReader;
import com.vmturbo.components.common.utils.EnvironmentUtils;

/**
 * Velocity-Oriented Lightweight Turbo Running On Native.
 */
@Configuration("theComponent")
@Import({ApiSecurityConfig.class, ApiRedirectAdapter.class})
public class Voltron extends BaseVmtComponent {

    static {
        final String rootDir = EnvironmentUtils.getOptionalEnvProperty("voltron.root").orElse("xl");
        final String propertiesYamlPath = getAbsolutePath("com.vmturbo.voltron/src/main/resources/config/configmap.yaml");
        Preconditions.checkArgument(new File(propertiesYamlPath).exists());

        // Required for initial injection of configmap properties.
        System.setProperty("propertiesYamlPath", "file:" + propertiesYamlPath);
        // Required for audit log utils.
        System.setProperty("rsyslog_host", "localhost");

        Map<String, Object> props = new HashMap<>();
        props.put("aggregator.base", "/tmp/turbonomic/data/" + rootDir + "/datapoint");
    }

    private static final Logger logger = LogManager.getLogger();

    /**
     * Controller that allows REST commands to shut down/start up sub-contexts.
     * Unfortunately we don't allow dynamically adding components.
     *
     * @return The controller.
     */
    @Bean
    public RefreshController refreshController() {
        return new RefreshController();
    }

    private static AnnotationConfigWebApplicationContext voltronComponentContext(final String shortName,
            final String topLevelFolder, final Class<?> componentConfiguration, @Nullable final AnnotationConfigWebApplicationContext parent,
            PropertyRegistry propRegistry, PropertySource<?>... additionalProperties) throws IOException {
        final List<PropertySource<?>> props = Lists.newArrayList(additionalProperties);
        props.add(propRegistry.getComponentProperties(shortName, topLevelFolder, ""));
        props.add(new PropertiesPropertySource("configmap",
                ConfigMapPropertiesReader.readConfigMap(shortName, "file:" + getAbsolutePath(topLevelFolder + "/src/main/resources/config/configmap.yaml"))));

        // See if this component has a properties file.
        ConfigurableEnvironment env = createEnvironment(props);

        final AnnotationConfigWebApplicationContext context = new AnnotationConfigWebApplicationContext();
        context.register(componentConfiguration);
        context.setParent(parent);
        context.setNamespace(topLevelFolder);
        context.setEnvironment(env);
        return context;
    }

    /**
     * Context of the Voltron instance. Wrapper to allow access to both the root and child contexts
     * easily.
     */
    static class VoltronContext {
        private final ConfigurableWebApplicationContext rootContext;
        private final Map<Component, AnnotationConfigWebApplicationContext> componentContexts = new HashMap<>();

        private VoltronContext(ConfigurableWebApplicationContext voltronContext) {
            this.rootContext = voltronContext;
        }

        public void addChildContext(Component component, AnnotationConfigWebApplicationContext childContext) {
            childContext.setParent(rootContext);
            componentContexts.put(component, childContext);
        }

        public ConfigurableWebApplicationContext getRootContext() {
            return rootContext;
        }

        public Map<Component, AnnotationConfigWebApplicationContext> getComponents() {
            return Collections.unmodifiableMap(componentContexts);
        }
    }

    @Nonnull
    private static VoltronContext createContext(
            @Nonnull ServletContextHandler contextServer, String namespace, VoltronConfiguration config)
            throws ContextConfigurationException, IOException {

        PropertyRegistry propertyRegistry = new PropertyRegistry(namespace, config.getDataPath(), config);

        // The root context.
        final AnnotationConfigWebApplicationContext voltronRootContext =
                voltronComponentContext("voltron", "com.vmturbo.voltron", Voltron.class, null, propertyRegistry);
        final Servlet servlet = new DispatcherServlet(voltronRootContext);
        final ServletHolder servletHolder = new ServletHolder(servlet);
        ((DispatcherServlet)servlet).setNamespace("voltron global");
        contextServer.addServlet(servletHolder, "/*");

        VoltronContext voltronContext = new VoltronContext(voltronRootContext);

        for (Component component : config.getComponents()) {
            component.addToContext(contextServer, propertyRegistry, voltronContext);
        }

        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(voltronRootContext);
        contextServer.addEventListener(springListener);
        return voltronContext;
    }

    static ConfigurableEnvironment createEnvironment(List<PropertySource<?>> properties) {
        final ConfigurableEnvironment tpEnvironment = new StandardEnvironment();
        for (PropertySource<?> prop : properties) {
            // First in list = first in priority.
            tpEnvironment.getPropertySources().addLast(prop);
        }
        return tpEnvironment;
    }

    static String migrationLocation(@Nonnull final String topFolder) {
        // TODO - detect JAVA migrations. This isn't that important for now because Java migrations
        // we use don't typically modify tables.
        return "filesystem:" + getAbsolutePath(topFolder + "/src/main/resources/");
    }

    static String getAbsolutePath(String pathInXl) {
        Path path = null;
        try {
            path = Paths.get(new File(Voltron.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getAbsolutePath());
        } catch (URISyntaxException e) {
            throw new IllegalStateException("Unable to get absolute path at runtime.", e);
        }

        // Climb up to "com.vmturbo.voltron".
        while (!path.endsWith("com.vmturbo.voltron")) {
            path = path.getParent();
        }
        // Up one more level, to get to the top "XL" directory.
        path = path.getParent();

        final Path fullPath = path.resolve(pathInXl);
        return fullPath.toString();
    }


    static void initializeClustermgr(@Nonnull final VoltronContext context) {
        // Initialize clusterMgr. This prevents the continual popup asking you whether
        // or not you are willing to enable telemetry. Without flagging clustermgr
        // as initialized this popup will appear on every new page you navigate
        // to in the UI.
        Optional.ofNullable(context.getComponents().get(Component.CLUSTERMGR)).ifPresent(clustermgr ->
            clustermgr.getBean(ClusterMgrService.class)
                .setClusterKvStoreInitialized(true));
    }

    /**
     * Start Voltron.
     * @param namespace The namespace to use. This namespace will propagate to all data folders, and
     *                  all data storage locations, used by the components in the configuration.
     * @param config The configuration for voltron.
     * @return An object containing references and helper methods to manage the running Voltron instance.
     */
    public static VoltronsContainer start(String namespace, VoltronConfiguration config) {
        final Stopwatch startupTime = Stopwatch.createStarted();
        // Required for crypto facility.
        System.setProperty("com.vmturbo.keydir", Paths.get(config.getDataPath(), namespace, "keydir").toString());
        System.setProperty("useInProcess", Boolean.toString(config.isUseInProcessGrpc()));
        System.setProperty("useLocalBus", Boolean.toString(config.isUseLocalBus()));
        System.setProperty(BaseVmtComponent.PROP_serverHttpPort, Integer.toString(config.getServerHttpPort()));
        System.setProperty(ComponentGrpcServer.PROP_SERVER_GRPC_PORT, Integer.toString(config.getServerGrpcPort()));

        final SetOnce<VoltronContext> voltronContext = new SetOnce<>();
        final Zarkon onEnterDemolisher = new Zarkon(namespace, config, voltronContext);
        final Zarkon onExitDemolisher = new Zarkon(namespace, config, voltronContext);
        // There are multiple ways in which the server startup can System.exit() on error.
        // Try to catch them by adding a shutdown hook which will tear down the data if it hasn't
        // been torn down yet.
        Runtime.getRuntime().addShutdownHook(new Thread(onExitDemolisher));
        try {
             startServer((contextServer) -> {
                try {
                    final VoltronContext context = createContext(contextServer, namespace, config);
                    voltronContext.trySetValue(context);
                    if (config.cleanSlate()) {
                        onEnterDemolisher.run();
                    }
                    WebSocketServerContainerInitializer.configureContext(contextServer);
                    return context.getRootContext();
                } catch (ServletException e) {
                    throw new ContextConfigurationException("Could not configure websockets", e);
                }
            });

             voltronContext.getValue().ifPresent(v -> {
                 RefreshController refreshCtrl = v.rootContext.getBean(RefreshController.class);
                 refreshCtrl.setComponentContexts(v.getComponents());
             });

            voltronContext.getValue().ifPresent(Voltron::initializeClustermgr);
        } catch (Exception e) {
            logger.error("Voltron failed to start up. Will demolish data. Error:", e);
        }

        if (config.isUseInProcessGrpc()) {
            // Start up a real gRPC server for voltron-cli use.
            final ServerBuilder serverBuilder = NettyServerBuilder.forPort(config.getServerGrpcPort());
            ComponentGrpcServer.get().getServiceDefinitions().forEach(serverBuilder::addService);
            serverBuilder.addService(ProtoReflectionService.newInstance());
            Server server = serverBuilder.build();
            try {
                server.start();
                // Make sure we release the socket when voltron is shut down.
                Runtime.getRuntime().addShutdownHook(new Thread(server::shutdownNow));
            } catch (IOException e) {
                logger.error("Failed to start nested gRPC server.");
            }
        }

        String image;
        try {
            // This won't work if Voltron is packaged in a JAR.
            image = new String(Files.readAllBytes(
                Paths.get(Voltron.class.getClassLoader()
                    .getResource("config/img").toURI())));
        } catch (IOException | RuntimeException | URISyntaxException e) {
            // No big deal if we can't print the image, although it's a bid sad.
            image = ":(";
        }

        logger.info("Welcome to Velocity-Oriented Lightweight Turbo Running On Native!\n{}\nVOLTRON came up in {}",
                image,
                startupTime.elapsed(TimeUnit.SECONDS));
        return new VoltronsContainer(voltronContext.getValue().get(), config, onExitDemolisher);
    }
}