package com.vmturbo.clustermgr;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.servlet.Servlet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.clustermgr.kafka.KafkaConfigurationService;
import com.vmturbo.clustermgr.kafka.KafkaConfigurationServiceConfig;

/**
 * The ClusterMgrMain is a utility to launch each of the VmtComponent Docker Containers configured to run on the
 * current node (server).
 * The list of VmtComponents to launch, including component name and version number, is maintained on the shared
 * persistent key/value server and is injected automatically by Spring Cloud Configuration.
 */
@Configuration("theComponent")
@Import({ClusterMgrConfig.class, SwaggerConfig.class, KafkaConfigurationServiceConfig.class})
public class ClusterMgrMain {

    private Logger log = LogManager.getLogger();

    @Value("${clustermgr.node_name}")
    private String nodeName;

    @Autowired
    private ClusterMgrConfig clusterMgrConfig;

    @Value("${kafkaConfigFile:/kafka-config.yml}")
    private String kafkaConfigFile;

    @Autowired
    private KafkaConfigurationService kafkaConfigurationService;

    private ExecutorService backgroundTaskRunner;

    /**
     * Returns environment variable value.
     *
     * @param propertyName environment variable name
     * @return evironment variable value
     * @throws NullPointerException if there is not such environment property set
     */
    @Nonnull
    protected static String requireEnvProperty(@Nonnull String propertyName) {
        final String sysPropValue = System.getProperty(propertyName);
        if (sysPropValue != null) {
            return sysPropValue;
        }
        return Objects.requireNonNull(System.getenv(propertyName),
                "System or environment property \"" + propertyName + "\" must be set");
    }


    public static void main(String[] args) throws Exception {
        final Logger logger = LogManager.getLogger();
        logger.info("Starting web server with spring context");
        final String serverPort = requireEnvProperty("server_port");

        final org.eclipse.jetty.server.Server server =
                new org.eclipse.jetty.server.Server(Integer.valueOf(serverPort));
        final ServletContextHandler contextServer =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        try {
            server.setHandler(contextServer);
            final AnnotationConfigWebApplicationContext applicationContext =
                    new AnnotationConfigWebApplicationContext();
            applicationContext.register(ClusterMgrMain.class);
            final Servlet dispatcherServlet = new DispatcherServlet(applicationContext);
            final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);
            contextServer.addServlet(servletHolder, "/*");
            // Setup Spring context
            final ContextLoaderListener springListener = new ContextLoaderListener(applicationContext);
            contextServer.addEventListener(springListener);

            server.start();
            if (!applicationContext.isActive()) {
                logger.error("Spring context failed to start. Shutting down.");
                System.exit(1);
            }
            applicationContext.getBean(ClusterMgrMain.class).run();
        } catch (Exception e) {
            logger.error("Web server failed to start. Shutting down.", e);
            System.exit(1);
        }
    }

    /*
     * Once the Spring context is initialized, perform the clustermgr process:
     * <ul>
     * <li>from the Consul K/V store, look up the set of components to load on this node
     * <li>if none, initialize the stored list of components to "all known components" (saved back to Consul K/V store)
     * <li>launch each component (by calling the Docker API)</li>
     * </ul>
     */
    public void run() {

        log.info(">>>>>>>>>  clustermgr beginning for " + nodeName);
        // configure kafka
        try {
            kafkaConfigurationService.loadConfiguration(kafkaConfigFile);
        } catch (TimeoutException te) {
            log.error("Kafka configuration timed out. Will continue retrying in background.");
            startBackgroundTask(new KafkaBackgroundConfigurationTask());
        } catch (InterruptedException ie) {
            log.warn("Kafka configuration interrupted. Configuration was not completed.");
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
        }

        // retrieve the configuration for this cluster
        ClusterConfiguration configuration =
                clusterMgrConfig.clusterMgrService().getClusterConfiguration();
        // launch each component instance in the cluster
        for (Map.Entry<String, ComponentInstanceInfo> componentInfo:  configuration.getInstances().entrySet()) {
            String instanceId = componentInfo.getKey();
            String componentType = componentInfo.getValue().getComponentType();
            String node = componentInfo.getValue().getNode();
            clusterMgrConfig.dockerInterfaceService()
                    .launchComponent(componentType, instanceId, node);
        }
    }

    private synchronized void startBackgroundTask(Runnable task) {
        if (backgroundTaskRunner == null) {
            log.info("Creating background task executor service.");
            backgroundTaskRunner = Executors.newCachedThreadPool();
        }
        backgroundTaskRunner.submit(task);
    }

    @PreDestroy
    private void shutDown() {
        log.info("<<<<<<<  clustermgr shutting down");

        // stop any background tasks
        synchronized (this) {
            if (backgroundTaskRunner != null) {
                log.info("Stopping background tasks...");
                backgroundTaskRunner.shutdownNow();
            }
        }

        ClusterConfiguration configuration =
                clusterMgrConfig.clusterMgrService().getClusterConfiguration();
        for (Map.Entry<String, ComponentInstanceInfo> componentInfo:  configuration.getInstances().entrySet()) {
            String instanceId = componentInfo.getKey();
            clusterMgrConfig.dockerInterfaceService().stopComponent(instanceId);
        }
    }

    /**
     * A Runnable that will keep trying kafka configuration in the background.
     */
    private class KafkaBackgroundConfigurationTask implements Runnable {
        @Override
        public void run() {
            Thread.currentThread().setName("kafka-background-configurator");
            log.info("Starting background thread for retrying Kafka configuration.");
            while (true) {
                try {
                    kafkaConfigurationService.loadConfiguration(kafkaConfigFile);
                    log.info("Kafka configuration successful -- exiting background thread.");
                    break;
                } catch (TimeoutException te) {
                    // try again.
                    log.error("Kafka background configuration timed out. Trying again.");
                } catch (InterruptedException ie) {
                    log.warn("Kafka background configuration interrupted. Configuration was not completed.");
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ie);
                }
            }

        }
    }
}

