package com.vmturbo.clustermgr;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.annotation.PreDestroy;
import javax.servlet.Servlet;

import io.grpc.BindableService;

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
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.clustermgr.kafka.KafkaConfigurationService;
import com.vmturbo.clustermgr.kafka.KafkaConfigurationServiceConfig;
import com.vmturbo.clustermgr.management.ComponentRegistrationConfig;
import com.vmturbo.components.api.grpc.ComponentGrpcServer;
import com.vmturbo.components.common.config.PropertiesLoader;

/**
 * The ClusterMgrMain is a utility to launch each of the VmtComponent Docker Containers configured to run on the
 * current node (server).
 * The list of VmtComponents to launch, including component name and version number, is maintained on the shared
 * persistent key/value server and is injected automatically by Spring Cloud Configuration.
 */
@Configuration("theComponent")
@Import({ClusterMgrConfig.class, SwaggerConfig.class, KafkaConfigurationServiceConfig.class})
public class ClusterMgrMain implements ApplicationListener<ContextRefreshedEvent> {

    private static final Logger log = LogManager.getLogger();

    private final AtomicBoolean started = new AtomicBoolean(false);

    @Value("${clustermgr.node_name}")
    private String nodeName;

    @Autowired
    private ClusterMgrConfig clusterMgrConfig;

    @Value("${kafkaConfigFile:/kafka-config.yml}")
    private String kafkaConfigFile;

    @Autowired
    private KafkaConfigurationService kafkaConfigurationService;

    @Autowired
    private ComponentRegistrationConfig componentRegistrationConfig;

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


    /**
     * Startup for the ClusterMgr component. ClusterMgr comes up first in XL, and initializatino for
     * all other component depends on ClusterMgr being already up, and so (chicken and egg)
     * ClusterMgr is the only component that does *not* inherit from BaseVmtComponent.
     *
     * <p>Create a Spring Context, start up a Jetty server, and then call the run() method
     * on the ClusterMgrMain instance.
     *
     * @param args command line arguments passed on startup - not used directly.
     */
    public static void main(String[] args) {
        final Logger logger = LogManager.getLogger();
        logger.info("Starting web server with spring context");
        final String serverPort = requireEnvProperty("serverHttpPort");

        final org.eclipse.jetty.server.Server server =
                new org.eclipse.jetty.server.Server(Integer.parseInt(serverPort));
        final ServletContextHandler contextServer =
                new ServletContextHandler(ServletContextHandler.SESSIONS);
        try {
            server.setHandler(contextServer);
            final AnnotationConfigWebApplicationContext applicationContext =
                    new AnnotationConfigWebApplicationContext();
            PropertiesLoader.addConfigurationPropertySources(applicationContext);
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

            // The starting of the component should add the gRPC services defined in the spring
            // context to the gRPC server.
            ComponentGrpcServer.get().start(applicationContext.getEnvironment());

            applicationContext.getBean(ClusterMgrMain.class).run();

        } catch (Exception e) {
            logger.error("Web server failed to start. Shutting down.", e);
            System.exit(1);
        }
    }

    /**
     * When ClusterMgr begins running, launch a background task to configure Kafka.
     */
    public void run() {
        // When clusterMgr begins running, mark kvInitialized as true to ClusterMgrService since
        // default configuration properties have been loaded from configMap.
        clusterMgrConfig.clusterMgrService().setClusterKvStoreInitialized(true);
        log.info(">>>>>>>>>  clustermgr beginning for " + nodeName);
        // configure kafka
        try {
            kafkaConfigurationService.loadConfiguration(kafkaConfigFile);
            log.info("<<<<<<<<<<<< kafka configured.");
        } catch (TimeoutException te) {
            log.error("Kafka configuration timed out. Will continue retrying in background.");
            startBackgroundTask(new KafkaBackgroundConfigurationTask());
        } catch (InterruptedException ie) {
            log.warn("Kafka configuration interrupted. Configuration was not completed.");
            Thread.currentThread().interrupt();
            throw new RuntimeException(ie);
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
        ComponentGrpcServer.get().stop();
        // stop any background tasks
        synchronized (this) {
            if (backgroundTaskRunner != null) {
                log.info("Stopping background tasks...");
                backgroundTaskRunner.shutdownNow();
            }
        }
    }

    @Override
    public void onApplicationEvent(final ContextRefreshedEvent contextRefreshedEvent) {
        if (started.compareAndSet(false, true)) {
            final List<BindableService> services = new ArrayList<>();
            services.add(clusterMgrConfig.logConfigurationService());
            services.add(clusterMgrConfig.tracingConfigurationRpcService());
            ComponentGrpcServer.get().addServices(services, Collections.emptyList());
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

