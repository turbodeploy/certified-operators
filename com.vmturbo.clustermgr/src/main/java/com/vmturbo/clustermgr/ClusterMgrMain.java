package com.vmturbo.clustermgr;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.clustermgr.kafka.KafkaConfigurationServiceConfig;
import com.vmturbo.clustermgr.kafka.KafkaConfigurationService;

/*
 * The ClusterMgrMain is a utility to launch each of the VmtComponent Docker Containers configured to run on the
 * current node (server).
 * The list of VmtComponents to launch, including component name and version number, is maintained on the shared
 * persistent key/value server and is injected automatically by Spring Cloud Configuration.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@Import({ClusterMgrConfig.class, KafkaConfigurationServiceConfig.class})
public class ClusterMgrMain implements CommandLineRunner {

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

    public static void main(String[] args) throws Exception {
        new SpringApplicationBuilder()
                .sources(ClusterMgrMain.class)
                .run(args);
    }

    /*
     * Once the Spring context is initialized, perform the clustermgr process:
     * <ul>
     * <li>from the Consul K/V store, look up the set of components to load on this node
     * <li>if none, initialize the stored list of components to "all known components" (saved back to Consul K/V store)
     * <li>launch each component (by calling the Docker API)</li>
     * </ul>
     */
    public void run(String[] args) {

        log.info(">>>>>>>>>  clustermgr beginning for " + nodeName);
        // initialize the KV Store component
        clusterMgrConfig.clusterMgrService().initializeClusterKVStore();

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

