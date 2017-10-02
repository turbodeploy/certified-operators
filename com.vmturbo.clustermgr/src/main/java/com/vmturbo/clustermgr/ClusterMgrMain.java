package com.vmturbo.clustermgr;

import java.util.Map;

import javax.annotation.PreDestroy;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/*
 * The ClusterMgrMain is a utility to launch each of the VmtComponent Docker Containers configured to run on the
 * current node (server).
 * The list of VmtComponents to launch, including component name and version number, is maintained on the shared
 * persistent key/value server and is injected automatically by Spring Cloud Configuration.
 */
@Configuration("theComponent")
@EnableAutoConfiguration
@Import(ClusterMgrConfig.class)
public class ClusterMgrMain implements CommandLineRunner {

    private Logger log = LogManager.getLogger();

    @Value("${clustermgr.node_name}")
    private String nodeName;

    @Autowired
    private ClusterMgrConfig clusterMgrConfig;

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


    @PreDestroy
    private void shutDown() {
        log.info("<<<<<<<  clustermgr shutting down");
        ClusterConfiguration configuration =
                clusterMgrConfig.clusterMgrService().getClusterConfiguration();
        for (Map.Entry<String, ComponentInstanceInfo> componentInfo:  configuration.getInstances().entrySet()) {
            String instanceId = componentInfo.getKey();
            clusterMgrConfig.dockerInterfaceService().stopComponent(instanceId);
        }
    }
}