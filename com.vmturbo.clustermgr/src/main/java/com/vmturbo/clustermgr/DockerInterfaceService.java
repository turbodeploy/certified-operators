package com.vmturbo.clustermgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Component;

/**
 * Wrapper for the Docker API
 **/
@Component
public class DockerInterfaceService {

    private Logger log = LogManager.getLogger();

    /**
     * Launch a docker-image based on the given component type on the given node. Pass the given "instanceId" to the
     * new docker-container in the system environment.
     *
     * @param componentType the base component type to be launched
     * @param instanceId the ID passed to the new instance for establishing communication with other components in the cluster.
     * @param node the host node (either hostname, IP, or "" -> "default" node on which the component should run.
     */
    public void launchComponent(String componentType, String instanceId, String node) {
        log.info("launch component: " + instanceId + " type: " + componentType + " on: " + node);
        // TODO: call docker API to start the desired container
    }

    /**
     * Terminate the docker-image, if any, running the given component.
     *
     * @param componentId the type of the VMT Component to launch
     */
    public void stopComponent(String componentId) {
        log.info("stopping component: " + componentId);
        // TODO: call docker API to stop the desired container
    }

}
