package com.vmturbo.clustermgr.management;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.clustermgr.management.ComponentRegistry.RegistryUpdateException;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStatusNotification;
import com.vmturbo.component.status.api.ComponentStatusListener;

/**
 * Listener for component status notifications. Updates component registry with status updates.
 */
public class ClustermgrComponentStatusListener implements ComponentStatusListener {

    private static final Logger logger = LogManager.getLogger();

    private final ComponentRegistry componentRegistry;

    /**
     * Constructor for the class.
     *
     * @param componentRegistry Registry to use to store component status information.
     */
    public ClustermgrComponentStatusListener(@Nonnull final ComponentRegistry componentRegistry) {
        this.componentRegistry = componentRegistry;
    }

    @Override
    public void onComponentNotification(@Nonnull final ComponentStatusNotification notification) {
        switch (notification.getTypeCase()) {
            case STARTUP:
                try {
                    componentRegistry.registerComponent(notification.getStartup());
                } catch (RegistryUpdateException e) {
                    logger.error("Failed to register component.", e);
                }
                break;
            case SHUTDOWN:
                try {
                    componentRegistry.deregisterComponent(notification.getShutdown().getComponentId());
                } catch (RegistryUpdateException e) {
                    logger.error("Failed to deregister component.", e);
                }
                break;
            default:
                logger.warn("Unrecognized component status notification: {}", notification);
        }
    }
}
