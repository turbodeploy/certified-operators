package com.vmturbo.components.common.health;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Clock;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentIdentifier;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentInfo;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStarting;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStopping;
import com.vmturbo.common.protobuf.cluster.ComponentStatus.UriInfo;
import com.vmturbo.common.protobuf.cluster.ComponentStatusProtoUtil;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.notification.ComponentStatusNotificationSender;
import com.vmturbo.components.common.utils.BuildProperties;

/**
 * Utility to broadcast notifications about this component's status - most notably when it's starting
 * up and shutting down.
 */
public class ComponentStatusNotifier {
    private static final Logger logger = LogManager.getLogger();

    private final ComponentStatusNotificationSender notificationSender;

    private final Boolean enableRegistration;

    private final long jvmId;

    private final long startupTime;

    private ComponentInfo componentInfo;

    /**
     * Create a new notifier.
     *
     * @param notificationSender The sender to use to actually send notifications.
     * @param enableRegistration Whether or not registration (and, therefore, notifications) are enabled.
     * @param componentType The type of the component.
     * @param instanceId The instance of the component.
     * @param instanceIp The IP of the instance.
     * @param instanceRoute The route to use as a prefix within the IP (if any).
     * @param serverPort The server port of the instance.
     * @param clock System clock.
     */
    public ComponentStatusNotifier(final ComponentStatusNotificationSender notificationSender,
                                   final Boolean enableRegistration,
                                   final String componentType,
                                   final String instanceId,
                                   final String instanceIp,
                                   final String instanceRoute,
                                   final Integer serverPort,
                                   final Clock clock) {
        this.notificationSender = notificationSender;
        this.enableRegistration = enableRegistration;

        this.startupTime = clock.millis();
        // We use k8s pod names as instance IDs. However, these names do not change when k8s pods
        // restart. This creates the possibility of race conditions, where an old pod's JVM is
        // shutting down as the "restarted" pod's JVM is starting up. The old pod's shutdown can
        // trigger the deregistration of the new pod's health check in Consul.
        //
        // To guard against this, we append the current epoch time at the end of the instance ID.
        this.jvmId = clock.millis();

        // for backwards compatibility with docker-compose invocation, look up instanceIp
        String finalInstanceIp;
        if (!StringUtils.isEmpty(instanceIp)) {
            finalInstanceIp = instanceIp;
        } else {
            try {
                finalInstanceIp = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Cannot get localhost IP address...cannot continue.", e);
            }
        }

        this.componentInfo = ComponentInfo.newBuilder()
            .setId(ComponentIdentifier.newBuilder()
                .setComponentType(componentType)
                .setInstanceId(instanceId)
                .setJvmId(jvmId))
            .setUriInfo(UriInfo.newBuilder()
                .setIpAddress(finalInstanceIp)
                .setPort(serverPort)
                .setRoute(instanceRoute))
            .build();
    }

    /**
     * Send the shutdown notification.
     */
    public void notifyComponentShutdown() {
        final String logId = ComponentStatusProtoUtil.getComponentLogId(componentInfo.getId());
        if (Boolean.TRUE.equals(enableRegistration)) {
            logger.info("Sending shutdown notification for component {}.", logId);
            try {
                notificationSender.sendStoppingNotification(ComponentStopping.newBuilder()
                    .setStartTimestamp(startupTime)
                    .setComponentId(componentInfo.getId())
                    .build());
                logger.info("Successfully sent de-registration notification for component {}", logId);
            } catch (CommunicationException e) {
                logger.error("Failed to send shutdown notification.", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Failed to send shutdown notification.", e);
            }
        } else {
            logger.info("Registration disabled. Not sending shutdown notification for component {}", logId);
        }
    }

    /**
     * Send the startup notification.
     */
    public void notifyComponentStartup() {
        final String logId = ComponentStatusProtoUtil.getComponentLogId(componentInfo.getId());
        if (Boolean.TRUE.equals(enableRegistration)) {
            logger.info("Sending startup notification for component {}.", logId);
            try {
                notificationSender.sendStartingNotification(ComponentStarting.newBuilder()
                        .setStartTimestamp(startupTime)
                        .setComponentInfo(componentInfo)
                        .setBuildProperties(BuildProperties.get().toProto())
                        .build());
            } catch (CommunicationException e) {
                logger.error("Failed to send startup notification.", e);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Failed to send startup notification.", e);
            }
        } else {
            logger.info("Startup notification disabled for component {}", logId);
        }
    }
}
