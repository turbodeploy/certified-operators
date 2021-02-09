package com.vmturbo.components.common;

import java.time.Clock;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.common.health.ComponentStatusNotifier;
import com.vmturbo.components.common.notification.ComponentStatusNotificationSenderConfig;

/**
 * This configuration holds all the dependency configuration, required to run Consul client.
 */
@Configuration
@Import({ComponentStatusNotificationSenderConfig.class})
public class ComponentStatusNotifierConfig {
    /**
     * Key for environment variable to control:  if "true" this component should do component
     * status notification; if "false" then there will be no component status notification - one
     * scenario is that the component is a remote probe and is running in a remote cluster that
     * is not necessarily equipped with Kafka.
     */
    public static final String ENABLE_COMPONENT_STATUS_NOTIFICATION =
            "enableComponentStatusNotification";

    @Autowired
    private ComponentStatusNotificationSenderConfig notificationSenderConfig;

    @Value("${" + BaseVmtComponent.PROP_serverHttpPort + '}')
    private Integer serverPort;

    @Value("${" + BaseVmtComponent.PROP_COMPONENT_TYPE + '}')
    private String componentType;
    @Value("${" + BaseVmtComponent.PROP_INSTANCE_ID + '}')
    private String instanceId;
    @Value("${" + BaseVmtComponent.PROP_INSTANCE_IP + ":}")
    private String instanceIp;
    @Value("${" + BaseVmtComponent.PROP_INSTANCE_ROUTE + ":}")
    private String instanceRoute;

    /**
     * This property is used to disable component status notification. This is necessary for tests
     * and for probes running outside the primary Turbonomic K8s cluster.
     */
    @Value("${" + ENABLE_COMPONENT_STATUS_NOTIFICATION + ":true}")
    private Boolean enableComponentStatusNotification;

    /**
     * Used to send notification about component status changes.
     * We wrap the return type with an {@link Optional} because, if notification is disabled in
     * the case that Kafka is not present in the environment, instantiating the Sender would
     * fail, preventing the component (likely a probe) to start up.
     *
     * @return The {@link ComponentStatusNotifier}.
     */
    @Bean
    public Optional<ComponentStatusNotifier> componentStatusNotifier() {
        if (!Boolean.TRUE.equals(enableComponentStatusNotification)) {
            return Optional.empty();
        }
        final ComponentStatusNotifier notifier = new ComponentStatusNotifier(
                notificationSenderConfig.componentStatusNotificationSender(),
                enableComponentStatusNotification, componentType, instanceId, instanceIp,
                instanceRoute, serverPort, Clock.systemUTC());
        return Optional.of(notifier);
    }
}
