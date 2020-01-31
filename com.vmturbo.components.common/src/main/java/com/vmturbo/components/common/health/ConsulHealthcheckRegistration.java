package com.vmturbo.components.common.health;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.StringJoiner;

import javax.annotation.Nonnull;

import com.ecwid.consul.v1.ConsulClient;
import com.ecwid.consul.v1.agent.model.NewCheck;
import com.ecwid.consul.v1.agent.model.NewService;

import org.apache.commons.lang.StringUtils;

import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.ComponentController;

/**
 * Handle Consul registration for components. Register the type, instance id, and IP for a
 * component, and add a health check definition.
 * Also handle de-registration.
 */
public class ConsulHealthcheckRegistration {

    /**
     * How often should a Consul health check be performed.
     */
    private static final String HEALTH_CHECK_PERIOD = "60s";

    /**
     * If a component is unhealthy for this period of time, automatically deregister
     * the component from Consul. This avoids "stale" component registrations which may
     * occur if a component ends abruptly and doesn't have time to de-register normally.
     */
    private static final String HEALTH_CHECK_CRITICAL_TIME = "60m";

    /**
     * Attach this tag to every Turbonomic component, to differentiate them from non-turbonomic
     * services registered in consul.
     */
    public static final String COMPONENT_TAG = "turbo_component";

    private final ConsulClient consulClient;
    private final Boolean enableConsulRegistration;
    private final String componentType;
    private final String instanceId;
    private final String instanceIp;
    private final String instanceRoute;
    private final Integer serverPort;

    /**
     * Create a handler for registration and deregistration for components to use
     * to contact Consul.
     *
     * @param consulClient the handler for the Consul REST API
     * @param enableConsulRegistration should registration be enabled or not - used for remote
     *                                  probes running in a disconnected cluster
     * @param componentType the type of the component, used as the name of the service
     * @param instanceId the unique ID for this component instance, recorded as the service id
     * @param instanceIp the IP for this component instance, recorded as the service address
     * @param instanceRoute the route prefix for this component instance.
     * @param serverPort the PORT for health checks for the component instances, used to construct
     *                   a health-check for this service
     */
    public ConsulHealthcheckRegistration(final ConsulClient consulClient,
                                         final Boolean enableConsulRegistration,
                                         final String componentType,
                                         final String instanceId,
                                         final String instanceIp,
                                         final String instanceRoute,
                                         final Integer serverPort) {
        this.consulClient = consulClient;
        this.enableConsulRegistration = enableConsulRegistration;
        this.componentType = componentType;
        this.instanceId = instanceId;
        this.instanceRoute = instanceRoute;
        // for backwards compatibility with docker-compose invocation, look up instanceIp
        if (!StringUtils.isEmpty(instanceIp)) {
            this.instanceIp = instanceIp;
        } else {
            try {
                this.instanceIp = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                throw new RuntimeException("Cannot get localhost IP address...cannot continue.", e);
            }
        }
        this.serverPort = serverPort;
    }

    /**
     * Register this component instance with Consul, if not disabled. Specify the service typo,
     * instance id, IP address and port. Also register a health check.
     */
    public void registerService() {
        if (Boolean.FALSE.equals(enableConsulRegistration)) {
            return;
        }
        final NewService svc = new NewService();
        svc.setName(componentType);
        svc.setId(instanceId);
        svc.setAddress(instanceIp);
        svc.setPort(serverPort);
        final List<String> tags = new ArrayList<>(2);
        tags.add(COMPONENT_TAG);
        if (!StringUtils.isEmpty(instanceRoute)) {
            tags.add(encodeInstanceRoute(instanceRoute));
        }
        svc.setTags(tags);
        consulClient.agentServiceRegister(svc);
        final NewCheck healthCheck = new NewCheck();
        healthCheck.setId("service:" + svc.getId());
        healthCheck.setName("Service '" + svc.getName() + "' check");

        final StringJoiner healthPathJoiner = new StringJoiner("");
        healthPathJoiner.add("http://" + svc.getAddress() + ':' + svc.getPort());
        if (!StringUtils.isEmpty(instanceRoute)) {
            healthPathJoiner.add(instanceRoute);
        }
        healthPathJoiner.add(ComponentController.HEALTH_PATH);
        healthCheck.setHttp(healthPathJoiner.toString());

        healthCheck.setServiceId(svc.getId());
        healthCheck.setInterval(HEALTH_CHECK_PERIOD);
        healthCheck.setDeregisterCriticalServiceAfter(HEALTH_CHECK_CRITICAL_TIME);
        consulClient.agentCheckRegister(healthCheck);
    }

    /**
     * Deregister service from consul.
     */
    public void deregisterService() {
        if (Boolean.TRUE.equals(enableConsulRegistration)) {
            consulClient.agentServiceDeregister(instanceId);
        }
    }

    /**
     * Encode an instance route into a string that's usable as a consul service tag.
     * The format is "instance_route=(route)".
     *
     * @param instanceRoute The route to encode.
     * @return The encoded string.
     */
    public static String encodeInstanceRoute(@Nonnull final String instanceRoute) {
        return BaseVmtComponent.PROP_INSTANCE_ROUTE + "=" + instanceRoute;
    }

    /**
     * Decode a route encoded by {@link ConsulHealthcheckRegistration#encodeInstanceRoute(String)} .
     *
     * @param tag The tag to decode.
     * @return The route contained in the tag, or empty if the tag doesn't match the encoded format.
     */
    public static Optional<String> decodeInstanceRoute(@Nonnull final String tag) {
        String[] kv = tag.split("=");
        if (kv.length == 2 && kv[0].equals(BaseVmtComponent.PROP_INSTANCE_ROUTE)) {
            return Optional.of(kv[1]);
        } else {
            return Optional.empty();
        }
    }

}
