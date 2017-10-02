package com.vmturbo.components.test.utilities.component;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;

import com.vmturbo.clustermgr.api.impl.ClusterMgrClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * Used to configure a service through Consul.
 *
 * If no instance is specified, overrides that service's default configurations.
 * If an instance is specified, overrides that service instance's configurations.
 */
public class ServiceConfiguration {
    private final String serviceName;
    private Optional<String> instanceName = Optional.empty();
    private final Map<String, String> configurations = new HashMap<>();

    private ServiceConfiguration(@Nonnull final String serviceName) {
        // Create ServiceConfiguration via the forService factory method.
        this.serviceName = serviceName;
    }

    /**
     * Create a new {@link ServiceConfiguration} that applies configurations for a particular service.
     * If no instance is supplied by a later call to {@link #instance(String)}, the configuration
     * will override the service's defaults.
     *
     * @param serviceName The name of the service to be configured.
     * @return A {@link ServiceConfiguration} instance for the given service.
     */
    public static ServiceConfiguration forService(@Nonnull final String serviceName) {
        return new ServiceConfiguration(serviceName);
    }

    /**
     * Set the instance of the service that the configuration should apply to.
     *
     * @param instanceName The name of the instance that this configuration should apply to.
     *                     This is the normally the instance_id in docker-compose.
     *                     Example: "topology-processor-1"
     * @return A reference to {@link this} to support method chaining.
     */
    public ServiceConfiguration instance(@Nonnull final String instanceName) {
        this.instanceName = Optional.of(instanceName);
        return this;
    }

    /**
     * Add a configuration to be set by this service. Sets the value at the given key.
     * Specifying the same key twice on the same {@link ServiceConfiguration} will overwrite
     * the earlier value with the later value.
     *
     * @param key The key at which to put the value
     * @param value The value where the key should be put
     * @return A reference to {@link this} to support method chaining.
     */
    public ServiceConfiguration withConfiguration(@Nonnull final String key, @Nonnull final String value) {
        configurations.put(key, value);

        return this;
    }

    /**
     * Apply all added configurations added to this ServiceConfiguration.
     *
     * @param clusterManagerContainer The container for the clustermgr. Clustermgr is used to set configurations.
     * @throws ServiceConfigurationException If at least one configuration fails to be applied.
     */
    public void apply(final Container clusterManagerContainer)
        throws ServiceConfigurationException {
        DockerPort dockerPort = clusterManagerContainer.port(ComponentUtils.GLOBAL_HTTP_PORT);
        ClusterMgrClient clusterMgrClient = ClusterMgrClient
            .rpcOnly(ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(dockerPort.getIp(), dockerPort.getExternalPort())
                .build());

        apply(clusterMgrClient);
    }

    /**
     * An exception thrown when a service configuration setting cannot be applied.
     */
    public static class ServiceConfigurationException extends RuntimeException {
        public ServiceConfigurationException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * Apply all added configurations added to this ServiceConfiguration using the supplied client to clusterMgr.
     *
     * @param clusterMgrClient The client to use when passing configuration to the clusterMgr.
     * @throws ServiceConfigurationException If at least one configuration fails to be applied.
     */
    @VisibleForTesting
    void apply(@Nonnull final ClusterMgrClient clusterMgrClient) throws ServiceConfigurationException {
        configurations.entrySet().forEach(entry -> putProperty(clusterMgrClient, entry.getKey(), entry.getValue()));
    }

    /**
     * Set a key/value property using clustermgr.
     *
     * @param clustermgr A client to the clustermgr for use in setting the property.
     * @param key The key at which to put the value
     * @param value The value where the key should be put
     */
    private void putProperty(@Nonnull final ClusterMgrClient clustermgr,
                             @Nonnull final String key, @Nonnull final String value)
        throws ServiceConfigurationException {
        try {
            if (instanceName.isPresent()) {
                clustermgr.setPropertyForComponentInstance(serviceName, instanceName.get(), key, value);
            } else {
                clustermgr.setPropertyForComponentType(serviceName, key, value);
            }
        } catch (RuntimeException e) {
            String instance = instanceName.orElse("default");
            final String message = String.format("Unexpected exception occurred while setting key \"%s\" to " +
                "value \"%s\" for service %s (%s)", key, value, serviceName, instance);
            throw new ServiceConfigurationException(message, e);
        }
    }
}
