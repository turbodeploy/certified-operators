package com.vmturbo.components.test.utilities.component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.palantir.docker.compose.connection.Container;
import com.palantir.docker.compose.connection.DockerPort;

import com.vmturbo.clustermgr.api.ClusterMgrClient;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * Used to configure a service through Consul.
 *
 * If an instance is specified, overrides that service instance's configurations.
 */
public class ServiceConfiguration {
    private final String serviceName;
    private final String instanceName;
    private final Map<String, String> configurations = new HashMap<>();

    private ServiceConfiguration(@Nonnull final String serviceName, @Nonnull final String instanceName) {
        // Create ServiceConfiguration via the forService factory method.
        this.serviceName = Objects.requireNonNull(serviceName);
        this.instanceName = Objects.requireNonNull(instanceName);
    }

    /**
     * Create a new {@link ServiceConfiguration} that applies configurations for a particular
     * service.
     *
     * @param serviceName The name of the service to be configured.
     * @param instanceName Instance name of the container configured
     * @return A {@link ServiceConfiguration} instance for the given service.
     */
    public static ServiceConfiguration forService(@Nonnull final String serviceName,
            @Nonnull String instanceName) {
        return new ServiceConfiguration(serviceName, instanceName);
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
    public void apply(@Nullable final Container clusterManagerContainer)
        throws ServiceConfigurationException {
        if (clusterManagerContainer != null) {
            DockerPort dockerPort = clusterManagerContainer.port(ComponentUtils.GLOBAL_HTTP_PORT);
            final ClusterMgrRestClient clusterMgrClient = ClusterMgrClient.createClient(
                    ComponentApiConnectionConfig.newBuilder()
                            .setHostAndPort(dockerPort.getIp(), dockerPort.getExternalPort(), "")
                            .build());

            apply(clusterMgrClient);
        }
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
    void apply(@Nonnull final ClusterMgrRestClient clusterMgrClient)
            throws ServiceConfigurationException {
        configurations.entrySet()
                .forEach(entry -> putProperty(clusterMgrClient, entry.getKey(), entry.getValue()));
    }

    /**
     * Set a key/value property using clustermgr.
     *
     * @param clustermgr A client to the clustermgr for use in setting the property.
     * @param key The key at which to put the value
     * @param value The value where the key should be put
     */
    private void putProperty(@Nonnull final ClusterMgrRestClient clustermgr,
                             @Nonnull final String key, @Nonnull final String value)
        throws ServiceConfigurationException {
        try {
            clustermgr.setComponentLocalProperty(serviceName, key, value);
        } catch (RuntimeException e) {
            final String message = String.format("Unexpected exception occurred while setting key \"%s\" to " +
                "value \"%s\" for service %s (%s)", key, value, serviceName, instanceName);
            throw new ServiceConfigurationException(message, e);
        }
    }
}
