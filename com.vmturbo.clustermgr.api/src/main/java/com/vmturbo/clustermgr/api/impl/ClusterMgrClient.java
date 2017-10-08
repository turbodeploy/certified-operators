package com.vmturbo.clustermgr.api.impl;

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * ClusterMgrClient provides the client-side functionality necccesary to interact with the Cluster Mgr API.
 **/
public class ClusterMgrClient extends ComponentApiClient<ClusterMgrRestClient>
    implements IClusterService {

    private ClusterMgrClient(@Nonnull ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
    }

    @Override
    public boolean isTelemetryInitialized() {
        return restClient.isTelemetryInitialized();
    }

    /**
     * Indicates whether the telemetry is enabled.
     *
     * @return {@code true} iff telemetry is enabled.
     */
    @Override
    public boolean isTelemetryEnabled() {
        return restClient.isTelemetryEnabled();
    }

    /**
     * Sets the telemetry enabled flag.
     *
     * @param enabled The telemetry enabled flag.
     */
    @Override
    public void setTelemetryEnabled(boolean enabled) {
        restClient.setTelemetryEnabled(enabled);
    }

    @Nonnull
    @Override
    protected ClusterMgrRestClient createRestClient(@Nonnull ComponentApiConnectionConfig connectionConfig) {
        return new ClusterMgrRestClient(connectionConfig);
    }

    public static ClusterMgrClient rpcOnly(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new ClusterMgrClient(connectionConfig);
    }

    @Override
    public boolean isXLEnabled() {
        return true;
    }

    @Override
    @Nonnull
    public Set<String> getKnownComponents() {
        return restClient.getKnownComponents();
    }

    @Override
    @Nonnull
    public String setPropertyForComponentType(@Nonnull String componentType,
                                              @Nonnull String propertyName,
                                              @Nonnull String propertyValue) {
        return restClient.setPropertyForComponentType(componentType, propertyName, propertyValue);
    }

    @Override
    @Nonnull
    public String setPropertyForComponentInstance(@Nonnull String componentType,
                                                  @Nonnull String instanceId,
                                                  @Nonnull String propertyName,
                                                  @Nonnull String propertyValue) {
        return restClient.setPropertyForComponentInstance(componentType, instanceId, propertyName, propertyValue);
    }

    @Override
    @Nonnull
    public Set<String> getComponentInstanceIds(@Nonnull String componentType) {
        return restClient.getComponentInstanceIds(componentType);
    }

    @Override
    @Nonnull
    public Map<String, String> getComponentsState() {
        return restClient.getComponentsState();
    }

    @Override
    public void collectComponentDiagnostics(@Nonnull OutputStream responseOutput) {
        restClient.collectComponentDiagnostics(responseOutput);
    }

    @Override
    @Nonnull
    public ClusterConfigurationDTO getClusterConfiguration() {
        return restClient.getClusterConfiguration();
    }

    @Override
    @Nonnull
    public ClusterConfigurationDTO setClusterConfiguration(@Nonnull ClusterConfigurationDTO newConfiguration) {
        return restClient.setClusterConfiguration(newConfiguration);
    }

    @Override
    @Nonnull
    public String getNodeForComponentInstance(String componentType, String instanceId) {
        return restClient.getNodeForComponentInstance(componentType, instanceId);
    }

    @Override
    @Nonnull
    public String setNodeForComponentInstance(@Nonnull String componentType,
                                              @Nonnull String instanceId,
                                              @Nonnull String nodeName) {
        return restClient.setNodeForComponentInstance(componentType, instanceId, nodeName);
    }

    @Override
    @Nonnull
    public ComponentPropertiesDTO getDefaultPropertiesForComponentType(@Nonnull String componentType) {
        return restClient.getDefaultPropertiesForComponentType(componentType);
    }

    @Override
    @Nonnull
    public ComponentPropertiesDTO putDefaultPropertiesForComponentType(@Nonnull String componentType,
                                                                       @Nonnull ComponentPropertiesDTO newProperties) {
        return restClient.putDefaultPropertiesForComponentType(componentType, newProperties);
    }

    @Override
    @Nonnull
    public ComponentPropertiesDTO getComponentInstanceProperties(@Nonnull String componentType,
                                                                 @Nonnull String componentInstanceId) {
        return restClient.getComponentInstanceProperties(componentType, componentInstanceId);
    }

    @Override
    @Nonnull
    public ComponentPropertiesDTO putComponentInstanceProperties(@Nonnull String componentType,
                                                                 @Nonnull String componentInstanceId,
                                                                 @Nonnull ComponentPropertiesDTO updatedProperties) {
        return restClient.putComponentInstanceProperties(componentType, componentInstanceId, updatedProperties);
    }

    @Override
    @Nonnull
    public String getComponentTypeProperty(@Nonnull String componentType, @Nonnull String propertyName) {
        return restClient.getComponentTypeProperty(componentType, propertyName);
    }

    @Override
    @Nonnull
    public String getComponentInstanceProperty(@Nonnull String componentType,
                                               @Nonnull String componentInstanceId,
                                               @Nonnull String propertyName) {
        return restClient.getComponentInstanceProperty(componentType, componentInstanceId, propertyName);
    }
}
