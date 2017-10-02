package com.vmturbo.api.component.external.api.service;

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.clustermgr.api.impl.ClusterMgrClient;

/**
 * Implementation for the Cluster Manager Service API calls.
 **/
public class ClusterService implements IClusterService {

    private IClusterService clusterMgrApi;

    public ClusterService(ClusterMgrClient clusterManagerClient) {
        clusterMgrApi = clusterManagerClient;
    }

    @Override
    public boolean isXLEnabled() {
        return true;
    }

    /**
     * Indicates whether the telemetry is enabled.
     *
     * @return {@code true} iff telemetry is enabled.
     */
    @Override
    public boolean isTelemetryEnabled() {
        return clusterMgrApi.isTelemetryEnabled();
    }

    /**
     * Sets the telemetry enabled flag.
     *
     * @param enabled The telemetry enabled flag.
     */
    @Override
    public void setTelemetryEnabled(boolean enabled) {
        clusterMgrApi.setTelemetryEnabled(enabled);
    }

    @Override
    public boolean isTelemetryInitialized() {
        return clusterMgrApi.isTelemetryInitialized();
    }

    @Nonnull
    @Override
    public Set<String> getKnownComponents() {
        return clusterMgrApi.getKnownComponents();
    }

    @Override
    public String setPropertyForComponentType(String componentType, String propertyName, String propertyValue) {
        return clusterMgrApi.setPropertyForComponentType(componentType, propertyName, propertyValue);
    }

    @Override
    public String setPropertyForComponentInstance(String componentType, String instanceId, String propertyName, String propertyValue) {
        return clusterMgrApi.setPropertyForComponentInstance( componentType,  instanceId,  propertyName,  propertyValue);
    }

    @Override
    public Set<String> getComponentInstanceIds(String componentType) {
        return clusterMgrApi.getComponentInstanceIds(componentType);
    }

    @Override
    public Map<String, String> getComponentsState() {
        return clusterMgrApi.getComponentsState();
    }

    @Override
    public void collectComponentDiagnostics(OutputStream responseOutput) {
        clusterMgrApi.collectComponentDiagnostics(responseOutput);
    }

    @Nonnull
    @Override
    public ClusterConfigurationDTO getClusterConfiguration() {
        return clusterMgrApi.getClusterConfiguration();
    }

    @Nonnull
    @Override
    public ClusterConfigurationDTO setClusterConfiguration(ClusterConfigurationDTO newConfiguration) {
        return clusterMgrApi.setClusterConfiguration(newConfiguration);
    }

    @Override
    public String getNodeForComponentInstance(String componentType, String instanceId) {
        return clusterMgrApi.getNodeForComponentInstance(componentType, instanceId);
    }

    @Override
    public String setNodeForComponentInstance(String componentType, String instanceId, String nodeName) {
        return clusterMgrApi.setNodeForComponentInstance(componentType, instanceId, nodeName);
    }

    @Override
    public ComponentPropertiesDTO getDefaultPropertiesForComponentType(String componentType) {
        return clusterMgrApi.getDefaultPropertiesForComponentType(componentType);
    }

    @Override
    public ComponentPropertiesDTO putDefaultPropertiesForComponentType(String componentType, ComponentPropertiesDTO newProperties) {
        return clusterMgrApi.putDefaultPropertiesForComponentType(componentType, newProperties);
    }

    @Override
    public ComponentPropertiesDTO getComponentInstanceProperties(String componentType, String componentInstanceId) {
        return clusterMgrApi.getComponentInstanceProperties(componentType, componentInstanceId);
    }

    @Override
    public ComponentPropertiesDTO putComponentInstanceProperties(String componentType, String componentInstanceId, ComponentPropertiesDTO updatedProperties) {
        return clusterMgrApi.putComponentInstanceProperties(componentType, componentInstanceId, updatedProperties);
    }

    @Override
    public String getComponentTypeProperty(String componentType, String propertyName) {
        return clusterMgrApi.getComponentTypeProperty(componentType, propertyName);
    }

    @Override
    public String getComponentInstanceProperty(String componentType, String componentInstanceId, String propertyName) {
        return clusterMgrApi.getComponentInstanceProperty(componentType, componentInstanceId, propertyName);
    }
}
