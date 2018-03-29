package com.vmturbo.api.component.external.api.service;

import java.io.OutputStream;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.clustermgr.api.impl.ClusterMgrClient;

/**
 * Implementation for the Cluster Manager Service API calls.
 **/
public class ClusterService implements IClusterService {

    public static final String ASTERISKS = "*****";
    private IClusterService clusterMgrApi;

    // the sensitive keys that we need to mask the values.
    // TODO centralized these keys, so we won't miss them if they are changed.
    private static final Set<String> sensitiveKeySet = ImmutableSet.of(
            "arangodbPass"
            , "userPassword"
            , "sslKeystorePassword"
            , "readonlyPassword");

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

    /**
     * Get cluster configuration. If the properties (keys) are in sensitiveKeySet,
     * the values will be masked as asterisks.
     *
     * @return ClusterConfigurationDTO
     */
    @Nonnull
    @Override
    public ClusterConfigurationDTO getClusterConfiguration() {
        final ClusterConfigurationDTO clusterConfiguration = clusterMgrApi.getClusterConfiguration();
        clusterConfiguration.getDefaults().values().forEach(dto -> {
            sensitiveKeySet.forEach((key -> {
                if (dto.containsKey(key)) dto.put(key, ASTERISKS);
            }));
        });
        return clusterConfiguration;
    }

    /**
     * Update cluster configuration.
     * Note: If calling by UI, before sending out for update, we need to restore the masked properties
     * values with original values if they are not updated (is "*****")
     *
     * @param newConfiguration new configurations
     * @return new configuration
     */
    @Nonnull
    @Override
    public ClusterConfigurationDTO setClusterConfiguration(ClusterConfigurationDTO newConfiguration) {
        final Map<String, ComponentPropertiesDTO> originalComponentPropertiesDTOMap = clusterMgrApi
                .getClusterConfiguration()
                .getDefaults();
        Map<String, ComponentPropertiesDTO> newComponentPropertiesDTOMap = newConfiguration.getDefaults();
        newComponentPropertiesDTOMap.forEach((k, v) -> {
            sensitiveKeySet.forEach((key -> {
                if (v.containsKey(key) && v.get(key).equals(ASTERISKS))
                    v.put(key, originalComponentPropertiesDTOMap.get(k).get(key));
            }));
        });
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

    /**
     * Get the default properties for a component type. If the properties (keys) are in sensitiveKeySet,
     * the values will be masked as asterisks.
     *
     * @param componentType component type
     * @return ComponentPropertiesDTO
     */
    @Override
    public ComponentPropertiesDTO getDefaultPropertiesForComponentType(String componentType) {
        ComponentPropertiesDTO dto = clusterMgrApi.getDefaultPropertiesForComponentType(componentType);
        sensitiveKeySet.forEach((key -> {
            if (dto.containsKey(key)) dto.put(key, ASTERISKS);
        }));
       return dto;
    }

    /**
     * Update new properties based on component type.
     * Note: it's called by UI, so before sending them out, we need to restore the masked values with
     * original values if they are not changed.
     *
     * @param componentType component type
     * @param newProperties new properties to be updated
     * @return
     */
    @Override
    public ComponentPropertiesDTO putDefaultPropertiesForComponentType(String componentType, ComponentPropertiesDTO newProperties) {
        ComponentPropertiesDTO originalDto = clusterMgrApi.getDefaultPropertiesForComponentType(componentType);
        sensitiveKeySet.forEach((key -> {
            if (newProperties.containsKey(key) && newProperties.get(key).equals(ASTERISKS))
                newProperties.put(key, originalDto.get(key));
        }));
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

    /**
     * Retrieve property in component, if the property (key) is in sensitiveKeySet,
     * the value will be masked as asterisks.
     *
     * @param componentType component type
     * @param propertyName property name
     * @return property value if the property is not in the sensitiveKeySet.
     */
    @Override
    public String getComponentTypeProperty(String componentType, String propertyName) {
        if (sensitiveKeySet.contains(propertyName)) {
            return ASTERISKS;
        }
        return clusterMgrApi.getComponentTypeProperty(componentType, propertyName);
    }

    @Override
    public String getComponentInstanceProperty(String componentType, String componentInstanceId, String propertyName) {
        return clusterMgrApi.getComponentInstanceProperty(componentType, componentInstanceId, propertyName);
    }
}
