package com.vmturbo.api.component.external.api.service;

import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.api.dto.cluster.ClusterConfigurationDTO;
import com.vmturbo.api.dto.cluster.ComponentInstanceDTO;
import com.vmturbo.api.dto.cluster.ComponentPropertiesDTO;
import com.vmturbo.api.serviceinterfaces.IClusterService;
import com.vmturbo.clustermgr.api.ClusterConfiguration;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.clustermgr.api.ComponentInstanceInfo;
import com.vmturbo.clustermgr.api.ComponentProperties;
import com.vmturbo.clustermgr.api.ComponentPropertiesMap;
import com.vmturbo.components.common.config.SensitiveDataUtil;

/**
 * Implementation for the Cluster Manager Service API calls.
 **/
public class ClusterService implements IClusterService {

    /**
     * String to use to obscure secret data items.
     */
    public static final String ASTERISKS = "*****";

    private ClusterMgrRestClient clusterMgrApi;

    /**
     * Create a ClusterManagerClient instance, used to make requests to the ClusterMgrApi.
     *
     * @param clusterManagerClient the handle to the ClusterMgrApi to call
     */
    public ClusterService(@Nonnull ClusterMgrRestClient clusterManagerClient) {
        clusterMgrApi = Objects.requireNonNull(clusterManagerClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isXLEnabled() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTelemetryEnabled() {
        return clusterMgrApi.isTelemetryEnabled();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setTelemetryEnabled(boolean enabled) {
        clusterMgrApi.setTelemetryEnabled(enabled);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isTelemetryInitialized() {
        return clusterMgrApi.isTelemetryInitialized();
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public Set<String> getKnownComponents() {
        return clusterMgrApi.getKnownComponents();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String setPropertyForComponentInstance(String componentType, String instanceId,
            String propertyName, String propertyValue) {
        return clusterMgrApi.setComponentInstanceProperty(componentType, instanceId,
                propertyName, propertyValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Set<String> getComponentInstanceIds(String componentType) {
        return clusterMgrApi.getComponentInstanceIds(componentType);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, String> getComponentsState() {
        return clusterMgrApi.getComponentsState();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void collectComponentDiagnostics(OutputStream responseOutput) {
        clusterMgrApi.collectComponentDiagnostics(responseOutput);
    }

    /**
     * {@inheritDoc}
     */
    @Nonnull
    @Override
    public ClusterConfigurationDTO getClusterConfiguration() {
        final ClusterConfigurationDTO clusterConfiguration =
            convert(clusterMgrApi.getClusterConfiguration());
        sanitizeConfiguration(clusterConfiguration);
        return clusterConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public ClusterConfigurationDTO setClusterConfiguration(
        @Nonnull ClusterConfigurationDTO newConfigurationDTO) {

        // fetch the current configuration
        final ClusterConfiguration originalConfiguration = clusterMgrApi.getClusterConfiguration();

        final ClusterConfiguration newConfiguration = convert(newConfigurationDTO);
        revertHiddenFields(newConfiguration, originalConfiguration);

        final ClusterConfigurationDTO storedClusterConfiguration =
            convert(clusterMgrApi.setClusterConfiguration(newConfiguration));
        sanitizeConfiguration(storedClusterConfiguration);
        return storedClusterConfiguration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String getComponentInstanceProperty(@Nonnull String componentType,
                                               @Nonnull String componentInstanceId,
                                               @Nonnull String propertyName) {
        return clusterMgrApi.getComponentInstanceProperty(componentType, componentInstanceId, propertyName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public ComponentPropertiesDTO getComponentDefaultProperties(@Nonnull String componentType) {
        ComponentPropertiesDTO dto = convert(clusterMgrApi.getComponentDefaultProperties(componentType));
        sanitizeProperties(dto);
       return dto;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public ComponentPropertiesDTO getEffectiveInstanceProperties(
        @Nonnull String componentType,
        @Nonnull String componentInstanceId) {
        return convert(
            clusterMgrApi.getEffectiveInstanceProperties(componentType, componentInstanceId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public ComponentPropertiesDTO getComponentInstanceProperties(
        @Nonnull String componentType,
        @Nonnull String componentInstanceId) {
        return convert(clusterMgrApi.getComponentInstanceProperties(componentType, componentInstanceId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public ComponentPropertiesDTO putComponentInstanceProperties(
        @Nonnull String componentType,
        @Nonnull String componentInstanceId,
        @Nonnull ComponentPropertiesDTO updatedProperties) {
        return convert(clusterMgrApi.putComponentInstanceProperties(componentType,
            componentInstanceId, convert(updatedProperties)));
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public String getComponentDefaultProperty(@Nonnull String componentType,
                                              @Nonnull String propertyName) {
        if (SensitiveDataUtil.hasSensitiveData(propertyName)) {
            return ASTERISKS;
        }
        return clusterMgrApi.getComponentDefaultProperty(componentType, propertyName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ComponentPropertiesDTO putComponentLocalProperties(
        final String componentType,
        final ComponentPropertiesDTO updatedProperties) {
        return convert(clusterMgrApi.putLocalComponentProperties(componentType,
            convert(updatedProperties)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ComponentPropertiesDTO getComponentLocalProperties(final String componentType) {
        return convert(clusterMgrApi.getComponentLocalProperties(componentType));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getComponentLocalProperty(@Nonnull final String componentType,
                                            @Nonnull final String propertyName) {
        return clusterMgrApi.getComponentLocalProperty(componentType, propertyName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String setComponentLocalProperty(@Nonnull final String componentType,
                                            @Nonnull final String propertyName,
                                            @Nullable final String newValue) {
        return clusterMgrApi.setComponentLocalProperty(componentType,
            propertyName, newValue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @Nonnull
    public String deleteComponentLocalProperty(@Nonnull final String componentType,
                                               @Nonnull final String propertyName) {
        return clusterMgrApi.deleteComponentLocalProperty(componentType,
            propertyName);
    }

    /**
     * Replace any hidden property value in the configuration with the corresponding
     * original value. This applies to default properties and local properties for each
     * component type, and the instance properties for each component instance.
     *
     * @param newConfiguration the new cluster configuration to be operated on
     * @param originalConfiguration the prior configuration from which original values
     *                              are restored
     */
    private void revertHiddenFields(@Nonnull final ClusterConfiguration newConfiguration,
                                    @Nonnull final ClusterConfiguration originalConfiguration) {
        revertHiddenFields(newConfiguration.getDefaults(), originalConfiguration.getDefaults());
        revertHiddenFields(newConfiguration.getLocalProperties(),
            originalConfiguration.getLocalProperties());

        newConfiguration.getInstances().forEach((instanceId, instanceInfo) ->
            revertHiddenFields(instanceInfo.getProperties(),
                originalConfiguration.getInstances().getOrDefault(instanceId,
                    new ComponentInstanceInfo(instanceInfo.getComponentType(),
                        instanceInfo.getComponentVersion(),
                        new ComponentProperties()))
                    .getProperties())
        );
    }

    /**
     * For each component type in the newProperties map, replace any hidden properties
     * with the corresponding type/property/value from the originalPropMap.
     *
     * @param newPropMap the new map from component type -> configuration properties
     * @param originalPropMap the reference map component type -> configuraiton properties
     *                        from which replacement values are taken for hidden properties in
     *                        the newPropMap
     */
    private void revertHiddenFields(final ComponentPropertiesMap newPropMap,
                                    final ComponentPropertiesMap originalPropMap) {
        newPropMap.getComponents().forEach((componentType, componentProperties) -> {
            if (originalPropMap.getComponentProperties(componentType) != null) {
                revertHiddenFields(componentProperties,
                    originalPropMap.getComponentProperties(componentType));
            }
        });
    }

    /**
     * Replace any hidden fields in the new properties with the corresponding value from
     * the original properties. If there is no corresponding original value, then replace with
     * null, effectively deleting that property.
     *
     * <p>Note: the property values are updated in place, modifying the 'newProperties' parameter.
     *
     * @param newProperties the new configuration properties to sanitize.
     * @param originalProperties the reference configuration properties, from which the
     *                           values are take for any hidden properties in the 'newProperties'
     */
    private void revertHiddenFields(@Nonnull final ComponentProperties newProperties,
                                    @Nonnull final ComponentProperties originalProperties) {
        newProperties.forEach((propKey, propValue) -> {
            if (propValue.equals(ASTERISKS)) {
                newProperties.put(propKey, originalProperties.getOrDefault(propKey, null));
            }
        });
    }

    /**
     * Replace any sensitive values in the cluster configuration with a masking value.
     * This includes default properties, local properties,
     * and for each instance the instanceProperties.
     *
     * <p>Note that the values are masked in-place, i.e. the different key/value maps are
     * modified directly.
     *
     * @param clusterConfigDTO the cluster configuration to be sanitized.
     */
    private void sanitizeConfiguration(@Nonnull final ClusterConfigurationDTO clusterConfigDTO) {
        sanitizeProperties(clusterConfigDTO.getDefaultProperties());
        sanitizeProperties(clusterConfigDTO.getLocalProperties());
        clusterConfigDTO.getInstances().values().forEach(instanceConfiguration ->
            sanitizeProperties(instanceConfiguration.getProperties()));
    }

    /**
     * Replace any sensitive values in any of the ComponentProperties map with a masking value.
     *
     * <p>Note that the values are masked in-place, i.e. each ComponentPropertiesDTO map is
     * modified directly.
     *
     * @param componentProperties The map of entity type -> ComponentPropertiesDTO to update
     */
    private void sanitizeProperties(final Map<String, ComponentPropertiesDTO> componentProperties) {
        componentProperties.values().forEach(this::sanitizeProperties);
    }

    /**
     * Replace any sensitive values in the ComponentPropertiesDTO map with a masking value.
     *
     * <p>Note that the values are masked in-place, i.e. each ComponentPropertiesDTO map is
     * modified directly.
     *
     * @param propertiesMap the map of key/value pairs to be sanitized
     */
    private void sanitizeProperties(final ComponentPropertiesDTO propertiesMap) {
        SensitiveDataUtil.getSensitiveKey().forEach((key -> {
            if (propertiesMap.containsKey(key)) {
                propertiesMap.put(key, ASTERISKS);
            }
        }));
    }

    @Nonnull
    private ComponentProperties convert(@Nonnull ComponentPropertiesDTO src) {
        final ComponentProperties result = new ComponentProperties();
        result.putAll(src);
        return result;
    }

    @Nonnull
    private ComponentPropertiesDTO convert(@Nonnull ComponentProperties src) {
        final ComponentPropertiesDTO result = new ComponentPropertiesDTO();
        result.putAll(src);
        return result;
    }

    @Nonnull
    private ComponentInstanceInfo convert(@Nonnull ComponentInstanceDTO src) {
        return new ComponentInstanceInfo(src.getComponentType(), src.getComponentVersion(),
            convert(src.getProperties()));
    }

    @Nonnull
    private ComponentInstanceDTO convert(@Nonnull ComponentInstanceInfo src) {
        return new ComponentInstanceDTO(src.getComponentType(), src.getComponentVersion(),
            convert(src.getProperties()));
    }

    @Nonnull
    private ClusterConfiguration convert(@Nonnull ClusterConfigurationDTO src) {
        final ClusterConfiguration result = new ClusterConfiguration();
        src.getInstances().forEach((instanceId, instanceInfo) ->
            result.getInstances().put(instanceId, convert(instanceInfo)));
        src.getDefaultProperties().forEach((componentType, defaultProperties) ->
            result.addComponentType(componentType, convert(defaultProperties)));
        src.getLocalProperties().forEach((componentType, localProperties) ->
            result.setLocalProperties(componentType, convert(localProperties)));
        return result;
    }

    @Nonnull
    private ClusterConfigurationDTO convert(@Nonnull ClusterConfiguration src) {
        Objects.requireNonNull(src);
        final ClusterConfigurationDTO result = new ClusterConfigurationDTO();
        src.getInstances().forEach((componentId, componentInstanceInfo) ->
            result.getInstances().put(componentId, convert(componentInstanceInfo)));
        src.getDefaults().getComponents().forEach((componentType, defaultProperties) ->
            result.getDefaultProperties().put(componentType, convert(defaultProperties)));
        src.getLocalProperties().getComponents().forEach((componentType, localProperties) ->
            result.setLocalProperties(componentType, convert(localProperties)));
        return result;
    }
}
