package com.vmturbo.clustermgr.api;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Capture the configuration of an Ops Manager cluster: nodes / components/ config properties;
 * components / default-properties; and components / local-properties.
 */
public class ClusterConfiguration {

    // map instance_id to component->properties map
    @JsonProperty("component_instances")
    private final Map<String, ComponentInstanceInfo> instances = new HashMap<>();

    @JsonProperty("component_type_properties")
    private final ComponentPropertiesMap defaults = new ComponentPropertiesMap();

    @JsonProperty("local_properties")
    private final ComponentPropertiesMap localProperties = new ComponentPropertiesMap();

    @Nonnull
    public Map<String, ComponentInstanceInfo> getInstances() {
        return instances;
    }

    public ComponentPropertiesMap getDefaults() {
        return defaults;
    }

    public ComponentPropertiesMap getLocalProperties() {
        return localProperties;
    }

    /**
     * Register a new component type with default configuration properties.
     *
     * @param componentType the name of the component type to register
     * @param defaultConfiguration the default configuration properties for the new component type
     */
    public void addComponentType(@Nonnull String componentType,
                                 @Nonnull ComponentProperties defaultConfiguration) {
        defaults.addComponentConfiguration(componentType, defaultConfiguration);
    }

    /**
     * Register the "local" properties for a component type. Local properties are an override
     * for the default configuration properties.
     *
     * @param componentType the type of component for these local properties
     * @param newLocalProperties a map of propertyName -> propertyValue
     */
    public void setLocalProperties(@Nonnull String componentType,
                                   @Nonnull ComponentProperties newLocalProperties) {
        localProperties.addComponentConfiguration(componentType, newLocalProperties);
    }

    /**
     * Add an instance of a component type, indicating the instance id, component type,
     * software version, and instance configuration properties.
     *
     * <p>The instance configuration properties have the highest priority when computing the
     * effect configuration properties passed to the component instance on startup.
     *
     * <p>TODO: the entire concept of separate configuration for instances is being reconsidered
     *
     * @param instanceId the unique id of this component instance
     * @param componentType the component type
     * @param componentVersion the sofware version of the component instance
     * @param instanceConfiguration the configuration properties for the component.
     */
    public void addComponentInstance(@Nonnull String instanceId,
                                     @Nonnull String componentType,
                                     @Nonnull String componentVersion,
                                     @Nonnull ComponentProperties instanceConfiguration) {
        instances.put(instanceId, new ComponentInstanceInfo(componentType, componentVersion,
            instanceConfiguration));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ClusterConfiguration that = (ClusterConfiguration)o;

        if (!instances.equals(that.instances)) {
            return false;
        }
        return defaults.equals(that.defaults);
    }

    @Override
    public int hashCode() {
        int result = instances.hashCode();
        result = 31 * result + defaults.hashCode();
        return result;
    }
}
