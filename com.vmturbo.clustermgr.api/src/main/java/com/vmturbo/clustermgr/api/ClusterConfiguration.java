package com.vmturbo.clustermgr.api;

import java.util.HashMap;
import java.util.Map;

import javax.annotation.Nonnull;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Capture the configuration of an Ops Manager cluster: nodes / components/ config properties; and
 * components / default-properties.
 */
public class ClusterConfiguration {

    // map instance_id to component->properties map
    @JsonProperty("component_instances")
    private Map<String, ComponentInstanceInfo> instances = new HashMap<>();

    private ComponentPropertiesMap defaults = new ComponentPropertiesMap();

    public Map<String, ComponentInstanceInfo> getInstances() {
        return instances;
    }

    @JsonProperty("component_types")
    public ComponentPropertiesMap getDefaults() {
        return defaults;
    }

    public void addComponentType(@Nonnull String componentType,
                                 @Nonnull ComponentProperties defaultConfiguration) {
        defaults.addComponentConfiguration(componentType, defaultConfiguration);
    }

    public void addComponentInstance(@Nonnull String instanceId,
                                     @Nonnull String componentType,
                                     @Nonnull String componentVersion,
                                     @Nonnull String node,
                                     @Nonnull ComponentProperties instanceConfiguration) {
        instances.put(instanceId, new ComponentInstanceInfo(componentType, componentVersion, node, instanceConfiguration));
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
