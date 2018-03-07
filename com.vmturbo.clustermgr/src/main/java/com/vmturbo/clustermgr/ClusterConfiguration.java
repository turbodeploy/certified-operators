package com.vmturbo.clustermgr;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

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

}
