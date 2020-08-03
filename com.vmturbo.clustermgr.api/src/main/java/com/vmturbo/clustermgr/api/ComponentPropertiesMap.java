package com.vmturbo.clustermgr.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * Capture the configuration properties of a set of named Components. The component name, i.e. the primary key,
 * may either be a component type, when representing the "defaults" for a component type, or a "component id",
 * when representing the actual configuration values for a given instance of a component type.
 *
 * Implement AnyGetter and AnySetter in order to support JSON serialization/deserialization of an arbitrary
 * Map of Properties.
 */
public class ComponentPropertiesMap {
    // map from component-name -> Map<key,value>
    private Map<String, ComponentProperties> components = new HashMap<>();

    @JsonIgnore
    public Set<String> getComponentNames() {
        return components.keySet();
    }

    public ComponentProperties getComponentProperties(String componentName) {
        return components.get(componentName);
    }

    @JsonAnyGetter
    public Map<String, ComponentProperties> getComponents() {
        return components;
    }

    @JsonAnySetter
    public void addComponentConfiguration(String componentName, ComponentProperties componentProperties) {
        components.put(componentName, componentProperties);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ComponentPropertiesMap)) {
            return false;
        }
        final ComponentPropertiesMap that = (ComponentPropertiesMap)o;
        return components.equals(that.components);
    }

    @Override
    public int hashCode() {
        return components.hashCode();
    }
}
