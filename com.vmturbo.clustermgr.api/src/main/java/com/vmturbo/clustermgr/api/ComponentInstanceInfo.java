package com.vmturbo.clustermgr.api;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Capture the configuration information related to a given VMT Component instance.
 *
 * Each instance belongs to a component type and runs on a given execution node (hostname, IP, or "" meaning "default".
 * Also, each instance has a map of propertyName -> propertyValue (each Strings).
 */
public class ComponentInstanceInfo {

    @JsonProperty
    private String node;
    @JsonProperty
    private String componentType;
    @JsonProperty
    private String componentVersion;
    @JsonProperty
    private ComponentProperties properties;

    private ComponentInstanceInfo() {
        // only used by Jackson Deserialize
    }

    public ComponentInstanceInfo(String componentType, String componentVersion, String node, ComponentProperties properties) {
        this.componentType = componentType;
        this.componentVersion = componentVersion;
        this.node = node;
        this.properties = properties;
    }

    public String getNode() {
        return node;
    }

    public String getComponentType() {
        return componentType;
    }

    public String getComponentVersion() {
        return componentVersion;
    }

    public ComponentProperties getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {

        if (!(o instanceof ComponentInstanceInfo)) {
            return false;
        }

        final ComponentInstanceInfo that = (ComponentInstanceInfo)o;

        return Objects.equals(node, that.node) &&
                Objects.equals(componentType, that.componentType) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(node, componentType, properties);
    }
}
