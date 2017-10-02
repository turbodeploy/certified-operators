package com.vmturbo.clustermgr;

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
    private ComponentProperties properties;

    private ComponentInstanceInfo() {
        // only used by Jackson Deserialize
    }

    public ComponentInstanceInfo(String componentType, String node, ComponentProperties properties) {
        this.componentType = componentType;
        this.node = node;
        this.properties = properties;
    }

    public String getNode() {
        return node;
    }

    public String getComponentType() {
        return componentType;
    }

    public ComponentProperties getProperties() {
        return properties;
    }
}
