package com.vmturbo.clustermgr.api;

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Capture the configuration information related to a given VMT Component instance.
 *
 * <p>Each instance belongs to a component type and version information.
 * Also, each instance has a map of propertyName -> propertyValue (each Strings) that
 * override the corresponding component-type default or local property values.
 */
public class ComponentInstanceInfo {

    @JsonProperty
    private String componentType;
    @JsonProperty
    private String componentVersion;
    @JsonProperty
    private ComponentProperties properties;

    @SuppressWarnings("unused")
    private ComponentInstanceInfo() {
        // only used by Jackson Deserialize
    }

    /**
     * Bean to represent an instance of an XL Component. Record the component type and version here,
     * as well as the component-specific override properties.
     *
     * @param componentType the component type name
     * @param componentVersion the version information for this component
     * @param properties the configuration property overrides for this component
     */
    public ComponentInstanceInfo(String componentType, String componentVersion, ComponentProperties properties) {
        this.componentType = componentType;
        this.componentVersion = componentVersion;
        this.properties = properties;
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

        return Objects.equals(componentType, that.componentType) &&
                Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(componentType, properties);
    }
}
