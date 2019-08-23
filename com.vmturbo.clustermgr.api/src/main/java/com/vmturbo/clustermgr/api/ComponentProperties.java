package com.vmturbo.clustermgr.api;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;

import java.util.HashMap;

/**
 * Capture the configuration properties for a single Component - either the "default" properties of a
 * Component Type in the key/value database, or the "current" properties of a Component Instance.
 */
public class ComponentProperties extends HashMap<String, String> {
    @Override
    @JsonAnySetter
    public String put(String key, String value) {
        return super.put(key, value);
    }

    @Override
    @JsonAnyGetter
    public String get(Object key) {
        return super.get(key);
    }
}
