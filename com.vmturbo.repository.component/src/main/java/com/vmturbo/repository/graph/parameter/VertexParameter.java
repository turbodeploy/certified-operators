package com.vmturbo.repository.graph.parameter;

import java.util.ArrayList;
import java.util.List;

public class VertexParameter {

    private String collection;

    private String key;

    private Object value;

    private List<String> keys;   // For batch mode

    private List<? extends Object> values; // For batch mode

    private boolean waitForSync;

    private VertexParameter(Builder b) {
        collection = b.collection;
        key = b.key;
        value = b.value;
        keys = b.keys;
        values = b.values;
        waitForSync = b.waitForSync;
    }

    public String getCollection() {
        return collection;
    }

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public List<String> getKeys() {
        return keys;
    }

    public List<? extends Object> getValues() {
        return values;
    }

    public boolean isWaitForSync() {
        return waitForSync;
    }

    public VertexParameter withKey(String key) {
        this.key = key;
        return this;
    }

    public VertexParameter withValue(Object value) {
        this.value = value;
        return this;
    }

    public VertexParameter withKeys(List<String> keys) {
        this.keys = keys;
        return this;
    }

    public VertexParameter withValues(List<? extends Object> values) {
        this.values = values;
        return this;
    }

    public static class Builder {
        private String collection;

        private String key = null;

        private Object value = null;

        private List<String> keys = new ArrayList<>();

        private List<Object> values = new ArrayList<>();

        private boolean waitForSync = false;

        public Builder(String collection) {
            this.collection = collection;
        }

        public Builder key(String key) {
            this.key = key;
            return this;
        }

        public Builder value(Object value) {
            this.value = value;
            return this;
        }

        public Builder keys(List<String> keys) {
            this.keys = keys;
            return this;
        }

        public Builder values(List<Object> values) {
            this.values = values;
            return this;
        }

        public Builder waitForSync() {
            waitForSync = true;
            return this;
        }

        public VertexParameter build() {
            return new VertexParameter(this);
        }
    }
}
