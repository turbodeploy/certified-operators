package com.vmturbo.extractor.snowflakeconnect.model;

import com.google.gson.annotations.SerializedName;

/**
 * Models the connector entity.
 */
public class Connector extends Base {
    @SerializedName("config")
    private Config config;

    public void setConfig(Config config) {
        this.config = config;
    }

    public Config getConfig() {
        return config;
    }

    @Override
    public String toString() {
        return "Connector{" + "name=" + this.getName() + ", config=" + config + '}';
    }
}
