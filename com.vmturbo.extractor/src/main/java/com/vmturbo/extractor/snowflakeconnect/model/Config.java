package com.vmturbo.extractor.snowflakeconnect.model;

import java.util.Objects;

import com.google.gson.annotations.SerializedName;

/**
 * Models the config info for a connector.
 */
public class Config extends Base {

    @SerializedName("connector.class")
    private String connectorClass;

    @SerializedName("topics")
    private String topics;

    @SerializedName("value.converter")
    private String valueConverter;

    @SerializedName("key.converter")
    private String keyConverter;

    @SerializedName("snowflake.database.name")
    private String snowflakeDatabaseName;

    @SerializedName("snowflake.schema.name")
    private String snowflakeSchemaName;

    @SerializedName("snowflake.topic2table.map")
    private String snowflakeTopic2TableMap;

    @SerializedName("snowflake.private.key.passphrase")
    private String snowflakePrivateKeyPassphrase;

    @SerializedName("snowflake.private.key")
    private String snowflakePrivateKey;

    @SerializedName("snowflake.user.name")
    private String snowflakeUserName;

    @SerializedName("snowflake.url.name")
    private String snowflakeUrlName;

    public String getConnectorClass() {
        return connectorClass;
    }

    public void setConnectorClass(String connectorClass) {
        this.connectorClass = connectorClass;
    }

    public String getTopics() {
        return topics;
    }

    public void setTopics(String topics) {
        this.topics = topics;
    }

    public String getValueConverter() {
        return valueConverter;
    }

    public void setValueConverter(String valueConverter) {
        this.valueConverter = valueConverter;
    }

    public String getKeyConverter() {
        return keyConverter;
    }

    public void setKeyConverter(String keyConverter) {
        this.keyConverter = keyConverter;
    }

    public String getSnowflakeDatabaseName() {
        return snowflakeDatabaseName;
    }

    public void setSnowflakeDatabaseName(String snowflakeDatabaseName) {
        this.snowflakeDatabaseName = snowflakeDatabaseName;
    }

    public String getSnowflakeSchemaName() {
        return snowflakeSchemaName;
    }

    public void setSnowflakeSchemaName(String snowflakeSchemaName) {
        this.snowflakeSchemaName = snowflakeSchemaName;
    }

    public String getSnowflakeTopic2TableMap() {
        return snowflakeTopic2TableMap;
    }

    public void setSnowflakeTopic2TableMap(String snowflakeTopic2TableMap) {
        this.snowflakeTopic2TableMap = snowflakeTopic2TableMap;
    }

    public void setSnowflakePrivateKeyPassphrase(String snowflakePrivateKeyPassphrase) {
        this.snowflakePrivateKeyPassphrase = snowflakePrivateKeyPassphrase;
    }

    public void setSnowflakePrivateKey(String snowflakePrivateKey) {
        this.snowflakePrivateKey = snowflakePrivateKey;
    }

    public String getSnowflakeUserName() {
        return snowflakeUserName;
    }

    public void setSnowflakeUserName(String snowflakeUserName) {
        this.snowflakeUserName = snowflakeUserName;
    }

    public String getSnowflakeUrlName() {
        return snowflakeUrlName;
    }

    public void setSnowflakeUrlName(String snowflakeUrlName) {
        this.snowflakeUrlName = snowflakeUrlName;
    }

    @Override
    public String toString() {
        return "Config{" + "connectorClass='" + connectorClass + '\'' + ", topics='" + topics + '\''
                + ", valueConverter='" + valueConverter + '\'' + ", keyConverter='" + keyConverter
                + '\'' + ", snowflakeDatabaseName='" + snowflakeDatabaseName + '\''
                + ", snowflakeSchemaName='" + snowflakeSchemaName + '\''
                + ", snowflakeTopic2TableMap='" + snowflakeTopic2TableMap + '\''
                + ", snowflakeUserName='" + snowflakeUserName + '\'' + ", snowflakeUrlName='"
                + snowflakeUrlName + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Config)) {
            return false;
        }
        Config config = (Config)o;
        return getName().equals(config.getName()) && getConnectorClass().equals(
                config.getConnectorClass()) && getTopics().equals(config.getTopics())
                && getValueConverter().equals(config.getValueConverter())
                && getKeyConverter().equals(config.getKeyConverter())
                && getSnowflakeDatabaseName().equals(config.getSnowflakeDatabaseName())
                && getSnowflakeSchemaName().equals(config.getSnowflakeSchemaName())
                && getSnowflakeTopic2TableMap().equals(config.getSnowflakeTopic2TableMap())
                && Objects.equals(snowflakePrivateKeyPassphrase,
                config.snowflakePrivateKeyPassphrase) && snowflakePrivateKey.equals(
                config.snowflakePrivateKey) && getSnowflakeUserName().equals(
                config.getSnowflakeUserName()) && getSnowflakeUrlName().equals(
                config.getSnowflakeUrlName());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getConnectorClass(), getTopics(), getValueConverter(),
                getKeyConverter(), getSnowflakeDatabaseName(), getSnowflakeSchemaName(),
                getSnowflakeTopic2TableMap(), snowflakePrivateKeyPassphrase, snowflakePrivateKey,
                getSnowflakeUserName(), getSnowflakeUrlName());
    }
}
