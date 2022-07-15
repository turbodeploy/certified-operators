package com.vmturbo.extractor.snowflakeconnect;

import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.extractor.snowflakeconnect.SnowflakeConnectManager.SnowflakeConnectManagerConfig;
import com.vmturbo.extractor.snowflakeconnect.client.SnowflakeConnectClient;
import com.vmturbo.extractor.snowflakeconnect.client.SnowflakeConnectClientConfig;
import com.vmturbo.extractor.snowflakeconnect.model.Config;

/**
 * Controls the automatic configuration of snowflake-connect for SaaS Reporting.
 */
@Configuration
public class SnowflakeConnectConfig {

    @Value("${snowflakeConnectHost:extractor-kafka-connect}")
    private String snowflakeConnectHost;

    @Value("${snowflakeConnectPort:8083}")
    private int snowflakeConnectPort;

    @Value("${snowflakeConnectRetryIntervalSeconds:30}")
    private int snowflakeConnectRetryIntervalSeconds;

    @Value("${snowflakeConnectorName:snowflake-kafka-connect}")
    private String snowflakeConnectorName;

    // Always use defaults
    @Value("${connectorClass:com.snowflake.kafka.connector.SnowflakeSinkConnector}")
    private String connectorClass;

    @Value("${valueConverter:com.snowflake.kafka.connector.records.SnowflakeJsonConverter}")
    private String valueConverter;

    @Value("${keyConverter:org.apache.kafka.connect.storage.StringConverter}")
    private String keyConverter;

    @Value("${snowflakeInputTable:TIMESERIES}")
    private String snowflakeInputTable;

    // Optionally configurable
    @Value("${topics:turbonomic.exporter}")
    private String topics;

    // Need to be configured
    @Value("${snowflakeUrlName:}")
    private String snowflakeUrlName;

    @Value("${snowflakeDatabaseName:}")
    private String snowflakeDatabaseName;

    @Value("${snowflakeSchemaName:}")
    private String snowflakeSchemaName;

    @Value("${snowflakePrivateKeyPassphrase:}")
    private String snowflakePrivateKeyPassphrase;

    @Value("${snowflakePrivateKey:}")
    private String snowflakePrivateKey;

    @Value("${snowflakeUserName:}")
    private String snowflakeUserName;

    /**
     * The client's configuration.
     *
     * @return the config object
     */
    @Bean
    public SnowflakeConnectClientConfig snowflakeConnectClientConfig() {
        return new SnowflakeConnectClientConfig()
                .setHost(snowflakeConnectHost)
                .setPort(snowflakeConnectPort);
    }

    /**
     * The client implementing the Kafka Connect API.
     *
     * @return the client
     */
    @Bean
    public SnowflakeConnectClient snowflakeConnectClient() {
        return new SnowflakeConnectClient(snowflakeConnectClientConfig());
    }

    /**
     * The bean that controls snowflake-connect initialization, only initialized for SaaS Reporting.
     *
     * @return the {@link SnowflakeConnectManager}
     */
    @Bean
    public SnowflakeConnectManager snowflakeConnectManager() {
        if (FeatureFlags.SAAS_REPORTING.isEnabled()) {

            Config connectorConfig = new Config();
            connectorConfig.setName(snowflakeConnectorName);
            connectorConfig.setConnectorClass(connectorClass);
            connectorConfig.setValueConverter(valueConverter);
            connectorConfig.setKeyConverter(keyConverter);
            connectorConfig.setTopics(topics);
            connectorConfig.setSnowflakeTopic2TableMap(
                    String.format("%s:%s", topics, snowflakeInputTable));
            connectorConfig.setSnowflakeUrlName(snowflakeUrlName);
            connectorConfig.setSnowflakeDatabaseName(snowflakeDatabaseName);
            connectorConfig.setSnowflakeSchemaName(snowflakeSchemaName);
            connectorConfig.setSnowflakeUserName(snowflakeUserName);
            connectorConfig.setSnowflakePrivateKey(snowflakePrivateKey);

            if (!snowflakePrivateKeyPassphrase.isEmpty()) {
                connectorConfig.setSnowflakePrivateKeyPassphrase(snowflakePrivateKeyPassphrase);
            }

            SnowflakeConnectManagerConfig config = new SnowflakeConnectManagerConfig()
                    .setRetryPeriodMs(
                            TimeUnit.SECONDS.toMillis(snowflakeConnectRetryIntervalSeconds))
                    .setConnectorName(snowflakeConnectorName)
                    .setConnectorConfig(connectorConfig);
            return new SnowflakeConnectManager(snowflakeConnectClient(), config);
        }
        return null;
    }
}
