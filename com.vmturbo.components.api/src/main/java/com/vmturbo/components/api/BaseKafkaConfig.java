package com.vmturbo.components.api;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;

import com.vmturbo.components.api.security.KafkaTlsProperty;
import com.vmturbo.components.api.security.TlsConfig;

/**
 * Base configuration for Kafka, holding kafka servers initial list as well as the namespace used
 * for prefix of all topics and consumer groups to support multi XL deployments on a single Kafka.
 */
@Import(TlsConfig.class)
public class BaseKafkaConfig  {
    private static final String PREFIX_DELIM = ".";

    /**
     * Tenant namespace; default to an empty string.
     */
    @Value("${kafkaNamespace:}")
    private String namespace;

    /**
     * Bootstrap server - a list of kafka servers to try to connect to. All the server in the
     * cluster should be specified here.
     */
    @Value("${kafkaServers:none}")
    private String bootstrapServer;

    @Value("${KAFKA_TLS_ENABLED:false}")
    private Boolean tlsEnabled;

    @Value("${KAFKA_TLS_ENABLED_PROTOCOLS:TLSv1.2}")
    private String tlsEnabledProtocols;

    /**
     * TLS configuration.
     *
     * @return TLS configuration.
     */
    @Bean
    public TlsConfig tlsConfig() {
        return new TlsConfig();
    }

    /**
     * Returns bootstrap servers list.
     * @return bootstrap servers list
     */
    @Bean
    protected String bootstrapServer() {
        return bootstrapServer;
    }

    @Bean
    protected KafkaTlsProperty kafkaTlsProperty() {
        return new KafkaTlsProperty(tlsEnabled, tlsConfig().tlsKeyStore(),
                tlsConfig().tlsKeystorePass(), tlsConfig().tlsKeyPass(),
                tlsConfig().tlsTrustStore(), tlsConfig().tlsTrustStorePass(), tlsEnabledProtocols);
    }

    /**
     * Construct the Kafka namespace prefix to be prepended to the topics and the consumer groups.
     *
     * @return the constructed namespace prefix
     */
    @Nonnull
    @Bean
    public String kafkaNamespacePrefix() {
        return createKafkaTopicPrefix(namespace);
    }

    /**
     * Given a namespace value, create the kafka topic prefix for it. Will be blank if no namespace
     * was specified.
     * @param namespace the namespace to crate a topic prefix based on.
     * @return the topic prefix to use (blank if no namespace).
     */
    public static String createKafkaTopicPrefix(String namespace) {
        return StringUtils.isEmpty(namespace) ? StringUtils.EMPTY : namespace + PREFIX_DELIM;
    }

    protected static boolean useLocalBus() {
        // Controlled via system property, false by default. Should only be used in development/test.
        return Boolean.getBoolean("useLocalBus");
    }
}
