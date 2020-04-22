package com.vmturbo.components.api;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;

/**
 * Base configuration for Kafka, holding kafka servers initial list as well as the namespace used
 * for prefix of all topics and consumer groups to support multi XL deployments on a single Kafka.
 */
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

    /**
     * Returns bootstrap servers list.
     * @return bootstrap servers list
     */
    @Bean
    protected String bootstrapServer() {
        return bootstrapServer;
    }

    /**
     * Construct the Kafka namespace prefix to be prepended to the topics and the consumer groups.
     *
     * @return the constructed namespace prefix
     */
    @Nonnull
    @Bean
    public String kafkaNamespacePrefix() {
        return StringUtils.isEmpty(namespace) ? StringUtils.EMPTY : namespace + PREFIX_DELIM;
    }

    protected static boolean useLocalBus() {
        // Controlled via system property, false by default. Should only be used in development/test.
        return Boolean.getBoolean("useLocalBus");
    }
}
