package com.vmturbo.components.api;

import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.Executors;

import com.salesforce.kafka.test.KafkaTestUtils;
import com.salesforce.kafka.test.junit4.SharedKafkaTestResource;
import com.salesforce.kafka.test.listeners.SslListener;

import org.junit.Before;
import org.junit.ClassRule;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;

import com.vmturbo.components.api.client.KafkaMessageConsumer;
import com.vmturbo.components.api.security.KafkaTlsProperty;
import com.vmturbo.components.api.server.KafkaMessageProducer;

/**
 * TLS enabled Kafka integration instance.
 */
public class KafkaSenderReceiverTlsIT extends KafkaSenderReceiverIT {

    private static final String STORE_PASS = "test123";
    private static final String KAFKA_KEYSTORE_JKS = "kafka.keystore.jks";
    private static final String KAFKA_TRUSTSTORE_JKS = "kafka.truststore.jks";
    /**
     * We have a single mTLS enabled embedded kafka server that gets started when this test class
     * is
     * initialized. It's automatically started before any methods are run via the @ClassRule
     * annotation.
     * It's automatically stopped after all of the tests are completed via the @ClassRule
     * annotation.
     */
    @ClassRule
    public static final SharedKafkaTestResource sharedKafkaTestResource =
            new SharedKafkaTestResource()
                    // Start a cluster with 1 broker.
                    .withBrokers(1)
                    .withBrokerProperty("auto.create.topics.enable", "true")
                    .withBrokerProperty("offsets.topic.replication.factor", "1")
                    .withBrokerProperty("message.max.bytes", Integer.toString(1024 * 1024 * 10))
                    .registerListener(
                            new SslListener().withClientAuthRequested()
                                    .withKeyStoreLocation(getFilePath(KAFKA_KEYSTORE_JKS))
                                    .withKeyStorePassword(STORE_PASS)
                                    .withTrustStoreLocation(getFilePath(KAFKA_TRUSTSTORE_JKS))
                                    .withTrustStorePassword(STORE_PASS)
                                    .withKeyPassword(STORE_PASS));

    private static String getFilePath(final String filename) {
        final Resource resource = new DefaultResourceLoader().getResource(filename);
        try {
            return resource.getFile().toString();
        } catch (IOException e) {
            fail(e.getMessage());
        }
        return null;
    }

    private KafkaTlsProperty kafkaTlsProperty() {
        return new KafkaTlsProperty(true, getFilePath(KAFKA_KEYSTORE_JKS), STORE_PASS, STORE_PASS,
                getFilePath(KAFKA_TRUSTSTORE_JKS), STORE_PASS, "TLSv1.2");
    }

    /**
     * Init test.
     *
     * @throws Exception on errors occur.
     */
    @Before
    public void init() throws Exception {
        kafkaConsumer = new KafkaMessageConsumer(sharedKafkaTestResource.getKafkaConnectString(),
                "test-consumer-group", "", Optional.ofNullable(kafkaTlsProperty()));
        kafkaProducer = new KafkaMessageProducer(sharedKafkaTestResource.getKafkaConnectString(),
                "", Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE, Integer.MAX_VALUE,
                Integer.MAX_VALUE, Optional.ofNullable(kafkaTlsProperty()));
        threadPool = Executors.newCachedThreadPool();
    }

    /**
     * Simple accessor.
     *
     * @return Kafka test utils.
     */
    protected KafkaTestUtils getKafkaTestUtils() {
        return sharedKafkaTestResource.getKafkaTestUtils();
    }

    /**
     * SSL protocol.
     *
     * @return SSL protocol
     */
    protected String getExpectedListenerProtocol() {
        return "SSL";
    }

    /**
     * Get Kafka test resource.
     *
     * @return Kafka test resource
     */
    protected SharedKafkaTestResource getSharedKafkaTestResource() {
        return sharedKafkaTestResource;
    }
}
