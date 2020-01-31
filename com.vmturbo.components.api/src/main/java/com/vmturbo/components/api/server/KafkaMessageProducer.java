package com.vmturbo.components.api.server;

import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.AbstractMessage;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import com.vmturbo.communication.CommunicationException;

public class KafkaMessageProducer implements AutoCloseable {

    /**
     * Interval between retrials to send the message.
     */
    private static final long RETRY_INTERVAL_MS = 1000;
    /**
     * Number of retries to perform on failures.
     */
    private static final int MAX_RETRIES = 30;
    // OM-25600: Adding metrics to help understand producer and consumer behavior and configuration
    // needs as a result.
    static private Counter MESSAGES_SENT_COUNT = Counter.build()
            .name("messages_sent")
            .help("Number of messages produced (per topic)")
            .labelNames("topic")
            .register();
    static private Counter MESSAGES_SENT_BYTES = Counter.build()
            .name("messages_sent_bytes")
            .help("Total size (in bytes) of all messages sent")
            .labelNames("topic")
            .register();
    static private Counter MESSAGES_SENT_MS = Counter.build()
            .name("messages_sent_ms")
            .help("Total time (in ms) taken to send all messages to kafka")
            .labelNames("topic")
            .register();
    static private Gauge LARGEST_MESSAGE_SENT = Gauge.build()
            .name("messages_sent_largest_bytes")
            .help("Size (bytes) of largest message sent")
            .labelNames("topic")
            .register();
    static private Counter MESSAGE_SEND_ERRORS_COUNT = Counter.build()
            .name("message_send_errors")
            .help("Number of message sends that resulted in errors (per topic)")
            .labelNames("topic")
            .register();

    private final KafkaProducer<String, byte[]> producer;
    private final Logger logger = LogManager.getLogger(getClass());
    private final AtomicLong msgCounter = new AtomicLong(0);
    private final String namespacePrefix;

    // boolean tracking if the last send was successful or not. Used as a simple health check.
    private AtomicBoolean lastSendFailed = new AtomicBoolean(false);

    /**
     * Construct a {@link KafkaMessageProducer} given the list of bootstrap servers and the
     * namespace prefix.
     *
     * @param bootstrapServer the list of Kafka bootstrap servers
     */
    public KafkaMessageProducer(@Nonnull String bootstrapServer) {
        this(bootstrapServer, "");
    }

    /**
     * Construct a {@link KafkaMessageProducer} given the list of bootstrap servers and the
     * namespace prefix.
     *
     * @param bootstrapServer the list of Kafka bootstrap servers
     * @param namespacePrefix the namespace prefix to be added to the topics
     */
    public KafkaMessageProducer(@Nonnull String bootstrapServer, @Nonnull String namespacePrefix) {
        this.namespacePrefix = Objects.requireNonNull(namespacePrefix);

        // set the default properties
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("acks", "all");
        props.put("retries", MAX_RETRIES);
        props.put("retry.backoff.ms", RETRY_INTERVAL_MS);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        // pjs: set a max request size based on standard max protobuf serializabe size for now
        props.put("max.request.size", 67108864); // 64mb
        props.put("buffer.memory", 67108864);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    /**
     * Did the last send attempt fail?
     *
     * @return true if the last send attempt resulted in an exception
     */
    public boolean lastSendFailed() {
        return lastSendFailed.get();
    }

    /**
     * The default message key generator will return a message key that is an incrementing number
     * for this producer. Other message key generators might select a key based on properties of
     * the message, or generated as a random value, etc.
     *
     * @param message The {@link AbstractMessage} to generate a message key for
     * @return a string message key to use for the message
     */
    private String defaultMessageKeyGenerator(@Nonnull final AbstractMessage message) {
        return Long.toString(msgCounter.incrementAndGet());
    }

    /**
     * Send a kafka message without a specific key. A key wll be automatically generated using the
     * default key generator.
     *
     * @param serverMsg the {@link AbstractMessage} message to send.
     * @param topic the topic to send it to
     * @return a Future expecting RecordMetadata confirming the send from the broker.
     */
    private Future<RecordMetadata> sendKafkaMessage(@Nonnull final AbstractMessage serverMsg,
                                                    @Nonnull final String topic) {
        return sendKafkaMessage(serverMsg, topic, defaultMessageKeyGenerator(serverMsg));
    }

    private Future<RecordMetadata> sendKafkaMessage(@Nonnull final AbstractMessage serverMsg,
            @Nonnull final String topic, @Nonnull final String key) {
        Objects.requireNonNull(serverMsg);
        long startTime = System.currentTimeMillis();
        byte[] payload = serverMsg.toByteArray();
        logger.debug("Sending message {}[{} bytes] to topic {} with key {}",
                serverMsg.getClass().getSimpleName(), payload.length, topic, key);
        MESSAGES_SENT_COUNT.labels(topic).inc();
        MESSAGES_SENT_BYTES.labels(topic).inc((double) payload.length);
        if (payload.length > LARGEST_MESSAGE_SENT.labels(topic).get()) {
            LARGEST_MESSAGE_SENT.labels(topic).set(payload.length);
        }
        return producer.send(
                new ProducerRecord<>(topic, key, payload),
                (metadata, exception) -> {
                    // update sent time
                    long sentTimeMs = System.currentTimeMillis() - startTime;
                    MESSAGES_SENT_MS.labels(topic).inc((double) sentTimeMs);
                    logger.debug("Message send of {} bytes took {} ms", payload.length, sentTimeMs);
                    // update failure count if there was an exception
                    if (exception != null) {
                        logger.warn("Error while sending kafka message", exception);
                        lastSendFailed.set(true);
                        MESSAGE_SEND_ERRORS_COUNT.labels(topic).inc();
                    } else {
                        lastSendFailed.set(false);
                    }
                });
    }

    @Override
    public void close() {
        producer.close();
    }

    /**
     * Creates message sender for the specific topic.
     *
     * @param topic topic to send messages to
     * @return message sender.
     */
    public <S extends AbstractMessage> IMessageSender<S> messageSender(@Nonnull String topic) {
        final String namespacedTopic = namespacePrefix + topic;
        return new BusMessageSender<>(namespacedTopic);
    }

    /**
     * Creates a message sender for the specific topic, with a specific key generator function.
     *
     * The key generator function should accept a message of the chosen type and return a string
     * to use as the message key when sending.
     *
     * @param topic The topic to send messages to
     * @param keyGenerator The key generation function to use
     * @param <S> The Type of the messages to send on this topic
     * @return a message sender configured for the topic and key generator
     */
    public <S extends AbstractMessage> IMessageSender<S> messageSender(@Nonnull String topic,
                                           @Nonnull Function<S, String> keyGenerator) {
        final String namespacedTopic = namespacePrefix + topic;
        return new BusMessageSender<>(namespacedTopic, keyGenerator);
    }

    /**
     * Real implementation of the sender.
     */
    private class BusMessageSender<S extends AbstractMessage> implements IMessageSender<S> {
        private final String topic;
        private final Function<S, String> keyGenerator;

        private BusMessageSender(@Nonnull String topic) {
            this.topic = Objects.requireNonNull(topic);
            keyGenerator = null;
        }

        private BusMessageSender(@Nonnull String topic, @Nonnull Function<S, String> keyGenerator) {
            this.topic = Objects.requireNonNull(topic);
            this.keyGenerator = Objects.requireNonNull(keyGenerator);
        }

        @Override
        public void sendMessage(@Nonnull S serverMsg)
                throws CommunicationException, InterruptedException {
            try {
                if (keyGenerator != null) {
                    sendKafkaMessage(serverMsg, topic, keyGenerator.apply(serverMsg));
                } else {
                    sendKafkaMessage(serverMsg, topic).get();
                }
            } catch (ExecutionException e) {
                throw new CommunicationException("Unexpected exception sending message " +
                        serverMsg.getClass().getSimpleName(), e);
            }
        }
    }
}
