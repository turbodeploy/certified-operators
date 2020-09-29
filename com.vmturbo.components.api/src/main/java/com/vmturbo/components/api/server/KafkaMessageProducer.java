package com.vmturbo.components.api.server;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.ConfigurableBackoffStrategy;
import com.vmturbo.components.api.RetriableOperation.ConfigurableBackoffStrategy.BackoffType;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.components.api.tracing.TracingKafkaProducerInterceptor;

/**
 * Creates kafka-based message senders.
 */
public class KafkaMessageProducer implements AutoCloseable, IMessageSenderFactory {

    /**
     * Interval between retrials to send the message.
     */
    private static final long RETRY_INTERVAL_MS = 1000;

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
    private final int maxRequestSizeBytes;
    private final int recommendedRequestSizeBytes;
    private final int totalSendRetrySecs;

    // boolean tracking if the last send was successful or not. Used as a simple health check.
    private AtomicBoolean lastSendAttemptFailed = new AtomicBoolean(false);

    /**
     * Construct a {@link KafkaMessageProducer} given the list of bootstrap servers and the
     * namespace prefix.
     *
     * @param bootstrapServer the list of Kafka bootstrap servers
     * @param namespacePrefix the namespace prefix to be added to the topics
     * @param maxRequestSizeBytes Maximum size of message that can be sent with this producer, in bytes.
     * @param recommendedRequestSizeBytes Recommended size for messages sent with this producer, in bytes.
     * @param maxBlockMs The maxmimum amount of time to block while synchronizing topic metadata before a send.
     * @param deliveryTimeoutMs The total length of time given to kafka to complete sending a message.
     *                          This includes retry attempts.
     * @param totalSendRetrySecs The total amount of time allowed for sending a single message, including
     *                       kafka retries. If the kafka send fails with a RetriableException but less
     *                       than "total retry secs" have elapsed, we will trigger a manual re-send
     *                       attempt. Note that this setting is optional -- we could have just as
     *                       easily set deliveryTimeoutMs to the same duration and have all retries
     *                       managed by kafka. The difference is, we would have no insight into
     *                       any delivery retry problems until deliveryTimeoutMs have elapsed. So, if
     *                       we want some delivery error exceptions but also want to provide retries,
     *                       we can set deliveryTimeoutMs < totalRetrySecs to get callbacks after
     *                       each deliveryTimeoutMs expiration. If you set the opposite case,
     *                       deliveryTimeout >= totalRetrySecs, then all retries are silently handled
     *                       by the kafka producer internally, and we will only see one exception if
     *                       the total delivery time elapses.
     */
    public KafkaMessageProducer(@Nonnull String bootstrapServer,
                                @Nonnull String namespacePrefix,
                                final int maxRequestSizeBytes,
                                final int recommendedRequestSizeBytes,
                                final int maxBlockMs,
                                final int deliveryTimeoutMs,
                                final int totalSendRetrySecs) {
        this.namespacePrefix = Objects.requireNonNull(namespacePrefix);
        this.maxRequestSizeBytes = maxRequestSizeBytes;
        this.recommendedRequestSizeBytes = recommendedRequestSizeBytes;
        this.totalSendRetrySecs = totalSendRetrySecs;

        // set the default properties
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("acks", "all");
        props.put("batch.size", 16384); // in bytes
        props.put("enable.idempotence", true); // when idempotence is set to true, kafka producer will guarantee
        // that duplicate messages will not be sent, even in retry situations.
        props.put("delivery.timeout.ms", deliveryTimeoutMs);
        props.put("retry.backoff.ms", RETRY_INTERVAL_MS);
        props.put("max.block.ms", maxBlockMs);
        props.put("linger.ms", 1);
        props.put("max.request.size", maxRequestSizeBytes);
        props.put("buffer.memory", maxRequestSizeBytes);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, TracingKafkaProducerInterceptor.class.getName());

        producer = new KafkaProducer<>(props);
        // if we are using a total retry window greater than the kafka retry window, log it, since it
        // means we may expect to see some manual retries occurring.
        if ((totalSendRetrySecs * 1000) > deliveryTimeoutMs) {
            logger.info("Kafka Producer configured with manual retry window of {} secs.", totalSendRetrySecs);
        }
    }

    @Override
    public boolean lastSendAttemptFailed() {
        return lastSendAttemptFailed.get();
    }

    /**
     * The default message key generator will return a message key that is an incrementing number
     * for this producer. Other message key generators might select a key based on properties of
     * the message, or generated as a random value, etc.
     *
     * @param message The {@link AbstractMessage} to generate a message key for
     * @return a string message key to use for the message
     */
    protected String defaultMessageKeyGenerator(@Nonnull final AbstractMessage message) {
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
        Instant startTime = Instant.now();
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
                    double sentTimeMs = Duration.between(startTime, Instant.now()).toMillis();
                    logger.debug("Message send of {} bytes took {} ms", payload.length, sentTimeMs);
                    if (sentTimeMs < 0) {
                        // safeguard against negative durations, which will generate an exception.
                        logger.warn("Nonsensical message send time of {} ms was observed.", sentTimeMs);
                    } else {
                        MESSAGES_SENT_MS.labels(topic).inc(sentTimeMs);
                    }
                    // update failure count if there was an exception
                    if (exception != null) {
                        logger.warn("Error while sending kafka message", exception);
                        lastSendAttemptFailed.set(true);
                        MESSAGE_SEND_ERRORS_COUNT.labels(topic).inc();
                    } else {
                        lastSendAttemptFailed.set(false);
                    }
                });
    }

    @Override
    public void close() {
        producer.close();
    }

    @Override
    public <S extends AbstractMessage> IMessageSender<S> messageSender(@Nonnull String topic) {
        final String namespacedTopic = namespacePrefix + topic;
        return new BusMessageSender<>(namespacedTopic);
    }

    @Override
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

        // we're only going to build this once.
        private final ConfigurableBackoffStrategy backoffStrategy = ConfigurableBackoffStrategy.newBuilder()
                .withBackoffType(BackoffType.FIXED)
                .withBaseDelay(RETRY_INTERVAL_MS)
                .build();

        private BusMessageSender(@Nonnull String topic) {
            this.topic = Objects.requireNonNull(topic);
            this.keyGenerator = null;
        }

        private BusMessageSender(@Nonnull String topic, @Nonnull Function<S, String> keyGenerator) {
            this.topic = Objects.requireNonNull(topic);
            this.keyGenerator = Objects.requireNonNull(keyGenerator);
        }

        @Override
        public void sendMessage(@Nonnull S serverMsg)
                throws CommunicationException {
            String messageKey = keyGenerator != null ? keyGenerator.apply(serverMsg)
                    : defaultMessageKeyGenerator(serverMsg);
            try {
                RetriableOperation.newOperation(() -> {
                    try {
                        return sendKafkaMessage(serverMsg, topic, messageKey).get();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException ee) {
                        // we expect this to be treated as a retry in RetriableOperation.run()
                        throw new RetriableOperation.RetriableOperationFailedException(ee.getCause());
                    }
                    return null;
                })
                .backoffStrategy(backoffStrategy)
                .retryOnException(e -> e.getCause() instanceof RetriableException)
                .run(totalSendRetrySecs, TimeUnit.SECONDS);

            } catch (RetriableOperationFailedException rofe) {
                Throwable e = rofe.getCause();
                logger.error("BusMessageSender.sendMessage() caught exception", e);
                throw new CommunicationException("Unexpected exception sending message " +
                        serverMsg.getClass().getSimpleName(), e);
            } catch (TimeoutException te) {
                logger.error("BusMessageSender timed out while sending message {}.");
            } catch (InterruptedException ie) {
                logger.warn("BusMessageSender.sendMessage() thread interrupted before it could complete.");
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public int getMaxRequestSizeBytes() {
            return maxRequestSizeBytes;
        }

        @Override
        public int getRecommendedRequestSizeBytes() {
            return recommendedRequestSizeBytes;
        }
    }
}
