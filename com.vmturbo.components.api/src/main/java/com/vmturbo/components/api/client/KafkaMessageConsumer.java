package com.vmturbo.components.api.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.StampedLock;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Kafka message consumer is a class to receive all the messages from Kafka. Received messages
 * will be routed to the child {@link IMessageReceiver} instances, added in
 * {@link #messageReceiver(String, Deserializer)} calls.
 */
public class KafkaMessageConsumer implements AutoCloseable {

    /**
     * Maximum size for a Protobuf message - 1 GB.
     */
    private static final int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20;

    private final Logger logger = LogManager.getLogger(getClass());
    /**
     * Kafka consumer. The object is not thread-safe, that's why we do require to have exclusive
     * access to the object in order to perform any operation. So, we use writeLock to access
     * Kafka consumer instance.
     */
    @GuardedBy("topicsLock")
    private final KafkaConsumer<String, byte[]> consumer;
    @GuardedBy("topicsLock")
    private final Map<String, KafkaMessageReceiver<?>> consumers = new HashMap<>();
    private final StampedLock topicsLock = new StampedLock();
    private final Thread pollingThread;

    public KafkaMessageConsumer(@Nonnull String bootstrapServer, @Nonnull String consumerGroup) {
        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("group.id", consumerGroup);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("session.timeout.ms", 90000);
        props.put("max.poll.records", 1);
        props.put("max.poll.interval.ms", 90000);
        consumer = new KafkaConsumer<>(props);
        pollingThread = new Thread(this::runPoll, "kafka-consumer");
        pollingThread.start();
    }

    /**
     * Creates message receiver for the specific topic. Kafka consumer will not subscribe to any
     * topics until this method is called.
     *
     * @param topic topic to subscribe to
     * @param deserializer function to deserialize the message from bytes
     * @param <T> type of messages to receive
     * @return message receiver implementation
     * @throws IllegalStateException if the topic has been already subscribed to
     */
    public <T> IMessageReceiver<T> messageReceiver(@Nonnull String topic,
            @Nonnull Deserializer<T> deserializer) {
        final KafkaMessageReceiver<T> receiver = new KafkaMessageReceiver<>(deserializer);
        final long lock = topicsLock.writeLock();
        try {
            logger.info("Subscribing to topic {}", topic);
            if (consumers.containsKey(topic)) {
                throw new IllegalStateException("Topic " + topic + " has been already added");
            }
            consumers.put(topic, receiver);
            final Set<String> newTopics = new HashSet<>();
            newTopics.addAll(consumers.keySet());
            newTopics.add(topic);
            consumer.subscribe(newTopics);
            logger.debug("Subscribed successfully");
            return receiver;
        } finally {
            topicsLock.unlockWrite(lock);
        }
    }

    /**
     * Method runs polling of the messages from Kafka brokers.
     */
    private void runPoll() {
        try {
            while (true) {
                if (!topicsRegistered()) {
                    // just sleep whenever no subscriptions open
                    Thread.sleep(100);
                    continue;
                }
                final long writeLock = topicsLock.writeLock();
                final ConsumerRecords<String, byte[]> records;
                try {
                    records = consumer.poll(100);
                } finally {
                    topicsLock.unlockWrite(writeLock);
                }
                for (ConsumerRecord<String, byte[]> record : records) {
                    logger.debug("Received message {} from topic {} ({})", record::offset,
                            record::topic, record::timestamp);
                    onNewMessage(record.value(),
                            new TopicPartition(record.topic(), record.partition()),
                            record.offset());
                }
            }
        } catch (Throwable t) {
            logger.warn("Error polling data", t);
        }
    }

    /**
     * Determines, whether there are any topics to request.
     *
     * @return whether there are topics requested to poll
     */
    private boolean topicsRegistered() {
        final long readLock = topicsLock.readLock();
        try {
            return !consumers.isEmpty();
        } finally {
            topicsLock.unlockRead(readLock);
        }
    }

    private void onNewMessage(@Nonnull byte[] message, @Nonnull TopicPartition topic, long offset) {
        final long lock = topicsLock.readLock();
        try {
            final KafkaMessageReceiver<?> receiver = consumers.get(topic.topic());
            receiver.onBytesAppear(message, topic, offset);
        } finally {
            topicsLock.unlockRead(lock);
        }
    }

    @Override
    public void close() {
        pollingThread.interrupt();
    }

    /**
     * Commit the specified offset for the specified topic and partition at Kafka broker.
     *
     * @param topic topic to commit
     * @param offset new offset to commic (should point to the next message to be received).
     */
    private void commitSync(@Nonnull TopicPartition topic, @Nonnull OffsetAndMetadata offset) {
        final long lock = topicsLock.writeLock();
        try {
            consumer.commitSync(Collections.singletonMap(topic, offset));
        } finally {
            topicsLock.unlockWrite(lock);
        }
    }

    /**
     * Message receiver implementation.
     *
     * @param <T> type of the message to receive.
     */
    private class KafkaMessageReceiver<T> implements IMessageReceiver<T> {

        @GuardedBy("lock")
        private final Set<BiConsumer<T, Runnable>> consumers = new HashSet<>();
        private final StampedLock lock = new StampedLock();
        private final Deserializer<T> deserializer;

        private KafkaMessageReceiver(@Nonnull Deserializer<T> deserializer) {
            this.deserializer = Objects.requireNonNull(deserializer);
        }

        @Override
        public void addListener(@Nonnull BiConsumer<T, Runnable> listener) {
            Objects.requireNonNull(listener);
            final long writeLock = lock.writeLock();
            try {
                consumers.add(listener);
            } finally {
                lock.unlockWrite(writeLock);
            }
        }

        private void onBytesAppear(@Nonnull byte[] buffer, @Nonnull TopicPartition topic,
                long offset) {
            try {
                // set the Protobuf message size limit to its max
                final CodedInputStream inputStream = CodedInputStream.newInstance(buffer);
                inputStream.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
                final T receivedMessage = deserializer.parseFrom(inputStream);
                logger.debug("Received message: {}[{}]",
                        () -> receivedMessage.getClass().getSimpleName(), () -> receivedMessage);
                final OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset + 1);
                final long readLock = lock.readLock();
                try {
                    for (BiConsumer<T, Runnable> listener : consumers) {
                        try {
                            listener.accept(receivedMessage, () -> KafkaMessageConsumer
                                    .this.commitSync(topic, offsetAndMetadata));
                        } catch (Throwable t) {
                            logger.error("Error processing message of type " +
                                    receivedMessage.getClass().getName() + " in listener " +
                                    listener.toString() + ":" + t);
                        }
                    }
                } finally {
                    lock.unlockRead(readLock);
                }
            } catch (InvalidProtocolBufferException e) {
                logger.error("Unable to deserialize raw data of " + buffer.length +
                        " bytes received from " + this.toString(), e);
            } catch (IOException e) {
                logger.error(toString() + ": Error operating with input byte message of " +
                        buffer.length, e);
            }
        }
    }
}
