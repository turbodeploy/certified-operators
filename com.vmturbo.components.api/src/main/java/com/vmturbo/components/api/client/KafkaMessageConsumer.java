package com.vmturbo.components.api.client;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
    private static final int POLL_AWAIT_TIME = 100;
    /**
     * Maximum amount of messages, that are buffered for each partition, while the other message
     * for the partition is being processed.
     */
    private static final int MAX_BUFFERED_MESSAGES = 1;

    private final Logger logger = LogManager.getLogger(getClass());
    /**
     * Kafka consumer. The object is not thread-safe, that's why we do require to have exclusive
     * access to the object in order to perform any operation. So, we use writeLock to access
     * Kafka consumer instance.
     */
    @GuardedBy("consumerLock")
    private final KafkaConsumer<String, byte[]> consumer;
    @GuardedBy("consumerLock")
    private final Map<String, KafkaMessageReceiver<?>> consumers = new HashMap<>();
    private final StampedLock consumerLock = new StampedLock();
    /**
     * Executor service to use.
     */
    private final ExecutorService threadPool;

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
        props.put("fetch.max.bytes", 67108864);
        props.put("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(props);
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("kconsumer-%d").build();
        threadPool = Executors.newCachedThreadPool(threadFactory);
        threadPool.submit(this::runPoll);
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
        final long lock = consumerLock.writeLock();
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
            consumerLock.unlockWrite(lock);
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
                    Thread.sleep(POLL_AWAIT_TIME);
                    continue;
                }
                final long writeLock = consumerLock.writeLock();
                final ConsumerRecords<String, byte[]> records;
                try {
                    logger.trace("polling for messages...");
                    records = consumer.poll(POLL_AWAIT_TIME);
                    logger.trace("polling finished (received {} messages)", records::count);
                } finally {
                    consumerLock.unlockWrite(writeLock);
                }
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        logger.debug("Received message {} from topic {} ({})", record::offset,
                                record::topic, record::timestamp);
                        onNewMessage(record.value(),
                                new TopicPartition(record.topic(), record.partition()),
                                record.offset());
                    }
                }
            }
        } catch (InterruptedException e) {
            logger.debug("Thread interrupted polling data from Kafka server");
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
        final long readLock = consumerLock.readLock();
        try {
            return !consumers.isEmpty();
        } finally {
            consumerLock.unlockRead(readLock);
        }
    }

    private void onNewMessage(@Nonnull byte[] message, @Nonnull TopicPartition partition,
            long offset) {
        final long lock = consumerLock.readLock();
        final KafkaMessageReceiver<?> receiver;
        try {
            receiver = consumers.get(partition.topic());
        } finally {
            consumerLock.unlockRead(lock);
        }
        if (receiver.hasMessage(partition, offset)) {
            logger.debug("Message {} from topic {} is already received by consumer. Skipping it",
                    offset, partition);
            return;
        }
        receiver.pushNextMessage(message, partition, offset);
    }

    private void resumePartition(@Nonnull TopicPartition topic) {
        final long lock = consumerLock.writeLock();
        try {
            consumer.resume(Collections.singleton(topic));
            logger.trace("Resumed topic {}", topic);
        } finally {
            consumerLock.unlockWrite(lock);
        }
    }

    private void pausePartition(@Nonnull TopicPartition topic) {
        final long lock = consumerLock.writeLock();
        try {
            consumer.pause(Collections.singleton(topic));
            logger.trace("Paused topic {}", topic);
        } finally {
            consumerLock.unlockWrite(lock);
        }
    }

    @Override
    public void close() {
        threadPool.shutdownNow();
    }

    /**
     * Commit the specified offset for the specified partition at Kafka broker.
     *
     * @param partition partition to commit
     * @param offset new offset to commic (should point to the next message to be received).
     */
    private void commitSync(@Nonnull TopicPartition partition, @Nonnull OffsetAndMetadata offset) {
        final long lock = consumerLock.writeLock();
        try {
            logger.trace("Committing partition {} with offset {}", partition, offset.offset());
            consumer.commitSync(Collections.singletonMap(partition, offset));
        } finally {
            consumerLock.unlockWrite(lock);
        }
    }

    /**
     * Message receiver implementation. This object is dedicated to one topic. It still can
     * process incoming messages from different partitions of this topic.
     *
     * @param <T> type of the message to receive.
     */
    private class KafkaMessageReceiver<T> implements IMessageReceiver<T> {

        @GuardedBy("lock")
        private final Set<BiConsumer<T, Runnable>> consumers = new HashSet<>();
        private final StampedLock lock = new StampedLock();
        private final Deserializer<T> deserializer;
        /**
         * Queue of the messages for this topic.
         */
        private final BlockingQueue<ReceivedMessage<T>> messagesQueue = new LinkedBlockingQueue<>();
        /**
         * Flag to understand, whether the queueing thread is already started or not. THe only
         * one available transition is false -> true.
         */
        private final AtomicBoolean queueStarted = new AtomicBoolean(false);
        /**
         * Map to store the last available offset of the topic-partition. As this object is
         * dedicated to a specific topic, so the map will container different partitions of the
         * topic. The value is the last message's offset, that has been received by the receiver.
         * Thus should be an concurrent map, as it is accessible from 2 different threads
         * (reading from {@link #runQueue()} and writing from
         * {@link #pushNextMessage(byte[], TopicPartition, long)}).
         *
         */
        private final ConcurrentMap<TopicPartition, Long> lastPartitionsOffset =
                new ConcurrentHashMap<>();

        /**
         * Map showing number of message, that are corrently in processing state for the specific
         * partition. This counter should increase as soon as message arrive at the queue. Counter
         * should be decreased as soon as message processing finishes. Acts really as a kind of a
         * semaphore to call {@link #pausePartition(TopicPartition)} and
         * {@link #resumePartition(TopicPartition)} accordingly.
         */
        private final ConcurrentMap<TopicPartition, AtomicLong> messagesInProcessing =
                new ConcurrentHashMap<>();

        private KafkaMessageReceiver(@Nonnull Deserializer<T> deserializer) {
            this.deserializer = Objects.requireNonNull(deserializer);
        }

        /**
         * Method puts message to a topic-specific queue for further processing of the message.
         * Method is not performing processing of the message, which will be done later in a
         * separate thread in {@link #runPoll()} method.
         *
         * @param buffer bytes buffer, containing message's bytes
         * @param partition topic message retrieved from
         * @param offset offset of this specific message
         */
        private void pushNextMessage(@Nonnull byte[] buffer, @Nonnull TopicPartition partition,
                long offset) {
            try {
                // set the Protobuf message size limit to its max
                final CodedInputStream inputStream = CodedInputStream.newInstance(buffer);
                inputStream.setSizeLimit(PROTOBUF_MESSAGE_MAX_LIMIT);
                final T receivedMessage = deserializer.parseFrom(inputStream);
                logger.debug("Received message: {}[{} bytes]",
                        receivedMessage.getClass().getSimpleName(), buffer.length);
                final ReceivedMessage<T> message =
                        new ReceivedMessage<>(receivedMessage, partition, offset);
                lastPartitionsOffset.put(partition, offset);
                final AtomicLong messagesInQueue =
                        messagesInProcessing.computeIfAbsent(partition, (v) -> new AtomicLong());
                if (messagesInQueue.incrementAndGet() > MAX_BUFFERED_MESSAGES) {
                    logger.trace("Pausing topic {} while at offset {}", partition, offset);
                    pausePartition(partition);
                }
                messagesQueue.add(message);
            } catch (InvalidProtocolBufferException e) {
                logger.error("Unable to deserialize raw data of " + buffer.length +
                        " bytes received from " + this.toString(), e);
            } catch (IOException e) {
                logger.error(toString() + ": Error operating with input byte message of " +
                        buffer.length, e);
            }
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
            // Start polling the queu as soon as we have the first listener appeared
            if (!queueStarted.getAndSet(true)) {
                threadPool.submit(this::runQueue);
            }
        }

        /**
         * Main worker method, that poll the queue, that is topic-specific. This method will call
         * listeners to process the messages.
         */
        private void runQueue() {
            try {
                while (true) {
                    final ReceivedMessage<T> message = messagesQueue.take();
                    final long readLock = lock.readLock();
                    try {
                        for (BiConsumer<T, Runnable> listener : consumers) {
                            try {
                                listener.accept(message.getMessage(), message::commit);
                            } catch (Throwable t) {
                                logger.error("Error processing message of type " +
                                        message.getMessage().getClass().getName() +
                                        " in listener " + listener.toString() + ":", t);
                            }
                        }
                    } finally {
                        lock.unlockRead(readLock);
                        final AtomicLong messagesInQueue =
                                messagesInProcessing.get(message.partition);
                        if (messagesInQueue.decrementAndGet() <= MAX_BUFFERED_MESSAGES) {
                            message.resumePartition();
                        }
                    }
                }
            } catch (InterruptedException e) {
                logger.debug("Thread interrupted while running a message queue", e);
            }
        }

        /**
         * Determines, whether the message with this offset has already been received by this
         * consumer.
         *
         * @param partition partition to test
         * @param offset offset to check
         * @return {@code true} if the message from this topic and partition with this offset has
         * already been received by this message receiver.
         */
        public boolean hasMessage(@Nonnull TopicPartition partition, long offset) {
            final Long lastOffset = lastPartitionsOffset.get(partition);
            return lastOffset != null && offset <= lastOffset;
        }
    }

    /**
     * An object to hold the incoming message data.
     *
     * @param <T> type of message to wrap
     */
    private class ReceivedMessage<T> {
        /**
         * Wrapped message itself
         */
        private final T message;
        /**
         * Topic+partition this message appeared from.
         */
        private final TopicPartition partition;
        /**
         * Offset of the message.
         */
        private final long offset;

        public ReceivedMessage(@Nonnull T message, @Nonnull TopicPartition partition, long offset) {
            this.message = Objects.requireNonNull(message);
            this.partition = Objects.requireNonNull(partition);
            this.offset = offset;
        }

        public T getMessage() {
            return message;
        }

        public void commit() {
            commitSync(partition, new OffsetAndMetadata(offset + 1));
        }

        public void resumePartition() {
            KafkaMessageConsumer.this.resumePartition(partition);
        }
    }
}
