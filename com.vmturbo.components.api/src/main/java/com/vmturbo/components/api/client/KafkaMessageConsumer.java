package com.vmturbo.components.api.client;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import com.google.common.collect.Collections2;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import io.prometheus.client.Counter;
import io.prometheus.client.Histogram;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;

/**
 * Kafka message consumer is a class to receive all the messages from Kafka. Received messages
 * will be routed to the child {@link IMessageReceiver} instances, added in
 * {@link #messageReceiver(String, Deserializer)} calls.
 */
public class KafkaMessageConsumer implements AutoCloseable, IMessageReceiverFactory {
    // OM-25600: Adding metrics to help understand producer and consumer behavior and configuration
    // needs as a result.
    static private Counter MESSAGES_RECEIVED_COUNT = Counter.build()
            .name("messages_received")
            .help("Number of messages received (per topic)")
            .labelNames("topic")
            .register();
    static private Counter MESSAGES_RECEIVED_BYTES = Counter.build()
            .name("messages_received_bytes")
            .help("Total size (in bytes) of all messages received")
            .labelNames("topic")
            .register();
    static private Histogram MESSAGES_RECEIVED_PROCESSING_MS = Histogram.build()
            .name("messages_received_processing_ms")
            .help("Total time (in ms) taken to process received messages")
            .labelNames("topic")
            .register();
    static private Histogram MESSAGES_RECEIVED_ENQUEUED_MS = Histogram.build()
            .name("messages_received_enqueued_ms")
            .help("Time (in ms) spent by messages waiting in the processing queue")
            .labelNames("topic")
            .register();
    static private Counter MESSAGES_RECEIVED_LATENCY_MS = Counter.build()
            .name("messages_received_latency_ms")
            .help("Time (in ms) between when a message was timestamped by kafka and when it was enqueued for processing in a consumer")
            .labelNames("topic")
            .register();

    /**
     * Maximum size for a Protobuf message - 1 GB.
     */
    private static final int PROTOBUF_MESSAGE_MAX_LIMIT = 1024 << 20;
    private static final int POLL_AWAIT_TIME = 100;
    private static final int POLL_INTERVAL_MS = 10; // sleep between polls to allow other threads to access the lock

    /**
     * Maximum amount of messages, that are buffered for each partition, while the other message
     * for the partition is being processed.
     */
    private static final int MAX_BUFFERED_MESSAGES = 5;

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
    private final Object consumerLock = new Object();
    /**
     * Executor service to use.
     */
    private final ExecutorService threadPool;

    /**
     * Namespace prefix for this consumer
     */
    private final String namespacePrefix;

    /**
     * map of topic -> settings.
     */
    @GuardedBy("consumerLock")
    private final Map<String, TopicSettings> topicSettingsMap = new HashMap<>();

    @GuardedBy("consumerLock")
    private boolean started = false;

    /**
     * Construct a {@link KafkaMessageConsumer} given the list of bootstrap servers, the consumer
     * group and the namespace prefix.
     *
     * @param bootstrapServer the list of bootstrap servers
     * @param consumerGroup the consumer group name
     */
    public KafkaMessageConsumer(@Nonnull String bootstrapServer, @Nonnull String consumerGroup) {
        this(bootstrapServer, consumerGroup, "");
    }

    /**
     * Construct a {@link KafkaMessageConsumer} given the list of bootstrap servers, the consumer
     * group and the namespace prefix.
     *
     * @param bootstrapServer the list of bootstrap servers
     * @param consumerGroup the consumer group name
     * @param namespacePrefix the namespace prefix to be added to the topics and consumer groups
     */
    public KafkaMessageConsumer(@Nonnull String bootstrapServer, @Nonnull String consumerGroup,
                                @Nonnull String namespacePrefix) {
        this.namespacePrefix = Objects.requireNonNull(namespacePrefix);
        final String namespacedConsumerGroup =
                namespacePrefix + Objects.requireNonNull(consumerGroup);

        final Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServer);
        props.put("group.id", namespacedConsumerGroup);
        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("session.timeout.ms", 90000);
        props.put("max.poll.records", 500);
        props.put("max.poll.interval.ms", 90000);
        props.put("fetch.max.bytes", 67108864);
        props.put("auto.offset.reset", "earliest");
        consumer = new KafkaConsumer<>(props);
        final ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("kconsumer-%d").build();
        threadPool = Executors.newCachedThreadPool(threadFactory);
    }

    public void start() {
        synchronized (consumerLock) {
            if (started) {
                throw new IllegalStateException("Kafka consumer is already started");
            }
            threadPool.submit(this::runPoll);
            for (KafkaMessageReceiver receiver : consumers.values()) {
                threadPool.submit(receiver::runQueue);
            }
            final Set<String> topics = consumers.keySet();
            logger.info("Subscribing to topics {}", topics);
            consumer.subscribe(topics, new RebalanceListener());
            logger.debug("Subscribed successfully");
            started = true;
        }
    }

    @Override
    public <T> IMessageReceiver<T> messageReceiver(@Nonnull String topic,
            @Nonnull Deserializer<T> deserializer) {
        final String namespacedTopic = namespacePrefix + topic;
        return createMessageReceiver(namespacedTopic, deserializer);
    }

    @Override
    public <T> IMessageReceiver<T> messageReceiver(@Nonnull Collection<String> topics,
                                                   @Nonnull Deserializer<T> deserializer) {
        final Collection<IMessageReceiver<T>> receivers = new ArrayList<>();
        synchronized (consumerLock) {
            for (String topic : topics) {
                final String namespaceTopic = namespacePrefix + topic;
                receivers.add(createMessageReceiver(namespaceTopic, deserializer));
            }
        }
        return new UmbrellaMessageReceiver<>(receivers);
    }

    @Override
    public <T> IMessageReceiver<T> messageReceiverWithSettings(@Nonnull TopicSettings topicSettings,
                                                   @Nonnull Deserializer<T> deserializer) {
        final String namespacedTopic = namespacePrefix + topicSettings.topic;
        final TopicSettings namespacedTopicSettings = new TopicSettings(namespacedTopic,
                topicSettings.startFrom);

        // store the settings in a map
        synchronized (consumerLock) {
            topicSettingsMap.put(namespacedTopicSettings.topic, namespacedTopicSettings);
            return messageReceiver(topicSettings.topic, deserializer);
        }
    }

    @Override
    public <T> IMessageReceiver<T> messageReceiversWithSettings(
            @Nonnull Collection<TopicSettings> topicSettings,
            @Nonnull Deserializer<T> deserializer) {
        synchronized (consumerLock) {
            topicSettings.stream()
                    .map(ts -> new TopicSettings(namespacePrefix + ts.topic, ts.startFrom))
                    .forEach(setting -> topicSettingsMap.put(setting.topic, setting));
            return messageReceiver(Collections2.transform(topicSettings, setting -> setting.topic),
                deserializer);
        }
    }

    /**
     * Internal method to create message receiver based on topic and deserializer.
     *
     * @param topic topic to subscribe to
     * @param deserializer function to deserialize the message from bytes
     * @param <T> type of messages to receive
     * @return message receiver implementation
     * @throws IllegalStateException if the topic has been already subscribed to, or
     *         consumer has been already started
     */
    private <T> IMessageReceiver<T> createMessageReceiver(@Nonnull String topic,
            @Nonnull Deserializer<T> deserializer) {
        final KafkaMessageReceiver<T> receiver = new KafkaMessageReceiver<>(deserializer);
        synchronized (consumerLock) {
            if (started) {
                throw new IllegalStateException("It is not allowed to add message receivers after" +
                        " consumer has been started");
            }
            logger.debug("Adding receiver for topic {}", topic);
            if (consumers.containsKey(topic)) {
                throw new IllegalStateException("Topic " + topic + " has been already added");
            }
            consumers.put(topic, receiver);
        }
        return receiver;
    }

    /**
     * Method runs polling of the messages from Kafka brokers.
     */
    private void runPoll() {
        try {
            while (true) {
                final ConsumerRecords<String, byte[]> records;
                synchronized (consumerLock) {
                    logger.trace("polling for messages...");
                    records = consumer.poll(POLL_AWAIT_TIME);
                    logger.trace("polling finished (received {} messages)", records::count);
                }
                if (!records.isEmpty()) {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        logger.debug("Received message {} (key: {}) from topic {} ({} bytes {} latency)",
                                record::offset, record::key, record::topic, record::serializedValueSize,
                                () -> (System.currentTimeMillis() - record.timestamp()));
                        onNewMessage(record);
                    }
                } else {
                    Thread.sleep(POLL_INTERVAL_MS);
                }
            }
        } catch (org.apache.kafka.common.errors.InterruptException e) {
            logger.debug("Thread interrupted polling data from Kafka server");
        } catch (Throwable t) {
            logger.warn("Error polling data", t);
        }
    }

    private void onNewMessage(ConsumerRecord<String, byte[]> record) {
        // time between now and the record timestamp represents transmission latency
        long latency = Instant.now().toEpochMilli() - record.timestamp();
        if ( latency < 0 ) {
            logger.warn("negative latency {} reported. message.timestamp is {}", latency, record.timestamp());
        }
        MESSAGES_RECEIVED_LATENCY_MS.labels(record.topic()).inc((double) Math.max(latency,0));
        // update the # received and total bytes received metrics
        MESSAGES_RECEIVED_COUNT.labels(record.topic()).inc();
        final byte[] payload = record.value();
        MESSAGES_RECEIVED_BYTES.labels(record.topic()).inc((double) payload.length);
        final TopicPartition partition = new TopicPartition(record.topic(), record.partition());
        final KafkaMessageReceiver<?> receiver = consumers.get(partition.topic());
        if (receiver.hasMessage(partition, record.offset())) {
            logger.debug("Message {} from topic {} is already received by consumer. Skipping it",
                    record.offset(), partition);
            return;
        }
        receiver.pushNextMessage(payload, partition, record.offset());
    }

    private void resumePartition(@Nonnull TopicPartition topic) {
        synchronized (consumerLock) {
            consumer.resume(Collections.singleton(topic));
            logger.trace("Resumed topic {}", topic);
        }
    }

    private void pausePartition(@Nonnull TopicPartition topic) {
        synchronized (consumerLock) {
            consumer.pause(Collections.singleton(topic));
            logger.trace("Paused topic {}", topic);
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
        synchronized (consumerLock) {
            logger.trace("Committing partition {} with offset {}", partition, offset.offset());
            consumer.commitSync(Collections.singletonMap(partition, offset));
        }
    }

    /**
     * Handles kafka rebalance events for the KafkaMessageConsumer
     */
    private class RebalanceListener implements ConsumerRebalanceListener {

        @Override
        public void onPartitionsRevoked(final Collection<TopicPartition> collection) {
            // noop at this time
        }

        /**
         * Called when partitions are assigned to our consumer.
         *
         * @param collection
         */
        @Override
        public void onPartitionsAssigned(final Collection<TopicPartition> collection) {
            // nothing to do if no topic settings to enforce
            if (topicSettingsMap.isEmpty()) return;

            // if we have received any new TopicPartitions, apply the initial read position here.
            // split the partitions into collections per-topic
            Map<String, List<TopicPartition>> partitionsByTopic = collection.stream()
                    .collect(Collectors.groupingBy(TopicPartition::topic));

            // iterate through our topic settings and apply the restart behavior to each topic
            // partition we haven't read yet, but have a customized setting for.
            for (TopicSettings topicSettings : topicSettingsMap.values()) {
                // if the topic setting wants to read from lastCommitted, this is the default
                // consumer behavior and we can move on.
                if (topicSettings.startFrom == StartFrom.LAST_COMMITTED) {
                    continue;
                }
                // otherwise, we will seek to beginning or end for any partitions we haven't seen yet.
                if (partitionsByTopic.containsKey(topicSettings.topic)) {
                    List<TopicPartition> partitions = partitionsByTopic.get(topicSettings.topic);

                    // use the receiver for this topic to filter out already-seen partitions
                    final KafkaMessageReceiver<?> receiver = consumers.get(topicSettings.topic);
                    List<TopicPartition> unreadPartitions = partitions.stream()
                            .filter(partition -> !receiver.hasSeenPartition(partition))
                            .collect(Collectors.toList());

                    // if no unseen partitions, there is nothing to do, so continue
                    if (unreadPartitions.isEmpty()) {
                        continue;
                    }

                    // apply the seek to the unread partitions
                    boolean fromBeginning = topicSettings.startFrom.equals(StartFrom.BEGINNING);
                    logger.debug("Kafka consumer reading {} partitions of topic {} from the {}.",
                            partitions.size(), topicSettings.topic,
                            fromBeginning ? "beginning" : "end");
                    synchronized (consumerLock) {
                        if (fromBeginning) {
                            consumer.seekToBeginning(unreadPartitions);
                        } else {
                            consumer.seekToEnd(unreadPartitions);
                        }
                    }
                }
            }
            logger.info("Done processing initial consumer topic settings.");
        }
    }

    /**
     * Message receiver implementation. This object is dedicated to one topic. It still can
     * process incoming messages from different partitions of this topic.
     *
     * @param <T> type of the message to receive.
     */
    private class KafkaMessageReceiver<T> implements IMessageReceiver<T> {

        private final Set<BiConsumer<T, Runnable>> consumers = new HashSet<>();
        private final Deserializer<T> deserializer;
        /**
         * Queue of the messages for this topic.
         */
        private final BlockingQueue<ReceivedMessage<T>> messagesQueue = new LinkedBlockingQueue<>();
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
                logger.debug("Received message: {} [{} bytes]",
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
            synchronized(consumerLock) {
                if (started) {
                    throw new IllegalStateException("Could not add listener to running consumer");
                }
                consumers.add(listener);
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
                    // update time enqueued metric
                    long processingStartTime = System.currentTimeMillis();
                    long enqueuedTime = processingStartTime - message.getEnqueueTime();
                    MESSAGES_RECEIVED_ENQUEUED_MS.labels(message.partition.topic()).observe((double) enqueuedTime);
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
                        final AtomicLong messagesInQueue =
                                messagesInProcessing.get(message.partition);
                        if (messagesInQueue.decrementAndGet() <= MAX_BUFFERED_MESSAGES) {
                            message.resumePartition();
                        }
                        long processingTime = System.currentTimeMillis() - processingStartTime;
                        logger.debug("Processing message from partition {} offset {} took {} ms", message.partition, message.offset, processingTime);
                        MESSAGES_RECEIVED_PROCESSING_MS.labels(message.partition.topic()).observe((double) processingTime);
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

        /**
         * Determine if a partition has already been seen by this reciever or not.
         *
         * @param partition partition to check
         * @return true, if the reciever remembers an offset for this partition or not.
         */
        public boolean hasSeenPartition(@Nonnull TopicPartition partition) {
            return lastPartitionsOffset.containsKey(partition);
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

        /**
         * Time the message was enqueued
         */
        private final long enqueueTime;

        public ReceivedMessage(@Nonnull T message, @Nonnull TopicPartition partition, long offset) {
            this.message = Objects.requireNonNull(message);
            this.partition = Objects.requireNonNull(partition);
            this.offset = offset;
            this.enqueueTime = System.currentTimeMillis();
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

        public long getEnqueueTime() { return enqueueTime; }
    }

    /**
     * Contains options for customizing consumer behavior for a specific topic. Currently the only
     * configurable option is <code>startFrom</code>, which controls where in the partition a
     * consumer will start reading from.
     */
    public static class TopicSettings {
        /**
         * Options for where to start reading a topic partition from.
         * <ul><li><code>LAST_COMMITTED</code>: The consumer will read from the last committed offset.
         * This is the default behavior.</li>
         * <li><code>BEGINNING</code>: The consumer will read from the earliest available offset in
         * the partition. Note that this implies that previously seen messages will be seen again, so
         * be certain your consumer is prepared to handle these.</li>
         * <li><code>END</code>: The consumer will read from the end of the partition, meaning only
         * new messages published after the partition was assigned will be seen by the consumer. Use
         * this setting if you don't care about catching up on missed messages.</li></ul>
         */
        public enum StartFrom {
            LAST_COMMITTED, // start reading from where we left off last time -- this is the default behavior
            BEGINNING, // always start from the beginning
            END // always start from the end
        }

        /**
         * The name of the topic the settings apply to.
         */
        public final String topic;

        /**
         * Controls where the consumer will start reading new partitions on the topic from. See
         * {@link StartFrom} for more details.
         */
        public final StartFrom startFrom;

        public TopicSettings(String topic, StartFrom startFrom) {
            this.topic = topic;
            this.startFrom = startFrom;
        }

    }
}
