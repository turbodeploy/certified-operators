package com.vmturbo.components.api.localbus;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.AbstractMessage;

import io.opentracing.SpanContext;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.Deserializer;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.IMessageReceiverFactory;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;
import com.vmturbo.components.api.client.TriConsumer;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.server.IMessageSenderFactory;
import com.vmturbo.components.api.tracing.Tracing;

/**
 * A local message bus, which can be used for integration testing to avoid Kafka (or other
 * message bus) dependencies.
 */
@ThreadSafe
public class LocalBus implements IMessageSenderFactory, IMessageReceiverFactory {

    private final Map<String, LocalBusTopic<?>> topicsMap =
        Collections.synchronizedMap(new HashMap<>());

    private static final LocalBus INSTANCE = new LocalBus();

    /**
     * Threadpool for consumer listeners.
     */
    private final ExecutorService executorService;

    private LocalBus() {
        this(Tracing.traceAwareExecutor(Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setNameFormat("busconsumer-%d").build())));
    }

    LocalBus(@Nonnull final ExecutorService executorService) {
        this.executorService = executorService;
    }

    /**
     * Get the reference to the global {@link LocalBus} instance.
     * We use a singleton outside of Spring, because depending on the test setup it may need to
     * be shared between multiple Spring contexts.
     *
     * @return The {@link LocalBus}.
     */
    public static LocalBus getInstance() {
        return INSTANCE;
    }

    @Override
    public <T> IMessageReceiver<T> messageReceiverWithSettings(@Nonnull final TopicSettings topicSettings, @Nonnull final Deserializer<T> deserializer) {
        return messageReceiversWithSettings(Collections.singletonList(topicSettings), deserializer);
    }

    @Override
    public <T> IMessageReceiver<T> messageReceiversWithSettings(@Nonnull final Collection<TopicSettings> topicSettings, @Nonnull final Deserializer<T> deserializer) {
        return messageReceiver(topicSettings.stream()
            .map(t -> t.topic)
            .collect(Collectors.toList()), deserializer);
    }

    @Override
    public <T> IMessageReceiver<T> messageReceiver(@Nonnull final String topics, @Nonnull final Deserializer<T> deserializer) {
        return messageReceiver(Collections.singleton(topics), deserializer);
    }

    @Override
    public <T> IMessageReceiver<T> messageReceiver(@Nonnull final Collection<String> topics, @Nonnull final Deserializer<T> deserializer) {
        return new LocalBusReceiver<>(topics.stream()
                .map(name -> (LocalBusTopic<T>)topicsMap.computeIfAbsent(name,
                    k -> new LocalBusTopic<>(executorService, name)))
                .collect(Collectors.toList()));
    }

    @Override
    public <S extends AbstractMessage> IMessageSender<S> messageSender(@Nonnull final String topic, @Nonnull final Function<S, String> keyGenerator) {
        return messageSender(topic);
    }

    @Override
    public <S extends AbstractMessage> IMessageSender<S> messageSender(@Nonnull final String topic) {
        LocalBusTopic<S> theTopic = (LocalBusTopic<S>)topicsMap.computeIfAbsent(topic,
            k -> new LocalBusTopic<>(executorService, topic));
        return new LocalBusSender<>(theTopic);
    }

    @Override
    public boolean lastSendAttemptFailed() {
        // Errors? Impossible!
        return false;
    }

    /**
     * Receiver on a local bus.
     *
     * @param <T> The message type for the topic.
     */
    private class LocalBusReceiver<T> implements IMessageReceiver<T> {

        private final List<LocalBusTopic<T>> topics;

        private LocalBusReceiver(final List<LocalBusTopic<T>> busTopics) {
            this.topics = busTopics;
        }

        @Override
        public void addListener(@Nonnull final TriConsumer<T, Runnable, SpanContext> listener) {
            topics.forEach(t -> t.addListener(listener));
        }
    }

    /**
     * Sender on a local bus.
     *
     * @param <T> The message type for the topic.
     */
    private class LocalBusSender<T> implements IMessageSender<T> {
        private final LocalBusTopic<T> localBusTopic;

        private LocalBusSender(final LocalBusTopic<T> theTopic) {
            this.localBusTopic = theTopic;
        }

        @Override
        public void sendMessage(@Nonnull final T serverMsg) throws CommunicationException, InterruptedException {
            localBusTopic.sendMessage(serverMsg);
        }

        @Override
        public int getMaxRequestSizeBytes() {
            return Integer.MAX_VALUE;
        }

        @Override
        public int getRecommendedRequestSizeBytes() {
            // Same as Kafka chunk size.
            return 126976;
        }
    }
}
