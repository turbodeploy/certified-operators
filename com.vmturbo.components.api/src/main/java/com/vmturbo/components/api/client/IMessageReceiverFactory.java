package com.vmturbo.components.api.client;

import java.util.Collection;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings;

/**
 * Factory class for {@link IMessageReceiver}s, used to abstract away the underlying bus
 * implementation.
 */
public interface IMessageReceiverFactory {
    /**
     * Creates a message receiver for a specific topic, with additional topic-specific settings
     * configured.
     *
     * @param topicSettings the {@link TopicSettings} to use for this topic
     * @param deserializer function to deserialize the message from bytes
     * @param <T> type of messages to receive
     * @return message receiver implementation
     */
    <T> IMessageReceiver<T> messageReceiverWithSettings(@Nonnull TopicSettings topicSettings,
                                                               @Nonnull Deserializer<T> deserializer);

    /**
     * Creates message receiver for the specific topic. Kafka consumer will not subscribe to any
     * topics until this method is called. Different topics specified here could be reported
     * in parallel, while all the messages within a topic are only delivered sequentially.
     *
     * @param topicSettings topic to subscribe to, with setting overrides.
     * @param deserializer function to deserialize the message from bytes
     * @param <T> type of messages to receive
     * @return message receiver implementation
     * @throws IllegalStateException if the topic has been already subscribed to
     */
    <T> IMessageReceiver<T> messageReceiversWithSettings(
        @Nonnull Collection<TopicSettings> topicSettings,
        @Nonnull Deserializer<T> deserializer);

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
    <T> IMessageReceiver<T> messageReceiver(@Nonnull String topic,
                                            @Nonnull Deserializer<T> deserializer);

    /**
     * Creates message receiver for the specific topic. Kafka consumer will not subscribe to any
     * topics until this method is called. Different topics specified here could be reported
     * in parallel, while all the messages within a topic are only delivered sequentially.
     *
     * @param topics topic to subscribe to
     * @param deserializer function to deserialize the message from bytes
     * @param <T> type of messages to receive
     * @return message receiver implementation
     * @throws IllegalStateException if the topic has been already subscribed to
     */
    <T> IMessageReceiver<T> messageReceiver(@Nonnull Collection<String> topics,
                                            @Nonnull Deserializer<T> deserializer);
}
