package com.vmturbo.components.api.server;

import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.protobuf.AbstractMessage;

/**
 * Factory class for {@link IMessageSender}s, used to abstract away the underlying bus
 * implementation.
 */
public interface IMessageSenderFactory {

    /**
     * Creates a message sender for the specific topic, with a specific key generator function.
     *
     * <p/>The key generator function should accept a message of the chosen type and return a string
     * to use as the message key when sending.
     *
     * @param topic The topic to send messages to
     * @param keyGenerator The key generation function to use
     * @param <S> The Type of the messages to send on this topic
     * @return a message sender configured for the topic and key generator
     */
    <S extends AbstractMessage> IMessageSender<S> messageSender(@Nonnull String topic,
                                                                @Nonnull Function<S, String> keyGenerator);

    /**
     * Creates message sender for the specific topic.
     *
     * @param topic topic to send messages to
     * @param <S> The Type of the messages to send on this topic
     * @return message sender.
     */
    <S extends AbstractMessage> IMessageSender<S> messageSender(@Nonnull String topic);

    /**
     * True if the last send operation failed. Used for health monitoring.
     *
     * @return True if the last send failed, false otherwise.
     */
    boolean lastSendFailed();
}
