package com.vmturbo.components.api.client;

import java.util.function.BiConsumer;

import javax.annotation.Nonnull;

/**
 * Interface of a facility, able to receive messages. Delivery of messages is
 * guaranteed by the sender since subscriber has been arrived at least once.
 *
 * @param <T> type of message to receive.
 */
public interface IMessageReceiver<T> extends AutoCloseable {
    /**
     * Method adds listeners to the receiver. Listener will receive the messages. There could be
     * multiple listeners for each message. As soon as consumer is sure, that the message has
     * been successfully processed (the message is not longer required by the receiver side,
     * receiver must call {@link Runnable#run} to commit the changed.
     *
     * @param listener listener to add
     */
    void addListener(@Nonnull BiConsumer<T, Runnable> listener);

    /**
     * Closes the receiver freeing all the resources and preventing underlying transport system
     * from sending data to this receiver furthermore.
     */
    void close();
}
