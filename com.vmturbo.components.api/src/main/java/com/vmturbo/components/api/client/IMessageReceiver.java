package com.vmturbo.components.api.client;

import javax.annotation.Nonnull;

import io.opentracing.SpanContext;

/**
 * Interface of a facility, able to receive messages. Delivery of messages is
 * guaranteed by the sender since subscriber has been arrived at least once.
 * <p/>
 * @param <T> type of message to receive.
 */
public interface IMessageReceiver<T> {
    /**
     * Method adds listeners to the receiver. Listener will receive the messages. There could be
     * multiple listeners for each message. As soon as consumer is sure, that the message has
     * been successfully processed (the message is not longer required by the receiver side,
     * receiver must call {@link Runnable#run} to commit the changed.
     * <p/>
     * A distributed tracing span context is also passed into listeners when they receive
     * messages in order to facilitate propagating distributed tracing spans from
     * message producers to message consumers. This context can be freely ignored
     * if there is no need to extend the distributed trace across the message boundary.
     *
     * @param listener listener to add
     */
    void addListener(@Nonnull TriConsumer<T, Runnable, SpanContext> listener);
}
