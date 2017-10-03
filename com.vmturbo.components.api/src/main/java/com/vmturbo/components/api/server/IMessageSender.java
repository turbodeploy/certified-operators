package com.vmturbo.components.api.server;

import javax.annotation.Nonnull;

import com.vmturbo.communication.CommunicationException;

/**
 * Interface of something, sending notifications. It is guaranteed, that notifications will be
 * delivered in the same order they have been sent.
 * Delivery is guaranteed to the sender since it subscribed for this notification.
 *
 * @param <S> type of message to send (notification)
 */
public interface IMessageSender<S> {
    /**
     * Sends the notification. It could be delivered to any number of recepients, depending on
     * subscription succeeded.
     *
     * @param serverMsg message to send.
     */
    void sendMessage(@Nonnull final S serverMsg);

    /**
     * Sends the notification. It could be delivered to any number of recepients, depending on
     * subscription succeeded.
     *
     * @param serverMsg message to send.
     */
    void sendMessageSync(@Nonnull final S serverMsg) throws InterruptedException;
}
