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
     * subscription succeeded. This is a blocking call. If it returns successfully (without any
     * exceptions thrown), this means, that message has been sent successfully.
     *
     * @param serverMsg message to send.
     * @throws InterruptedException if sending thread has been interrupted. This does not
     *      guarantee, that message has ben sent nor it has not been sent
     * @throws CommunicationException if persistent communication error occurred (message could
     *      not be sent in future).
     */
    void sendMessage(@Nonnull final S serverMsg) throws CommunicationException,
            InterruptedException;

    /**
     * Get the maximum size of messages allowed to be sent via this sender. Any messages larger
     * than this may not be sent.
     *
     * @return The maximum size of messages sent via this sender, in bytes.
     */
    int getMaxRequestSizeBytes();

    /**
     * Get the recommended size for messages sent via this sender. Clients should try to stick to this
     * size for optimal performance.
     *
     * @return The recommended size for messages sent via this sender, in bytes.
     */
    int getRecommendedRequestSizeBytes();
}
