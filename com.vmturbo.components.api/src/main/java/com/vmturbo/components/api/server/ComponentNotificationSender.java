package com.vmturbo.components.api.server;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.AbstractMessage;

import com.vmturbo.communication.CommunicationException;

/**
 * Contains server-side boilerplate to send Protobuf notifications using underlying
 * {@link IMessageSender}.
 *
 * @param <ServerMsg> The protobuf class of the notification messages the server will send.
 */
public abstract class ComponentNotificationSender<ServerMsg extends AbstractMessage> {

    private final Logger logger = LogManager.getLogger(getClass());

    private final AtomicLong messageChainIdCounter = new AtomicLong(0);
    /**
     * Return the ID to use for a new message chain. All subclasses should use this method instead
     * of assigning their own message IDs.
     *
     * @return A new message chain ID.
     */
    protected long newMessageChainId() {
        return messageChainIdCounter.incrementAndGet();
    }

    protected final void sendMessage(@Nonnull IMessageSender<ServerMsg> sender,
            @Nonnull final ServerMsg serverMsg)
            throws InterruptedException, CommunicationException {
        Objects.requireNonNull(sender);
        Objects.requireNonNull(serverMsg);
        final String messageDescription = describeMessage(serverMsg);
        logger.info("Sending message " + messageDescription + " for broadcast to listeners.");
        sender.sendMessage(serverMsg);
    }

    /**
     * Returns user-readable representation of the message to be sent. Used in logs and
     * exceptions in order to increase debugability.
     *
     * @param serverMsg server message to get representation for
     * @return string representation of a message
     */
    protected abstract String describeMessage(@Nonnull ServerMsg serverMsg);

    @Nonnull
    protected Logger getLogger() {
        return logger;
    }
}
