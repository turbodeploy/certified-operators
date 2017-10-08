package com.vmturbo.components.api.server;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.AbstractMessage;

import com.vmturbo.components.api.client.ComponentNotificationReceiver;

/**
 * Contains server-side boilerplate to send Protobuf notifications using underlying
 * {@link IMessageSender}.
 *
 * @param <ServerMsg> The protobuf class of the notification messages the server will send.
 */
public abstract class ComponentNotificationSender<ServerMsg extends AbstractMessage> {

    private final Logger logger = LogManager.getLogger(getClass());

    private final ExecutorService executorService;
    private final AtomicLong messageChainIdCounter = new AtomicLong(0);

    protected ComponentNotificationSender(@Nonnull final ExecutorService executorService) {
        this.executorService = Objects.requireNonNull(executorService);
    }

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
            @Nonnull final ServerMsg serverMsg) {
        Objects.requireNonNull(sender);
        Objects.requireNonNull(serverMsg);
        final String messageDescription = describeMessage(serverMsg);
        logger.info("Sending message " + messageDescription + " for broadcast to listeners.");
        sender.sendMessage(serverMsg);
    }

    protected final void sendMessageSync(@Nonnull IMessageSender<ServerMsg> sender,
            @Nonnull final ServerMsg serverMsg) throws InterruptedException {
        Objects.requireNonNull(sender);
        Objects.requireNonNull(serverMsg);
        final String messageDescription = describeMessage(serverMsg);
        logger.info("Sending message " + messageDescription + " for broadcast to listeners.");
        sender.sendMessageSync(serverMsg);
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
    protected ExecutorService getExecutorService() {
        return executorService;
    }

    @Nonnull
    protected Logger getLogger() {
        return logger;
    }
}
