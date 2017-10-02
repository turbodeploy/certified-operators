package com.vmturbo.components.api.server;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Empty;

import com.vmturbo.communication.AbstractProtobufEndpoint;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.ITransport.EventHandler;
import com.vmturbo.communication.WebsocketServerTransport;
import com.vmturbo.communication.WebsocketServerTransportManager;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;

/**
 * Contains server-side boilerplate to send Protobuf notifications
 * to connected {@link ComponentNotificationReceiver} over websocket.
 *
 * @param <ServerMsg> The protobuf class of the notification messages the server will send.
 */
public abstract class ComponentNotificationSender<ServerMsg extends AbstractMessage> {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * A map of (the endpoint used to connect to the client) -> (the message queue used to send messages to the endpoint).
     */
    private final Map<ITransport<ServerMsg, Empty>, ClientMessageQueue> registeredEndpoints =
            new ConcurrentHashMap<>();

    private final ExecutorService executorService;

    private final WebsocketServerTransportManager transportManager;

    private final AtomicLong messageChainIdCounter = new AtomicLong(0);

    protected ComponentNotificationSender(@Nonnull final ExecutorService executorService) {
        this.executorService = Objects.requireNonNull(executorService);
        this.transportManager =
                new WebsocketServerTransportManager(this::onNewTransport, executorService);
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

    /**
     * Create a {@link AbstractProtobufEndpoint} that sends {@link ServerMsg} and receives {@link Empty}.
     *
     * @param transport The underlying transport to use.
     * @return The {@link ITransport}.
     */
    @Nonnull
    private ITransport<ServerMsg, Empty> createEndpoint(
            @Nonnull final WebsocketServerTransport transport) {
        return new AbstractProtobufEndpoint<ServerMsg, Empty>(transport) {
            @Override
            protected Empty parseFromData(CodedInputStream bytes) throws IOException {
                return Empty.parseFrom(bytes);
            }
        };
    }

    protected final void sendMessage(final long messageChainId, @Nonnull final ServerMsg serverMsg) {
        Objects.requireNonNull(serverMsg);
        final String messageDescription = describeMessage(serverMsg);
        logger.info("Message Chain: " + messageChainId + " queueing message " + messageDescription +
                " for broadcast to listeners.");
        registeredEndpoints.values().forEach(clientMessageQueue -> executorService.submit(() -> {
            try {
                clientMessageQueue.queueMessage(serverMsg);
            } catch (Exception e) {
                logger.error("In Message Chain: " + messageChainId +
                        ", failed to notify endpoint due to exception " +
                        messageDescription, e);
            }
        }));
    }

    /**
     * Returns user-readable representation of the message to be sent. Used in logs and
     * exceptions in order to increase debugability.
     *
     * @param serverMsg server message to get representation for
     * @return string representation of a message
     */
    protected abstract String describeMessage(@Nonnull ServerMsg serverMsg);

    /**
     * Sends message synchronously to all the receipients. Method blocks until the message is
     * sent to every recipient, or some communication error occur between sender and recipient.
     *
     * @param messageChainId message id to send
     * @param serverMsg message to send
     * @throws InterruptedException if thread interrupted during the call
     */
    protected final void sendMessageSync(final long messageChainId,
            @Nonnull final ServerMsg serverMsg) throws InterruptedException {
        Objects.requireNonNull(serverMsg);
        final String serverMsgDescription = describeMessage(serverMsg);
        logger.debug(
                "Message Chain: " + messageChainId + " queueing message " + serverMsgDescription +
                        " for broadcast to listeners. " + serverMsgDescription);
        // Copy transports set to avoid race condition.
        final Set<ITransport<ServerMsg, Empty>> transports =
                ImmutableSet.copyOf(registeredEndpoints.keySet());
        final CountDownLatch latch = new CountDownLatch(transports.size());
        transports.forEach(transport -> executorService.submit(() -> {
            try {
                transport.send(serverMsg);
            } catch (InterruptedException e) {
                logger.info("Thread interrupted during processing message chain " + messageChainId,
                        e);
            } catch (Exception e) {
                logger.error("In Message Chain: " + messageChainId +
                        ", failed to notify endpoint due to exception. " + serverMsgDescription, e);
            } finally {
                latch.countDown();
            }
        }));
        latch.await();
    }

    private void onNewTransport(@Nonnull final WebsocketServerTransport transport) {
        final ITransport<ServerMsg, Empty> endpoint = createEndpoint(transport);
        registeredEndpoints.put(endpoint, new ClientMessageQueue(endpoint));
        endpoint.addEventHandler(new EventHandler<Empty>() {
            @Override
            public void onClose() {
                final ClientMessageQueue clientQueue = registeredEndpoints.remove(endpoint);
                clientQueue.close();
            }

            @Override
            public void onMessage(@Nonnull final Empty message) {
                logger.error("Unexpected client message.");
            }
        });
    }

    @VisibleForTesting
    public void waitForEndpoints(final int numEndpoints, final long time, final TimeUnit timeUnit)
            throws InterruptedException, TimeoutException {
        final Instant startTime = Instant.now();
        while (registeredEndpoints.size() != numEndpoints) {
            if (!Duration.between(startTime, Instant.now())
                    .minusMillis(timeUnit.toMillis(time))
                    .isNegative()) {
                throw new TimeoutException("Required number of endpoints: " + numEndpoints
                        + " not registered on the server. Current value: "
                        + registeredEndpoints.size());
            } else {
                Thread.sleep(100);
            }
        }
    }

    @Nonnull
    public WebsocketServerTransportManager getWebsocketEndpoint() {
        return transportManager;
    }

    @Nonnull
    protected ExecutorService getExecutorService() {
        return executorService;
    }

    @Nonnull
    protected Logger getLogger() {
        return logger;
    }

    /**
     * A queue that sends messages in order of queueing (via {@link #queueMessage(ServerMsg)}.
     */
    private class ClientMessageQueue implements AutoCloseable {
        /**
         * The task that is sending the messages from the queue.
         */
        private final Future<?> sendTask;

        private final BlockingQueue<PendingMessage> pendingMessages;

        ClientMessageQueue(@Nonnull final ITransport<ServerMsg, Empty> endpoint) {
            pendingMessages = new LinkedBlockingQueue<>();
            sendTask = executorService.submit(() -> {
                try {
                    while (true) {
                        final PendingMessage message = pendingMessages.take();
                        try {
                            endpoint.send(message.message);
                            message.onComplete.complete(null);
                        } catch (CommunicationException e) {
                            logger.error("Error occurred while sending message " + message
                                    + " to transport " + endpoint, e);
                            endpoint.close();
                            message.onComplete.completeExceptionally(e);
                            return;
                        }
                    }
                } catch (RuntimeException e) {
                    logger.error("Unexpected error occurred during processing messages queue for transport "
                            + endpoint, e);
                } catch (InterruptedException ie) {
                    logger.error("Message sending thread for transport " + endpoint + " interrupted");
                } finally {
                    cancelPendingTasks();
                }
            });
        }

        void queueMessage(ServerMsg msg) {
            pendingMessages.add(new PendingMessage(msg));
        }

        private void cancelPendingTasks() {
            for (final PendingMessage pendingMessage : pendingMessages) {
                pendingMessage.onComplete.cancel(true);
            }
        }

        @Override
        public void close() {
            sendTask.cancel(true);
            cancelPendingTasks();
        }

    }

    /**
     * A {@link ServerMsg} queued for sending to a client.
     */
    private class PendingMessage {
        final ServerMsg message;
        final CompletableFuture<Void> onComplete;

        PendingMessage(ServerMsg message) {
            this.message = message;
            this.onComplete = new CompletableFuture<>();
        }

    }

}
