package com.vmturbo.components.api.server;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Empty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.ITransport.EventHandler;

/**
 * Notification sender, based on websocket communication.
 */
public class WebsocketNotificationSender<T> implements IMessageSender<T>,
        IByteTransferNotificationSender<T> {

    /**
     * A map of (the endpoint used to connect to the client) -> (the message queue used to send
     * messages to the endpoint).
     */
    private final Map<ITransport<T, Empty>, ClientMessageQueue> registeredEndpoints =
            new ConcurrentHashMap<>();

    private final Logger logger = LogManager.getLogger(getClass());
    private final ExecutorService threadPool;
    private final int maxRequestSizeBytes;
    private final int recommendedRequestSizeBytes;


    WebsocketNotificationSender(@Nonnull ExecutorService threadPool,
                                       final int maxRequestSizeBytes,
                                       final int recommendedRequestSizeBytes) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.maxRequestSizeBytes = maxRequestSizeBytes;
        this.recommendedRequestSizeBytes = recommendedRequestSizeBytes;
    }

    private Collection<Future<Void>> sendMessageInternal(@Nonnull final T serverMsg) {
        Objects.requireNonNull(serverMsg);
        logger.debug("Sending message {}", () -> serverMsg.getClass().getSimpleName());
        return registeredEndpoints.values()
                .stream()
                .map(clientMessagQueue -> clientMessagQueue.queueMessage(serverMsg))
                .collect(Collectors.toList());
    }

    @Override
    public void sendMessage(@Nonnull T serverMsg)
            throws CommunicationException, InterruptedException {
        try {
            for (Future<Void> future : sendMessageInternal(serverMsg)) {
                future.get();
            }
        } catch (ExecutionException e) {
            throw new CommunicationException(
                    "Unexpected exception occurred while waiting for the message " +
                            serverMsg.getClass().getSimpleName() + " sending", e.getCause());
        }
    }

    @Override
    public int getMaxRequestSizeBytes() {
        return maxRequestSizeBytes;
    }

    @Override
    public int getRecommendedRequestSizeBytes() {
        return recommendedRequestSizeBytes;
    }

    @Nonnull
    public void addTransport(@Nonnull final ITransport<T, Empty> endpoint) {
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

    // TODO remove this method when using tests with direct sender-receiver interfaces.
    @VisibleForTesting
    public void waitForEndpoints(final int numEndpoints, final long time, final TimeUnit timeUnit)
            throws InterruptedException, TimeoutException {
        final Instant startTime = Instant.now();
        while (registeredEndpoints.size() != numEndpoints) {
            if (!Duration.between(startTime, Instant.now())
                    .minusMillis(timeUnit.toMillis(time))
                    .isNegative()) {
                throw new TimeoutException("Required number of endpoints: " + numEndpoints +
                        " not registered on the server. Current value: " +
                        registeredEndpoints.size());
            } else {
                Thread.sleep(100);
            }
        }
    }

    /**
     * A queue that sends messages in order of queueing (via {@link #sendMessageInternal(T)}.
     */
    private class ClientMessageQueue implements AutoCloseable {
        /**
         * The task that is sending the messages from the queue.
         */
        private final Future<?> sendTask;

        private final BlockingQueue<PendingMessage> pendingMessages;

        ClientMessageQueue(@Nonnull final ITransport<T, Empty> endpoint) {
            pendingMessages = new LinkedBlockingQueue<>();
            sendTask = threadPool.submit(() -> {
                try {
                    while (true) {
                        final PendingMessage message = pendingMessages.take();
                        try {
                            endpoint.send(message.message);
                            message.onComplete.complete(null);
                        } catch (CommunicationException e) {
                            logger.error("Error occurred while sending message " + message +
                                    " to transport " + endpoint, e);
                            endpoint.close();
                            message.onComplete.completeExceptionally(e);
                            return;
                        }
                    }
                } catch (RuntimeException e) {
                    logger.error(
                            "Unexpected error occurred during processing messages queue for transport " +
                                    endpoint, e);
                } catch (InterruptedException ie) {
                    logger.error(
                            "Message sending thread for transport " + endpoint + " interrupted");
                } finally {
                    cancelPendingTasks();
                }
            });
        }

        private Future<Void> queueMessage(T msg) {
            final PendingMessage message = new PendingMessage(msg);
            pendingMessages.add(message);
            return message.onComplete;
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
     * A {@link T} queued for sending to a client.
     */
    private class PendingMessage {
        final T message;
        final CompletableFuture<Void> onComplete;

        PendingMessage(T message) {
            this.message = message;
            this.onComplete = new CompletableFuture<>();
        }
    }
}
