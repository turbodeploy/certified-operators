package com.vmturbo.components.api.client;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.AbstractMessage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.components.api.server.ComponentNotificationSender;

/**
 * Parent class to receive protobuf notification messages with a {@link ComponentNotificationSender}.
 *
 * <p>Handles the boilerplate work of connecting and reconnecting to the server.
 *
 * @param <RecvMsg> The protobuf notification the server will send to the client.
 */
public abstract class ComponentNotificationReceiver<RecvMsg extends AbstractMessage> {

    private final ExecutorService executorService;

    private final Logger logger = LogManager.getLogger();

    /**
     * Creates an instance of the client. This instance will hold the connection to
     * the component and try to reconnect if the connection goes down.
     *
     * <p>>The connection is initialized asynchronously.
     *
     * @param messageReceiver Message receiver to await messages from
     * @param executorService Executor service to use for communication with the server.
     */
    protected ComponentNotificationReceiver(
            @Nullable final IMessageReceiver<RecvMsg> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        this.executorService =
                Objects.requireNonNull(executorService, "Thread pool should not be null");
        if (messageReceiver != null) {
            messageReceiver.addListener((message, commitCmd) -> {
                try {
                    processMessage(message);
                    commitCmd.run();
                } catch (ApiClientException | InterruptedException e) {
                    logger.error("Error occurred while processing message " +
                            message.getClass().getSimpleName(), e);
                }
            });
        }
    }

    /**
     * Process a message received from the server.
     *
     * @param message The received message.
     */
    protected abstract void processMessage(@Nonnull final RecvMsg message)
            throws ApiClientException, InterruptedException;

    protected ExecutorService getExecutorService() {
        return executorService;
    }

    protected Logger getLogger() {
        return logger;
    }
}
