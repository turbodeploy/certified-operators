package com.vmturbo.components.api.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Empty;
import com.google.protobuf.InvalidProtocolBufferException;

import com.vmturbo.communication.AbstractProtobufEndpoint;
import com.vmturbo.communication.ConnectionConfig;
import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.ITransport.EventHandler;
import com.vmturbo.communication.WebSocketClientTransport;
import com.vmturbo.components.api.server.ComponentNotificationSender;

/**
 * Parent class to receive protobuf notification messages with a {@link ComponentNotificationSender}.
 *
 * <p>Handles the boilerplate work of connecting and reconnecting to the server.
 *
 * @param <RecvMsg> The protobuf notification the server will send to the client.
 */
public abstract class ComponentNotificationReceiver<RecvMsg extends AbstractMessage>
        implements AutoCloseable {

    private AbstractProtobufEndpoint<Empty, RecvMsg> endpoint;

    protected final ExecutorService executorService;

    protected final Logger logger = LogManager.getLogger();

    /**
     * Websocket connection initialization task.
     */
    private final Future<?> initializationTask;

    /**
     * Creates an instance of the client. This instance will hold the connection to
     * the component and try to reconnect if the connection goes down.
     *
     * <p>>The connection is initialized asynchronously.
     *
     * @param connectionConfig Configuration to use to connect to the server.
     * @param executorService Executor service to use for communication with the server.
     */
    protected ComponentNotificationReceiver(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        this.executorService = Objects.requireNonNull(executorService, "Thread pool should not be null");

        initializationTask = executorService.submit(() -> {
            final String destinationAddress = connectionConfig.getHost() + ":" + connectionConfig.getPort();
            logger.info("Starting connection to " + destinationAddress);
            try {
                final URI fullUri = URI.create(
                        addWebsocketPath("ws://" + connectionConfig.getHost() + ":" + connectionConfig.getPort()));
                final ConnectionConfig finalConnectionConfig =
                        new InternalConnectionConfig(fullUri, connectionConfig);
                final ITransport<ByteBuffer, InputStream> underlyingTransport =
                        new WebSocketClientTransport(executorService, finalConnectionConfig, null);
                endpoint = createEndpoint(underlyingTransport);
            } catch (Throwable e) {
                logger.error("Failed to establish connection to: " + destinationAddress +
                        " due to error: ", e);
            }
        });
    }

    @Override
    public void close() {
        logger.info("Component client is closing (class: " + getClass().getSimpleName() + ")");
        initializationTask.cancel(true);
        if (endpoint != null) {
            endpoint.close();
        }
    }

    /**
     * Append the client's websocket path to the given a server address.
     *
     * @param serverAddress The server address path (e.g. ws://component:123)
     * @return The full path to the websocket (e.g. ws://component:123/websocket-path)
     */
    @Nonnull
    protected abstract String addWebsocketPath(@Nonnull String serverAddress);

    /**
     * Create the instance of {@link AbstractProtobufEndpoint} that sends and
     * receives the client's client and server protobuf messages.
     *
     * @param transport The underlying transport.
     * @return The {@link AbstractProtobufEndpoint} to use.
     */
    @Nonnull
    private AbstractProtobufEndpoint<Empty, RecvMsg> createEndpoint(
            @Nonnull final ITransport<ByteBuffer, InputStream> transport) {
        final AbstractProtobufEndpoint<Empty, RecvMsg> endpoint =
                new AbstractProtobufEndpoint<Empty, RecvMsg>(transport) {
            @Override
            protected RecvMsg parseFromData(CodedInputStream bytes) throws IOException {
                return ComponentNotificationReceiver.this.parseMessage(bytes);
            }
        };
        endpoint.addEventHandler(new EventHandler<RecvMsg>() {
            @Override
            public void onMessage(RecvMsg message) {
                try {
                    processMessage(message);
                } catch (ApiClientException e) {
                    logger.error("Error processing message " + message, e);
                }
            }

            @Override
            public void onClose() {
                // Do nothing, as automatic reconnect should be handled by inner implementation of
                // WebsocketClientTransport
            }
        });
        return endpoint;
    }

    /**
     * Deserializes the data into a Protobuf java class.
     *
     * @param bytes data to deserialize
     * @return Protobuf java class
     * @throws InvalidProtocolBufferException thrown if Protobuf cannot parse data
     * @throws IOException on error reading inpurt stream
     */
    @Nonnull
    protected abstract RecvMsg parseMessage(@Nonnull CodedInputStream bytes) throws IOException;

    /**
     * Process a message received from the server.
     *
     * @param message The received message.
     * @throws ApiClientException If an error is encountered during the processing.
     */
    protected abstract void processMessage(@Nonnull final RecvMsg message)
            throws ApiClientException;

    protected ExecutorService getExecutorService() {
        return executorService;
    }

    protected Logger getLogger() {
        return logger;
    }

    /**
     * A connection config backed by {@link ComponentApiConnectionConfig} for everything
     * except the server address.
     */
    private static class InternalConnectionConfig implements ConnectionConfig {
        private final URI finalUri;
        private final ComponentApiConnectionConfig innerConfig;

        InternalConnectionConfig(@Nonnull final URI finalUri,
                                 @Nonnull final ComponentApiConnectionConfig innerConfig) {
            this.finalUri = finalUri;
            this.innerConfig = innerConfig;
        }

        @Nonnull
        @Override
        public URI getServerAddress() {
            return finalUri;
        }

        @Nullable
        @Override
        public String getUserName() {
            return innerConfig.getUserName();
        }

        @Nullable
        @Override
        public String getUserPassword() {
            return innerConfig.getUserPassword();
        }

        @Nullable
        @Override
        public File getSSLKeystoreFile() {
            return innerConfig.getSSLKeystoreFile();
        }

        @Nullable
        @Override
        public String getSSLKeystorePassword() {
            return innerConfig.getSSLKeystorePassword();
        }

        @Override
        public long getSilentRetriesTime() {
            return innerConfig.getSilentRetriesTime();
        }

        @Override
        public long getConnRetryIntervalSeconds() {
            return innerConfig.getConnRetryIntervalSeconds();
        }

        @Override
        public long getPongMessageTimeout() {
            return innerConfig.getPongMessageTimeout();
        }
    }
}
