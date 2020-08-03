package com.vmturbo.components.api.client;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.BiConsumer;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.protobuf.AbstractMessage;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.Empty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.communication.AbstractProtobufEndpoint;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.communication.ConnectionConfig;
import com.vmturbo.communication.ITransport;
import com.vmturbo.communication.ITransport.EventHandler;
import com.vmturbo.communication.WebSocketClientTransport;

/**
 * Implementation of {@link IMessageReceiver} using WebSockets to await messages.
 *
 * @param <T> type of messge to receive.
 */
public class WebsocketNotificationReceiver<T extends AbstractMessage> implements
        IMessageReceiver<T>, AutoCloseable {

    private final Logger logger = LogManager.getLogger(getClass());
    private final Deserializer<T> deserializer;
    private final Collection<BiConsumer<T, Runnable>> listeners = new CopyOnWriteArrayList<>();
    private final Future<?> initializationTask;

    private ITransport<Empty, T> endpoint;

    /**
     * Connstructs websocket notification receiver and starts listening immediately (as soon as
     * websocket connection is established).
     *
     * @param connectionConfig connection configuration
     * @param websocketPath websocket path to append to host:port URI
     * @param threadPool thread pool to use
     * @param deserializer deserializer for the notification message.
     */
    public WebsocketNotificationReceiver(
            @Nonnull final WebsocketConnectionConfig connectionConfig,
            @Nonnull String websocketPath, @Nonnull ExecutorService threadPool,
            @Nonnull Deserializer<T> deserializer) {
        this.deserializer = Objects.requireNonNull(deserializer);
        final URI fullUri = URI.create(
                "ws://" + connectionConfig.getHost() + ":" + connectionConfig.getPort() +
                        websocketPath);
        final ConnectionConfig finalConnectionConfig =
                new InternalConnectionConfig(fullUri, connectionConfig);
        initializationTask = threadPool.submit(() -> {
            try {
                final ITransport<ByteBuffer, InputStream> underlyingTransport =
                        new WebSocketClientTransport(threadPool, finalConnectionConfig, null);
                endpoint = createEndpoint(underlyingTransport);
            } catch (CommunicationException | InterruptedException | RuntimeException e) {
                logger.error("Exception connecting to endpoint", e);
            }
        });
    }

    /**
     * Create the instance of {@link AbstractProtobufEndpoint} that sends and
     * receives the client's client and server protobuf messages.
     *
     * @param transport The underlying transport.
     * @return The {@link AbstractProtobufEndpoint} to use.
     */
    @Nonnull
    private AbstractProtobufEndpoint<Empty, T> createEndpoint(
            @Nonnull final ITransport<ByteBuffer, InputStream> transport) {
        final AbstractProtobufEndpoint<Empty, T>  endpoint =
                new AbstractProtobufEndpoint<Empty, T>(transport) {
                    @Override
                    protected T parseFromData(CodedInputStream bytes) throws IOException {
                        return deserializer.parseFrom(bytes);
                    }
                };
        endpoint.addEventHandler(new EventHandler<T>() {
            @Override
            public void onMessage(T message) {
                try {
                    processMessage(message);
                } catch (IOException e) {
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

    private void processMessage(@Nonnull T message) throws IOException {
        // Websockets right now do not support message committing
        listeners.forEach(listener -> listener.accept(message, () -> {}));
    }

    @Override
    public void addListener(@Nonnull BiConsumer<T, Runnable> listener) {
        listeners.add(listener);
    }

    @Override
    public void close() {
        initializationTask.cancel(true);
        if (endpoint != null) {
            endpoint.close();
        }
    }

    /**
     * A connection config backed by {@link ComponentApiConnectionConfig} for everything
     * except the server address.
     */
    private static class InternalConnectionConfig implements ConnectionConfig {
        private final URI finalUri;
        private final WebsocketConnectionConfig innerConfig;

        InternalConnectionConfig(@Nonnull final URI finalUri,
                @Nonnull final WebsocketConnectionConfig innerConfig) {
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

        @Override
        public boolean getSSLTrustAllServerCertificates() {
            return false;
        }

        @Nullable
        @Override
        public File getSSLTruststoreFile() {
            return null;
        }

        @Nullable
        @Override
        public String getSSLTruststorePassword() {
            return null;
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

        @Nullable
        @Override
        public String getSSLKeystoreKeyPassword() {
            return null;
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

        @Override
        public long getAtomicSendTimeoutSec() {
            return 30L;
        }
    }
}

