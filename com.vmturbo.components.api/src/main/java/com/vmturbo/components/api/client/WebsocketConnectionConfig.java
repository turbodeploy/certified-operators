package com.vmturbo.components.api.client;

import java.io.File;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class WebsocketConnectionConfig extends ComponentApiConnectionConfig {

    private static final long DEFAULT_CONN_RETRY_INTERVAL_S = 30;

    private final long silentRetriesTime;

    private final long connRetryIntervalSeconds;

    private final long pongMessageTimeout;
    /**
     * Timeout applied for atomic sending operation. Some large data buffers are chunked into a less
     * ones in order to send over the wire. This timeout applies to the 2nd ones, i.e. the buffers
     * that are really sent to the underlying Websocket transport.
     */
    private final long atomicSendTimeoutSec;

    private WebsocketConnectionConfig(@Nonnull final String host, final int port,
            @Nullable final String userName, @Nullable final String userPassword,
            @Nullable final File sslKeystoreFile, @Nullable final String sslKeystorePassword,
            final long silentRetriesTime, final long connRetryIntervalSeconds,
            final long pongMessageTimeout, final long atomicSendTimeoutSec) {
        super(host, port, userName, userPassword, sslKeystoreFile, sslKeystorePassword);
        this.silentRetriesTime = silentRetriesTime;
        this.connRetryIntervalSeconds = connRetryIntervalSeconds;
        this.pongMessageTimeout = pongMessageTimeout;
        this.atomicSendTimeoutSec = atomicSendTimeoutSec;
    }

    public long getSilentRetriesTime() {
        return silentRetriesTime;
    }

    public long getConnRetryIntervalSeconds() {
        return connRetryIntervalSeconds;
    }

    public long getPongMessageTimeout() {
        return pongMessageTimeout;
    }

    public static Builder newBuilder(@Nonnull String host, int port) {
        return new Builder(host, port);
    }

    /**
     * Builder for the {@link WebsocketConnectionConfig}.
     *
     * <p>Use the {@link WebsocketConnectionConfig#newBuilder()} to initialize.
     */
    public static class Builder extends AbstractBuilder<Builder> {

        private long silentRetriesTime = 0;
        private long connRetryIntervalSeconds = DEFAULT_CONN_RETRY_INTERVAL_S;
        private long pongMessageTimeout = 10000;
        private long atomicSendTimeoutSec = 30;

        public Builder(@Nonnull String host, int port) {
            super(host, port);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }

        public Builder setConnRetryIntervalSeconds(final long connRetryIntervalSeconds) {
            this.connRetryIntervalSeconds = connRetryIntervalSeconds;
            return this;
        }

        public Builder setPongMessageTimeout(final long pongMessageTimeout) {
            this.pongMessageTimeout = pongMessageTimeout;
            return this;
        }

        public Builder setSilentRetriesTime(final long silentRetriesTime) {
            this.silentRetriesTime = silentRetriesTime;
            return getSelf();
        }

        public WebsocketConnectionConfig build() {
            return new WebsocketConnectionConfig(getHost(), getPort(), getUserName(),
                    getUserPassword(), getSslKeystoreFile(), getSslKeystorePassword(),
                    silentRetriesTime, connRetryIntervalSeconds, pongMessageTimeout,
                    atomicSendTimeoutSec);
        }
    }
}
