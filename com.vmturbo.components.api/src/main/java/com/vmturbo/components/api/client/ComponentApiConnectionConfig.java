package com.vmturbo.components.api.client;

import java.io.File;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

/**
 * Connection config implementation for component API implementations.
 */
@Immutable
public class ComponentApiConnectionConfig {

    private static final long DEFAULT_CONN_RETRY_INTERVAL_S = 30;

    private final String host;

    private final int port;

    private final String userName;

    private final String userPassword;

    private final File sslKeystoreFile;

    private final String sslKeystorePassword;

    private final long silentRetriesTime;

    private final long connRetryIntervalSeconds;

    private final long pongMessageTimeout;

    private ComponentApiConnectionConfig(@Nonnull final String host,
                                         final int port,
                                         @Nullable final String userName,
                                         @Nullable final String userPassword,
                                         @Nullable final File sslKeystoreFile,
                                         @Nullable final String sslKeystorePassword,
                                         final long silentRetriesTime,
                                         final long connRetryIntervalSeconds,
                                         final long pongMessageTimeout) {
        this.host = host;
        this.port = port;
        this.userName = userName;
        this.userPassword = userPassword;
        this.sslKeystoreFile = sslKeystoreFile;
        this.sslKeystorePassword = sslKeystorePassword;
        this.silentRetriesTime = silentRetriesTime;
        this.connRetryIntervalSeconds = connRetryIntervalSeconds;
        this.pongMessageTimeout = pongMessageTimeout;
    }

    @Nonnull
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Nullable
    public String getUserName() {
        return userName;
    }

    @Nullable
    public String getUserPassword() {
        return userPassword;
    }

    @Nullable
    public File getSSLKeystoreFile() {
        return sslKeystoreFile;
    }

    @Nullable
    public String getSSLKeystorePassword() {
        return sslKeystorePassword;
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

    public static Builder newBuilder(@Nonnull final ComponentApiConnectionConfig prototype) {
        return new Builder(Objects.requireNonNull(prototype));

    }

    public static AddressBuilder newBuilder() {
        return new AddressBuilder();
    }

    /**
     * Initial builder to force users of {@link ComponentApiConnectionConfig}
     * to set host and port at compile-time.
     */
    public static class AddressBuilder {
        public Builder setHostAndPort(@Nonnull final String host, final int port) {
            return new Builder(host, port);
        }
    }

    /**
     * Builder for the {@link ComponentApiConnectionConfig}.
     *
     * <p>Use the {@link ComponentApiConnectionConfig#newBuilder()} or
     * {@link ComponentApiConnectionConfig#newBuilder(ComponentApiConnectionConfig)} to initialize.
     */
    public static class Builder {
        private final String host;
        private final int port;

        private String userName = null;
        private String userPassword = null;
        private File sslKeystoreFile = null;
        private String sslKeystorePassword = null;
        private long silentRetriesTime = 0;
        private long connRetryIntervalSeconds = DEFAULT_CONN_RETRY_INTERVAL_S;
        private long pongMessageTimeout = 10000;

        private Builder(@Nonnull final String host, final int port) {
            this.host = Objects.requireNonNull(host);
            this.port = port;
        }

        private Builder(@Nonnull final ComponentApiConnectionConfig prototype) {
            this.host = Objects.requireNonNull(prototype.host);
            this.port = prototype.port;

            this.userName = prototype.getUserName();
            this.userPassword = prototype.getUserPassword();
            this.sslKeystoreFile = prototype.getSSLKeystoreFile();
            this.sslKeystorePassword = prototype.getSSLKeystorePassword();
            this.silentRetriesTime = prototype.getSilentRetriesTime();
            this.connRetryIntervalSeconds = prototype.getConnRetryIntervalSeconds();
            this.pongMessageTimeout = prototype.getPongMessageTimeout();
        }

        public Builder setUserName(@Nonnull final String userName) {
            this.userName = Objects.requireNonNull(userName);
            return this;
        }

        public Builder setUserPassword(@Nonnull final String userPassword) {
            this.userPassword = Objects.requireNonNull(userPassword);
            return this;
        }

        public Builder setSslKeystoreFile(@Nonnull final File sslKeystoreFile) {
            this.sslKeystoreFile = Objects.requireNonNull(sslKeystoreFile);
            return this;
        }

        public Builder setSslKeystorePassword(@Nonnull final String sslKeystorePassword) {
            this.sslKeystorePassword = Objects.requireNonNull(sslKeystorePassword);
            return this;
        }

        public Builder setSilentRetriesTime(final long silentRetriesTime) {
            this.silentRetriesTime = silentRetriesTime;
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

        public ComponentApiConnectionConfig build() {
            return new ComponentApiConnectionConfig(host, port, userName, userPassword,
                    sslKeystoreFile, sslKeystorePassword, silentRetriesTime,
                    connRetryIntervalSeconds, pongMessageTimeout);
        }
    }
}
