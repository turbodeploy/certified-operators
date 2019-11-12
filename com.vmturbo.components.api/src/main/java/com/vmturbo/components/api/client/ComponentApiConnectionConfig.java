package com.vmturbo.components.api.client;

import java.io.File;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.StringUtils;

/**
 * Connection config implementation for component API implementations.
 */
@Immutable
public class ComponentApiConnectionConfig {


    private final String host;

    private final int port;

    private final String route;

    private final String userName;

    private final String userPassword;

    private final File sslKeystoreFile;

    private final String sslKeystorePassword;

    protected ComponentApiConnectionConfig(@Nonnull final String host,
                                         final int port,
                                         @Nonnull final String route,
                                         @Nullable final String userName,
                                         @Nullable final String userPassword,
                                         @Nullable final File sslKeystoreFile,
                                         @Nullable final String sslKeystorePassword) {
        this.host = host;
        this.port = port;
        this.route = route;
        this.userName = userName;
        this.userPassword = userPassword;
        this.sslKeystoreFile = sslKeystoreFile;
        this.sslKeystorePassword = sslKeystorePassword;
    }

    @Nonnull
    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    @Nonnull
    public String getRoute() {
        return route;
    }

    /**
     * Get the URI to use to access the component.
     *
     * @return A URI that can be used for http connections to the component's Jetty server.
     */
    @Nonnull
    public String getUri() {
         String uri = "http://" + getHost() + ":" + getPort();
         if (!StringUtils.isEmpty(getRoute())) {
             return uri + "/" + getRoute();
         } else {
             return uri;
         }
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

    public static AddressBuilder newBuilder() {
        return new AddressBuilder();
    }

    /**
     * Initial builder to force users of {@link ComponentApiConnectionConfig}
     * to set host and port at compile-time.
     */
    public static class AddressBuilder {

        /**
         * Set the host, port, and route to use to connect to the target component.
         * @param host The hostname.
         * @param port The port.
         * @param route The route to use as a prefix for all URIs used to access this component.
         * @return A {@link Builder} to finish customizing the connection config.
         */
        public Builder setHostAndPort(@Nonnull final String host, final int port, @Nonnull final String route) {
            return new Builder(host, port, route);
        }
    }

    /**
     * Abstract implementation of connection configuration builder.
     *
     * @param <T> type of descendant class
     */
    protected abstract static class AbstractBuilder<T extends AbstractBuilder> {
        private final String host;
        private final int port;
        private final String route;

        private String userName = null;
        private String userPassword = null;
        private File sslKeystoreFile = null;
        private String sslKeystorePassword = null;

        protected AbstractBuilder(@Nonnull final String host,
                                  final int port,
                                  @Nonnull final String route) {
            this.host = Objects.requireNonNull(host);
            this.port = port;
            this.route = route;
        }

        public T setUserName(@Nonnull final String userName) {
            this.userName = Objects.requireNonNull(userName);
            return getSelf();
        }

        public T setUserPassword(@Nonnull final String userPassword) {
            this.userPassword = Objects.requireNonNull(userPassword);
            return getSelf();
        }

        public T setSslKeystoreFile(@Nonnull final File sslKeystoreFile) {
            this.sslKeystoreFile = Objects.requireNonNull(sslKeystoreFile);
            return getSelf();
        }

        public T setSslKeystorePassword(@Nonnull final String sslKeystorePassword) {
            this.sslKeystorePassword = Objects.requireNonNull(sslKeystorePassword);
            return getSelf();
        }


        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }

        public String getRoute() {
            return route;
        }

        public String getUserName() {
            return userName;
        }

        public String getUserPassword() {
            return userPassword;
        }

        public File getSslKeystoreFile() {
            return sslKeystoreFile;
        }

        public String getSslKeystorePassword() {
            return sslKeystorePassword;
        }

        protected abstract T getSelf();
    }

    /**
     * Builder for the {@link ComponentApiConnectionConfig}.
     *
     * <p>Use the {@link ComponentApiConnectionConfig#newBuilder()} or
     * {@link ComponentApiConnectionConfig#newBuilder()} to initialize.
     */
    public static class Builder extends AbstractBuilder<Builder> {

        private Builder(@Nonnull String host, int port, @Nonnull final String route) {
            super(host, port, route);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }

        public ComponentApiConnectionConfig build() {
            return new ComponentApiConnectionConfig(getHost(), getPort(), getRoute(), getUserName(),
                    getUserPassword(), getSslKeystoreFile(), getSslKeystorePassword());
        }
    }
}
