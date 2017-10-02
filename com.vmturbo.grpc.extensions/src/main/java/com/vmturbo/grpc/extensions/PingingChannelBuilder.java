package com.vmturbo.grpc.extensions;

import java.net.SocketAddress;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;

import io.grpc.Internal;
import io.grpc.internal.ClientTransportFactory;
import io.grpc.internal.ConnectionClientTransport;
import io.grpc.netty.NettyChannelBuilder;

/**
 * A ChannelBuilder that permits configuration for sending keep-alive pings over a Netty channel.
 *
 * Note that this class extends a class marked as @Experimental in the gRPC library.
 * See https://github.com/grpc/grpc-java/issues/1784
 */
public class PingingChannelBuilder extends NettyChannelBuilder {
    private long pingIntervalNanos = TimeUnit.MINUTES.toNanos(1);

    /**
     * Creates a new builder with the given host and port.
     *
     * @param host The host at which the transport will connect.
     * @param port The port to connect to.
     * @return a new builder with the given host and port.
     */
    public static PingingChannelBuilder forAddress(String host, int port) {
        return new PingingChannelBuilder(host, port);
    }

    /**
     * Creates a new builder with the given target string that will be resolved by
     * {@link io.grpc.NameResolver}.
     *
     * @param target The target to be resolved.
     * @return a new builder with the given target string that will be resolved by
     *         {@link io.grpc.NameResolver}.
     */
    public static PingingChannelBuilder forTarget(String target) {
        return new PingingChannelBuilder(target);
    }

    /**
     * Construct a {@link PingingChannelBuilder} given a host and port.
     *
     * @param host The host to which the channel should connect.
     * @param port The port to which the client should connect.
     */
    protected PingingChannelBuilder(@Nonnull final String host, final int port) {
        super(host, port);
    }

    /**
     * Creates a new builder with the given target string that will be resolved by
     * {@link io.grpc.NameResolver}.
     *
     * @param target The target to be resolved.
     */
    protected PingingChannelBuilder(String target) {
        super(target);
    }

    @Override
    protected ClientTransportFactory buildTransportFactory() {
        return buildTransportFactory(super.buildTransportFactory());
    }

    @VisibleForTesting
    ClientTransportFactory buildTransportFactory(@Nonnull final ClientTransportFactory delegateFactory) {
        return new PingingTransportFactory(delegateFactory, pingIntervalNanos);
    }

    /**
     * Set the interval at which we will send pings over the transport.
     * The intention is that these pings will prevent an inactivity timeout from being hit that silently
     * kills the underlying channel over which our RPCs are sent.
     *
     * The interval is given a floor of {@link PingRunner#MIN_PING_DELAY_NANOS}.
     *
     * @param pingInterval The interval at which to send pings.
     * @param intervalUnits The units for the interval.
     * @return A reference to {@link this} for method chaining.
     */
    public PingingChannelBuilder setPingInterval(final long pingInterval, @Nonnull final TimeUnit intervalUnits) {
        pingIntervalNanos = intervalUnits.toNanos(pingInterval);

        return this;
    }

    /**
     * Creates Pinging transports. Exposed for internal use.
     */
    @Internal
    protected static final class PingingTransportFactory implements ClientTransportFactory {
        private boolean closed;
        private long pingIntervalNanos;
        private ClientTransportFactory delegateTransportFactory;

        public PingingTransportFactory(@Nonnull final ClientTransportFactory delegateTransportFactory,
                                       final long pingIntervalNanos) {
            this.delegateTransportFactory = Objects.requireNonNull(delegateTransportFactory);
            this.pingIntervalNanos = pingIntervalNanos;
        }

        @Override
        public ConnectionClientTransport newClientTransport(SocketAddress serverAddress,
                                                            String authority, @Nullable String userAgent) {
            final ConnectionClientTransport delegateTransport = delegateTransportFactory
                .newClientTransport(serverAddress, authority, userAgent);

            return new PingingClientTransport(delegateTransport, pingIntervalNanos);
        }

        @Override
        public void close() {
            if (closed) {
                return;
            }

            delegateTransportFactory.close();
            closed = true;
        }
    }
}
