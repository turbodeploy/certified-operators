package com.vmturbo.components.api.test;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

/**
 * A wrapper around {@link io.grpc.inprocess.InProcessServer} with built-in
 * handling of names and channels.
 */
public class GrpcTestServer implements AutoCloseable {
    private static AtomicLong INSTANCE_COUNTER = new AtomicLong(0);

    private final Server grpcServer;

    private final ManagedChannel channel;

    private GrpcTestServer(final BindableService... services) throws IOException {
        String name = "grpc-test-" + INSTANCE_COUNTER.getAndIncrement();
        final InProcessServerBuilder serverBuilder = InProcessServerBuilder.forName(name);
        for (final BindableService service : services) {
            serverBuilder.addService(service);
        }
        grpcServer = serverBuilder.build();
        grpcServer.start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    public static GrpcTestServer withServices(final BindableService... services) throws IOException {
        return new GrpcTestServer(services);
    }

    /**
     * Get a channel to communicate with the server.
     *
     * @return The channel. All calls to this method return the same channel.
     */
    @Nonnull
    public Channel getChannel() {
        return channel;
    }

    @Override
    public void close() {
        channel.shutdownNow();
        grpcServer.shutdownNow();
    }
}
