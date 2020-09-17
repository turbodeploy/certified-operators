package com.vmturbo.components.api.test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nonnull;

import org.junit.rules.ExternalResource;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

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
public class GrpcTestServer extends ExternalResource implements AutoCloseable {
    private static AtomicLong INSTANCE_COUNTER = new AtomicLong(0);

    private final List<BindableService> services;

    private Server grpcServer;

    private ManagedChannel channel;

    private GrpcTestServer(final List<BindableService> services) {
        this.services = services;
    }

    /**
     * Create a new {@link GrpcTestServer} with a list of services.
     *
     * @param services The services to add to the server.
     * @return The {@link GrpcTestServer}.
     */
    public static GrpcTestServer newServer(BindableService... services) {
        return new GrpcTestServer(Arrays.asList(services));
    }

    @Override
    protected void before() throws Throwable {
        start();
    }

    @Override
    protected void after() {
        close();
    }

    /**
     * Get a channel to communicate with the server.
     *
     * @return The channel. All calls to this method return the same channel.
     */
    @Nonnull
    public ManagedChannel getChannel() {
        if (channel == null) {
            throw new IllegalStateException(getClass().getSimpleName()
                    + " has not been started yet. Please call start() before");
        }
        return channel;
    }

    /**
     * Start the server. The preferred method for tests is to start the {@link GrpcTestServer} in
     * a @Rule annotation. If you can't use @Rule, create a server and use
     * {@link GrpcTestServer#start()}. If you start it this way you have to call
     * {@link GrpcTestServer#close()}!
     *
     * @throws IOException If there is an error starting up.
     */
    public void start() throws IOException {
        final String name = "grpc-test-" + INSTANCE_COUNTER.getAndIncrement();
        final InProcessServerBuilder serverBuilder = InProcessServerBuilder.forName(name);
        for (final BindableService service : services) {
            serverBuilder.addService(service);
        }
        grpcServer = serverBuilder.build();
        grpcServer.start();
        channel = InProcessChannelBuilder.forName(name).build();
    }

    @Override
    public void close() {
        channel.shutdownNow();
        grpcServer.shutdownNow();
    }
}
