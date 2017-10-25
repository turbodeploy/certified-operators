package com.vmturbo.components.test.utilities.communication;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.rules.ExternalResource;

import com.google.common.collect.ImmutableList;

import io.grpc.BindableService;
import io.grpc.ServerBuilder;

import com.vmturbo.components.test.utilities.component.ComponentUtils;

/**
 * The {@link ComponentStubHost} is a single class that can host various websocket notification
 * stubs and gRPC services. Internally it uses a single Jetty server and a gRPC (Netty) server.
 * <p>
 * We want to allow test writers to stub out multiple components (e.g. the History component may
 * want stubs for Market and Topology Processor), and this allows us to do that.
 * <p>
 * For gRPC stubs, the test can inject stub service implementations directly via the
 * {@link ComponentStubHost.Builder#withGrpcServices} method. There is a single gRPC
 * server that contains all the services.
 */
public class ComponentStubHost extends ExternalResource implements AutoCloseable {

    /**
     * The number of seconds to wait for gRPC server to shutdown
     * during the shutdown procedure for the component.
     */
    private static final int GRPC_SHUTDOWN_WAIT_S = 10;

    private final Logger logger = LogManager.getLogger();

    private final List<BindableService> grpcServices;

    private AtomicBoolean started = new AtomicBoolean(false);

    private io.grpc.Server grpcServer;

    private ComponentStubHost(
            @Nonnull final List<BindableService> grpcServices) {
        this.grpcServices = grpcServices;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            logger.info("Component stub already started.");
            return;
        }

        startGrpcServer();
    }

    @Override
    public void close() {
        if (started.get()) {
            stopGrpcServer();
        }
    }

    private void startGrpcServer() {
        if (grpcServices.isEmpty()) {
            logger.info("Not starting a gRPC server - no gRPC service stubs requested.");
            return;
        }

        final ServerBuilder serverBuilder = ServerBuilder.forPort(ComponentUtils.GLOBAL_GRPC_PORT);
        grpcServices.forEach(serverBuilder::addService);
        grpcServer = serverBuilder.build();
        try {
            grpcServer.start();
            logger.info("Initialized gRPC server on port {}.", ComponentUtils.GLOBAL_GRPC_PORT);
        } catch (IOException e) {
            logger.error("Failed to start gRPC server!", e);
            throw new RuntimeException("Unable to start component stub host's gRPC server.", e);
        }
    }

    private void stopGrpcServer() {
        if (grpcServer != null) {
            grpcServer.shutdownNow();
            try {
                if (grpcServer.awaitTermination(GRPC_SHUTDOWN_WAIT_S, TimeUnit.SECONDS)) {
                    logger.info("gRPC server successfully stopped.");
                } else {
                    logger.error("gRPC server failed to stop after {} seconds!",
                            GRPC_SHUTDOWN_WAIT_S);
                }
            } catch (InterruptedException e) {
                logger.error("Interrupted while waiting for gRPC server to stop. " +
                                "gRPC server is: {}",
                        grpcServer.isTerminated() ? "stopped" : "running");
            }
            grpcServer = null;
        }
    }

    /**
     * Builder class for the {@link ComponentStubHost}.
     */
    public static class Builder {

        private ImmutableList.Builder<BindableService> grpcServices = ImmutableList.builder();

        @Nonnull
        public <Service extends BindableService> Builder withGrpcServices(
                @Nonnull final Service... services) {
            for (Service service : services) {
                grpcServices.add(Objects.requireNonNull(service));
            }
            return this;
        }

        @Nonnull
        public ComponentStubHost build() {
            return new ComponentStubHost(grpcServices.build());
        }
    }
}
