package com.vmturbo.components.common;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import io.grpc.BindableService;
import io.grpc.Channel;
import io.grpc.Server;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.NettyServerBuilder;
import io.opentracing.contrib.grpc.ServerTracingInterceptor;

import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.core.env.ConfigurableEnvironment;

import com.vmturbo.components.api.GrpcChannelFactory;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.components.common.grpc.RequestLoggingInterceptor;

/**
 * A wrapper around a gRPC {@link Server} that can be used inside a component. Used to isolate
 * the construction and lifecycle of the gRPC server from other parts of component initialization.
 *
 * <p>The {@link ComponentGrpcServer} is a singleton instance - there is only one per component
 * JVM. Obtain references to the singleton using {@link ComponentGrpcServer#get()}.
 *
 * <p>The intended lifecycle is - call {@link ComponentGrpcServer#addServices(List, List)} during
 * component construction to configure the services that will be bound to this server. Then call
 * {@link ComponentGrpcServer#start(ConfigurableEnvironment)} to initialize the server. At shutdown or restart, call
 * {@link ComponentGrpcServer#stop()}.
 */
@ThreadSafe
public class ComponentGrpcServer {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The number of seconds to wait for gRPC server to shutdown
     * during the shutdown procedure for the component.
     */
    private static final int GRPC_SHUTDOWN_WAIT_S = 10;

    /**
     * The minimum acceptable server-side keepalive rate.
     *
     * <p>In gRPC, the server only accepts keepalives every 5 minutes by default.
     * We want to set it a little lower. The server will reject keepalives coming
     * in at a greater rate.
     */
    private static final int GRPC_MIN_KEEPALIVE_TIME_MIN = 1;

    /**
     * The name of the environment property that should be overridden to specify a port
     * for the gRPC server to listen on.
     */
    public static final String PROP_SERVER_GRPC_PORT = "serverGrpcPort";

    /**
     * The name of the environment property that should be overriden to specify the maximum
     * message size for the gRPC server of this component.
     */
    public static final String PROP_GRPC_MAX_MESSAGE_BYTES = "grpcMaxMessageBytes";

    /**
     * The default max message size - 4MB - which can be overriden by setting the
     * {@link ComponentGrpcServer#PROP_GRPC_MAX_MESSAGE_BYTES} property.
     */
    private static final int DEFAULT_GRPC_MAX_MESSAGE_BYTES = 4194304;

    /**
     * The singleton instance.
     */
    private static final ComponentGrpcServer INSTANCE = new ComponentGrpcServer();

    /**
     * The actual server, initialized during the call to {@link ComponentGrpcServer#start(ConfigurableEnvironment)}.
     */
    @GuardedBy("grpcServerLock")
    private Server grpcServer = null;

    @GuardedBy("grpcServerLock")
    private ConfigurableEnvironment configurableEnvironment = null;

    private final Object grpcServerLock = new Object();

    /**
     * Interceptors to add to every service in the gRPC server.
     */
    private final List<ServerInterceptor> defaultInterceptors;

    private Map<String, ServerServiceDefinition> serviceDefinitions = new HashMap<>();

    private ComponentGrpcServer() {
        defaultInterceptors = ImmutableList.of(
            // Interceptor for gRPC Prometheus metrics.
            MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics()),
            // Add tracing interceptor at the end, so that it gets called first (Matthew 20:16 :P),
            // and the other interceptors get traced too.
            new ServerTracingInterceptor(Tracing.tracer()),
            // Log all the requests timings
            new RequestLoggingInterceptor(),
            // Last, so that it gets called first, and catches any unhandled exceptions from the
            // call or any of the other interceptors.
            new GrpcCatchExceptionInterceptor());
    }

    /**
     * Get the singleton instance of the server. There is only one server per component.
     *
     * @return The {@link ComponentGrpcServer}.
     */
    @Nonnull
    public static ComponentGrpcServer get() {
        return INSTANCE;
    }

    /**
     * Gets a channel to the server over "localhost". Should only be used for testing!
     *
     * @return The {@link Channel}.
     */
    @VisibleForTesting
    Channel getChannel() {
        synchronized (grpcServerLock) {
            return GrpcChannelFactory.newChannelBuilder("localhost", grpcServer.getPort())
                .build();
        }
    }

    /**
     * Add gRPC services to the server. Call this as many times as necessary before starting the
     * server. After starting the server (via {@link ComponentGrpcServer#start(ConfigurableEnvironment)}) no more calls
     * to this method are allowed.
     *
     * @param services The list of services to add.
     * @param interceptors The interceptors to add to these services. Each of these interceptors
     *                     will be added to each of the services.
     */
    public void addServices(@Nonnull final List<BindableService> services,
                            @Nonnull final List<ServerInterceptor> interceptors) {
        synchronized (grpcServerLock) {
            final List<ServerInterceptor> allInterceptors = Lists.newArrayList(interceptors);
            allInterceptors.addAll(defaultInterceptors);
            services.forEach(service -> {
                ServerServiceDefinition serviceDefn = ServerInterceptors.intercept(service, allInterceptors);
                serviceDefinitions.put(serviceDefn.getServiceDescriptor().getName(), serviceDefn);
            });

            // If the gRPC server is already running, we need to stop and restart it so that the
            // new services can register.
            if (grpcServer != null) {
                logger.info("Restarting gRPC server...");
                stop();
                start(configurableEnvironment);
            }
        }
    }

    /**
     * Start the gRPC server.
     *
     * <p>This should be called after all services are added to the server
     * (via {@link ComponentGrpcServer#addServices(List, List)}). Any service additions after
     * that will have no effect.
     */
    public void start(@Nonnull final ConfigurableEnvironment environment) {
        synchronized (grpcServerLock) {
            final int serverPort = Integer.parseInt(environment.getRequiredProperty(PROP_SERVER_GRPC_PORT));
            final int grpcMaxMessageBytes = Integer.parseInt(
                environment.getProperty(PROP_GRPC_MAX_MESSAGE_BYTES,
                Integer.toString(DEFAULT_GRPC_MAX_MESSAGE_BYTES)));
            final NettyServerBuilder serverBuilder = NettyServerBuilder.forPort(serverPort)
                // Allow keepalives even when there are no existing calls, because we want
                // to send intermittent keepalives to keep the http2 connections open.
                .permitKeepAliveWithoutCalls(true)
                .permitKeepAliveTime(GRPC_MIN_KEEPALIVE_TIME_MIN, TimeUnit.MINUTES)
                .maxMessageSize(grpcMaxMessageBytes);

            serviceDefinitions.values().forEach(serverBuilder::addService);

            try {
                configurableEnvironment = environment;
                grpcServer = serverBuilder.build();
                grpcServer.start();
                logger.info("Initialized gRPC with services: {} on port {}.",
                    serviceDefinitions.keySet(), serverPort);
            } catch (IOException e) {
                logger.error("Failed to start gRPC server. gRPC methods will not be available!", e);
                stop();
            }
        }
    }

    /**
     * Stop the component's gRPC server. This method will block while the server shuts down
     * (completing/terminating existing calls).
     */
    public void stop() {
        synchronized (grpcServerLock) {
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
    }
}
