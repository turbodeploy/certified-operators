package com.vmturbo.components.test.utilities.communication;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;
import javax.servlet.Servlet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.eclipse.jetty.websocket.jsr356.server.deploy.WebSocketServerContainerInitializer;
import org.junit.rules.ExternalResource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;
import org.springframework.web.socket.server.standard.ServerEndpointExporter;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

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
 * For websocket stubs, each component should have an associated {@link NotificationSenderStub} that
 * provides a {@link StubConfiguration} to create the component's {@link ComponentApiBackend} and
 * register the URL to access the backend. We put all of these configurations into a single Spring
 * context, and boot up a Jetty server with that context.
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

    private final List<NotificationSenderStub> notificationSenderStubs;

    private final List<BindableService> grpcServices;

    private AnnotationConfigWebApplicationContext applicationContext;

    private Server webSocketServer;

    private AtomicBoolean started = new AtomicBoolean(false);

    private io.grpc.Server grpcServer;

    private ComponentStubHost(@Nonnull final List<NotificationSenderStub> notificationSenderStubs,
                              @Nonnull final List<BindableService> grpcServices) {
        this.notificationSenderStubs = notificationSenderStubs;
        this.grpcServices = grpcServices;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    /**
     * The main server configuration contains beans that are shared by
     * all {@link StubConfiguration}s.
     */
    @Configuration
    public static class MainConfiguration {

        @Bean
        public ExecutorService apiServerThreadPool() {
            final ThreadFactory threadFactory =
                    new ThreadFactoryBuilder().setNameFormat("perf-test-%d").build();
            return Executors.newCachedThreadPool(threadFactory);
        }

        /**
         * This single bean exports all endpoints registered via ServerEndpointRegistration beans.
         */
        @Bean
        public ServerEndpointExporter endpointExporter() {
            return new ServerEndpointExporter();
        }
    }

    /**
     * The parent class for individual components' stub configurations.
     * <p>
     * Child classes have to define two beans:
     * 1) The <Component>ApiBackend bean.
     * 2) A ServerEndpointRegistration bean to register the backend.
     * See {@link MarketStub} for a reference implementation.
     *
     * IMPORTANT: the bean names have to be unique (e.g. prefixed with the component name).
     *
     * TODO (roman, April 17 2017): We can add runtime analysis of the configuration to verify
     * that everything is setup correctly, but the Spring wrangling to do that isn't worth it
     * given the small amount of stubs we actually need.
     */
    @Configuration
    public static class StubConfiguration {

        /**
         * This should get wired in from {@link MainConfiguration} once all the configurations
         * are registered in the application context.
         */
        @Autowired
        protected ExecutorService threadPool;
    }

    public void start() {
        if (!started.compareAndSet(false, true)) {
            logger.info("Component stub already started.");
            return;
        }

        startWebsocketServer();
        startGrpcServer();
    }

    @Override
    public void close() {
        if (started.get()) {
            stopGrpcServer();
            stopWebsocketServer();
        }
    }

    private void startWebsocketServer() {
        if (webSocketServer != null) {
            throw new IllegalStateException(
                    "Websocket server should not be started before it is stopped");
        }

        if (notificationSenderStubs.isEmpty()) {
            logger.info("Not starting a websocket server - no websocket stubs requested.");
            return;
        }

        final QueuedThreadPool jettyPool = new QueuedThreadPool();
        webSocketServer = new Server(jettyPool);

        final ServerConnector connector = new ServerConnector(webSocketServer);
        connector.setPort(ComponentUtils.GLOBAL_HTTP_PORT);
        connector.setReuseAddress(true);

        webSocketServer.addConnector(connector);

        final ServletContextHandler contextServer =
                new ServletContextHandler(ServletContextHandler.SESSIONS);

        applicationContext = new AnnotationConfigWebApplicationContext();

        final MockEnvironment env = new MockEnvironment();
        applicationContext.setEnvironment(env);

        // Add all the stub configs.
        applicationContext.register(MainConfiguration.class);
        notificationSenderStubs.forEach(stub -> applicationContext.register(stub.getConfiguration()));

        final Servlet dispatcherServlet = new DispatcherServlet(applicationContext);
        final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);
        contextServer.addServlet(servletHolder, "/*");

        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(applicationContext);
        contextServer.addEventListener(springListener);

        webSocketServer.setHandler(contextServer);

        try {
            // Enable websocket in Jetty
            WebSocketServerContainerInitializer.configureContext(contextServer);

            // Start the jetty server
            webSocketServer.start();
        } catch (Exception e) {
            throw new RuntimeException("Failed to start component stub host websocket server.", e);
        }

        logger.info("Started websocket server...");

        try {
            notificationSenderStubs.forEach(stub -> stub.initialize(applicationContext));
        } catch (RuntimeException e) {
            logger.info("Error initializing stub.", e);
        }
    }

    private void stopWebsocketServer() {
        if (webSocketServer != null) {
            // TODO (roman, March 17 2017): There are a bunch of error logs that get printed
            // after this point, which suggests that there may be a cleaner way to shut
            // everything down. It's not a high priority, but we should fix it.
            try {
                webSocketServer.stop();
                webSocketServer.join();
            } catch (Exception e) {
                logger.error("Failed to stop websocket server.", e);
            }
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

        private ImmutableList.Builder<NotificationSenderStub> websocketStubs = ImmutableList.builder();
        private ImmutableList.Builder<BindableService> grpcServices = ImmutableList.builder();

        @Nonnull
        public <Stub extends NotificationSenderStub> Builder withNotificationStubs(
                @Nonnull final Stub... stubs) {
            for (Stub stub : stubs) {
                websocketStubs.add(Objects.requireNonNull(stub));
            }
            return this;
        }

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
            return new ComponentStubHost(websocketStubs.build(), grpcServices.build());
        }
    }
}
