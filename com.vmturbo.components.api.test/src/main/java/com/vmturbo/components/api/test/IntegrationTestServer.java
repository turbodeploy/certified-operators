package com.vmturbo.components.api.test;

import java.net.URISyntaxException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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
import org.junit.rules.TestName;
import org.springframework.mock.env.MockEnvironment;
import org.springframework.web.context.ContextLoaderListener;
import org.springframework.web.context.support.AnnotationConfigWebApplicationContext;
import org.springframework.web.servlet.DispatcherServlet;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.server.WebsocketNotificationSender;

/**
 * Contains a websocket-enabled server initialized from
 * a customizable configuration class for integration
 * tests of client API's communicating over websocket.
 */
public class IntegrationTestServer implements AutoCloseable {
    public static final String FIELD_TEST_NAME = "test.name";

    private AnnotationConfigWebApplicationContext applicationContext;

    private Server webSocketServer;

    private int serverPort;

    private static final Logger logger = LogManager.getLogger();

    public IntegrationTestServer(TestName testName, Class<?> configurationClass) throws Exception {
        Objects.requireNonNull(testName);
        Objects.requireNonNull(configurationClass);
        logger.trace("Starting web socket endpoint on free port...");
        if (webSocketServer != null) {
            throw new IllegalStateException(
                    "Websocket server should not be started before it is stopped");
        }
        final QueuedThreadPool jettyPool = new QueuedThreadPool();
        jettyPool.setName(testName.getMethodName() + "-jetty");
        webSocketServer = new Server(jettyPool);
        // webSocketServer.set
        final ServerConnector connector = new ServerConnector(webSocketServer);
        connector.setPort(0);
        connector.setReuseAddress(true);
        webSocketServer.addConnector(connector);

        final ServletContextHandler contextServer =
                new ServletContextHandler(ServletContextHandler.SESSIONS);

        applicationContext = new AnnotationConfigWebApplicationContext();
        final MockEnvironment env = new MockEnvironment();
        env.setProperty(FIELD_TEST_NAME, testName.getMethodName());
        env.setProperty("websocket.pong.timeout", Long.toString(10000));
        applicationContext.setEnvironment(env);
        applicationContext.register(configurationClass);

        final Servlet dispatcherServlet = new DispatcherServlet(applicationContext);
        final ServletHolder servletHolder = new ServletHolder(dispatcherServlet);
        contextServer.addServlet(servletHolder, "/*");

        // Setup Spring context
        final ContextLoaderListener springListener = new ContextLoaderListener(applicationContext);
        contextServer.addEventListener(springListener);

        webSocketServer.setHandler(contextServer);

        // Enable websocket in Jetty
        WebSocketServerContainerInitializer.configureContext(contextServer);
        // Start the jetty server
        webSocketServer.start();
        serverPort = connector.getLocalPort();
        connector.setPort(serverPort);

        logger.trace("Started on port " + serverPort);
    }

    public void waitForRegisteredEndpoints(final int numEndpoints, final long timeoutMs)
            throws InterruptedException, TimeoutException {
        final WebsocketNotificationSender notificationSender =
                getBean(WebsocketNotificationSender.class);
        notificationSender.waitForEndpoints(numEndpoints, timeoutMs, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() throws Exception {
        webSocketServer.stop();
        webSocketServer.join();
    }

    public <T> T getBean(Class<T> requiredType) {
        return applicationContext.getBeansOfType(requiredType).values().iterator().next();
    }

    @SuppressWarnings("unchecked")
    public <T> T getBean(@Nonnull String name) {
        return (T)applicationContext.getBean(name);
    }

    public ComponentApiConnectionConfig connectionConfig() throws URISyntaxException {
        return ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort("localhost", serverPort)
                .setConnRetryIntervalSeconds(1)
                .build();
    }
}
