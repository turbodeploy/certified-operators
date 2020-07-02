package com.vmturbo.components.api;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.context.ConfigurableWebApplicationContext;

/**
 * Utility class that allows static singletons to hook into the initialization of a component's server
 * and Spring context.
 *
 * <p/>The singleton should implement the {@link ServerStartedListener} interface, and call
 * {@link ServerStartedNotifier#registerListener(ServerStartedListener)}. It will receive a
 * callback once the component server is up, and the Spring context is initialized.
 */
public class ServerStartedNotifier {
    private static final Logger logger = LogManager.getLogger();

    private static final ServerStartedNotifier INSTANCE = new ServerStartedNotifier();

    private final Set<ServerStartedListener> listeners = Collections.synchronizedSet(new HashSet<>());

    private volatile ConfigurableWebApplicationContext serverContext = null;

    private ServerStartedNotifier() {
    }

    /**
     * Return the singleton instance of the {@link ServerStartedNotifier}.
     *
     * @return The {@link ServerStartedNotifier} instance.
     */
    @Nonnull
    public static ServerStartedNotifier get() {
        return INSTANCE;
    }

    /**
     * Method called to notify all instances that the component server started.
     *
     * @param serverContext The Spring context of the component server.
     */
    public void notifyServerStarted(ConfigurableWebApplicationContext serverContext) {
        synchronized (listeners) {
            this.serverContext = serverContext;
            logger.info("Notifying listeners about server start. Listeners: {}",
                listeners.stream()
                    .map(l -> l.getClass().getSimpleName())
                    .collect(Collectors.joining(",")));
            listeners.forEach(listener -> {
                try {
                    listener.onServerStarted(serverContext);
                } catch (RuntimeException e) {
                    logger.error("Listener {} failed.", listener.getClass().getSimpleName(), e);
                }
            });
        }
    }

    /**
     * Register a listener, which will receive a callback once the component server started.
     * Note - if you register a listener AFTER the server started, the listener will not be called.
     *
     * @param listener The {@link ServerStartedListener}.
     */
    public void registerListener(ServerStartedListener listener) {
        if (this.serverContext != null) {
            logger.warn("Listener {} registered after server started. Giving it the saved context.",
                listener.getClass().getSimpleName());
            listener.onServerStarted(this.serverContext);
        } else {
            listeners.add(listener);
        }
    }

    /**
     * A listener for the component startup notification.
     */
    @FunctionalInterface
    public interface ServerStartedListener {
        /**
         * The component server started. The Spring context is initialized.
         *
         * @param serverContext The Spring context of the component.
         */
        void onServerStarted(ConfigurableWebApplicationContext serverContext);
    }

}
