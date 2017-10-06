package com.vmturbo.components.api.client;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The root class API Client implementations should extend.
 *
 * @param <RestClient> The REST client used for RPC's to the server.
 * @param <WebsocketClient> The websocket client used to listen for notifications from the server.
 */
public abstract class ComponentApiClient<RestClient extends ComponentRestClient,
        WebsocketClient extends ComponentNotificationReceiver>
        implements AutoCloseable {

    protected final Logger logger = LogManager.getLogger();

    /**
     * The client used for Websocket communication. It's only
     * initialized if the user wants to receive notifications.
     */
    private final WebsocketClient websocketClient;

    /**
     * The client used for REST API calls.
     */
    protected final RestClient restClient;

    protected ComponentApiClient(
            @Nonnull final ComponentApiConnectionConfig connectionConfig) {
        this.restClient = createRestClient(connectionConfig);
        this.websocketClient = null;
    }

    protected ComponentApiClient(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        this.restClient = createRestClient(connectionConfig);
        this.websocketClient = createWebsocketClient(connectionConfig, executorService);
    }

    @Nonnull
    protected WebsocketClient websocketClient() {
        if (websocketClient == null) {
            throw new IllegalStateException("Attempting to use the websocket client of a RPC-only "
                    + getClass() + "! Create an RPC and Notification client instead!");
        }
        return websocketClient;
    }

    @Override
    public void close() {
        if (websocketClient != null) {
            logger.debug("closing websocketClient {}", websocketClient);
            this.websocketClient.close();
        }
    }

    @Nonnull
    protected abstract RestClient createRestClient(@Nonnull final ComponentApiConnectionConfig connectionConfig);

    @Nonnull
    protected abstract WebsocketClient createWebsocketClient(
            @Nonnull final ComponentApiConnectionConfig apiClient,
            @Nonnull final ExecutorService executorService);
}
