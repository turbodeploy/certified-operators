package com.vmturbo.priceindex.api.impl;

import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.priceindex.api.PriceIndexListener;

import javax.annotation.Nonnull;
import java.util.Objects;
import java.util.concurrent.ExecutorService;

/**
 * Receiver-side implementation of the PriceIndex Component.
 */
public class PriceIndexReceiver
        extends
        ComponentApiClient<PriceIndexRestReceiver, PriceIndexNotificationReceiver> {

    /**
     * The Websocket path used to connect to the service.
     */
    public static final String WEBSOCKET_PATH = "/priceindex-api";

    private PriceIndexReceiver(final @Nonnull ComponentApiConnectionConfig connectionConfig,
                               final @Nonnull ExecutorService executorService) {
        super(connectionConfig, executorService);
    }

    private PriceIndexReceiver(
            final @Nonnull ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
    }

    public static PriceIndexReceiver rpcAndNotification(
            final @Nonnull ComponentApiConnectionConfig connectionConfig,
            final @Nonnull ExecutorService executorService) {
        return new PriceIndexReceiver(connectionConfig, executorService);
    }

    public static PriceIndexReceiver rpcOnly(
            final @Nonnull ComponentApiConnectionConfig connectionConfig) {
        return new PriceIndexReceiver(connectionConfig);
    }

    @Override
    @Nonnull
    protected PriceIndexRestReceiver createRestClient(
            final @Nonnull ComponentApiConnectionConfig connectionConfig) {
        return new PriceIndexRestReceiver(connectionConfig);
    }

    @Override
    @Nonnull
    protected PriceIndexNotificationReceiver createWebsocketClient(
            final @Nonnull ComponentApiConnectionConfig connectionConfig,
            final @Nonnull ExecutorService executorService) {
        return new PriceIndexNotificationReceiver(connectionConfig, executorService);
    }

    public void setPriceIndexListener(final @Nonnull PriceIndexListener listener) {
        websocketClient().setPriceIndexListener(Objects.requireNonNull(listener));
    }
}
