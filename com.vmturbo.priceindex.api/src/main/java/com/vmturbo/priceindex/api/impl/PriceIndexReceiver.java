package com.vmturbo.priceindex.api.impl;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.priceindex.api.PriceIndexListener;

/**
 * Receiver-side implementation of the PriceIndex Component.
 */
public class PriceIndexReceiver extends ComponentApiClient<PriceIndexRestReceiver> {

    private final PriceIndexNotificationReceiver notificationReceiver;

    /**
     * The Websocket path used to connect to the service.
     */
    public static final String WEBSOCKET_PATH = "/priceindex-api";

    private PriceIndexReceiver(
            final @Nonnull ComponentApiConnectionConfig connectionConfig,
            final @Nonnull IMessageReceiver<PriceIndexMessage> messageReceiver,
            final @Nonnull ExecutorService executorService) {
        super(connectionConfig);
        this.notificationReceiver =
                new PriceIndexNotificationReceiver(messageReceiver, executorService);

    }

    private PriceIndexReceiver(
            final @Nonnull ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
        this.notificationReceiver = null;
    }

    public static PriceIndexReceiver rpcAndNotification(
            final @Nonnull ComponentApiConnectionConfig connectionConfig,
            final @Nonnull ExecutorService executorService,
            final @Nonnull IMessageReceiver<PriceIndexMessage> messageReceiver) {
        return new PriceIndexReceiver(connectionConfig, messageReceiver, executorService);
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

    public void setPriceIndexListener(final @Nonnull PriceIndexListener listener) {
        if (notificationReceiver == null) {
            throw new IllegalStateException(
                    "Price index client is not set up to receive notifications");
        }
        notificationReceiver.setPriceIndexListener(Objects.requireNonNull(listener));
    }
}
