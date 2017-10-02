package com.vmturbo.priceindex.api.impl;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.google.protobuf.CodedInputStream;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.priceindex.api.PriceIndexListener;

/**
 * The websocket client connecting to the Price Index sender.
 */
class PriceIndexNotificationReceiver extends
        ComponentNotificationReceiver<PriceIndexMessage> {

    /**
     * The listener for the server-side message processing.
     */
    private PriceIndexListener listener_ = null;

    /**
     * Constructs the websocket client.
     *
     * @param connectionConfig The connection config.
     * @param executorService  The executor service.
     */
    PriceIndexNotificationReceiver(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        super(connectionConfig, executorService);
    }

    /**
     * Sets the websocket path. The method name is weird.
     *
     * @param serverAddress The server address path (e.g. ws://component:123)
     * @return The websocket path.
     */
    @Nonnull
    @Override
    protected String addWebsocketPath(@Nonnull final String serverAddress) {
        return serverAddress + PriceIndexReceiver.WEBSOCKET_PATH;
    }

    @Nonnull
    @Override
    protected PriceIndexDTOs.PriceIndexMessage parseMessage(@Nonnull CodedInputStream bytes) throws IOException {
        return PriceIndexDTOs.PriceIndexMessage.parseFrom(bytes);
    }

    /**
     * Adds the server-side (receiver) messages listener.
     *
     * @param listener The server-side (receiver) messages listener.
     */
    void setPriceIndexListener(@Nonnull final PriceIndexListener listener) {
        listener_ = listener;
    }

    /**
     * Processes the server-side (receiver) messages listener.
     *
     * @param message The received message.
     * @throws ApiClientException In case of an error processing the message.
     */
    @Override
    protected void processMessage(@Nonnull final PriceIndexDTOs.PriceIndexMessage message)
            throws ApiClientException {
        if (listener_ != null) {
            try {
                listener_.onPriceIndexReceived(message);
            } catch (RuntimeException e) {
                logger.error("Error executing entities notification for listener " + listener_,
                             e);
            }
        }
    }

}
