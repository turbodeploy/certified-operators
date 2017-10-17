package com.vmturbo.priceindex.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.ComponentNotificationReceiver;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs;
import com.vmturbo.platform.analysis.protobuf.PriceIndexDTOs.PriceIndexMessage;
import com.vmturbo.priceindex.api.PriceIndexListener;

/**
 * A receiver for price index messages
 */
public class PriceIndexNotificationReceiver extends
        ComponentNotificationReceiver<PriceIndexMessage> {

    public static final String PRICE_INDICES_TOPIC = "price-indices";

    /**
     * The listener for the server-side message processing.
     */
    private PriceIndexListener listener_ = null;

    /**
     * Constructs the websocket client.
     *
     * @param messageReceiver The message receiver to get messages from
     * @param executorService  The executor service.
     */
    public PriceIndexNotificationReceiver(
            @Nonnull final IMessageReceiver<PriceIndexMessage> messageReceiver,
            @Nonnull final ExecutorService executorService) {
        super(messageReceiver, executorService);
    }

    /**
     * Adds the server-side (receiver) messages listener.
     *
     * @param listener The server-side (receiver) messages listener.
     */
    public void setPriceIndexListener(@Nonnull final PriceIndexListener listener) {
        if (listener_ != null) {
            throw new IllegalStateException("Listener is already set");
        }
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
                getLogger().error("Error executing entities notification for listener " + listener_,
                        e);
            }
        }
    }

}
