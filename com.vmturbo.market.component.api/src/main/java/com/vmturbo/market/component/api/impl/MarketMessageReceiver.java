package com.vmturbo.market.component.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.market.component.dto.MarketMessages.MarketComponentNotification;

/**
 * Websocket message receiver for Market clients.
 */
public class MarketMessageReceiver extends
        WebsocketNotificationReceiver<MarketComponentNotification> {

    /**
     * Constructs market message receiver.
     * @param connectionConfig connection configuration
     * @param threadPool thread pool to use
     */
    public MarketMessageReceiver(@Nonnull ComponentApiConnectionConfig connectionConfig,
            @Nonnull ExecutorService threadPool) {
        super(connectionConfig, MarketComponentClient.WEBSOCKET_PATH, threadPool,
                MarketComponentNotification::parseFrom);
    }
}


