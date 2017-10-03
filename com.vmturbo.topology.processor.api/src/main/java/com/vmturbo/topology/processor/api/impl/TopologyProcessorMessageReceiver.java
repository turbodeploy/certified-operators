package com.vmturbo.topology.processor.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.topology.processor.api.TopologyProcessorDTO.TopologyProcessorNotification;

/**
 * Websocket message receiver for topology notifications.
 */
public class TopologyProcessorMessageReceiver extends
        WebsocketNotificationReceiver<TopologyProcessorNotification> {

    /**
     * Constructs topology processor message receiver
     * @param connectionConfig connection configuration
     * @param threadPool thread pool to use
     */
    public TopologyProcessorMessageReceiver(@Nonnull ComponentApiConnectionConfig connectionConfig,
            @Nonnull ExecutorService threadPool) {
        super(connectionConfig, TopologyProcessorClient.WEBSOCKET_PATH, threadPool,
                TopologyProcessorNotification::parseFrom);
    }
}
