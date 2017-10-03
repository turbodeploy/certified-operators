package com.vmturbo.repository.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;
import com.vmturbo.repository.api.RepositoryDTO.RepositoryNotification;

/**
 * Websocket message receiver for repository component.
 */
public class RepositoryMessageReceiver extends
        WebsocketNotificationReceiver<RepositoryNotification> {

    /**
     * Constructs repository message receiver.
     *
     * @param connectionConfig connection configuration
     * @param threadPool thread pool to use
     */
    public RepositoryMessageReceiver(@Nonnull ComponentApiConnectionConfig connectionConfig,
            @Nonnull ExecutorService threadPool) {
        super(connectionConfig, RepositoryNotificationReceiver.WEBSOCKET_PATH, threadPool,
                RepositoryNotification::parseFrom);
    }
}

