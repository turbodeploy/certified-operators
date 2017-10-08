package com.vmturbo.action.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.WebsocketNotificationReceiver;

/**
 * ActionOrchestrator specific websocket message receiver.
 */
public class ActionOrchestratorMessageReceiver extends
        WebsocketNotificationReceiver<ActionOrchestratorNotification> {

    /**
     * Constructs action orchestrator message receiver.
     *
     * @param connectionConfig connection configuration
     * @param threadPool thread pool to use
     */
    public ActionOrchestratorMessageReceiver(@Nonnull ComponentApiConnectionConfig connectionConfig,
            @Nonnull ExecutorService threadPool) {
        super(connectionConfig, ActionOrchestratorClient.WEBSOCKET_PATH, threadPool,
                ActionOrchestratorNotification::parseFrom);
    }
}
