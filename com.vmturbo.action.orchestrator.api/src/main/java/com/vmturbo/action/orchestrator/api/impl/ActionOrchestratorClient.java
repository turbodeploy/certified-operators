package com.vmturbo.action.orchestrator.api.impl;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;

/**
 * Implementation of the {@link ActionOrchestrator} client API.
 */
public class ActionOrchestratorClient
        extends ComponentApiClient<ActionOrchestratorRestClient, ActionOrchestratorNotificationReceiver>
        implements ActionOrchestrator {

    public static final String WEBSOCKET_PATH = "/action-orchestrator";

    private ActionOrchestratorClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
    }

    private ActionOrchestratorClient(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        super(connectionConfig, executorService);
    }

    public static ActionOrchestratorClient rpcOnly(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new ActionOrchestratorClient(connectionConfig);
    }

    public static ActionOrchestratorClient rpcAndNotification(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService) {
        return new ActionOrchestratorClient(connectionConfig, executorService);
    }

    @Nonnull
    @Override
    protected ActionOrchestratorRestClient createRestClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new ActionOrchestratorRestClient(connectionConfig);
    }

    @Nonnull
    @Override
    protected ActionOrchestratorNotificationReceiver createWebsocketClient(
            @Nonnull final ComponentApiConnectionConfig apiClient,
            @Nonnull final ExecutorService executorService) {
        return new ActionOrchestratorNotificationReceiver(apiClient, executorService);
    }

    @Override
    public void addActionsListener(@Nonnull final ActionsListener listener) {
        websocketClient().addActionsListener(Objects.requireNonNull(listener));
    }
}
