package com.vmturbo.action.orchestrator.api.impl;

import java.util.Objects;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.ActionOrchestrator;
import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.components.api.client.ComponentApiClient;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.components.api.client.IMessageReceiver;

/**
 * Implementation of the {@link ActionOrchestrator} client API.
 */
public class ActionOrchestratorClient
        extends ComponentApiClient<ActionOrchestratorRestClient>
        implements ActionOrchestrator {

    public static final String WEBSOCKET_PATH = "/action-orchestrator";
    private final ActionOrchestratorNotificationReceiver notificationReceiver;

    private ActionOrchestratorClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        super(connectionConfig);
        this.notificationReceiver = null;
    }

    private ActionOrchestratorClient(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService,
            @Nonnull final IMessageReceiver<ActionOrchestratorNotification> messageReceiver) {
        super(connectionConfig);
        this.notificationReceiver =
                new ActionOrchestratorNotificationReceiver(messageReceiver, executorService);
    }

    public static ActionOrchestratorClient rpcOnly(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new ActionOrchestratorClient(connectionConfig);
    }

    public static ActionOrchestratorClient rpcAndNotification(
            @Nonnull final ComponentApiConnectionConfig connectionConfig,
            @Nonnull final ExecutorService executorService,
            @Nonnull IMessageReceiver<ActionOrchestratorNotification> messageReceiver) {
        return new ActionOrchestratorClient(connectionConfig, executorService, messageReceiver);
    }

    @Nonnull
    @Override
    protected ActionOrchestratorRestClient createRestClient(@Nonnull final ComponentApiConnectionConfig connectionConfig) {
        return new ActionOrchestratorRestClient(connectionConfig);
    }

    @Override
    public void addActionsListener(@Nonnull final ActionsListener listener) {
        if (notificationReceiver == null) {
            throw new IllegalStateException(
                    "Action client is not configured to receive notifications");
        }
        notificationReceiver.addActionsListener(Objects.requireNonNull(listener));
    }

}
