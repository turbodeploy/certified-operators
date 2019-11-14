package com.vmturbo.action.orchestrator.api.impl;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.components.api.client.ApiClientException;
import com.vmturbo.components.api.client.IMessageReceiver;
import com.vmturbo.components.api.client.MulticastNotificationReceiver;

/**
 * Receiver to accept notifications from Action Orhcestrator.
 */
public class ActionOrchestratorNotificationReceiver extends
        MulticastNotificationReceiver<ActionOrchestratorNotification, ActionsListener> {

    /**
     * Kafka topic to receive action orchestrator actions notifications.
     */
    public static final String ACTIONS_TOPIC = "action-orchestrator-actions";

    public ActionOrchestratorNotificationReceiver(
            @Nonnull final IMessageReceiver<ActionOrchestratorNotification> messageReceiver,
            @Nonnull final ExecutorService executorService, int kafkaReceiverTimeoutSeconds) {
        super(messageReceiver, executorService, kafkaReceiverTimeoutSeconds);
    }

    @Override
    protected void processMessage(@Nonnull final ActionOrchestratorNotification message)
            throws ApiClientException {
        switch (message.getTypeCase()) {
            case ACTION_PLAN: // DEPRECATED
                invokeListeners(listener -> listener.onActionsReceived(message.getActionPlan()));
                break;
            case ACTIONS_UPDATED:
                invokeListeners(listener -> listener.onActionsUpdated(message.getActionsUpdated()));
                break;
            case ACTION_PROGRESS:
                invokeListeners(listener -> listener.onActionProgress(message.getActionProgress()));
                break;
            case ACTION_SUCCESS:
                invokeListeners(listener -> listener.onActionSuccess(message.getActionSuccess()));
                break;
            case ACTION_FAILURE:
                invokeListeners(listener -> listener.onActionFailure(message.getActionFailure()));
                break;
            default:
                throw new ApiClientException("Message type unrecognized: " + message);
        }
    }
}
