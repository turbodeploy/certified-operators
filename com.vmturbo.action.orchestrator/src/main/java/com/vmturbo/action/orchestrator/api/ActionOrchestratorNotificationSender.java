package com.vmturbo.action.orchestrator.api;

import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.dto.ActionMessages.ActionOrchestratorNotification;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlan;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.components.api.server.ComponentNotificationSender;

/**
 * Handles the websocket connections with clients using the
 * {@link com.vmturbo.action.orchestrator.api.ActionOrchestrator} API.
 */
public class ActionOrchestratorNotificationSender extends
        ComponentNotificationSender<ActionOrchestratorNotification> {

    ActionOrchestratorNotificationSender(@Nonnull final ExecutorService threadPool) {
        super(threadPool);
    }

    /**
     * Notify currently connected clients about actions the orchestrator is recommending.
     *
     * <p>Sends the notifications asynchronously to all clients connected at the time of the method call.
     * If the sending of a notification fails for any reason the notification does not get re-sent.
     *
     * @param actionPlan The {@link ActionPlan} protobuf objects describing the actions to execute.
     */
    public void notifyActionsRecommended(@Nonnull final ActionPlan actionPlan) {
        final ActionOrchestratorNotification serverMessage = createNewMessage()
                .setActionPlan(actionPlan)
                .build();
        sendMessage(serverMessage.getBroadcastId(), serverMessage);
    }

    public void notifyActionProgress(@Nonnull final ActionProgress actionProgress) {
        final ActionOrchestratorNotification serverMessage = createNewMessage()
                .setActionProgress(actionProgress)
                .build();
        sendMessage(serverMessage.getBroadcastId(), serverMessage);
    }

    public void notifyActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        final ActionOrchestratorNotification serverMessage = createNewMessage()
                .setActionSuccess(actionSuccess)
                .build();
        sendMessage(serverMessage.getBroadcastId(), serverMessage);
    }

    public void notifyActionFailure(@Nonnull final ActionFailure actionFailure) {
        final ActionOrchestratorNotification serverMessage = createNewMessage()
                .setActionFailure(actionFailure)
                .build();
        sendMessage(serverMessage.getBroadcastId(), serverMessage);
    }

    @Nonnull
    private ActionOrchestratorNotification.Builder createNewMessage() {
        return ActionOrchestratorNotification.newBuilder()
            .setBroadcastId(newMessageChainId());
    }

    @Override
    protected String describeMessage(@Nonnull ActionOrchestratorNotification actionNotification) {
        return ActionOrchestratorNotification.class.getSimpleName() + "[" +
                actionNotification.getBroadcastId() + "]";
    }

}
