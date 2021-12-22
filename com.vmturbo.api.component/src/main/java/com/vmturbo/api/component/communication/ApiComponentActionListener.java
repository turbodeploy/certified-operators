package com.vmturbo.api.component.communication;

import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification.Status;
import com.vmturbo.api.ActionNotificationDTO.ActionsChangedNotification;
import com.vmturbo.api.component.external.api.mapper.ActionSpecMapper;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;

public class ApiComponentActionListener implements ActionsListener {
    private static final Logger logger = LogManager.getLogger();

    private final UINotificationChannel uiNotificationChannel;
    private final long realtimeContextId;
    private final ActionsServiceBlockingStub actionsServiceBlockingStub;
    private final boolean useStableId;

    public ApiComponentActionListener(@Nonnull final UINotificationChannel uiNotificationChannel,
                                      @Nonnull final ActionsServiceBlockingStub actionsServiceBlockingStub,
                                      final boolean useStableId,
                                      final long realtimeContextId) {
        this.uiNotificationChannel = uiNotificationChannel;
        this.actionsServiceBlockingStub = actionsServiceBlockingStub;
        this.useStableId = useStableId;
        this.realtimeContextId = realtimeContextId;
    }

    @Override
    public void onActionProgress(@Nonnull final ActionProgress actionProgress) {
        Optional<Long> actionId = getActionId(actionProgress.getActionId());
        if (actionId.isPresent()) {
            final ActionNotification notification = ActionNotification.newBuilder()
                    .setActionProgressNotification(ActionStatusNotification.newBuilder()
                            .setActionId(Long.toString(actionId.get()))
                            .setStatus(Status.IN_PROGRESS)
                            .setDescription(actionProgress.getDescription())
                            .setProgressPercentage(actionProgress.getProgressPercentage())
                            .build()).build();

            uiNotificationChannel.broadcastActionNotification(notification);
        } else {
            logger.error("Cannot lookup the action with Id {} so its progress update can not be sent to UI.",
                    actionProgress.getActionId());
        }
    }

    @Override
    public void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        Optional<Long> actionId = getActionId(actionSuccess.getActionId());
        if (actionId.isPresent()) {
            final ActionNotification notification = ActionNotification.newBuilder()
                    .setActionStatusNotification(ActionStatusNotification.newBuilder()
                            .setActionId(Long.toString(actionId.get()))
                            .setStatus(Status.SUCCEEDED)
                            .setDescription(actionSuccess.getSuccessDescription())
                            .setProgressPercentage(100)
                            .build()).build();

            uiNotificationChannel.broadcastActionNotification(notification);
        } else {
            logger.error("Cannot lookup the action with Id {} so its success can not be sent to UI.",
                    actionSuccess.getActionId());
        }
    }

    @Override
    public void onActionFailure(@Nonnull final ActionFailure actionFailure) {
        Optional<Long> actionId = getActionId(actionFailure.getActionId());
        if (actionId.isPresent()) {
            final ActionNotification notification = ActionNotification.newBuilder()
                .setActionStatusNotification(ActionStatusNotification.newBuilder()
                        .setActionId(Long.toString(actionId.get()))
                        .setStatus(Status.FAILED)
                        .setDescription(actionFailure.getErrorDescription())
                        .setProgressPercentage(100)
                        .build()).build();

            uiNotificationChannel.broadcastActionNotification(notification);
        } else {
            logger.error("Cannot lookup the action with Id {} so its failure can not be sent to UI.",
                    actionFailure.getActionId());
        }
    }

    /**
     * Gets the id that API uses for identifying action.
     *
     * @param actionInstanceId the action instance id.
     * @return the action stable id if the stable id flag is enabled or instance id otherwise
     */
    private Optional<Long> getActionId(long actionInstanceId) {
        if (useStableId) {
            final ActionDTO.ActionOrchestratorAction action = actionsServiceBlockingStub
                    .getAction(ActionDTO.SingleActionRequest.newBuilder()
                    .setTopologyContextId(realtimeContextId)
                    .setActionId(actionInstanceId)
                    .build());
            if (action.hasActionSpec()) {
                return Optional.of(action.getActionSpec().getRecommendationId());
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.of(actionInstanceId);
        }
    }

    @Override
    public void onActionsUpdated(@Nonnull final ActionsUpdated actionsUpdated) {
        if (!actionsUpdated.hasActionPlanInfo()) {
            return;
        }

        final long contextId = ActionDTOUtil.getActionPlanContextId(actionsUpdated.getActionPlanInfo());
        if (contextId != realtimeContextId) {
            return;
        }

        final ActionNotification notification = ActionNotification.newBuilder()
                .setActionChangedNotification(ActionsChangedNotification.newBuilder()
                    .setActionCount(Math.toIntExact(actionsUpdated.getActionCount()))
                    .build()).build();
        uiNotificationChannel.broadcastActionNotification(notification);
    }
}
