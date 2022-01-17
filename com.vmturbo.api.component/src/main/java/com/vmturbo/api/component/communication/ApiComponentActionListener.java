package com.vmturbo.api.component.communication;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification.Status;
import com.vmturbo.api.ActionNotificationDTO.ActionsChangedNotification;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionsUpdated;

public class ApiComponentActionListener implements ActionsListener {

    private final UINotificationChannel uiNotificationChannel;
    private final long realtimeContextId;
    private final boolean useStableId;

    public ApiComponentActionListener(@Nonnull final UINotificationChannel uiNotificationChannel,
                                      final boolean useStableId,
                                      final long realtimeContextId) {
        this.uiNotificationChannel = uiNotificationChannel;
        this.useStableId = useStableId;
        this.realtimeContextId = realtimeContextId;
    }

    @Override
    public void onActionProgress(@Nonnull final ActionProgress actionProgress) {
        final long actionId = useStableId
                ? actionProgress.getActionStableId()
                : actionProgress.getActionId();
        final ActionNotification notification = ActionNotification.newBuilder()
                .setActionProgressNotification(ActionStatusNotification.newBuilder()
                        .setActionId(Long.toString(actionId))
                        .setStatus(Status.IN_PROGRESS)
                        .setDescription(actionProgress.getDescription())
                        .setProgressPercentage(actionProgress.getProgressPercentage())
                        .build()).build();

        uiNotificationChannel.broadcastActionNotification(notification);
    }

    @Override
    public void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        final long actionId = useStableId
                ? actionSuccess.getActionStableId()
                : actionSuccess.getActionId();
        final ActionNotification notification = ActionNotification.newBuilder()
            .setActionStatusNotification(ActionStatusNotification.newBuilder()
                    .setActionId(Long.toString(actionId))
                    .setStatus(Status.SUCCEEDED)
                    .setDescription(actionSuccess.getSuccessDescription())
                    .setProgressPercentage(100)
                    .build()).build();

        uiNotificationChannel.broadcastActionNotification(notification);
    }

    @Override
    public void onActionFailure(@Nonnull final ActionFailure actionFailure) {
        final long actionId = useStableId
                ? actionFailure.getActionStableId()
                : actionFailure.getActionId();
        final ActionNotification notification = ActionNotification.newBuilder()
            .setActionStatusNotification(ActionStatusNotification.newBuilder()
                    .setActionId(Long.toString(actionId))
                    .setStatus(Status.FAILED)
                    .setDescription(actionFailure.getErrorDescription())
                    .setProgressPercentage(100)
                    .build()).build();

        uiNotificationChannel.broadcastActionNotification(notification);
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
