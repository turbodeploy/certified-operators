package com.vmturbo.api.component.communication;

import javax.annotation.Nonnull;

import com.vmturbo.action.orchestrator.api.ActionsListener;
import com.vmturbo.api.ActionNotificationDTO.ActionNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification;
import com.vmturbo.api.ActionNotificationDTO.ActionStatusNotification.Status;
import com.vmturbo.api.component.external.api.websocket.UINotificationChannel;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionFailure;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionProgress;
import com.vmturbo.common.protobuf.action.ActionNotificationDTO.ActionSuccess;

public class ApiComponentActionListener implements ActionsListener {

    private final UINotificationChannel uiNotificationChannel;

    public ApiComponentActionListener(@Nonnull final UINotificationChannel uiNotificationChannel) {
        this.uiNotificationChannel = uiNotificationChannel;
    }

    @Override
    public void onActionProgress(@Nonnull final ActionProgress actionProgress) {
        final ActionNotification notification = ActionNotification.newBuilder()
            .setActionId(Long.toString(actionProgress.getActionId()))
            .setActionProgressNotification(ActionStatusNotification.newBuilder()
                    .setStatus(Status.IN_PROGRESS)
                .setDescription(actionProgress.getDescription())
                .setProgressPercentage(actionProgress.getProgressPercentage())
                .build()).build();

        uiNotificationChannel.broadcastActionNotification(notification);
    }

    @Override
    public void onActionSuccess(@Nonnull final ActionSuccess actionSuccess) {
        final ActionNotification notification = ActionNotification.newBuilder()
            .setActionId(Long.toString(actionSuccess.getActionId()))
            .setActionStatusNotification(ActionStatusNotification.newBuilder()
                .setStatus(Status.SUCCEEDED)
                .setDescription(actionSuccess.getSuccessDescription())
                .setProgressPercentage(100)
                .build()).build();

        uiNotificationChannel.broadcastActionNotification(notification);
    }

    @Override
    public void onActionFailure(@Nonnull final ActionFailure actionFailure) {
        final ActionNotification notification = ActionNotification.newBuilder()
            .setActionId(Long.toString(actionFailure.getActionId()))
            .setActionStatusNotification(ActionStatusNotification.newBuilder()
                .setStatus(Status.FAILED)
                .setDescription(actionFailure.getErrorDescription())
                .setProgressPercentage(100)
                .build()).build();

        uiNotificationChannel.broadcastActionNotification(notification);
    }
}
