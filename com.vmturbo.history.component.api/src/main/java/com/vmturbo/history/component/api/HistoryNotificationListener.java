package com.vmturbo.history.component.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.history.VolAttachmentHistoryOuterClass;

/**
 * The listener for the history component.
 */
public interface HistoryNotificationListener {

    /**
     * It receives the messages from the repo.
     *
     * @param historyNotification The history notification
     */
    void onHistoryNotificationReceived(@Nonnull VolAttachmentHistoryOuterClass.VolAttachmentHistory historyNotification);

}
