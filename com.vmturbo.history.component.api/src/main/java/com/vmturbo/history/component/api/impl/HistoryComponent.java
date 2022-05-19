package com.vmturbo.history.component.api.impl;

import javax.annotation.Nonnull;

import com.vmturbo.history.component.api.HistoryNotificationListener;

/**
 * The client interface to a remote history component.
 */
public interface HistoryComponent {

    /**
     * It receives the messages from the repo.
     * @param listener The volume history notification.
     */
    void addVolumeHistoryNotificationListener(@Nonnull HistoryNotificationListener listener);
}
