package com.vmturbo.component.status.api;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.cluster.ComponentStatus.ComponentStatusNotification;

/**
 * Listeners for component status notifications should implement this interface.
 */
public interface ComponentStatusListener {

    /**
     * Called when a component status notification is available.
     *
     * @param notification A message describing the notification.
     */
    void onComponentNotification(@Nonnull ComponentStatusNotification notification);
}
