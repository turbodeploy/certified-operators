package com.vmturbo.history.component.api;

import com.vmturbo.history.component.api.HistoryComponentNotifications.ApplicationServiceHistoryNotification;

/**
 * A listener for application service app history from the history component.
 * For example a listener to calculate the days empty of an app service.
 */
public interface ApplicationServiceHistoryListener {
    /**
     * Indicates that the history component has new app service history notification available for
     * the live topology.
     *
     * @param appSvcHistoryNotification A message containing the app service history notification.
     */
    void onApplicationServiceHistoryNotification(ApplicationServiceHistoryNotification appSvcHistoryNotification);
}
