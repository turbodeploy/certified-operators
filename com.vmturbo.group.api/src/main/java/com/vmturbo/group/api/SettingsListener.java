package com.vmturbo.group.api;

import com.vmturbo.group.api.SettingMessages.SettingNotification;

/**
 * Interface to implement for listeners who listen to the settings-updates topic.
 * Only global settings' value changes and resets are notified.
 */
public interface SettingsListener {

    /**
     * What the listener is supposed to do after getting a settings updates message.
     *
     * @param notification SettingNotification
     */
    void onSettingsUpdated(SettingNotification notification);
}
