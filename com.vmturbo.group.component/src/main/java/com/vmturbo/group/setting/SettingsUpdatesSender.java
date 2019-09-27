package com.vmturbo.group.setting;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.group.api.SettingMessages.GlobalSettingChange;
import com.vmturbo.group.api.SettingMessages.SettingNotification;

// Class used to broadcast a settings update notification message.
public class SettingsUpdatesSender extends
        ComponentNotificationSender<SettingNotification> {

    private final IMessageSender<SettingNotification> sender;

    SettingsUpdatesSender(
            @Nonnull IMessageSender<SettingNotification> sender) {
        this.sender = Objects.requireNonNull(sender);
    }

    public void notifySettingsUpdated(Setting setting)
            throws CommunicationException, InterruptedException {
        final SettingNotification message = SettingNotification.newBuilder()
                    .setGlobal(GlobalSettingChange.newBuilder()
                    .setSetting(setting).build()).build();
        sendMessage(sender, message);
    }

    @Override
    protected String describeMessage(@Nonnull SettingNotification settingNotification) {
        return SettingNotification.class.getSimpleName() + "[" +
                settingNotification.getGlobal().getSetting().getSettingSpecName() + "]";
    }
}