package com.vmturbo.api.component.external.api.service;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.notification.NotificationSettingsApiDTO;
import com.vmturbo.api.exceptions.UnauthorizedObjectException;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.INotificationSettingsServices;
import javax.annotation.Nonnull;
import java.util.List;

public class NotificationSettingsService implements INotificationSettingsServices {

    @Override
    public List<NotificationSettingsApiDTO> getNotificationSettings() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public NotificationSettingsApiDTO getNotificationSettingsByUuid(@Nonnull String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public NotificationSettingsApiDTO createNotificationSettings(@Nonnull NotificationSettingsApiDTO notificationSettings)
            throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public NotificationSettingsApiDTO editNotificationSettings(@Nonnull String uuid, @Nonnull NotificationSettingsApiDTO notificationSettingsApiDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteNotificationSettings(@Nonnull String uuid) throws UnknownObjectException, InterruptedException, UnauthorizedObjectException {
        throw ApiUtils.notImplementedInXL();
    }

}