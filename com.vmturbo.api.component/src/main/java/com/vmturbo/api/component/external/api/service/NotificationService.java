package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.LogEntryApiDTO;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.input.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.serviceinterfaces.INotificationService;

/**
 * Service implementation of Notifications
 */
public class NotificationService implements INotificationService {
    @Override
    public List<LogEntryApiDTO> getNotifications() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public LogEntryApiDTO getNotificationByUuid(String uuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ActionApiDTO> getRelatedActionsByUuid(String nUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getNotificationStats(StatPeriodApiInputDTO statPeriodApiInputDTO)
        throws Exception {
        // TODO: OM-23665
        return ImmutableList.of();
    }
}
