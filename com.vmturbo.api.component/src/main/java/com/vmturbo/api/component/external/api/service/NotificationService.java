package com.vmturbo.api.component.external.api.service;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.springframework.util.StringUtils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.action.ActionApiDTO;
import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.api.pagination.NotificationPaginationRequest;
import com.vmturbo.api.pagination.NotificationPaginationRequest.NotificationPaginationResponse;
import com.vmturbo.api.serviceinterfaces.INotificationService;
import com.vmturbo.api.utils.UrlsHelp;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.notification.NotificationStore;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;

/**
 * Service implementation of Notifications
 */
public class NotificationService implements INotificationService {
    /**
     * UI notification category, it's also used in the unit tests.
     */
    public static final String UI_NOTIFICATION_CATEGORY = "Notification";

    public static final float ZERO = 0.0f;

    private final NotificationStore notificationStore;

    public NotificationService(@Nonnull final NotificationStore notificationStore) {
        this.notificationStore = notificationStore;
    }

    @Override
    public List<LogEntryApiDTO> getNotifications() throws Exception {
        final List<LogEntryApiDTO> notificationList = getUniqueNotifications();
        Collections.reverse(notificationList); // UI expect ascending order.
        // add external links as required by UI
        final LogEntryApiDTO logEntryApiDTO = new LogEntryApiDTO();
        UrlsHelp.setNotificationHelp(logEntryApiDTO);
        notificationList.add(logEntryApiDTO);
        return notificationList;
    }

    private List<LogEntryApiDTO> getUniqueNotifications() {
        final List<LogEntryApiDTO> notificationList = removeDuplicate(notificationStore
                .getAllNotifications()
                .stream()
                .sorted(Comparator.comparing(SystemNotification::getGenerationTime).reversed()) // sort descending to keep latest entry
                .map(this::toLogEntryApiDTO)
                .collect(Collectors.toList()));
        return notificationList;
    }

    /**
     * Remove duplicate notification.
     *
     * @param notificationList list of notifications may have duplicates.
     * @return list of unique notification.
     */
    private List<LogEntryApiDTO> removeDuplicate(@Nonnull final List<LogEntryApiDTO> notificationList) {
        final Set<CustomKey> set = Sets.newHashSet();
        final List<LogEntryApiDTO> list = Lists.newArrayList();
        for (LogEntryApiDTO dto : notificationList) {
            CustomKey customKey = new CustomKey(dto);
            if (!set.contains(customKey)) {
                set.add(customKey);
                list.add(dto);
            }
        }
        return list;
    }

    @Override
    public NotificationPaginationResponse getNotifications(String startTime, String endTime, String category, NotificationPaginationRequest request) throws Exception {
        return request.allResultsResponse(getNotifications());
    }

    @Override
    public LogEntryApiDTO getNotificationByUuid(String uuid) throws Exception {
        Preconditions.checkArgument(!StringUtils.isEmpty(uuid));

        final Optional<SystemNotification> systemNotification = notificationStore.getNotification(Long.valueOf(uuid));
        return systemNotification.map(this::toLogEntryApiDTO).orElse(new LogEntryApiDTO());
    }

    // TODO (Gary, Dec 21 2018) confirm with PM to remove this endpoint. It seems not used.
    @Override
    public List<ActionApiDTO> getRelatedActionsByUuid(String nUuid) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<StatSnapshotApiDTO> getNotificationStats(StatPeriodApiInputDTO inputDTO)
            throws Exception {
        Preconditions.checkArgument(Long.valueOf(inputDTO.getStartDate()) > 0);

        final long startDate = Long.valueOf(inputDTO.getStartDate());
        final long totalNotificationSize = getUniqueNotifications().stream()
                .filter(logEntryApiDTO -> logEntryApiDTO.getLogActionTime() > startDate).count();
        final StatSnapshotApiDTO retDto = new StatSnapshotApiDTO();
        // UI requires offset data time, e.g. 2018-12-11T00:00:00-05:00
        final OffsetDateTime offset = OffsetDateTime.ofInstant(Instant.ofEpochMilli(startDate),
                TimeZone.getDefault().toZoneId());
        retDto.setDate(offset.toString());

        // setup StatApiDTO
        final StatApiDTO statDto = new StatApiDTO();
        statDto.setName(StringConstants.NUM_NOTIFICATIONS);

        final StatValueApiDTO valueDto = new StatValueApiDTO();
        valueDto.setAvg((float)totalNotificationSize);
        // need to include max, min and total value, even though they are always 0.0f
        valueDto.setMax(ZERO);
        valueDto.setMin(ZERO);
        valueDto.setTotal((float)totalNotificationSize);
        statDto.setValues(valueDto);
        statDto.setValue((float)totalNotificationSize);

        retDto.setStatistics(ImmutableList.of(statDto));
        return ImmutableList.of(retDto);
    }

    /**
     * Convert from {@link SystemNotification) to {@link LogEntryApiDTO).
     */
    @VisibleForTesting
    LogEntryApiDTO toLogEntryApiDTO(@Nonnull final SystemNotification notification) {
        final LogEntryApiDTO logEntryApiDTO = new LogEntryApiDTO();
        logEntryApiDTO.setCategory(UI_NOTIFICATION_CATEGORY);
        logEntryApiDTO.setDescription(notification.getDescription());
        logEntryApiDTO.setLogActionTime(notification.getGenerationTime());
        logEntryApiDTO.setSeverity(notification.getSeverity().name());
        logEntryApiDTO.setShortDescription(notification.getShortDescription());
        logEntryApiDTO.setState(notification.getState().name());

        logEntryApiDTO.setImportance(ZERO);
        logEntryApiDTO.setUuid(String.valueOf(notification.getBroadcastId()));
        // target related notification
        if (notification.getCategory().hasTarget()) {
            if (notification.getCategory().getTarget().hasOid()) {
                logEntryApiDTO.setUuid(String.valueOf(notification.getCategory().getTarget().getOid()));
            }
            logEntryApiDTO.setTargetSE(notification.getCategory().getTarget().getDisplayName());
            logEntryApiDTO.setSubCategory(notification.getCategory().getTarget().getDisplayName());
        }

        // license related notification
        if (notification.getCategory().hasLicense()) {
            logEntryApiDTO.setTargetSE(notification.getCategory().getLicense().getDisplayName());
            logEntryApiDTO.setSubCategory(notification.getCategory().getLicense().getDisplayName());
        }
        return logEntryApiDTO;
    }

    /**
     * Build custom key based on {@link LogEntryApiDTO}, which is composite key from
     * description, severity, and targetSE.
     */
    private static class CustomKey {
        private final LogEntryApiDTO logEntryApiDTO;

        CustomKey(LogEntryApiDTO logEntryApiDTO) {
            this.logEntryApiDTO = logEntryApiDTO;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CustomKey customKey = (CustomKey)o;
            return same(logEntryApiDTO, customKey.logEntryApiDTO);
        }

        @Override
        public int hashCode() {
            return Objects.hash(logEntryApiDTO.getDescription(),
                    logEntryApiDTO.getTargetSE(),
                    logEntryApiDTO.getSeverity());
        }

        /**
         * If descriptions are the same, severities are the same and targetSEs are the same, they are the same.
         * We treat "null" as not duplicated, which should not happen.
         *
         * @param oldDto old notification dto
         * @param newDto new notification dto
         * @return true if same, false otherwise
         */
        private boolean same(@Nonnull final LogEntryApiDTO oldDto, @Nonnull final LogEntryApiDTO newDto) {
            return (oldDto.getDescription() != null && oldDto.getDescription().equalsIgnoreCase(newDto.getDescription())
                    && oldDto.getSeverity() != null && oldDto.getSeverity().equalsIgnoreCase(newDto.getSeverity())
                    && oldDto.getTargetSE() != null && oldDto.getTargetSE().equalsIgnoreCase(newDto.getTargetSE()));
        }
    }
}
