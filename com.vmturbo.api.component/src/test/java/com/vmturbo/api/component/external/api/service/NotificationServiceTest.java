package com.vmturbo.api.component.external.api.service;

import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import com.google.common.collect.ImmutableList;

import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Spy;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.vmturbo.api.dto.notification.LogEntryApiDTO;
import com.vmturbo.api.dto.statistic.StatApiDTO;
import com.vmturbo.api.dto.statistic.StatPeriodApiInputDTO;
import com.vmturbo.api.dto.statistic.StatSnapshotApiDTO;
import com.vmturbo.api.dto.statistic.StatValueApiDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.notification.NotificationStore;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.State;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.License;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Target;

/**
 * Unit tests for the {@link NotificationService} class.
 * Note: NotificationService has codes indirectly require Spring web context:
 * UrlsHelp.setNotificationHelp(logEntryApiDTO);
 */
@RunWith(SpringJUnit4ClassRunner.class)
@WebAppConfiguration
public class NotificationServiceTest {
    private static final long OID1 = 111l;
    private static final long GENERATION_TIME1 = 111111111L;
    private static final String SHORT_DESCRIPTION1 = "short description";
    private static final String LONG_DESCRIPTION1 = "long description";
    private static final long BROADCAST_ID1 = 222L;

    private static final long OID2 = 2111L;
    private static final long GENERATION_TIME2 = GENERATION_TIME1 + 1L;
    private static final long GENERATION_TIME3 = GENERATION_TIME1 - 1L;
    private static final String SHORT_DESCRIPTION2 = "2short description";
    private static final String LONG_DESCRIPTION2 = "2long description";
    private static final long BROADCAST_ID2 = 2222L;

    private static final SystemNotification NOTIFICATION_1 = SystemNotification.newBuilder()
            .setGenerationTime(GENERATION_TIME1)
            .setState(State.NOTIFY)
            .setSeverity(Severity.CRITICAL)
            .setShortDescription(SHORT_DESCRIPTION1)
            .setDescription(LONG_DESCRIPTION1)
            .setCategory(Category.newBuilder().setLicense(License.newBuilder().build()).build())
            .setBroadcastId(BROADCAST_ID1)
            .build();

    private static final SystemNotification NOTIFICATION_2 = SystemNotification.newBuilder()
            .setGenerationTime(GENERATION_TIME2)
            .setState(State.NOTIFY)
            .setSeverity(Severity.CRITICAL)
            .setShortDescription(SHORT_DESCRIPTION2)
            .setDescription(LONG_DESCRIPTION2)
            .setCategory(Category.newBuilder().setLicense(License.newBuilder().build()).build())
            .setBroadcastId(BROADCAST_ID2)
            .build();

    private static final SystemNotification NOTIFICATION_3 = buildNewNotification(GENERATION_TIME1);

    private static final SystemNotification NOTIFICATION_PROBE1 = buildNewNotification(GENERATION_TIME3);

    private static final SystemNotification NOTIFICATION_PROBE2 = buildNewNotification(GENERATION_TIME3);

    private static SystemNotification buildNewNotification(long generationTime3) {
        return SystemNotification.newBuilder()
                .setGenerationTime(generationTime3)
                .setState(State.NOTIFY)
                .setSeverity(Severity.CRITICAL)
                .setShortDescription(SHORT_DESCRIPTION1)
                .setDescription(LONG_DESCRIPTION1)
                .setCategory(Category.newBuilder().setTarget(Target.newBuilder().setOid(OID2).build())
                        .build())
                .setBroadcastId(BROADCAST_ID1)
                .build();
    }


    private static NotificationService notificationService;

    @Spy
    private static NotificationStore notificationStore;

    private static Collection<SystemNotification> notifications = ImmutableList.of(NOTIFICATION_1, NOTIFICATION_2);

    @BeforeClass
    public static void setUp() {
        notificationStore = mock(NotificationStore.class);
        notificationService = new NotificationService(notificationStore);
        when(notificationStore.getAllNotifications()).thenReturn(notifications);
    }

    /**
     * Test positive case for getNotifications
     */
    @Test
    public void testGetNotifications() throws Exception {
        final List<LogEntryApiDTO> results = notificationService.getNotifications();
        // it's three, because added external links as required by UI
        assertEquals(3, results.size());
        assertThat(results.get(0), samePropertyValuesAs(notificationService.toLogEntryApiDTO(NOTIFICATION_1)));
        assertThat(results.get(1), samePropertyValuesAs(notificationService.toLogEntryApiDTO(NOTIFICATION_2)));
        // don't test the external link object, since it's covered by Class codes.
    }

    /**
     * Test positive case for getNotificationStats
     */
    @Test
    public void testGetNotificationStats() throws Exception {
        final StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        statPeriodApiInputDTO.setStartDate(String.valueOf(GENERATION_TIME2 - 1));
        final List<StatSnapshotApiDTO> results = notificationService.getNotificationStats(statPeriodApiInputDTO);
        assertEquals(1, results.size());
        assertEquals(getExpectedStatSnapshotApiDTO(GENERATION_TIME2 - 1).getDate(), results.get(0).getDate());
        assertThat(results.get(0).getStatistics(), samePropertyValuesAs(getExpectedStatSnapshotApiDTO(GENERATION_TIME2 - 1).getStatistics()));
    }

    /**
     * Test positive case for getNotificationStats with duplicate notifications.
     * Ensure always return the latest notification for duplicates.
     * @throws Exception when failing to get notifications.
     */
    @Test
    public void testGetNotificationStatsWithDuplicate() throws Exception {
        final StatPeriodApiInputDTO statPeriodApiInputDTO = new StatPeriodApiInputDTO();
        statPeriodApiInputDTO.setStartDate(String.valueOf(GENERATION_TIME2));
        final List<StatSnapshotApiDTO> results = notificationService.getNotificationStats(statPeriodApiInputDTO);
        assertEquals(1, results.size());
        assertEquals(getExpectedStatSnapshotApiDTO(GENERATION_TIME2).getDate(), results.get(0).getDate());
        assertThat(results.get(0).getStatistics(), samePropertyValuesAs(getExpectedStatSnapshotApiDTO(GENERATION_TIME2).getStatistics()));
    }

    /**
     * Test positive case for getNotificationByUuid
     */
    @Test
    public void testGetNotificationByUuid() throws Exception {
        when(notificationStore.getNotification(OID1)).thenReturn(Optional.of(NOTIFICATION_1));
        final LogEntryApiDTO result = notificationService.getNotificationByUuid(String.valueOf(OID1));
        assertThat(result, samePropertyValuesAs(notificationService.toLogEntryApiDTO(NOTIFICATION_1)));
    }

    /**
     * Test positive case for getNotificationByInvalidUuid
     */
    @Test
    public void testGetNotificationByInvalidUuid() throws Exception {
        when(notificationStore.getNotification(OID1)).thenReturn(Optional.empty());
        final LogEntryApiDTO result = notificationService.getNotificationByUuid(String.valueOf(OID1));
        assertThat(result, samePropertyValuesAs(new LogEntryApiDTO()));
    }

    /**
     * Test positive case for toLogEntryApiDTO with license category type
     */
    @Test
    public void testToLogEntryApiDTOWithLicense() {
        final LogEntryApiDTO result = notificationService.toLogEntryApiDTO(NOTIFICATION_1);
        final LogEntryApiDTO logEntryApiDTO = new LogEntryApiDTO();
        logEntryApiDTO.setCategory(NotificationService.UI_NOTIFICATION_CATEGORY);
        logEntryApiDTO.setDescription(LONG_DESCRIPTION1);
        logEntryApiDTO.setLogActionTime(GENERATION_TIME1);
        logEntryApiDTO.setSeverity(Severity.CRITICAL.name());
        logEntryApiDTO.setShortDescription(SHORT_DESCRIPTION1);
        logEntryApiDTO.setState(State.NOTIFY.name());
        logEntryApiDTO.setSubCategory(Category.newBuilder().setLicense(License.newBuilder().build()).build().getLicense().getDisplayName());
        logEntryApiDTO.setUuid(String.valueOf(BROADCAST_ID1));
        logEntryApiDTO.setImportance(NotificationService.ZERO);
        logEntryApiDTO.setTargetSE(Category.newBuilder().setLicense(License.newBuilder().build()).build().getLicense().getDisplayName());

        assertThat(result, samePropertyValuesAs(logEntryApiDTO));
    }

    /**
     * Test removing duplicate notifications.
     *
     * @throws Exception if failing to to get notifications.
     */
    @Test
    public void testRemoveDuplicateNotifications() throws Exception {
        final Collection<SystemNotification> notificationsDuplicate = ImmutableList
                .of(NOTIFICATION_1, NOTIFICATION_PROBE1, NOTIFICATION_PROBE1, NOTIFICATION_PROBE2);
        when(notificationStore.getAllNotifications()).thenReturn(notificationsDuplicate);

        final List<LogEntryApiDTO> results = notificationService.getNotifications();
        // it's three, because added external links as required by UI
        assertEquals(3, results.size());
        assertThat(results.get(1), samePropertyValuesAs(notificationService.toLogEntryApiDTO(NOTIFICATION_1)));
        assertThat(results.get(0), samePropertyValuesAs(notificationService.toLogEntryApiDTO(NOTIFICATION_PROBE1)));
    }

    /**
     * Test positive case for toLogEntryApiDTO with target category type.
     */
    @Test
    public void testToLogEntryApiDTOWithTarget() {
        final LogEntryApiDTO result = notificationService.toLogEntryApiDTO(NOTIFICATION_3);
        final LogEntryApiDTO logEntryApiDTO = new LogEntryApiDTO();
        logEntryApiDTO.setCategory(NotificationService.UI_NOTIFICATION_CATEGORY);
        logEntryApiDTO.setDescription(LONG_DESCRIPTION1);
        logEntryApiDTO.setLogActionTime(GENERATION_TIME1);
        logEntryApiDTO.setSeverity(Severity.CRITICAL.name());
        logEntryApiDTO.setShortDescription(SHORT_DESCRIPTION1);
        logEntryApiDTO.setState(State.NOTIFY.name());
        logEntryApiDTO.setSubCategory(Category.newBuilder().setTarget(Target.newBuilder().build())
                .build().getTarget().getDisplayName());
        logEntryApiDTO.setUuid(String.valueOf(OID2));
        logEntryApiDTO.setImportance(NotificationService.ZERO);
        logEntryApiDTO.setTargetSE(Category.newBuilder().setTarget(Target.newBuilder().build())
                .build().getTarget().getDisplayName());

        assertThat(result, samePropertyValuesAs(logEntryApiDTO));
    }

    private StatSnapshotApiDTO getExpectedStatSnapshotApiDTO(long epochMilli) {
        final StatSnapshotApiDTO retDto = new StatSnapshotApiDTO();
        final OffsetDateTime time = OffsetDateTime.ofInstant(Instant.ofEpochMilli(epochMilli),
                TimeZone.getDefault().toZoneId());
        retDto.setDate(time.toString());

        // setup StatApiDTO
        final StatApiDTO statDto = new StatApiDTO();
        statDto.setName(StringConstants.NUM_NOTIFICATIONS);

        final StatValueApiDTO valueDto = new StatValueApiDTO();
        valueDto.setAvg(1f);
        // need to include max, min and total value, even though they are always 0.0f
        valueDto.setMax(NotificationService.ZERO);
        valueDto.setMin(NotificationService.ZERO);
        valueDto.setTotal(1f);
        statDto.setValues(valueDto);
        statDto.setValue(1f);
        retDto.setStatistics(ImmutableList.of(statDto));
        return retDto;
    }
}
