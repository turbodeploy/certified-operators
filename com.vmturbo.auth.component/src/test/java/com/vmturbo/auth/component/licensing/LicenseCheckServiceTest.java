package com.vmturbo.auth.component.licensing;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collections;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import reactor.core.publisher.Flux;

import com.vmturbo.api.dto.license.ILicense;
import com.vmturbo.api.dto.license.ILicense.CountedEntity;
import com.vmturbo.auth.api.auditing.AuditLogUtils;
import com.vmturbo.auth.component.licensing.LicenseManagerService.LicenseManagementEvent;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.cost.CostMoles.CostServiceMole;
import com.vmturbo.common.protobuf.cost.CostMoles.ReservedInstanceUtilizationCoverageServiceMole;
import com.vmturbo.common.protobuf.group.GroupDTOMoles.GroupServiceMole;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseDTO;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;
import com.vmturbo.common.protobuf.plan.PlanDTOMoles.PlanServiceMole;
import com.vmturbo.common.protobuf.repository.RepositoryDTOMoles.RepositoryServiceMole;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsMoles.StatsHistoryServiceMole;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.licensing.License;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.State;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.repository.api.impl.RepositoryNotificationReceiver;

/**
 * Test cases for {@link LicenseCheckService}.
 */
@RunWith(MockitoJUnitRunner.class)
public class LicenseCheckServiceTest {

    private static final int NUM_BEFORE_LICENSE_EXPIRATION_DAYS = 2;

    private static final String EMAIL = "test@test.com";

    private LicenseCheckService licenseCheckService;

    private LicenseManagerService licenseManagerService;

    private StatsHistoryServiceMole statsHistoryServiceSpy = spy(new StatsHistoryServiceMole());

    private CostServiceMole costServiceSpy = spy(new CostServiceMole());

    private GroupServiceMole groupServiceSpy = spy(new GroupServiceMole());

    private PlanServiceMole planServiceSpy = spy(new PlanServiceMole());

    private ReservedInstanceUtilizationCoverageServiceMole riUtilizationCoverageSpy =
            spy(new ReservedInstanceUtilizationCoverageServiceMole());

    private RepositoryServiceMole repositoryServiceSpy = spy(new RepositoryServiceMole());

    @Rule
    public GrpcTestServer testServer = GrpcTestServer.newServer(statsHistoryServiceSpy,
            groupServiceSpy, planServiceSpy, repositoryServiceSpy, costServiceSpy, riUtilizationCoverageSpy);

    private IMessageSender<SystemNotification> systemNotificationIMessageSender;

    private MailManager mailManager;

    private MutableFixedClock clock = new MutableFixedClock(Clock.systemUTC().instant(), ZoneId.systemDefault());

    private License aggregateLicense;

    @Before
    public void setup() {
        aggregateLicense = new License();
        aggregateLicense.setNumInUseEntities(70);
        aggregateLicense.setNumLicensedEntities(1000);
        licenseManagerService = mock(LicenseManagerService.class);
        final SearchServiceBlockingStub searchServiceClient =
                SearchServiceGrpc.newBlockingStub(testServer.getChannel());
        final RepositoryNotificationReceiver repository = mock(RepositoryNotificationReceiver.class);
        final IMessageSender<LicenseSummary> licenseSummaryIMessageSender = mock(IMessageSender.class);
        systemNotificationIMessageSender = mock(IMessageSender.class);
        mailManager = mock(MailManager.class);

        final Flux<LicenseManagementEvent> flux = Flux.fromIterable(Collections.emptyList());
        when(licenseManagerService.getEventStream()).thenReturn(flux);
        licenseCheckService = new LicenseCheckService(licenseManagerService,
                searchServiceClient,
                repository,
                licenseSummaryIMessageSender,
                systemNotificationIMessageSender,
                mailManager,
                clock,
                NUM_BEFORE_LICENSE_EXPIRATION_DAYS,
                false); // no scheduled license checks
    }

    /**
     * Permanent license should never expire.
     */
    @Test
    public void testIsGoingToExpirePermanentLicense() {
        assertFalse(licenseCheckService.isGoingToExpire(ILicense.PERM_LIC, NUM_BEFORE_LICENSE_EXPIRATION_DAYS));
    }

    /**
     * If the number of days before license expiration is two, verify:
     * 1. when the license will expire tomorrow, return true
     * 2. when the license will expire the day after tomorrow, return true
     * 3. when the license will expire three days, return false
     * 4. when the license expires today, return false (it's already expired)
     * 5. when the license expired yesterday, return false (it's already expired)
     */
    @Test
    public void testIsGoingToExpire() {
        // if the license will expire tomorrow, return true
        LocalDate localDate = LocalDate.now().plusDays(NUM_BEFORE_LICENSE_EXPIRATION_DAYS - 1);
        assertTrue(licenseCheckService.isGoingToExpire(localDate.toString(),
            NUM_BEFORE_LICENSE_EXPIRATION_DAYS));

        // if the license will expire the day after tomorrow, return true
        localDate = LocalDate.now().plusDays(NUM_BEFORE_LICENSE_EXPIRATION_DAYS);
        assertTrue(licenseCheckService.isGoingToExpire(localDate.toString(),
            NUM_BEFORE_LICENSE_EXPIRATION_DAYS));

        // if the license will expire in three days, return false
        localDate = LocalDate.now().plusDays(NUM_BEFORE_LICENSE_EXPIRATION_DAYS + 1);
        assertFalse(licenseCheckService.isGoingToExpire(localDate.toString(),
            NUM_BEFORE_LICENSE_EXPIRATION_DAYS));

        // if the license expires today, return false (it's already expired)
        localDate = LocalDate.now();
        assertFalse(licenseCheckService.isGoingToExpire(localDate.toString(),
            NUM_BEFORE_LICENSE_EXPIRATION_DAYS));

        // if the license expired yesterday, return false (it's already expired)
        localDate = LocalDate.now().minusDays(1L);
        assertFalse(licenseCheckService.isGoingToExpire(localDate.toString(),
            NUM_BEFORE_LICENSE_EXPIRATION_DAYS));
    }

    /**
     * Test when license expired, both system notification and email will be sent.
     *
     * @throws Exception not supposed to happen
     */
    @Test
    public void testPublishNotificationLicenseExpired() throws Exception {
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().toString())
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(false, Collections.singleton(license), aggregateLicense);
        final SystemNotification notification =
                notification(LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT,
                    LicenseCheckService.LICENSE_HAS_EXPIRED);
        verify(systemNotificationIMessageSender).sendMessage(notification);
        verify(mailManager).sendMail(Collections.singletonList(EMAIL),
                LicenseCheckService.LICENSE_HAS_EXPIRED,
                LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT);
    }

    /**
     * Test when license was over limit, both system notification and email will be sent.
     *
     * @throws Exception not supposed to happen
     */
    @Test
    public void testPublishNotificationLicenseOverLimit() throws Exception {
        final String description = String.format(LicenseCheckService.LICENSE_WORKLOAD_COUNT_HAS_OVER_LIMIT,
            AuditLogUtils.getLocalIpAddress(), 70, 1000);
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().plusDays(10L).toString())
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(true, Collections.singleton(license), aggregateLicense);
        SystemNotification notification =
                    notification(description, LicenseCheckService.WORKLOAD_COUNT_IS_OVER_LIMIT);
        verify(systemNotificationIMessageSender).sendMessage(notification);
        verify(mailManager).sendMail(Collections.singletonList(EMAIL),
                LicenseCheckService.WORKLOAD_COUNT_IS_OVER_LIMIT, description);
    }

    /**
     * Test when license expired and was over limit, only license expired notification will be sent
     * out, since expired license overrides over limit.
     *
     * @throws Exception not supposed to happen
     */
    @Test
    public void testPublishNotificationLicenseExpiredAndOverLimit() throws Exception {
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().toString())
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(true, Collections.singleton(license), aggregateLicense);
        final SystemNotification notification =
                notification(LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT,
                    LicenseCheckService.LICENSE_HAS_EXPIRED);
        verify(systemNotificationIMessageSender).sendMessage(notification);
        verify(mailManager).sendMail(Collections.singletonList(EMAIL),
                LicenseCheckService.LICENSE_HAS_EXPIRED,
                LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT);
    }

    /**
     * Test when license is going to expire and was over limit.
     * Both license will be expire and over limit warning will be send
     *
     * @throws Exception not supposed to happen
     */
    @Test
    public void testPublishNotificationLicenseGoingToExpirAndOverLimit() throws Exception {
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().plusDays(NUM_BEFORE_LICENSE_EXPIRATION_DAYS - 1).toString())
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(true, Collections.singleton(license), aggregateLicense);
        verify(systemNotificationIMessageSender, times(2)).sendMessage(any());
        verify(mailManager, times(2)).sendMail(anyList(), any(), any());
    }

    /**
     * Test if license is going to expire, both system notification and email will be sent.
     *
     * @throws Exception not supposed to happen
     */
    @Test
    public void testPublishNotificationLicenseGoingToExpire() throws Exception {
        final String expirationDate = LocalDate.now().plusDays(1L).toString();
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(expirationDate)
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(false, Collections.singleton(license), aggregateLicense);
        final String description = String.format(LicenseCheckService.TURBONOMIC_LICENSE_WILL_EXPIRE,
            AuditLogUtils.getLocalIpAddress(), expirationDate);
        final SystemNotification notification = notification(description, LicenseCheckService.LICENSE_IS_ABOUT_TO_EXPIRE);
        verify(systemNotificationIMessageSender).sendMessage(notification);
        verify(mailManager).sendMail(Collections.singletonList(EMAIL),
            LicenseCheckService.LICENSE_IS_ABOUT_TO_EXPIRE, description);
    }

    /**
     * Test when license is empty, will send notification to UI.
     *
     * @throws Exception not supposed to happen
     */
    @Test
    public void testEmptyLicense() throws Exception {
        when(licenseManagerService.getLicenses()).thenReturn(Collections.emptyList());
        licenseCheckService.checkLicensesForNotification();
        SystemNotification notification =
                    notification(String.format(LicenseCheckService.TURBONOMIC_LICENSE_IS_MISSING,
                            AuditLogUtils.getLocalIpAddress()),
                        LicenseCheckService.LICENSE_IS_MISSING);
        verify(systemNotificationIMessageSender).sendMessage(notification);
        verify(mailManager, never()).sendMail(anyList(), any(), any());
    }

    /**
     * Test after source topology is available, system doesn't send out notification.
     * Note: system will still send out notification for daily check.
     *
     * @throws Exception not supposed to happen
     */
    @Test
    public void testStopSendingNotificationAfterTopologyBroadcast() throws Exception {
        when(licenseManagerService.getLicenses()).thenReturn(Collections.emptyList());
        licenseCheckService.onSourceTopologyAvailable(1L, 777777L);
        verify(systemNotificationIMessageSender, never()).sendMessage(any());
        verify(mailManager, never()).sendMail(anyList(), any(), any());
    }

    /**
     * Test when license is valid, no notification or email is sent.
     *
     * @throws Exception not supposed to happen
     */
    @Test
    public void testValidLicense() throws Exception {
        final LicenseDTO licenseDTO = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().plusYears(1L).toString())
                .setCountedEntity(CountedEntity.SOCKET.name())
                .build();
        when(licenseManagerService.getLicenses()).thenReturn(Collections.singleton(licenseDTO));
        licenseCheckService.onSourceTopologyAvailable(1L, 777777L);
        verify(systemNotificationIMessageSender, never()).sendMessage(any());
        verify(mailManager, never()).sendMail(anyList(), any(), any());
    }

    SystemNotification notification(String description, String shortDescription) {
        return SystemNotification.newBuilder()
                        .setBroadcastId(1L)
                        .setCategory(Category.newBuilder()
                            .setLicense(SystemNotification.License.getDefaultInstance()))
                        .setDescription(description)
                        .setShortDescription(shortDescription)
                        .setSeverity(Severity.CRITICAL)
                        .setState(State.NOTIFY)
                        .setGenerationTime(clock.millis())
                        .build();
    }
}
