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

import java.io.IOException;
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
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.MutableFixedClock;
import com.vmturbo.components.common.mail.MailConfigException;
import com.vmturbo.components.common.mail.MailException;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.State;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Builder;
import com.vmturbo.notification.api.dto.SystemNotificationDTO.SystemNotification.Category;
import com.vmturbo.repository.api.Repository;

/**
 * Test cases for {@link LicenseCheckService}
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

    @Before
    public void setup() {
        licenseManagerService = mock(LicenseManagerService.class);
        final SearchServiceBlockingStub searchServiceClient =
                SearchServiceGrpc.newBlockingStub(testServer.getChannel());
        final Repository repository = mock(Repository.class);
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
                NUM_BEFORE_LICENSE_EXPIRATION_DAYS);
    }

    /**
     * Permanent license should never expire
     */
    @Test
    public void testIsGoingToExpiredPermanentLicense() {
        assertFalse(licenseCheckService.isGoingToExpired(ILicense.PERM_LIC, NUM_BEFORE_LICENSE_EXPIRATION_DAYS));
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
    public void testIsGoingToExpired() {
        // if the license will expire tomorrow, return true
        LocalDate localDate = LocalDate.now().plusDays(NUM_BEFORE_LICENSE_EXPIRATION_DAYS - 1);
        assertTrue(licenseCheckService.isGoingToExpired(localDate.toString(), NUM_BEFORE_LICENSE_EXPIRATION_DAYS));

        // if the license will expire the day after tomorrow, return true
        localDate = LocalDate.now().plusDays(NUM_BEFORE_LICENSE_EXPIRATION_DAYS);
        assertTrue(licenseCheckService.isGoingToExpired(localDate.toString(), NUM_BEFORE_LICENSE_EXPIRATION_DAYS));

        // if the license will expire three days, return false
        localDate = LocalDate.now().plusDays((NUM_BEFORE_LICENSE_EXPIRATION_DAYS) + 1);
        assertFalse(licenseCheckService.isGoingToExpired(localDate.toString(), NUM_BEFORE_LICENSE_EXPIRATION_DAYS));

        // if the license expires today, return false (it's already expired)
        localDate = LocalDate.now();
        assertFalse(licenseCheckService.isGoingToExpired(localDate.toString(), NUM_BEFORE_LICENSE_EXPIRATION_DAYS));

        // if the license expired yesterday, return false (it's already expired)
        localDate = LocalDate.now().minusDays(1L);
        assertFalse(licenseCheckService.isGoingToExpired(localDate.toString(), NUM_BEFORE_LICENSE_EXPIRATION_DAYS));
    }

    /**
     * Test when license expired, both system notification and email will be sent
     */
    @Test
    public void testPublishNotificationLicenseExpired() throws CommunicationException, InterruptedException, MailException, MailConfigException {
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().toString())
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(false, Collections.singleton(license));
        final long notificationTime = clock.millis();
        final Builder builder = SystemNotification.newBuilder();
        builder.setBroadcastId(1L)
                .setCategory(Category.newBuilder().setLicense(SystemNotification.License.newBuilder().build()).build())
                .setDescription(LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT)
                .setShortDescription(LicenseCheckService.LICENSE_HAS_EXPIRED)
                .setSeverity(Severity.CRITICAL)
                .setState(State.NOTIFY)
                .setGenerationTime(notificationTime);
        verify(systemNotificationIMessageSender).sendMessage(builder.build());
        verify(mailManager).sendMail(Collections.singletonList(EMAIL),
                LicenseCheckService.LICENSE_HAS_EXPIRED,
                LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT);

    }

    /**
     * Test when license was over limit, both system notification and email will be sent
     */
    @Test
    public void testPublishNotificationLicenseOverLimit() throws CommunicationException, InterruptedException, MailException, MailConfigException {
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().plusDays(10L).toString())
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(true, Collections.singleton(license));

        final long notificationTime = clock.millis();
        final Builder builder = SystemNotification.newBuilder();
        builder.setBroadcastId(1L)
                .setCategory(Category.newBuilder().setLicense(SystemNotification.License.newBuilder().build()).build())
                .setDescription(LicenseCheckService.LICENSE_WORKLOAD_COUNT_HAS_OVER_LIMIT)
                .setShortDescription(LicenseCheckService.WORKLOAD_COUNT_IS_OVER_LIMIT)
                .setSeverity(Severity.CRITICAL)
                .setState(State.NOTIFY)
                .setGenerationTime(notificationTime);
        verify(systemNotificationIMessageSender).sendMessage(builder.build());
        verify(mailManager).sendMail(Collections.singletonList(EMAIL),
                LicenseCheckService.WORKLOAD_COUNT_IS_OVER_LIMIT,
                LicenseCheckService.LICENSE_WORKLOAD_COUNT_HAS_OVER_LIMIT);
    }

    /**
     * Test when license expired and was over limit, license will expired warning (only) will be send out.
     * Since expired license overrides over limit.
     */
    @Test
    public void testPublishNotificationLicenseExpiredAndOverLimit() throws CommunicationException, InterruptedException, MailException, MailConfigException {
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().toString())
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(true, Collections.singleton(license));

        final long notificationTime = clock.millis();
        final Builder builder = SystemNotification.newBuilder();
        builder.setBroadcastId(1L)
                .setCategory(Category.newBuilder().setLicense(SystemNotification.License.newBuilder().build()).build())
                .setDescription(LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT)
                .setShortDescription(LicenseCheckService.LICENSE_HAS_EXPIRED)
                .setSeverity(Severity.CRITICAL)
                .setState(State.NOTIFY)
                .setGenerationTime(notificationTime);
        verify(systemNotificationIMessageSender).sendMessage(builder.build());
        verify(mailManager).sendMail(Collections.singletonList(EMAIL),
                LicenseCheckService.LICENSE_HAS_EXPIRED,
                LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT);
    }


    /**
     * Test when license is going to expire and was over limit.
     * Both license will be expire and overlimit warning will be send
     */
    @Test
    public void testPublishNotificationLicenseGoingToExpirAndOverLimit() throws CommunicationException,
            InterruptedException, MailException, MailConfigException {
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().plusDays(NUM_BEFORE_LICENSE_EXPIRATION_DAYS - 1).toString())
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(true, Collections.singleton(license));

        final long notificationTime = clock.millis();
        final Builder builder = SystemNotification.newBuilder();
        builder.setBroadcastId(1L)
                .setCategory(Category.newBuilder().setLicense(SystemNotification.License.newBuilder().build()).build())
                .setDescription(LicenseCheckService.TURBONOMIC_LICENSE_HAS_EXPIRED_PLEASE_UPDATE_IT)
                .setShortDescription(LicenseCheckService.LICENSE_HAS_EXPIRED)
                .setSeverity(Severity.CRITICAL)
                .setState(State.NOTIFY)
                .setGenerationTime(notificationTime);
        verify(systemNotificationIMessageSender, times(2)).sendMessage(any());
        verify(mailManager, times(2)).sendMail(anyList(), any(), any());
    }

    /**
     * Test if license is going to expire, both system notification and email will be sent
     */
    @Test
    public void testPublishNotificationLicenseGoingToExpire() throws MailException, MailConfigException {

        final String expirationDate = LocalDate.now().plusDays(1L).toString();
        final LicenseDTO license = LicenseDTO.newBuilder()
                .setExpirationDate(expirationDate)
                .setEmail(EMAIL)
                .build();
        licenseCheckService.publishNotification(false, Collections.singleton(license));

        final long notificationTime = clock.millis();
        final Builder builder = SystemNotification.newBuilder();
        final String description = String.format(LicenseCheckService.TURBONOMIC_LICENSE_WILL_EXPIRE,
                expirationDate);

        builder.setBroadcastId(1L)
                .setCategory(Category.newBuilder().setLicense(SystemNotification.License.newBuilder()
                        .build())
                        .build())
                .setDescription(description)
                .setShortDescription(description)
                .setSeverity(Severity.CRITICAL)
                .setState(State.NOTIFY)
                .setGenerationTime(notificationTime);

        verify(mailManager).sendMail(Collections.singletonList(EMAIL),
                description, description);
    }

    /**
     * Test when license is empty, will send notification to UI
     */
    @Test
    public void testEmptyLicense() throws IOException,
            CommunicationException, InterruptedException, MailException, MailConfigException {
        when(licenseManagerService.getLicenses()).thenReturn(Collections.emptyList());
        licenseCheckService.onSourceTopologyAvailable(1L, 777777L);
        final long notificationTime = clock.millis();
        final Builder builder = SystemNotification.newBuilder();
        builder.setBroadcastId(1L)
                .setCategory(Category.newBuilder().setLicense(SystemNotification.License.newBuilder()
                        .build())
                        .build())
                .setDescription(LicenseCheckService.TURBONOMIC_LICENSE_IS_MISSING)
                .setShortDescription(LicenseCheckService.LICENSE_IS_MISSING)
                .setSeverity(Severity.CRITICAL)
                .setState(State.NOTIFY)
                .setGenerationTime(notificationTime);

        verify(systemNotificationIMessageSender).sendMessage(builder.build());
        verify(mailManager, never()).sendMail(anyList(), any(), any());
    }

    /**
     * Test when license is valid, no notification or email is sent.
     */
    @Test
    public void testValidLicense() throws IOException,
            CommunicationException, InterruptedException, MailException, MailConfigException {
        final LicenseDTO licenseDTO = LicenseDTO.newBuilder()
                .setExpirationDate(LocalDate.now().plusYears(1L).toString())
                .setCountedEntity(CountedEntity.SOCKET.name())
                .build();
        when(licenseManagerService.getLicenses()).thenReturn(Collections.singleton(licenseDTO));
        licenseCheckService.onSourceTopologyAvailable(1L, 777777L);
        verify(systemNotificationIMessageSender, never()).sendMessage(any());
        verify(mailManager, never()).sendMail(anyList(), any(), any());
    }
}
