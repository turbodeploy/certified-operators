package com.vmturbo.reports.component;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.commons.mail.EmailException;
import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.internal.exceptions.MockitoLimitations;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reports.component.communication.ReportNotificationSender;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.instances.ReportInstanceRecord;
import com.vmturbo.reports.component.instances.ReportsGenerator;
import com.vmturbo.reports.component.schedules.ScheduleDAO;
import com.vmturbo.reports.component.templates.TemplateWrapper;
import com.vmturbo.reports.component.schedules.Scheduler;
import com.vmturbo.reports.component.templates.TemplatesOrganizer;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit test to cover all the cases of generating report in {@link ReportingServiceRpc}.
 */
public class ReportingServiceReportGenerationTest {

    private static final long TIMEOUT_MS = 30 * 1000;

    private static final String EXCEPTION_MESSAGE = "Some underlying error";
    private static final String EMAIL_EXCEPTION_MESSAGE = "email provided";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private TemplatesOrganizer templatesOrganizer;
    private ReportInstanceDao instancesDao;
    private ScheduleDAO scheduleDAO;
    private ComponentReportRunner reportRunner;
    private ReportTemplate reportTemplate;
    private ReportInstanceRecord dirtyRecord;
    private GenerateReportRequest request;
    private StreamObserver<ReportInstanceId> observer;
    private ReportingServiceRpc reportingServer;
    private ReportNotificationSender notificationSender;
    private ExecutorService threadPool;
    private ReportsGenerator reportsGenerator;

    @Before
    public void init() throws Exception {
        reportTemplate = ReportTemplate.newBuilder()
                .setId(ReportTemplateId.newBuilder().setReportType(1).setId(100).build())
                .setDescription("Faculties of Hogwarts School of Whitchcraft and Wizardry")
                .build();
        final TemplateWrapper wrapper = Mockito.mock(TemplateWrapper.class);
        Mockito.when(wrapper.toProtobuf()).thenReturn(reportTemplate);
        Mockito.when(wrapper.getTemplateFile()).thenReturn("hogwarts-faculties");

        request = GenerateReportRequest.newBuilder()
                .setFormat(ReportOutputFormat.PDF.getLiteral())
                .setTemplate(reportTemplate.getId())
                .build();

        templatesOrganizer = Mockito.mock(TemplatesOrganizer.class);
        Mockito.when(templatesOrganizer.getTemplateById(Mockito.any(ReportType.class),
                Mockito.anyInt())).thenReturn(Optional.of(wrapper));
        reportRunner = Mockito.mock(ComponentReportRunner.class);
        dirtyRecord = Mockito.mock(ReportInstanceRecord.class);
        Mockito.when(dirtyRecord.getId()).thenReturn(200L);
        instancesDao = Mockito.mock(ReportInstanceDao.class);
        Mockito.when(
                instancesDao.createInstanceRecord(Mockito.any(ReportType.class), Mockito.anyInt(),
                        Mockito.any(ReportOutputFormat.class))).thenReturn(dirtyRecord);
        scheduleDAO = Mockito.mock(ScheduleDAO.class);

        observer = (StreamObserver<ReportInstanceId>)Mockito.mock(StreamObserver.class);

        notificationSender = Mockito.mock(ReportNotificationSender.class);
        threadPool = Executors.newCachedThreadPool();

        final File outputDir = tmpFolder.newFolder();
        reportsGenerator = new ReportsGenerator(reportRunner, templatesOrganizer, instancesDao,
                        outputDir, threadPool, notificationSender, Mockito.mock(MailManager.class));
        reportingServer = new ReportingServiceRpc(templatesOrganizer, instancesDao,
                        outputDir, reportsGenerator, Mockito.mock(Scheduler.class));
    }

    @After
    public void cleanup() {
        threadPool.shutdownNow();
    }

    /**
     * Test proper report generation.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testGoodReport() throws Exception {
        reportingServer.generateReport(request, observer);
        // Steps from background steps are verified with timeout
        verifyGoodReport();
    }

    private void verifyGoodReport()
                    throws DbException, InterruptedException, CommunicationException {
        Mockito.verify(dirtyRecord, Mockito.timeout(TIMEOUT_MS)).commit();
        Mockito.verify(dirtyRecord, Mockito.never()).rollback();
        Mockito.verify(observer).onCompleted();
        Mockito.verify(observer).onNext(Mockito.any());
        Mockito.verify(observer, Mockito.never()).onError(Mockito.any());
        Mockito.verify(notificationSender, Mockito.timeout(TIMEOUT_MS))
                .notifyReportGenerated(Mockito.anyLong());
        Mockito.verify(notificationSender, Mockito.never())
                .notifyReportGenerationFailed(Mockito.anyLong(), Mockito.anyString());
    }

    /**
     * Test proper absence of template in the DB. Failure is expected to be returned.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testNoTemplate() throws Exception {
        Mockito.when(templatesOrganizer.getTemplateById(Mockito.any(ReportType.class),
                Mockito.anyInt())).thenReturn(Optional.empty());
        reportingServer.generateReport(request, observer);
        Assert.assertThat(expectSyncFailure().getMessage(),
                CoreMatchers.containsString("Could not find report template by id"));
    }

    /**
     * Test DB error operating with templates. Failure is expected.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testDbErrorFromTemplate() throws Exception {
        Mockito.when(templatesOrganizer.getTemplateById(Mockito.any(ReportType.class),
                Mockito.anyInt())).thenThrow(new DbException(EXCEPTION_MESSAGE));
        reportingServer.generateReport(request, observer);
        Assert.assertThat(expectSyncFailure().getMessage(),
                CoreMatchers.containsString(EXCEPTION_MESSAGE));
    }

    /**
     * Test DB error operating with report instances records. Failure is expected.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testDbErrorFromInstances() throws Exception {
        Mockito.when(
                instancesDao.createInstanceRecord(Mockito.any(ReportType.class), Mockito.anyInt(),
                        Mockito.any())).thenThrow(new DbException(EXCEPTION_MESSAGE));
        reportingServer.generateReport(request, observer);
        Assert.assertThat(expectSyncFailure().getMessage(),
                CoreMatchers.containsString(EXCEPTION_MESSAGE));
    }

    /**
     * Tests error generating the report itself. Failure is expected. Record is expected to be
     * rolled back.
     *
     * @throws Exception if exceptions occurred
     */
    @Test
    public void testErrorGeneratingReport() throws Exception {
        Mockito.doThrow(new ReportingException(EXCEPTION_MESSAGE, new NullPointerException()))
                .when(reportRunner)
                .createReport(Mockito.any(), Mockito.any());
        reportingServer.generateReport(request, observer);
        Mockito.verify(dirtyRecord, Mockito.timeout(TIMEOUT_MS)).rollback();
        Assert.assertThat(expectAsyncFailure(), CoreMatchers.containsString(EXCEPTION_MESSAGE));
    }

    /**
     * Tests that generating of report with correct email provided is succeed.
     *
     * @throws Exception if exception occured.
     */
    @Test
    public void testGenerateReportWithCorrectEmail() throws Exception{
        reportingServer.generateReport(request.toBuilder().addSubcribersEmails("correct@email.com")
                        .build(), observer);
        verifyGoodReport();
    }

    /**
     * Tests that generating of report with incorrect email provided is failed.
     */
    @Test
    public void testGenerateReportWithIncorrectCorrectEmail() {
        final ArgumentCaptor<StatusRuntimeException> argument =
                        ArgumentCaptor.forClass(StatusRuntimeException.class);
        reportingServer.generateReport(request.toBuilder()
                        .addSubcribersEmails("incorrectemail").build(), observer);
        Mockito.verify(observer).onError(argument.capture());
        Assert.assertThat(argument.getValue().getStatus().getDescription(),
                        CoreMatchers.containsString(EMAIL_EXCEPTION_MESSAGE));
    }

    /**
     * Expects synchronous report generation error (befor the generation itself). Initial GRPC call
     * is expected to finish with failure. No notifications are expected.
     *
     * @return exception thrown while submitting for the report generation
     * @throws Exception if some exception occur
     */
    private Throwable expectSyncFailure() throws Exception {
        Mockito.verify(observer, Mockito.never()).onCompleted();
        Mockito.verify(observer, Mockito.never()).onNext(Mockito.any());
        final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(observer).onError(captor.capture());
        Mockito.verify(dirtyRecord, Mockito.never()).commit();
        Mockito.verify(notificationSender, Mockito.never())
                .notifyReportGenerated(Mockito.anyLong());
        Mockito.verify(notificationSender, Mockito.never())
                .notifyReportGenerationFailed(Mockito.anyLong(), Mockito.anyString());
        return captor.getValue();
    }

    /**
     * Expect failure in asyncrhonous part of report generation (BIRT itself and later). Initial
     * GRPC call is expected to finish successfully. Failure should be reported by notifications
     * later.
     *
     * @return failure message string
     * @throws Exception if some exception occur
     */
    private String expectAsyncFailure() throws Exception {
        Mockito.verify(observer).onCompleted();
        Mockito.verify(observer).onNext(Mockito.any());
        Mockito.verify(observer, Mockito.never()).onError(Mockito.any());
        Mockito.verify(dirtyRecord, Mockito.never()).commit();
        Mockito.verify(notificationSender, Mockito.never())
                .notifyReportGenerated(Mockito.anyLong());
        final ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        Mockito.verify(notificationSender, Mockito.timeout(TIMEOUT_MS))
                .notifyReportGenerationFailed(Mockito.anyLong(), captor.capture());
        return captor.getValue();
    }
}
