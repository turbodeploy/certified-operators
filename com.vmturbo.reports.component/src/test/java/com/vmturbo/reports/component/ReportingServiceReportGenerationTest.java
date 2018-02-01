package com.vmturbo.reports.component;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reports.component.communication.ReportNotificationSender;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.instances.ReportInstanceRecord;
import com.vmturbo.reports.component.schedules.ScheduleDAO;
import com.vmturbo.reports.component.templates.TemplatesDao;
import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit test to cover all the cases of generating report in {@link ReportingServiceRpc}.
 */
public class ReportingServiceReportGenerationTest {

    private static final long TIMEOUT_MS = 30 * 1000;

    private static final String EXCEPTION_MESSAGE = "Some underlying error";

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private TemplatesDao templatesDao;
    private ReportInstanceDao instancesDao;
    private ScheduleDAO scheduleDAO;
    private ComponentReportRunner reportRunner;
    private StandardReportsRecord reportTemplate;
    private ReportInstanceRecord dirtyRecord;
    private GenerateReportRequest request;
    private StreamObserver<ReportInstanceId> observer;
    private ReportingServiceRpc reportingServer;
    private ReportNotificationSender notificationSender;
    private ExecutorService threadPool;

    @Before
    public void init() throws Exception {
        reportTemplate = new StandardReportsRecord();
        reportTemplate.setId(100);
        reportTemplate.setFilename("some-file-name");
        request = GenerateReportRequest.newBuilder()
                .setFormat(ReportOutputFormat.PDF.getLiteral())
                .setReportId(reportTemplate.getId())
                .build();

        templatesDao = Mockito.mock(TemplatesDao.class);
        Mockito.when(templatesDao.getTemplateById(Mockito.anyInt()))
                .thenReturn(Optional.of(reportTemplate));
        reportRunner = Mockito.mock(ComponentReportRunner.class);
        dirtyRecord = Mockito.mock(ReportInstanceRecord.class);
        Mockito.when(dirtyRecord.getId()).thenReturn(200L);
        instancesDao = Mockito.mock(ReportInstanceDao.class);
        Mockito.when(instancesDao.createInstanceRecord(Mockito.anyInt(),
                Mockito.any(ReportOutputFormat.class))).thenReturn(dirtyRecord);
        scheduleDAO = Mockito.mock(ScheduleDAO.class);

        observer = (StreamObserver<ReportInstanceId>)Mockito.mock(StreamObserver.class);

        notificationSender = Mockito.mock(ReportNotificationSender.class);
        threadPool = Executors.newCachedThreadPool();

        reportingServer = new ReportingServiceRpc(reportRunner, templatesDao, instancesDao, scheduleDAO,
                tmpFolder.newFolder(), threadPool,  notificationSender);
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
        Mockito.when(templatesDao.getTemplateById(Mockito.anyInt())).thenReturn(Optional.empty());
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
        Mockito.when(templatesDao.getTemplateById(Mockito.anyInt()))
                .thenThrow(new DbException(EXCEPTION_MESSAGE));
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
        Mockito.when(instancesDao.createInstanceRecord(Mockito.anyInt(), Mockito.any()))
                .thenThrow(new DbException(EXCEPTION_MESSAGE));
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
     * Expects synchronous report generation error (befor the generation itself). Initial GRPC call
     * is expected to finish with failure. No notifications are expected.
     *
     * @return exception thrown while submitting for the report generation
     * @throws Exception if sume exception occur
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
     * @throws Exception if sume exception occur
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
