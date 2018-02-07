package com.vmturbo.reports.component;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.Reporting.Empty;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reports.component.communication.ReportNotificationSender;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.db.tables.pojos.ReportInstance;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.schedules.ScheduleDAO;
import com.vmturbo.reports.component.templates.TemplatesOrganizer;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit test for report instance operations of {@link ReportingServiceRpc} class.
 */
public class ReportingServiceInstanceOperationsTest {

    private static final int TEMPLATE_1 = 100;
    private static final DbException DB_EXCEPTION = new DbException("Avada Kedavra");

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private ReportInstanceDao instancesDao;
    private ReportInstance reportInstance1;
    private ReportInstance reportInstance2;
    private StreamObserver<Reporting.ReportInstance> observer;
    private ReportingServiceRpc reportingServer;
    private ExecutorService threadPool;
    private ArgumentCaptor<Reporting.ReportInstance> instanceCaptor;
    private ArgumentCaptor<Throwable> exceptionCaptor;

    @Before
    public void init() throws Exception {
        final ReportTemplate reportTemplate = ReportTemplate.newBuilder()
                .setId(ReportTemplateId.newBuilder().setId(100).setReportType(1))
                .setFilename("hogwarts-faculties")
                .setDescription("Faculties of Hogwarts School of Whitchcraft and Wizardry")
                .build();

        reportInstance1 = new ReportInstance();
        reportInstance1.setId(1L);
        reportInstance1.setGenerationTime(new Timestamp(1234L));
        reportInstance1.setOutputFormat(ReportOutputFormat.PDF);
        reportInstance1.setTemplateId(TEMPLATE_1);
        reportInstance1.setReportType(ReportType.BIRT_STANDARD);

        reportInstance2 = new ReportInstance();
        reportInstance2.setId(2L);
        reportInstance2.setGenerationTime(new Timestamp(33344L));
        reportInstance2.setOutputFormat(ReportOutputFormat.PDF);
        reportInstance2.setTemplateId(TEMPLATE_1);
        reportInstance2.setReportType(ReportType.BIRT_STANDARD);

        final TemplatesOrganizer templatesOrganizer = Mockito.mock(TemplatesOrganizer.class);
        Mockito.when(templatesOrganizer.getTemplateById(Mockito.any(ReportType.class),
                Mockito.anyInt())).thenReturn(Optional.of(reportTemplate));
        final ComponentReportRunner reportRunner = Mockito.mock(ComponentReportRunner.class);
        instancesDao = Mockito.mock(ReportInstanceDao.class);
        final ScheduleDAO scheduleDAO = Mockito.mock(ScheduleDAO.class);
        observer = (StreamObserver<Reporting.ReportInstance>)Mockito.mock(StreamObserver.class);

        final ReportNotificationSender notificationSender =
                Mockito.mock(ReportNotificationSender.class);
        threadPool = Executors.newCachedThreadPool();

        reportingServer =
                new ReportingServiceRpc(reportRunner, templatesOrganizer, instancesDao, scheduleDAO,
                        tmpFolder.newFolder(), threadPool, notificationSender);
        instanceCaptor = ArgumentCaptor.forClass(Reporting.ReportInstance.class);
        exceptionCaptor = ArgumentCaptor.forClass(Throwable.class);
    }

    /**
     * Tests retrieval of several report instances. All the values are expected to be processed and
     * passed back to GRPC service response
     *
     * @throws Exception if errors occur
     */
    @Test
    public void testRetrieveAllReports() throws Exception {
        Mockito.when(instancesDao.getAllInstances())
                .thenReturn(Arrays.asList(reportInstance1, reportInstance2));
        reportingServer.listAllInstances(Empty.getDefaultInstance(), observer);
        Mockito.verify(observer, Mockito.times(2)).onNext(instanceCaptor.capture());
        Mockito.verify(observer).onCompleted();
        Mockito.verify(observer, Mockito.never()).onError(Mockito.any());

        assertEquals(reportInstance1, instanceCaptor.getAllValues().get(0));
        assertEquals(reportInstance2, instanceCaptor.getAllValues().get(1));
    }

    /**
     * Tests failed report instances retrieval. Error response is expected to be sent to GRPC
     * service call.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testDbErrorRetrievingReports() throws Exception {
        Mockito.when(instancesDao.getAllInstances()).thenThrow(DB_EXCEPTION);
        reportingServer.listAllInstances(Empty.getDefaultInstance(), observer);
        Mockito.verify(observer, Mockito.never()).onNext(Mockito.any());
        Mockito.verify(observer, Mockito.never()).onCompleted();
        Mockito.verify(observer).onError(exceptionCaptor.capture());
        Assert.assertThat(exceptionCaptor.getValue(),
                CoreMatchers.instanceOf(StatusRuntimeException.class));
        Assert.assertThat(exceptionCaptor.getValue().getMessage(),
                CoreMatchers.containsString(DB_EXCEPTION.getMessage()));
    }

    /**
     * Tests retrieving of report instance by template id.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testRetrieveByTemplate() throws Exception {
        Mockito.when(instancesDao.getInstancesByTemplate(ReportType.BIRT_STANDARD, TEMPLATE_1))
                .thenReturn(Arrays.asList(reportInstance1, reportInstance2));
        reportingServer.getInstancesByTemplate(ReportTemplateId.newBuilder()
                .setReportType(ReportType.BIRT_STANDARD.getValue())
                .setId(TEMPLATE_1)
                .build(), observer);
        Mockito.verify(observer, Mockito.times(2)).onNext(instanceCaptor.capture());
        Mockito.verify(observer).onCompleted();
        Mockito.verify(observer, Mockito.never()).onError(Mockito.any());
        assertEquals(reportInstance1, instanceCaptor.getAllValues().get(0));
        assertEquals(reportInstance2, instanceCaptor.getAllValues().get(1));
    }

    /**
     * Tests retrieving of report instance by template id.
     *
     * @throws Exception if exceptions occur
     */
    @Test
    public void testRetrieveByTemplateFailure() throws Exception {
        Mockito.when(instancesDao.getInstancesByTemplate(Mockito.any(ReportType.class),
                Mockito.anyInt())).thenThrow(DB_EXCEPTION);
        reportingServer.getInstancesByTemplate(ReportTemplateId.newBuilder()
                .setReportType(ReportType.BIRT_STANDARD.getValue())
                .setId(TEMPLATE_1)
                .build(), observer);
        Mockito.verify(observer, Mockito.never()).onNext(Mockito.any());
        Mockito.verify(observer, Mockito.never()).onCompleted();
        Mockito.verify(observer).onError(exceptionCaptor.capture());
        Assert.assertThat(exceptionCaptor.getValue(),
                CoreMatchers.instanceOf(StatusRuntimeException.class));
        Assert.assertThat(exceptionCaptor.getValue().getMessage(),
                CoreMatchers.containsString(DB_EXCEPTION.getMessage()));
    }

    private static void assertEquals(@Nonnull ReportInstance expected,
            @Nonnull Reporting.ReportInstance actual) {
        Assert.assertEquals((long)expected.getId(), actual.getId());
        Assert.assertEquals(expected.getGenerationTime().getTime(), actual.getGenerationTime());
        Assert.assertEquals(expected.getOutputFormat().getLiteral(), actual.getFormat());
        Assert.assertEquals(ReportType.BIRT_STANDARD.getValue(),
                actual.getTemplate().getReportType());
        Assert.assertEquals((int)expected.getTemplateId(), actual.getTemplate().getId());
    }
}
