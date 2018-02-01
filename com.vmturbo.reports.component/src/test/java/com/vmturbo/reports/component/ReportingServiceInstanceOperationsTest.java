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
import com.vmturbo.reports.component.communication.ReportNotificationSender;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.db.tables.pojos.ReportInstance;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.schedules.ScheduleDAO;
import com.vmturbo.reports.component.templates.TemplatesDao;
import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit test for report instance operations of {@link ReportingServiceRpc} class.
 */
public class ReportingServiceInstanceOperationsTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private ReportInstanceDao instancesDao;
    private ReportInstance reportInstance1;
    private ReportInstance reportInstance2;
    private StreamObserver<Reporting.ReportInstance> observer;
    private ReportingServiceRpc reportingServer;
    private ExecutorService threadPool;
    private ArgumentCaptor<Reporting.ReportInstance> instanceCaptor;

    @Before
    public void init() throws Exception {
        final StandardReportsRecord reportTemplate = new StandardReportsRecord();
        reportTemplate.setId(100);
        reportTemplate.setFilename("some-file-name");

        reportInstance1 = new ReportInstance();
        reportInstance1.setId(1L);
        reportInstance1.setGenerationTime(new Timestamp(1234L));
        reportInstance1.setOutputFormat(ReportOutputFormat.PDF);
        reportInstance1.setTemplateId(100);

        reportInstance2 = new ReportInstance();
        reportInstance2.setId(2L);
        reportInstance2.setGenerationTime(new Timestamp(33344L));
        reportInstance2.setOutputFormat(ReportOutputFormat.PDF);
        reportInstance2.setTemplateId(100);

        final TemplatesDao templatesDao = Mockito.mock(TemplatesDao.class);
        Mockito.when(templatesDao.getTemplateById(Mockito.anyInt()))
                .thenReturn(Optional.of(reportTemplate));
        final ComponentReportRunner reportRunner = Mockito.mock(ComponentReportRunner.class);
        instancesDao = Mockito.mock(ReportInstanceDao.class);
        final ScheduleDAO scheduleDAO = Mockito.mock(ScheduleDAO.class);
        observer = (StreamObserver<Reporting.ReportInstance>)Mockito.mock(StreamObserver.class);

        final ReportNotificationSender notificationSender =
                Mockito.mock(ReportNotificationSender.class);
        threadPool = Executors.newCachedThreadPool();

        reportingServer =
                new ReportingServiceRpc(reportRunner, templatesDao, instancesDao, scheduleDAO,
                        tmpFolder.newFolder(), threadPool, notificationSender);
        instanceCaptor = ArgumentCaptor.forClass(Reporting.ReportInstance.class);
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
        Mockito.when(instancesDao.getAllInstances())
                .thenThrow(new DbException("Exception for test"));
        reportingServer.listAllInstances(Empty.getDefaultInstance(), observer);
        Mockito.verify(observer, Mockito.never()).onNext(instanceCaptor.capture());
        Mockito.verify(observer, Mockito.never()).onCompleted();
        final ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
        Mockito.verify(observer).onError(captor.capture());
        Assert.assertThat(captor.getValue(), CoreMatchers.instanceOf(StatusRuntimeException.class));
        Assert.assertThat(captor.getValue().getMessage(),
                CoreMatchers.containsString("Exception for test"));
    }

    private static void assertEquals(@Nonnull ReportInstance expected,
            @Nonnull Reporting.ReportInstance actual) {
        Assert.assertEquals((long)expected.getId(), actual.getId());
        Assert.assertEquals(expected.getGenerationTime().getTime(), actual.getGenerationTime());
        Assert.assertEquals(expected.getOutputFormat().getLiteral(), actual.getFormat());
        Assert.assertEquals(ReportType.BIRT_STANDARD.getValue(), actual.getReportType());
        Assert.assertEquals((int)expected.getTemplateId(), actual.getTemplateId());
    }
}
