package com.vmturbo.reports.component;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.reporting.api.ReportListener;
import com.vmturbo.reporting.api.protobuf.Reporting.Empty;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportData;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstance;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;

/**
 * Integration tests to test all the functionality from the point of view of GRPC client.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
        classes = {ReportingTestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class ReportingServiceTest {

    private static long TIMEOUT_MS = 60 * 1000;

    @Autowired
    private ReportingTestConfig reportingConfig;

    /**
     * Tests retrieval of the standard report templates. They are pre-created in the DB. It is
     * expected, that return list of templates will not be empty.
     *
     * @throws Exception if exception raised.
     */
    @Test
    public void testRetrieveAllTemplates() throws Exception {
        final ReportingServiceBlockingStub stub =
                ReportingServiceGrpc.newBlockingStub(reportingConfig.planGrpcServer().getChannel());
        final Iterator<ReportTemplate> templatesIterator =
                stub.listAllTemplates(Empty.getDefaultInstance());
        final Collection<ReportTemplate> templates = Lists.newArrayList(templatesIterator);
        Assert.assertFalse(templates.isEmpty());
        Assert.assertEquals(Sets.newHashSet(ReportType.BIRT_STANDARD.getValue(),
                ReportType.BIRT_ON_DEMAND.getValue()), templates.stream()
                .map(ReportTemplate::getId)
                .map(ReportTemplateId::getReportType)
                .collect(Collectors.toSet()));
        final int standardTemplateId = templates.stream()
                .filter(reportTemplate -> reportTemplate.getId().getReportType() ==
                        ReportType.BIRT_STANDARD.getValue())
                .findFirst()
                .get()
                .getId().getId();
        final ReportTemplate standardTemplate = reportingConfig.templatesOrganizer()
                .getTemplateById(ReportType.BIRT_STANDARD, standardTemplateId)
                .get()
                .toProtobuf();
        Assert.assertEquals(ReportType.BIRT_STANDARD.getValue(),
                standardTemplate.getId().getReportType());
        Assert.assertEquals(standardTemplateId, standardTemplate.getId().getId());
        final int onDemandTemplateId = templates.stream()
                .filter(reportTemplate -> reportTemplate.getId().getReportType() ==
                        ReportType.BIRT_ON_DEMAND.getValue())
                .findFirst()
                .get()
                .getId()
                .getId();
        final ReportTemplate onDemandTemplate = reportingConfig.templatesOrganizer()
                .getTemplateById(ReportType.BIRT_ON_DEMAND, onDemandTemplateId)
                .get()
                .toProtobuf();
        Assert.assertEquals(ReportType.BIRT_ON_DEMAND.getValue(),
                onDemandTemplate.getId().getReportType());
        Assert.assertEquals(onDemandTemplateId, onDemandTemplate.getId().getId());
    }

    /**
     * Tests creation of the report. Report is expected to be generated.
     *
     * @throws Exception if exception raised.
     */
    @Test
    public void testCreateReport() throws Exception {
        final ReportingServiceBlockingStub stub =
                ReportingServiceGrpc.newBlockingStub(reportingConfig.planGrpcServer().getChannel());
        final Collection<ReportInstance> initialInstances =
                Sets.newHashSet(stub.listAllInstances(Empty.getDefaultInstance()));
        Assert.assertEquals(Collections.emptySet(), initialInstances);
        final ReportTemplate template = stub.listAllTemplates(Empty.getDefaultInstance()).next();
        final ReportInstanceId response = stub.generateReport(GenerateReportRequest.newBuilder()
                .setFormat(ReportOutputFormat.PDF.getLiteral())
                .setTemplate(template.getId())
                .build());
        Assert.assertTrue(response.getId() > 0);
        final ReportListener listener = Mockito.mock(ReportListener.class);

        reportingConfig.notificationReceiver().addListener(listener);
        Mockito.verify(listener, Mockito.timeout(TIMEOUT_MS)).onReportGenerated(response.getId());
        Mockito.verify(listener, Mockito.never())
                .onReportFailed(Mockito.anyLong(), Mockito.anyString());

        final ReportData data = stub.getReportData(response);
        Assert.assertTrue(data.getData().size() > 0);
        Assert.assertEquals(ReportOutputFormat.PDF.getLiteral(), data.getFormat());

        final Collection<ReportInstance> resultInstances =
                Sets.newHashSet(stub.listAllInstances(Empty.getDefaultInstance()));
        Assert.assertEquals(1, resultInstances.size());
        final ReportInstance instance = resultInstances.iterator().next();
        Assert.assertEquals(ReportOutputFormat.PDF.getLiteral(), instance.getFormat());
        Assert.assertEquals(response.getId(), instance.getId());
        Assert.assertEquals(template.getId(), instance.getTemplate());

        final Collection<ReportInstance> templateInstances =
                Sets.newHashSet(stub.getInstancesByTemplate(template.getId()));
        Assert.assertEquals(1, templateInstances.size());
        Assert.assertEquals(instance, templateInstances.iterator().next());

        Assert.assertEquals(Collections.emptySet(), Sets.newHashSet(stub.getInstancesByTemplate(
                ReportTemplateId.newBuilder()
                        .setId(-1)
                        .setReportType(ReportType.BIRT_STANDARD.getValue())
                        .build())));
    }
}
