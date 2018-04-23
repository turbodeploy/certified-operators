package com.vmturbo.reports.component;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

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
import com.vmturbo.reporting.api.protobuf.Reporting.ReportAttribute;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportData;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstance;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;
import com.vmturbo.sql.utils.DbException;

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
        testReportWithIntegerAndBoolParam(templates);
        testGroupScoped(templates);
    }

    private void testGroupScoped(@Nonnull Collection<ReportTemplate> templates) throws Exception {
        for (ReportTemplate template : templates) {
            final int reportTypeInt = template.getId().getReportType();
            final ReportType reportType = ReportType.get(reportTypeInt);
            if (reportType == null) {
                Assert.fail("Unknown report type found: " + reportTypeInt);
            }
            switch (reportType) {
                case BIRT_STANDARD:
                    Assert.assertFalse(template.hasIsGroupScoped());
                    break;
                case BIRT_ON_DEMAND:
                    Assert.assertTrue(template.hasIsGroupScoped());
                    break;
                default:
                    Assert.fail(
                            "Report type " + reportType + " is not supported. Found for report " +
                                    template.getId());
            }
        }
        final ReportTemplate hostSummary = getTemplate(templates, ReportType.BIRT_ON_DEMAND, 2);
        Assert.assertFalse(hostSummary.getIsGroupScoped());
        final ReportTemplate hostGroupSummary =
                getTemplate(templates, ReportType.BIRT_ON_DEMAND, 1);
        Assert.assertTrue(hostGroupSummary.getIsGroupScoped());
    }

    private void testReportWithIntegerAndBoolParam(final Collection<ReportTemplate> templates)
            throws DbException {
        final int onDemandTemplateId =
                getTemplate(templates, ReportType.BIRT_ON_DEMAND, 6).getId().getId();
        final ReportTemplate onDemandTemplate = reportingConfig.templatesOrganizer()
                .getTemplateById(ReportType.BIRT_ON_DEMAND, onDemandTemplateId)
                .get()
                .toProtobuf();
        Assert.assertEquals(ReportType.BIRT_ON_DEMAND.getValue(),
                onDemandTemplate.getId().getReportType());
        Assert.assertEquals(onDemandTemplateId, onDemandTemplate.getId().getId());
        Assert.assertEquals(2, onDemandTemplate.getAttributesCount());
        final ReportAttribute boolAttr = onDemandTemplate.getAttributesList()
                .stream()
                .filter(att -> att.getName().equals("show_charts"))
                .findFirst()
                .get();
        Assert.assertEquals("True", boolAttr.getDefaultValue());
        Assert.assertEquals("BOOLEAN", boolAttr.getValueType());

        final ReportAttribute intAttr = onDemandTemplate.getAttributesList()
                .stream()
                .filter(att -> att.getName().equals("num_days_ago"))
                .findFirst()
                .get();
        Assert.assertEquals("30", intAttr.getDefaultValue());
        Assert.assertEquals("INTEGER", intAttr.getValueType());
    }

    @Nonnull
    private static ReportTemplate getTemplate(@Nonnull Collection<ReportTemplate> templates,
            @Nonnull ReportType reportType, int reportId) {
        return templates.stream()
                .filter(reportTemplate -> reportTemplate.getId().getReportType() ==
                        reportType.getValue())
                .filter(reportTemplate -> reportTemplate.getId().getId() == reportId)
                .findFirst()
                .get();
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
        final GenerateReportRequest request = GenerateReportRequest.newBuilder()
                .setFormat(ReportOutputFormat.PDF.getLiteral())
                .setTemplate(template.getId())
                .build();

        final ReportInstanceId response = stub.generateReport(request);
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
