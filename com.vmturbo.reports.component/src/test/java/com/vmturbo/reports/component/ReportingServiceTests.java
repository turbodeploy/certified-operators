package com.vmturbo.reports.component;

import java.util.Collection;
import java.util.Iterator;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.reporting.api.protobuf.Reporting.EmptyRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportResponse;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;

/**
 * Integration tests to test all the functionality from the point of view of GRPC client.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
        classes = {ReportingTestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class ReportingServiceTests {

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
                stub.listAllTemplates(EmptyRequest.getDefaultInstance());
        final Collection<ReportTemplate> templates = Lists.newArrayList(templatesIterator);
        Assert.assertFalse(templates.isEmpty());
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
        final ReportTemplate template =
                stub.listAllTemplates(EmptyRequest.getDefaultInstance()).next();
        final ReportResponse response = stub.generateReport(GenerateReportRequest.newBuilder()
                .setFormat(ReportOutputFormat.PDF.getLiteral())
                .setReportId(template.getId())
                .build());
        Assert.assertTrue(response.getFileName() > 0);
    }
}
