package com.vmturbo.reports.component;

import java.io.File;
import java.util.Collections;

import org.eclipse.birt.core.exception.BirtException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.annotation.DirtiesContext.ClassMode;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.vmturbo.api.enums.ReportOutputFormat;

/**
 * Test to cover functionality of {@link ComponentReportRunner}.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(loader = AnnotationConfigContextLoader.class,
        classes = {ReportingTestConfig.class})
@DirtiesContext(classMode = ClassMode.BEFORE_EACH_TEST_METHOD)
public class ComponentReportRunnerTest {

    private static final String EXISTING_REPORT_ID =
            "/VmtReports/daily_infra_utilization_levels_by_hour_line.rptdesign";

    @Autowired
    private ReportingTestConfig reportingTestConfig;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    private ComponentReportRunner reportRunner;

    @Before
    public void init() throws BirtException {
        reportRunner = reportingTestConfig.reportRunner();
    }

    /**
     * Tests the existing report and verifies, that size of generated PDF is non-zero.
     *
     * @throws Exception in any exceptions occurred.
     */
    @Test
    public void testComponentReportRunner() throws Exception {
        final ReportRequest request =
                new ReportRequest(EXISTING_REPORT_ID, ReportOutputFormat.PDF,
                        Collections.emptyMap());
        final File outputFile = tmpFolder.newFile();
        reportRunner.createReport(request, outputFile);
        Assert.assertTrue(outputFile.isFile());
        Assert.assertTrue(outputFile.length() > 0);
    }

    /**
     * Tests non existing report.
     *
     * @throws Exception in any exceptions occurred.
     */
    @Test
    public void testNonExistingReport() throws Exception {
        final ReportRequest request =
                new ReportRequest("some-non-existing-report", ReportOutputFormat.PDF,
                        Collections.emptyMap());
        expectedException.expect(ReportingException.class);
        reportRunner.createReport(request, tmpFolder.newFile());
    }
}
