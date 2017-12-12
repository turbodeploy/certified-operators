package com.vmturbo.reports.component;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.eclipse.birt.core.exception.BirtException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import com.vmturbo.api.enums.ReportOutputFormat;

/**
 * Test to cover functionality of {@link ComponentReportRunner}.
 */
public class ComponentReportRunnerTest {

    private static final String EXISTING_REPORT_ID =
            "/VmtReports/daily_infra_utilization_levels_by_hour_line.rptdesign";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();
    private ComponentReportRunner reportRunner;
    private ExecutorService threadPool;
    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void init() throws BirtException {
        threadPool = Executors.newCachedThreadPool();
        reportRunner = new ComponentReportRunner();
    }

    @After
    public void destroy() {
        reportRunner.close();
        threadPool.shutdownNow();
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
        try (final InputStream is = reportRunner.createReport(request);) {
            Files.copy(is, outputFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            Assert.assertTrue(outputFile.isFile());
            Assert.assertTrue(outputFile.length() > 0);
        }
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
        reportRunner.createReport(request);
    }
}
