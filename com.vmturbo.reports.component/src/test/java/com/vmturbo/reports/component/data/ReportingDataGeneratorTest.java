package com.vmturbo.reports.component.data;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reports.component.data.vm.Daily_vm_rightsizing_advice_grid;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit test to cover all the cases of generating missing data for report in {@link ReportsDataGenerator}.
 */
public class ReportingDataGeneratorTest {

    private ReportsDataContext reportsDataContext;
    private ReportsDataGenerator reportsDataGenerator;
    private Map<Long, com.vmturbo.reports.component.data.ReportTemplate> reportMap;
    private Daily_vm_rightsizing_advice_grid daily_vm_rightsizing_advice_grid =
        mock(Daily_vm_rightsizing_advice_grid.class);

    @Before
    public void init() throws Exception {
        reportsDataContext = mock(ReportsDataContext.class);
        reportMap = getReportMap();
        reportsDataGenerator = new ReportsDataGenerator(reportsDataContext, reportMap);
        final ReportDBDataWriter reportDBDataWriter = mock(ReportDBDataWriter.class);
        when(reportsDataContext.getReportDataWriter()).thenReturn(reportDBDataWriter);

    }

    @After
    public void cleanup() {
    }

    @Test
    public void testGenerateByTemplateId() throws DbException {
        final GenerateReportRequest request = GenerateReportRequest.newBuilder()
            .setFormat(ReportOutputFormat.PDF.getLiteral())
            .setTemplate((ReportTemplateId.newBuilder().setId(150).setReportType(1).build()))
            .build();
        reportsDataGenerator.generateDataByRequest(request);
        verify(daily_vm_rightsizing_advice_grid).generateData(any(), any());

    }

    @Test
    public void testGenerateByUndefinedTemplateId() throws DbException {
        final GenerateReportRequest request = GenerateReportRequest.newBuilder()
            .setFormat(ReportOutputFormat.PDF.getLiteral())
            .setTemplate((ReportTemplateId.newBuilder().setId(151).setReportType(1).build()))
            .build();
        reportsDataGenerator.generateDataByRequest(request);
        verify(daily_vm_rightsizing_advice_grid, never()).generateData(any(), any());

    }

    private Map<Long, com.vmturbo.reports.component.data.ReportTemplate> getReportMap() {
        return ImmutableMap.of(150L, daily_vm_rightsizing_advice_grid);
    }
}
