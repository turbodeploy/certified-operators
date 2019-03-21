package com.vmturbo.reports.component.data;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.reports.component.data.vm.Daily_vm_rightsizing_advice_grid;
import com.vmturbo.sql.utils.DbException;

/**
 * Unit test to cover all the cases of generating missing data for report in {@link ReportsDataGenerator}.
 */
public class ReportingDataGeneratorTest {

    private ReportsDataContext reportsDataContext;
    private ReportsDataGenerator reportsDataGenerator;
    private Map<Integer, com.vmturbo.reports.component.data.ReportTemplate> reportMap;
    private Daily_vm_rightsizing_advice_grid daily_vm_rightsizing_advice_grid =
        mock(Daily_vm_rightsizing_advice_grid.class);

    @Before
    public void init() throws Exception {
        reportsDataContext = mock(ReportsDataContext.class);
        reportMap = getReportMap();
        reportsDataGenerator = new ReportsDataGenerator(reportsDataContext, reportMap);
    }

    @After
    public void cleanup() {
    }

    @Test
    public void testGenerateByTemplateId() throws DbException {
        reportsDataGenerator.generateByTemplateId(150);
        verify(daily_vm_rightsizing_advice_grid).generateData(any());

    }

    @Test
    public void testGenerateByUndefinedTemplateId() throws DbException {
        reportsDataGenerator.generateByTemplateId(151);
        verify(daily_vm_rightsizing_advice_grid, never()).generateData(any());

    }

    private Map<Integer, com.vmturbo.reports.component.data.ReportTemplate> getReportMap() {
        return ImmutableMap.of(150, daily_vm_rightsizing_advice_grid);
    }
}
