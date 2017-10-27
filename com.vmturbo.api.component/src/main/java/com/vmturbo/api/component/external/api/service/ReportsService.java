package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.report.ReportApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiInputDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiInputDTO;
import com.vmturbo.api.dto.report.ReportTemplateApiInputDTO;
import com.vmturbo.api.serviceinterfaces.IReportsService;

/**
 * Service implementation of Reports
 **/
public class ReportsService implements IReportsService {
    @Override
    public ReportApiDTO getReports() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportInstanceApiDTO> getInstancesList() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportInstanceApiDTO getReportInstanceByID(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportScheduleApiDTO> getSchedulesList() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO getReportScheduleByID(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportApiDTO> getTemplatesList() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportApiDTO getReportTemplateByID(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void createReportTemplate(final ReportTemplateApiInputDTO reportTemplateApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteReportTemplate(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportInstanceApiDTO> getReportTemplateInstanceList(final String s, final String s1, final String s2, final boolean b) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportInstanceApiDTO generateReportTemplateInstance(final String s, final ReportInstanceApiInputDTO reportInstanceApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportInstanceApiDTO getReportTemplateInstance(final String s, final String s1) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportScheduleApiDTO> getReportTemplateScheduleList(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO addReportTemplateSchedule(final String s, final ReportScheduleApiInputDTO reportScheduleApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO getReportTemplateSchedule(final String s, final int i) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO editReportTemplateSchedule(final String s, final int i, final ReportScheduleApiInputDTO reportScheduleApiInputDTO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteReportTemplateSchedule(final String s, final int i) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
