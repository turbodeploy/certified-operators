package com.vmturbo.api.component.external.api.service;

import java.util.List;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.ReportApiDTO;
import com.vmturbo.api.dto.ReportInstanceApiDTO;
import com.vmturbo.api.dto.ReportScheduleApiDTO;
import com.vmturbo.api.dto.input.ReportInstanceApiInputDTO;
import com.vmturbo.api.dto.input.ReportScheduleApiInputDTO;
import com.vmturbo.api.dto.input.ReportTemplateApiInputDTO;
import com.vmturbo.api.serviceinterfaces.IReportsService;

/**
 * Service implementation of Reports
 **/
public class ReportsService implements IReportsService {
    @Override
    public List<ReportApiDTO> getFullReportsList(String userName, Integer reportType) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportApiDTO getReportByID(String templateId) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void createReportTemplate(ReportTemplateApiInputDTO instanceParams) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteReportTemplate(String templateId) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportInstanceApiDTO> getReportInstanceList(String templateId, String startTimeISO, String endTimeISO) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportInstanceApiDTO generateReportInstance(String templateId,
                                                       ReportInstanceApiInputDTO reportInstanceApiInputDTO)
        throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportInstanceApiDTO getReportInstance(String templateId, String instanceId) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportScheduleApiDTO> getScheduleList(String templateId) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO addReportSchedule(String templateId, ReportScheduleApiInputDTO scheduleParams) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO getReportSchedule(String templateId, int scheduleId) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO editReportSchedule(String templateId, int scheduleId, ReportScheduleApiInputDTO scheduleParams) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteReportSchedule(String templateId, int scheduleId) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }
}
