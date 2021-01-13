package com.vmturbo.api.component.external.api.service;

import java.time.Instant;
import java.util.List;

import javax.annotation.Nonnull;

import org.springframework.web.multipart.MultipartFile;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.report.ReportApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiInputDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiInputDTO;
import com.vmturbo.api.dto.report.ReportTemplateApiInputDTO;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.api.serviceinterfaces.IReportsService;

/**
 * The classic reports interface is not supported in XL.
 */
public class ReportsService implements IReportsService {

    @Override
    public ReportApiDTO getReports() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportInstanceApiDTO> getInstancesList() {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportInstanceApiDTO getReportInstanceByID(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportScheduleApiDTO> getSchedulesList() {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO getReportScheduleByID(final String scheduleId) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    //TODO : Add implementation to use parameter "ReportType... types".
    public List<ReportApiDTO> getTemplatesList(ReportType... types) {
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
    public List<ReportInstanceApiDTO> getReportTemplateInstanceList(
            final @Nonnull String templateApiId, final @Nonnull Instant startTime,
            final @Nonnull Instant endTime, final boolean exteractZip) {
        throw ApiUtils.notImplementedInXL();
    }

    /**
     * Generates a new report of the specified template id and report parameters.
     *
     * @param templateApiId report template id
     * @param reportApiRequest report parameters for generation
     * @return report instance id. Really, UI does not use it, but we still return to REST API for
     *         other users
     */
    @Override
    public ReportInstanceApiDTO generateReportTemplateInstance(final String jSessionId,
            final String templateApiId, final ReportInstanceApiInputDTO reportApiRequest) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportInstanceApiDTO getReportTemplateInstance(final String s, final String s1) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportScheduleApiDTO> getReportTemplateScheduleList(final String reportTypeAndTemplateId) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    @Nonnull
    public ReportScheduleApiDTO addReportTemplateSchedule(final String reportTypeAndTemplateId,
            final ReportScheduleApiInputDTO reportScheduleApiInputDTO) {
        throw ApiUtils.notImplementedInXL();
    }
    @Override
    public ReportScheduleApiDTO getReportTemplateSchedule(final String s, final long i) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportScheduleApiDTO editReportTemplateSchedule(final String s, final long i, final ReportScheduleApiInputDTO reportScheduleApiInputDTO) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public void deleteReportTemplateSchedule(final String s, final long i) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportApiDTO createCustomReportTemplate(String arg0, ReportApiDTO arg1) {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public ReportApiDTO validateCustomReportTemplate(String arg0, String arg1, MultipartFile arg2) {
        throw ApiUtils.notImplementedInXL();
    }
}
