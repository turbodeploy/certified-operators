package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.report.ReportApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiInputDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiInputDTO;
import com.vmturbo.api.dto.report.ReportTemplateApiInputDTO;
import com.vmturbo.api.enums.Period;
import com.vmturbo.api.serviceinterfaces.IReportsService;
import com.vmturbo.reporting.api.protobuf.Reporting.EmptyRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportResponse;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;

/**
 * Service implementation of Reports.
 **/
public class ReportsService implements IReportsService {

    private final Logger logger = LogManager.getLogger(getClass());

    private final ReportingServiceBlockingStub reportingService;

    /**
     * Constructs reporting service with active connection to reporting component.
     *
     * @param reportingService reporting service GRPC connection.
     */
    public ReportsService(@Nonnull ReportingServiceBlockingStub reportingService) {
        this.reportingService = Objects.requireNonNull(reportingService);
    }

    @Override
    public ReportApiDTO getReports() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportInstanceApiDTO> getInstancesList() throws Exception {
        // TODO implement OM-28795
        return Collections.emptyList();
    }

    @Override
    public ReportInstanceApiDTO getReportInstanceByID(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportScheduleApiDTO> getSchedulesList() throws Exception {
        // TODO implement OM-28924
        return Collections.emptyList();
    }

    @Override
    public ReportScheduleApiDTO getReportScheduleByID(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportApiDTO> getTemplatesList() {
        final List<ReportApiDTO> result = new ArrayList<>();
        final Iterator<ReportTemplate> iterator =
                reportingService.listAllTemplates(EmptyRequest.getDefaultInstance());
        while (iterator.hasNext()) {
            final ReportTemplate template = iterator.next();
            result.add(convert(template));
        }
        return result;
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
    public ReportInstanceApiDTO generateReportTemplateInstance(final String templateApiId,
            final ReportInstanceApiInputDTO reportApiRequest) {
        logger.debug("Report generation requested for template {} and format {} with attributes {}",
                templateApiId::toString, reportApiRequest::getFormat,
                reportApiRequest::getAttributes);
        final int templateId = getReportTemplateId(templateApiId);
        final GenerateReportRequest.Builder builder =
                GenerateReportRequest.newBuilder().setReportId(templateId);
        builder.setFormat(reportApiRequest.getFormat().ordinal());
        // TODO add attributes, as soon as we face at least one real use case
        final ReportResponse response = reportingService.generateReport(builder.build());
        final ReportInstanceApiDTO result = new ReportInstanceApiDTO();
        result.setFilename(Long.toString(response.getFileName()));
        logger.trace("Report generation finished successfully into file {}", result::getFilename);
        return result;
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

    /**
     * Converts reporting template from Protobuf representation into REST API representation.
     *
     * @param src protobuf representation
     * @return REST API reporesentation
     */
    @Nonnull
    private static ReportApiDTO convert(@Nonnull ReportTemplate src) {
        final ReportApiDTO dst = new ReportApiDTO();
        dst.setTemplateID(1, src.getId());
        dst.setFileName(src.getFilename());
        dst.setTitle(src.getTitle());
        dst.setCategory(src.getCategory());
        dst.setDescription(src.getDescription());
        if (src.hasPeriod()) {
            dst.setPeriod(Period.values()[src.getPeriod()]);
        }
        if (src.hasDayType()) {
            dst.setScheduled(false);
        }
        return dst;
    }

    /**
     * Extracts report templated id from the IU supplied Id.In the UI report template is combined in
     * form (reportType)_(templateId).
     *
     * @param reportApiId UI supplied Id.
     * @return internal report template id.
     */
    private int getReportTemplateId(@Nonnull String reportApiId) {
        final String[] parts = reportApiId.split("_");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Report id is malformed. Should be <reportType>_<id>. but is " + reportApiId);
        }
        try {
            return Integer.valueOf(parts[1]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Report id is not parsable: " + parts[0] + " from report id request " +
                            reportApiId);
        }
    }
}
