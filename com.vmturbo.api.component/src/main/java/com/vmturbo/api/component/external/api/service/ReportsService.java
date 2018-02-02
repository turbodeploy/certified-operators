package com.vmturbo.api.component.external.api.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.report.ReportApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiInputDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiInputDTO;
import com.vmturbo.api.dto.report.ReportTemplateApiInputDTO;
import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.Period;
import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.api.exceptions.UnknownObjectException;
import com.vmturbo.api.serviceinterfaces.IReportsService;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.Reporting.Empty;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstance;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;

/**
 * Service implementation of Reports.
 */
public class ReportsService implements IReportsService {

    private final Logger logger = LogManager.getLogger(getClass());

    private final ReportingServiceBlockingStub reportingService;

    private final GroupsService groupsService;

    /**
     * Constructs reporting service with active connection to reporting component.
     *
     * @param reportingService reporting service GRPC connection.
     */
    public ReportsService(@Nonnull ReportingServiceBlockingStub reportingService,
                    @Nonnull GroupsService groupsService) {
        this.reportingService = Objects.requireNonNull(reportingService);
        this.groupsService = Objects.requireNonNull(groupsService);
    }

    @Override
    public ReportApiDTO getReports() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportInstanceApiDTO> getInstancesList() throws Exception {
        final Map<ReportTemplateId, ReportTemplate> templatesMap = new HashMap<>();
        final Iterator<ReportTemplate> templates =
                reportingService.listAllTemplates(Empty.getDefaultInstance());
        templates.forEachRemaining(template -> templatesMap.put(ReportTemplateId.newBuilder()
                .setReportType(template.getReportType())
                .setId(template.getId())
                .build(), template));

        final List<ReportInstanceApiDTO> result = new ArrayList<>();
        final Iterator<ReportInstance> reportIterator =
                reportingService.listAllInstances(Empty.getDefaultInstance());
        while (reportIterator.hasNext()) {
            final ReportInstance instance = reportIterator.next();
            final ReportTemplate template = templatesMap.get(instance.getTemplate());
            final ReportInstanceApiDTO reportInstance = new ReportInstanceApiDTO();
            reportInstance.setFilename(template.getFilename());
            reportInstance.setFormat(Collections.singletonMap(instance.getFormat(),
                    String.valueOf(instance.getId())));
            reportInstance.setScope(new BaseApiDTO());
            result.add(reportInstance);
        }
        return result;
    }

    @Override
    public ReportInstanceApiDTO getReportInstanceByID(final String s) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportScheduleApiDTO> getSchedulesList() {
        final Iterator<Reporting.ScheduleDTO> scheduleDtos =
                reportingService.getAllSchedules(Empty.newBuilder().build());
        final List<ReportScheduleApiDTO> allApiSchedules = new ArrayList<>();
        scheduleDtos.forEachRemaining(dto -> allApiSchedules.add(toReportScheduleApiDTO(dto)));
        return allApiSchedules;
    }

    @Override
    public ReportScheduleApiDTO getReportScheduleByID(final String scheduleId) {
        if (!StringUtils.isNumeric(scheduleId)) {
            throw new IllegalArgumentException("For report schedule provided not numeric id: " + scheduleId);
        }
        final Reporting.ScheduleDTO scheduleDto = reportingService.getSchedule(
                        Reporting.ScheduleId.newBuilder()
                                        .setId(Long.parseLong(scheduleId))
                                        .build());
        return toReportScheduleApiDTO(scheduleDto);
    }

    @Override
    public List<ReportApiDTO> getTemplatesList() {
        final List<ReportApiDTO> result = new ArrayList<>();
        final Iterator<ReportTemplate> iterator =
                reportingService.listAllTemplates(Empty.getDefaultInstance());
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
    public List<ReportInstanceApiDTO> getReportTemplateInstanceList(final String templateId,
            final String startTime, final String endTime, final boolean exteractZip)
            throws Exception {
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
    public ReportInstanceApiDTO generateReportTemplateInstance(final String templateApiId,
            final ReportInstanceApiInputDTO reportApiRequest) {
        logger.debug("Report generation requested for template {} and format {} with attributes {}",
                templateApiId::toString, reportApiRequest::getFormat,
                reportApiRequest::getAttributes);
        final ReportTemplateId templateId = getReportTemplateId(templateApiId);
        final GenerateReportRequest.Builder builder =
                GenerateReportRequest.newBuilder().setTemplate(templateId);
        builder.setFormat(reportApiRequest.getFormat().getLiteral());
        // TODO add attributes, as soon as we face at least one real use case
        final ReportInstanceId response = reportingService.generateReport(builder.build());
        final ReportInstanceApiDTO result = new ReportInstanceApiDTO();
        result.setFilename(Long.toString(response.getId()));
        result.setReportType(ReportType.get(templateId.getReportType()));
        result.setTemplateId(templateId.getReportType(), templateId.getId());
        result.setUserName(reportApiRequest.getUserName());
        result.setScope(new BaseApiDTO());
        logger.trace("Report generation triggered successfully into file {}", result::getFilename);
        return result;
    }

    @Override
    public ReportInstanceApiDTO getReportTemplateInstance(final String s, final String s1) throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportScheduleApiDTO> getReportTemplateScheduleList(final String reportTypeAndTemplateId) {
        final ReportTemplateId templateId = getReportTemplateId(reportTypeAndTemplateId);
        final Reporting.GetSchedulesByRequest request = Reporting.GetSchedulesByRequest
                        .newBuilder()
                        .setReportType(templateId.getReportType())
                        .setTemplateId(templateId.getId())
                        .build();
        final List<ReportScheduleApiDTO> scheduleApiDTOS = new ArrayList<>();
        reportingService.getSchedulesBy(request).forEachRemaining(
                        scheduleDto -> scheduleApiDTOS.add(toReportScheduleApiDTO(scheduleDto)));
        return scheduleApiDTOS;
    }

    @Override
    @Nonnull
    public ReportScheduleApiDTO addReportTemplateSchedule(final String reportTypeAndTemplateId,
            final ReportScheduleApiInputDTO reportScheduleApiInputDTO) {
        final Reporting.ScheduleInfo scheduleInfo = toScheduleInfo(reportTypeAndTemplateId,
                        reportScheduleApiInputDTO);
        final Reporting.ScheduleDTO scheduleDTO = reportingService.addSchedule(scheduleInfo);
        return toReportScheduleApiDTO(scheduleDTO);
    }

    /**
     * Converts schedule into protobuf representation
     *
     * @param restTemplateId template id
     * @param reportScheduleApiInputDTO schedule REST representation
     * @return protobuf representation
     */
    @Nonnull
    private Reporting.ScheduleInfo toScheduleInfo(@Nonnull String restTemplateId,
            @Nonnull ReportScheduleApiInputDTO reportScheduleApiInputDTO) {
        final ReportTemplateId templateId = getReportTemplateId(restTemplateId);
        final List<String> emails = Arrays.asList(reportScheduleApiInputDTO.getEmail().split(","));
        emails.forEach(String::trim);
        final Reporting.ScheduleInfo.Builder infoBuilder = Reporting.ScheduleInfo.newBuilder()
                .setReportType(templateId.getReportType())
                .setTemplateId(templateId.getId())
                .setDayOfWeek(reportScheduleApiInputDTO.getDow().getName())
                .setFormat(reportScheduleApiInputDTO.getFormat().getLiteral())
                .setPeriod(reportScheduleApiInputDTO.getPeriod().getName())
                .setShowCharts(reportScheduleApiInputDTO.isShowCharts())
                .addAllSubscribersEmails(emails);
        if (reportScheduleApiInputDTO.getScope() != null) {
            infoBuilder.setScopeOid(reportScheduleApiInputDTO.getScope());
        }
        return infoBuilder.build();
    }

    @Override
    public ReportScheduleApiDTO getReportTemplateSchedule(final String s, final long i) {
        final Reporting.ScheduleDTO scheduleDto = reportingService.getSchedule(
                        Reporting.ScheduleId.newBuilder().setId(i).build());
        return toReportScheduleApiDTO(scheduleDto);
    }

    @Override
    public ReportScheduleApiDTO editReportTemplateSchedule(final String s, final long i, final ReportScheduleApiInputDTO reportScheduleApiInputDTO) {
        final Reporting.ScheduleInfo info = toScheduleInfo(s, reportScheduleApiInputDTO);
        final Reporting.ScheduleDTO scheduleDTO = Reporting.ScheduleDTO.newBuilder()
                .setId(i).setScheduleInfo(info).build();
        final Reporting.ScheduleDTO edited = reportingService.editSchedule(scheduleDTO);
        return toReportScheduleApiDTO(edited);
    }

    @Override
    public void deleteReportTemplateSchedule(final String s, final long i) {
        reportingService.deleteSchedule(Reporting.ScheduleId.newBuilder().setId(i).build());
    }

    private ReportScheduleApiDTO toReportScheduleApiDTO(final Reporting.ScheduleDTO scheduleDTO) {
        final Reporting.ScheduleInfo info = scheduleDTO.getScheduleInfo();
        final BaseApiDTO scope = new BaseApiDTO();
        final String scopeOid = info.getScopeOid();
        scope.setUuid(scopeOid);
        scope.setDisplayName(getScopeDisplayName(scopeOid));

        final ReportScheduleApiDTO apiDTO = new ReportScheduleApiDTO();
        apiDTO.setSubcriptionId(scheduleDTO.getId());
        apiDTO.setScope(scope);
        apiDTO.setDayOfWeek(DayOfWeek.valueOf(info.getDayOfWeek()));
        apiDTO.setEmail(info.getSubscribersEmailsList()
                .stream().collect(Collectors.joining(",")));
        apiDTO.setFormat(ReportOutputFormat.valueOf(info.getFormat()));
        apiDTO.setPeriod(Period.valueOf(info.getPeriod()));
        apiDTO.setReportType(info.getReportType());
        apiDTO.setShowCharts(info.getShowCharts());
        apiDTO.setTemplateId(info.getReportType(), info.getTemplateId());
        return apiDTO;
    }

    private String getScopeDisplayName(@Nonnull String uuid) {
        if (StringUtils.isBlank(uuid)) {
            return "";
        }
        try {
            return groupsService.getGroupByUuid(uuid, true).getDisplayName();
        } catch (UnknownObjectException e) {
            logger.warn("Cannot resolve group with oid: {}", uuid);
            return "";
        }
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
        dst.setTemplateID(src.getReportType(), src.getId());
        dst.setReportType(src.getReportType());
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
     * Extracts report templated id and report type from the UI supplied Id. In the UI report
     * template is combined in form (reportType)_(templateId).
     *
     * @param reportApiId UI supplied Id.
     * @return report template ID protobuf representation
     */
    @Nonnull
    private ReportTemplateId getReportTemplateId(@Nonnull String reportApiId) {
        final String[] parts = reportApiId.split("_");
        if (parts.length != 2) {
            throw new IllegalArgumentException(
                    "Report id is malformed. Should be <reportType>_<id>. but is " + reportApiId);
        }
        final String reportIdString = parts[1];
        final int templateId;
        if (!StringUtils.isNumeric(reportIdString)) {
            throw new IllegalArgumentException(
                    "Report id is not parsable: " + reportIdString + " from report id request " +
                            reportApiId);
        } else {
            templateId = Integer.valueOf(parts[1]);
        }
        final String reportTypeString = parts[0];
        final ReportType reportType;
        if (!StringUtils.isNumeric(reportTypeString)) {
            throw new IllegalArgumentException("Report type is not parsable: " + reportTypeString +
                    " from report id request " + reportApiId);
        } else {
            // This conversion is used only to ensure, that input report type, arrived from REST api
            // is correct. Really, the enum value is not required
            final ReportType result = ReportType.get(Integer.valueOf(reportTypeString));
            if (result == null) {
                throw new IllegalArgumentException(
                        "Could not find ReportType by id " + reportTypeString);
            } else {
                reportType = result;
            }
        }
        return ReportTemplateId.newBuilder()
                .setId(templateId)
                .setReportType(reportType.getValue())
                .build();
    }
}
