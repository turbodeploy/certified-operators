package com.vmturbo.api.component.external.api.service;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.multipart.MultipartFile;

import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.BaseApiDTO;
import com.vmturbo.api.dto.report.AttributeValueApiDTO;
import com.vmturbo.api.dto.report.ReportApiDTO;
import com.vmturbo.api.dto.report.ReportAttributeApiDto;
import com.vmturbo.api.dto.report.ReportAttributeType;
import com.vmturbo.api.dto.report.ReportInstanceApiDTO;
import com.vmturbo.api.dto.report.ReportInstanceApiInputDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiDTO;
import com.vmturbo.api.dto.report.ReportScheduleApiInputDTO;
import com.vmturbo.api.dto.report.ReportTemplateApiInputDTO;
import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.Period;
import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.api.serviceinterfaces.IGroupsService;
import com.vmturbo.api.serviceinterfaces.IReportsService;
import com.vmturbo.api.utils.DateTimeUtil;
import com.vmturbo.reporting.api.ReportingConstants;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.Reporting.Empty;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportAttribute;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstance;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;

/**
 * Service implementation of Reports.
 */
public class ReportsService implements IReportsService {

    private static final ReportOutputFormat DEFAULT_REPORT_FORMAT = ReportOutputFormat.PDF;

    private final Logger logger = LogManager.getLogger(getClass());

    private final ReportingServiceBlockingStub reportingService;

    private final IGroupsService groupsService;

    /**
     * Constructs reporting service with active connection to reporting component.
     *
     * @param reportingService reporting service GRPC connection.
     * @param groupsService groups service.
     */
    public ReportsService(@Nonnull ReportingServiceBlockingStub reportingService,
                    @Nonnull IGroupsService groupsService) {
        this.reportingService = Objects.requireNonNull(reportingService);
        this.groupsService = Objects.requireNonNull(groupsService);
    }

    @Override
    public ReportApiDTO getReports() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public List<ReportInstanceApiDTO> getInstancesList() {
        final List<ReportInstanceApiDTO> result = new ArrayList<>();
        final Iterator<ReportInstance> reportIterator =
                reportingService.listAllInstances(Empty.getDefaultInstance());
        while (reportIterator.hasNext()) {
            final ReportInstance instance = reportIterator.next();
            result.add(convert(instance));
        }
        return result;
    }

    @Nonnull
    private static ReportInstanceApiDTO convert(@Nonnull ReportInstance instance) {
        final ReportInstanceApiDTO reportInstance = new ReportInstanceApiDTO();
        reportInstance.setFilename(createFakeFilename(instance.getTemplate()));
        reportInstance.setFormat(
                Collections.singletonMap(instance.getFormat(), String.valueOf(instance.getId())));
        reportInstance.setScope(new BaseApiDTO());
        reportInstance.setTimestamp(
                DateTimeUtil.formatDate(new Date(instance.getGenerationTime())));
        reportInstance.setUrl("output%3D" + instance.getId());
        return reportInstance;
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
    //TODO : Add implementation to use parameter "ReportType... types".
    public List<ReportApiDTO> getTemplatesList(ReportType... types) {
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
    public List<ReportInstanceApiDTO> getReportTemplateInstanceList(
            final @Nonnull String templateApiId, final @Nonnull Instant startTime,
            final @Nonnull Instant endTime, final boolean exteractZip) {
        logger.debug("Retrieving report instances for template {}", templateApiId);
        final ReportTemplateId templateId = getReportTemplateId(templateApiId);
        final Iterator<ReportInstance> iterator = reportingService.getInstancesByTemplate(templateId);
        final List<ReportInstanceApiDTO> result = new ArrayList<>();
        while (iterator.hasNext()) {
            final ReportInstance instance = iterator.next();
            result.add(convert(instance));
        }
        return result;
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
        logger.debug("Report generation requested for template {} and format {} with attributes {}",
                templateApiId::toString, reportApiRequest::getFormat,
                reportApiRequest::getAttributes);
        final ReportTemplateId templateId = getReportTemplateId(templateApiId);
        final GenerateReportRequest.Builder builder =
                GenerateReportRequest.newBuilder().setTemplate(templateId);
        final ReportOutputFormat reportFormat = reportApiRequest.getFormat() == null
            ? DEFAULT_REPORT_FORMAT
            : reportApiRequest.getFormat();
        builder.setFormat(reportFormat.getLiteral());
        if (!StringUtils.isBlank(reportApiRequest.getEmailAddress())) {
            builder.addAllSubscribersEmails(splitToEmails(reportApiRequest.getEmailAddress()));
        }
        if (reportApiRequest.getAttributes() != null) {
            for (AttributeValueApiDTO attributeValue : reportApiRequest.getAttributes()) {
                builder.putParameters(attributeValue.getName(), attributeValue.getValue());
            }
        }
        if (reportApiRequest.getScopeUuid() != null) {
            builder.putParameters(ReportingConstants.ITEM_UUID_PROPERTY,
                    reportApiRequest.getScopeUuid());
        }
        final ReportInstanceId response = reportingService.generateReport(builder.build());
        final ReportInstanceApiDTO result = new ReportInstanceApiDTO();
        result.setFilename(Long.toString(response.getId()));
        final ReportType reportType = ReportType.get(templateId.getReportType());
        if (reportType == null) {
            throw new IllegalArgumentException(
                    "Could not find report type by id " + templateId.getReportType());
        }
        result.setReportType(reportType);
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

        final GenerateReportRequest.Builder requestBuilder = GenerateReportRequest.newBuilder()
                .setTemplate(templateId)
                .setFormat(reportScheduleApiInputDTO.getFormat().getLiteral());
        if (reportScheduleApiInputDTO.getScope() != null) {
            requestBuilder.putParameters(ReportingConstants.ITEM_UUID_PROPERTY,
                    reportScheduleApiInputDTO.getScope());
        }
        if (reportScheduleApiInputDTO.isShowCharts() != null) {
            requestBuilder.putParameters(ReportingConstants.SHOW_CHARTS_PROPERTY,
                    Boolean.toString(reportScheduleApiInputDTO.isShowCharts()));
        }
        if (reportScheduleApiInputDTO.getEmail() != null) {
            requestBuilder.addAllSubscribersEmails(
                    splitToEmails(reportScheduleApiInputDTO.getEmail()));
        }
        final Reporting.ScheduleInfo.Builder infoBuilder = Reporting.ScheduleInfo.newBuilder()
                .setReportRequest(requestBuilder.build())
                .setPeriod(reportScheduleApiInputDTO.getPeriod().getName());
        if (reportScheduleApiInputDTO.getDow() != null) {
            infoBuilder.setDayOfWeek(reportScheduleApiInputDTO.getDow().getName());
        } else if (reportScheduleApiInputDTO.getPeriod() == Period.Weekly) {
            infoBuilder.setDayOfWeek(DayOfWeek.today().getName());
        }
        if (reportScheduleApiInputDTO.getPeriod() == Period.Monthly) {
            infoBuilder.setDayOfMonth(LocalDate.now().getDayOfMonth());
        }
        return infoBuilder.build();
    }

    @Nonnull
    private List<String> splitToEmails(@Nonnull String emails) {
        return Arrays.stream(emails.split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());
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
        final GenerateReportRequest request = info.getReportRequest();
        final BaseApiDTO scope = new BaseApiDTO();
        final String scopeOid = request.getParametersMap().get(ReportingConstants.ITEM_UUID_PROPERTY);
        scope.setUuid(scopeOid);
        scope.setDisplayName(getScopeDisplayName(scopeOid));

        final ReportScheduleApiDTO apiDTO = new ReportScheduleApiDTO();
        apiDTO.setSubscriptionId(scheduleDTO.getId());
        apiDTO.setScope(scope);
        if (info.hasDayOfWeek()) {
            apiDTO.setDayOfWeek(DayOfWeek.valueOf(info.getDayOfWeek()));
        }
        apiDTO.setEmail(
                request.getSubscribersEmailsList().stream().collect(Collectors.joining(",")));
        apiDTO.setFormat(ReportOutputFormat.valueOf(request.getFormat()));
        apiDTO.setPeriod(Period.valueOf(info.getPeriod()));
        apiDTO.setReportType(request.getTemplate().getReportType());
        final String showCharts = request.getParametersMap().get(ReportingConstants.SHOW_CHARTS_PROPERTY);
        if (showCharts != null) {
            apiDTO.setShowCharts(Boolean.getBoolean(showCharts));
        }
        apiDTO.setTemplateId(request.getTemplate().getReportType(), request.getTemplate().getId());
        return apiDTO;
    }

    private String getScopeDisplayName(@Nonnull String uuid) {
        if (StringUtils.isBlank(uuid)) {
            return "";
        }
        try {
            return groupsService.getGroupByUuid(uuid, true).getDisplayName();
        } catch (Exception e) {
            logger.warn("Cannot resolve group with oid: {}", uuid);
            return "";
        }
    }

    /**
     * Converts reporting template from Protobuf representation into REST API representation.
     *
     * @param src protobuf representation
     * @return REST API representation
     */
    @Nonnull
    private static ReportApiDTO convert(@Nonnull ReportTemplate src) {
        final ReportApiDTO dst = new ReportApiDTO();
        dst.setTemplateID(src.getId().getReportType(), src.getId().getId());
        dst.setReportType(src.getId().getReportType());
        dst.setFileName(createFakeFilename(src.getId()));
        dst.setTitle(src.getTitle());
        dst.setCategory(src.getCategory());
        dst.setDescription(src.getDescription());
        if (src.hasPeriod()) {
            dst.setPeriod(Period.values()[src.getPeriod()]);
        }
        if (src.hasDayType()) {
            dst.setScheduled(false);
        }
        dst.setAttributes(src.getAttributesList()
                .stream()
                .map(ReportsService::convert)
                .collect(Collectors.toList()));
        if (src.hasIsGroupScoped()) {
            dst.setGroupScoped(src.getIsGroupScoped());
        }
        return dst;
    }

    @Nonnull
    private static ReportAttributeApiDto convert(@Nonnull ReportAttribute src) {
        final ReportAttributeApiDto dst = new ReportAttributeApiDto();
        dst.setName(src.getName());
        dst.setDefaultValue(src.getDefaultValue());
        dst.setValueType(ReportAttributeType.valueOf(src.getValueType()));
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

    /**
     * File name was used in UI in order to identify the report template. We do not support this
     * data in XL, so we form a fake filename, unique to each report template.
     *
     * @param templateId report template id
     * @return string representation of templates
     */
    private static String createFakeFilename(@Nonnull ReportTemplateId templateId) {
        return Integer.toString(templateId.getReportType()) + '-' + templateId.getId();
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
