package com.vmturbo.reports.component.communication;

import static com.vmturbo.reports.component.data.ReportDataUtils.getReportMap;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.commons.mail.EmailException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.Reporting.Empty;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportData;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplateId;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceImplBase;
import com.vmturbo.reports.component.ReportingException;
import com.vmturbo.reports.component.db.tables.pojos.ReportInstance;
import com.vmturbo.reports.component.instances.ReportInstanceConverter;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.instances.ReportsGenerator;
import com.vmturbo.reports.component.schedules.Scheduler;
import com.vmturbo.reports.component.templates.TemplateWrapper;
import com.vmturbo.reports.component.templates.TemplatesOrganizer;
import com.vmturbo.sql.utils.DbException;

/**
 * GRPC service implementation to run reports.
 */
public class ReportingServiceRpc extends ReportingServiceImplBase {

    /**
     * Logger to use.
     */
    private final Logger logger = LogManager.getLogger();

    private final TemplatesOrganizer templatesOrganizer;
    private final ReportInstanceDao reportInstanceDao;
    private final ReportsGenerator reportsGenerator;
    private final File outputDirectory;
    private final Scheduler scheduler;
    private final Set<Integer> enabledReports;

    /**
     * Creates reporting GRPC service.
     *
     * @param templatesOrganizer DAO resposible for template-related queries
     * @param reportInstanceDao DAO for accessing report instances records in the DB
     * @param outputDirectory directory to put generated report files into
     */
    public ReportingServiceRpc(@Nonnull TemplatesOrganizer templatesOrganizer,
            @Nonnull ReportInstanceDao reportInstanceDao, @Nonnull File outputDirectory,
                    @Nonnull ReportsGenerator reportsGenerator,
            @Nonnull Scheduler scheduler) {
        this.templatesOrganizer = Objects.requireNonNull(templatesOrganizer);
        this.reportInstanceDao = Objects.requireNonNull(reportInstanceDao);
        this.outputDirectory = Objects.requireNonNull(outputDirectory);
        this.reportsGenerator = Objects.requireNonNull(reportsGenerator);
        this.scheduler = Objects.requireNonNull(scheduler);
        this.enabledReports = reportsGenerator.getSupportedReport().keySet().stream()
            .map(longId -> longId.intValue()).collect(Collectors.toSet());;
    }

    /**
     * Triggeres generating a report and returns report id. Report generation will be executed in a
     * separate thread.
     * Report instance data (generated reports) are stored in the filesystem in
     * {@link #outputDirectory} and have associated DB record in {@code report_instances} table.
     * Name of the file to store report data is a report instance Id (converted to {@link String})
     *
     * @param request report generation request
     * @param responseObserver observer to send responses to
     */
    @Override
    public void generateReport(GenerateReportRequest request,
            StreamObserver<ReportInstanceId> responseObserver) {
        try {
            responseObserver.onNext(reportsGenerator.generateReport(request));
            responseObserver.onCompleted();
        } catch (ReportingException | DbException e) {
            logger.error("Failed to generate report " + request.getTemplate().getId(), e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        } catch (EmailException ex) {
            onEmailError(responseObserver, request.getSubscribersEmailsList(), ex);
        }
    }

    @Override
    public void listAllTemplates(Empty request,
            StreamObserver<ReportTemplate> responseObserver) {
        try {
            templatesOrganizer.getAllTemplates()
                    .stream()
                    .map(TemplateWrapper::toProtobuf)
                    .filter(template -> enabledReports.contains(template.getId().getId()))
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DbException e) {
            logger.error("Failed fetch report templates", e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        }
    }

    @Override
    public void listAllInstances(Empty request,
            StreamObserver<Reporting.ReportInstance> responseObserver) {
        try {
            reportInstanceDao.getAllInstances()
                    .stream()
                    .map(ReportInstanceConverter::convert)
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DbException e) {
            logger.error("Failed fetching all the report instances", e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        }
    }

    @Override
    public void getInstancesByTemplate(ReportTemplateId request,
            StreamObserver<Reporting.ReportInstance> responseObserver) {
        try {
            final ReportType reportType = ReportType.get(request.getReportType());
            reportInstanceDao.getInstancesByTemplate(reportType, request.getId())
                    .stream()
                    .map(ReportInstanceConverter::convert)
                    .forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DbException e) {
            logger.error(
                    "Failed fetching report instances for template " + request.getReportType() +
                            '-' + request.getId(), e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        }
    }

    /**
     * Retrieves report data (bytes) and some meta-information. All the data except of bytes is
     * stored in the DB, bytes are stored only in the filesystem.
     *
     * @param request report instance id
     * @param responseObserver observer to put result into
     */
    @Override
    public void getReportData(ReportInstanceId request,
            StreamObserver<ReportData> responseObserver) {
        try {
            final ReportInstance instance = reportInstanceDao.getInstanceRecord(request.getId())
                    .orElseThrow(() -> new ReportingException(
                            "Report instance not found by id " + request.getId()));
            final ReportOutputFormat format = instance.getOutputFormat();
            final File reportFile = new File(outputDirectory, instance.getId().toString());
            final ByteString data;
            try (final InputStream stream = new FileInputStream(reportFile)) {
                data = ByteString.readFrom(stream);
            }
            responseObserver.onNext(ReportData.newBuilder()
                    .setReportName(instance.getId().toString())
                    .setData(data)
                    .setFormat(format.getLiteral())
                    .build());
            responseObserver.onCompleted();
        } catch (DbException | IOException e) {
            logger.error("Failed to retrieve report instance by id " + request, e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        } catch (ReportingException e) {
            logger.error(e.getMessage(), e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage())));
        }
    }

    @Override
    public void addSchedule(Reporting.ScheduleInfo scheduleInfo,
                    StreamObserver<Reporting.ScheduleDTO> responseObserver) {
        try {
            final Reporting.ScheduleDTO scheduleDTO = scheduler.addSchedule(scheduleInfo);
            responseObserver.onNext(scheduleDTO);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(
                            Status.ABORTED.withDescription(e.getMessage())));
        } catch (EmailException ex) {
            onEmailError(responseObserver,
                    scheduleInfo.getReportRequest().getSubscribersEmailsList(), ex);
        }
    }

    private <T> void onEmailError(@Nonnull StreamObserver<T> responseObserver,
                    @Nonnull List<String> emailAddresses, @Nonnull EmailException ex) {
        logger.error("Invalid email address provided " + emailAddresses, ex);
        responseObserver.onError(new StatusRuntimeException(
                        Status.INVALID_ARGUMENT.withDescription(ex.getMessage())));
    }

    @Override
    public void getAllSchedules(Reporting.Empty empty, StreamObserver<Reporting.ScheduleDTO> responseObserver) {
        try {
            final Set<Reporting.ScheduleDTO> allSchedules = scheduler.getAllSchedules();
            allSchedules.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage())));
        }
    }

    @Override
    public void deleteSchedule(Reporting.ScheduleId id, StreamObserver<Empty> responseObserver) {
        try {
            scheduler.deleteSchedule(id.getId());
            responseObserver.onNext(Empty.newBuilder().build());
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage())));
        }
    }

    @Override
    public void editSchedule(Reporting.ScheduleDTO scheduleDTO,
                    StreamObserver<Reporting.ScheduleDTO> responseObserver) {
        try {
            scheduler.editSchedule(scheduleDTO);
            responseObserver.onNext(scheduleDTO);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(
                            Status.ABORTED.withDescription(e.getMessage())));
        } catch (EmailException ex) {
            onEmailError(responseObserver,
                    scheduleDTO.getScheduleInfo().getReportRequest().getSubscribersEmailsList(),
                    ex);
        }
    }

    @Override
    public void getSchedule(Reporting.ScheduleId id, StreamObserver<Reporting.ScheduleDTO> responseObserver) {
        try {
            final Reporting.ScheduleDTO schedule = scheduler.getSchedule(id.getId());
            responseObserver.onNext(schedule);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage())));
        }
    }

    @Override
    public void getSchedulesBy(Reporting.GetSchedulesByRequest request,
                    StreamObserver<Reporting.ScheduleDTO> responseObserver) {
        try {
            final int reportType = request.getReportType();
            final int templateId = request.getTemplateId();
            final Set<Reporting.ScheduleDTO> scheduleDTOS =
                            scheduler.getSchedulesBy(reportType, templateId);
            scheduleDTOS.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage())));
        }
    }
}
