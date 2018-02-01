package com.vmturbo.reports.component.communication;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.protobuf.ByteString;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reporting.api.protobuf.Reporting.Empty;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportData;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceImplBase;
import com.vmturbo.reports.component.ComponentReportRunner;
import com.vmturbo.reports.component.ReportRequest;
import com.vmturbo.reports.component.ReportingException;
import com.vmturbo.reports.component.db.tables.pojos.ReportInstance;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.instances.ReportInstanceRecord;
import com.vmturbo.reports.component.schedules.ScheduleDAO;
import com.vmturbo.reports.component.templates.TemplateConverter;
import com.vmturbo.reports.component.templates.TemplatesDao;
import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;
import com.vmturbo.sql.utils.DbException;

/**
 * GRPC service implementation to run reports.
 */
public class ReportingServiceRpc extends ReportingServiceImplBase {

    /**
     * Report runner to use for reports generation.
     */
    private final ComponentReportRunner reportRunner;
    /**
     * Logger to use.
     */
    private final Logger logger = LogManager.getLogger();

    private final TemplatesDao templatesDao;
    private final ReportInstanceDao reportInstanceDao;
    private final ScheduleDAO scheduleDAO;

    private final File outputDirectory;
    private final Executor executor;
    private final ReportNotificationSender notificationSender;

    /**
     * Creates reporting GRPC service.
     *
     * @param reportRunner report runner to use
     * @param templatesDao DAO resposible for template-related queries
     * @param reportInstanceDao DAO for accessing report instances records in the DB
     * @param scheduleDAO DAO for accessing to schedules in the DB
     * @param outputDirectory directory to put generated report files into
     * @param executor thead pool to be used for report generation (they are asynchronous)
     * @param notificationSender notification sender to broadcast report generation results
     */
    public ReportingServiceRpc(@Nonnull ComponentReportRunner reportRunner,
            @Nonnull TemplatesDao templatesDao, @Nonnull ReportInstanceDao reportInstanceDao,
            @Nonnull ScheduleDAO scheduleDAO, @Nonnull File outputDirectory, @Nonnull Executor executor,
            @Nonnull ReportNotificationSender notificationSender) {
        this.reportRunner = Objects.requireNonNull(reportRunner);
        this.templatesDao = Objects.requireNonNull(templatesDao);
        this.reportInstanceDao = Objects.requireNonNull(reportInstanceDao);
        this.scheduleDAO = scheduleDAO;
        this.outputDirectory = Objects.requireNonNull(outputDirectory);
        this.executor = Objects.requireNonNull(executor);
        this.notificationSender = Objects.requireNonNull(notificationSender);
    }

    /**
     * Triggeres generating a report and returns report id. Report generation will be executed in a
     * separate thread, notifications will be sent on finish using {@link #notificationSender}.
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
            final StandardReportsRecord template =
                    templatesDao.getTemplateById(request.getReportId())
                            .orElseThrow(() -> new ReportingException(
                                    "Could not find report template by id " +
                                            request.getReportId()));
            final String reportPath = "/VmtReports/" + template.getFilename() + ".rptdesign";
            final ReportOutputFormat format =
                    Objects.requireNonNull(ReportOutputFormat.get(request.getFormat()));
            final ReportInstanceRecord reportInstance =
                    reportInstanceDao.createInstanceRecord(request.getReportId(), format);
            final ReportRequest report =
                    new ReportRequest(reportPath, format, request.getParametersMap());
            final File file = new File(outputDirectory, Long.toString(reportInstance.getId()));
            executor.execute(() -> generateReportInternal(report, file, reportInstance));
            final ReportInstanceId.Builder resultBuilder = ReportInstanceId.newBuilder();
            resultBuilder.setId(reportInstance.getId());
            responseObserver.onNext(resultBuilder.build());
            responseObserver.onCompleted();
        } catch (ReportingException | DbException e) {
            logger.error("Failed to generate report " + request.getReportId(), e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        }
    }

    /**
     * Method performes report generation, wrapping all the exceptions. It is designed to be the top
     * method called in a thread (it is catching {@link InterruptedException});
     *
     * @param reportRequest report request to generate
     * @param outputFile file to put generated report into
     * @param reportInstance report instance record from the DB
     */
    private void generateReportInternal(@Nonnull ReportRequest reportRequest, @Nonnull File outputFile,
            @Nonnull ReportInstanceRecord reportInstance) {
        try {
            try {
                try {
                    reportRunner.createReport(reportRequest, outputFile);
                    reportInstance.commit();
                    notificationSender.notifyReportGenerated(reportInstance.getId());
                } catch (ReportingException e) {
                    logger.warn(
                            "Error generating a report {}. Removing report record from the DB...",
                            reportInstance.getId());
                    reportInstance.rollback();
                    throw e;
                }
            } catch (DbException | ReportingException e) {
                notificationSender.notifyReportGenerationFailed(reportInstance.getId(),
                        e.getMessage());
            }
        } catch (InterruptedException e) {
            logger.info("Generating a report " + reportInstance.getId() + " interrupted", e);
        } catch (CommunicationException e) {
            logger.error("Could not send notification about report " + reportInstance.getId() +
                    " generation", e);
        }
    }

    @Override
    public void listAllTemplates(Empty request,
            StreamObserver<ReportTemplate> responseObserver) {
        try {
            for (StandardReportsRecord report : templatesDao.getAllReports()) {
                responseObserver.onNext(TemplateConverter.convert(report));
            }
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
            for (ReportInstance reportInstance : reportInstanceDao.getAllInstances()) {
                final Reporting.ReportInstance.Builder builder =
                        Reporting.ReportInstance.newBuilder();
                builder.setFormat(reportInstance.getOutputFormat().getLiteral());
                builder.setId(reportInstance.getId());
                builder.setReportType(ReportType.BIRT_STANDARD.getValue());
                builder.setTemplateId(reportInstance.getTemplateId());
                builder.setGenerationTime(reportInstance.getGenerationTime().getTime());
                responseObserver.onNext(builder.build());
            }
            responseObserver.onCompleted();
        } catch (DbException e) {
            logger.error("Failed fetching all the report instances", e);
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
            final StandardReportsRecord template =
                    templatesDao.getTemplateById(instance.getTemplateId())
                            .orElseThrow(() -> new ReportingException(
                                    "Template " + instance.getTemplateId() +
                                            " not found for report " + request.getId()));
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
            final Reporting.ScheduleDTO scheduleDTO = scheduleDAO.addSchedule(scheduleInfo);
            responseObserver.onNext(scheduleDTO);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage())));
        }
    }

    @Override
    public void getAllSchedules(Reporting.Empty empty, StreamObserver<Reporting.ScheduleDTO> responseObserver) {
        try {
            final Set<Reporting.ScheduleDTO> allSchedules = scheduleDAO.getAllSchedules();
            allSchedules.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage())));
        }
    }

    @Override
    public void deleteSchedule(Reporting.ScheduleId id, StreamObserver<Empty> responseObserver) {
        try {
            scheduleDAO.deleteSchedule(id.getId());
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
            scheduleDAO.editSchedule(scheduleDTO);
            responseObserver.onNext(scheduleDTO);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage())));
        }
    }

    @Override
    public void getSchedule(Reporting.ScheduleId id, StreamObserver<Reporting.ScheduleDTO> responseObserver) {
        try {
            final Reporting.ScheduleDTO schedule = scheduleDAO.getSchedule(id.getId());
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
                            scheduleDAO.getSchedulesBy(reportType, templateId);
            scheduleDTOS.forEach(responseObserver::onNext);
            responseObserver.onCompleted();
        } catch (DbException e) {
            responseObserver.onError(new StatusRuntimeException(Status.ABORTED.withDescription(e.getMessage())));
        }
    }
}
