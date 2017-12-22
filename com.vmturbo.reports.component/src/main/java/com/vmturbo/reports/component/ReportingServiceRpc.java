package com.vmturbo.reports.component;

import java.io.File;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.reporting.api.protobuf.Reporting.EmptyRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.GenerateReportRequest;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportResponse;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportTemplate;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceImplBase;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.instances.ReportInstanceRecord;
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

    private final File outputDirectory;

    /**
     * Creates reporting GRPC service.
     *
     * @param reportRunner report runner to use
     * @param templatesDao DAO resposible for template-related queries
     * @param reportInstanceDao DAO for accessing report instances records in the DB
     * @param outputDirectory directory to put generated report files into
     */
    public ReportingServiceRpc(@Nonnull ComponentReportRunner reportRunner,
            @Nonnull TemplatesDao templatesDao, @Nonnull ReportInstanceDao reportInstanceDao,
            @Nonnull File outputDirectory) {
        this.reportRunner = Objects.requireNonNull(reportRunner);
        this.templatesDao = Objects.requireNonNull(templatesDao);
        this.reportInstanceDao = Objects.requireNonNull(reportInstanceDao);
        this.outputDirectory = Objects.requireNonNull(outputDirectory);
    }

    @Override
    public void generateReport(GenerateReportRequest request,
            StreamObserver<ReportResponse> responseObserver) {
        try {
            final StandardReportsRecord template =
                    templatesDao.getTemplateById(request.getReportId())
                            .orElseThrow(() -> new ReportingException(
                                    "Could not find report template by id " +
                                            request.getReportId()));
            final String reportPath = "/VmtReports/" + template.getFilename() + ".rptdesign";
            final ReportInstanceRecord reportInstance =
                    reportInstanceDao.createInstanceRecord(request.getReportId());
            final ReportRequest report =
                    new ReportRequest(reportPath, ReportOutputFormat.get(request.getFormat()),
                            request.getParametersMap());
            try {
                final File file = new File(outputDirectory, Long.toString(reportInstance.getId()));
                reportRunner.createReport(report, file);
                reportInstance.commit();
            } catch (ReportingException e) {
                logger.warn("Error generating a report {}. Removing report record from the DB...",
                        reportInstance.getId());
                reportInstance.rollback();
                throw e;
            }
            final ReportResponse.Builder resultBuilder = ReportResponse.newBuilder();
            resultBuilder.setFileName(reportInstance.getId());
            responseObserver.onNext(resultBuilder.build());
            responseObserver.onCompleted();
        } catch (ReportingException | DbException e) {
            logger.error("Failed to generate report " + request.getReportId(), e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        }
    }

    @Override
    public void listAllTemplates(EmptyRequest request,
            StreamObserver<ReportTemplate> responseObserver) {
        try {
            for (StandardReportsRecord report : templatesDao.getAllReports()) {
                responseObserver.onNext(TemplateConverter.convert(report));
            }
            responseObserver.onCompleted();
        } catch (DbException e) {
            logger.error("Failed fetch report templates ", e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        }
    }
}
