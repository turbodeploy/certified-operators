package com.vmturbo.reports.component;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
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

    /**
     * Creates reporting GRPC service.
     *
     * @param reportRunner report runner to use
     * @param templatesDao templates DAO to use
     */
    public ReportingServiceRpc(@Nonnull ComponentReportRunner reportRunner,
            @Nonnull TemplatesDao templatesDao) {
        this.reportRunner = Objects.requireNonNull(reportRunner);
        this.templatesDao = Objects.requireNonNull(templatesDao);
    }

    @Override
    public void generateReport(GenerateReportRequest request,
            StreamObserver<ReportResponse> responseObserver) {
        try {
            final StandardReportsRecord reportTemplate =
                    templatesDao.getTemplateById(request.getReportId())
                            .orElseThrow(() -> new ReportingException(
                                    "Could not find report template by id " +
                                            request.getReportId()));
            final ReportRequest report = new ReportRequest(reportTemplate.getFilename(),
                    ReportOutputFormat.values()[request.getFormat()], request.getParametersMap());
            final InputStream reportStream = reportRunner.createReport(report);
            final ReportResponse.Builder resultBuilder = ReportResponse.newBuilder();
            final File outputFile = File.createTempFile("report-", ".report");
            Files.copy(reportStream, outputFile.toPath());
            // TODO fill appropriate file id
            resultBuilder.setFileName(1L);
            responseObserver.onNext(resultBuilder.build());
        } catch (ReportingException | IOException | DbException e) {
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
            logger.error("Failed getrieve all the report templates", e);
            responseObserver.onError(
                    new StatusRuntimeException(Status.INTERNAL.withDescription(e.getMessage())));
        }
    }
}
