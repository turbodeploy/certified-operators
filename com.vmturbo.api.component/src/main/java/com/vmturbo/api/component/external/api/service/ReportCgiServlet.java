package com.vmturbo.api.component.external.api.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportData;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;

/**
 * A servlet to substitute cgi-scripts in Legacy. Should be reworked to be an ordinal REST call
 * later.
 */
public class ReportCgiServlet extends HttpServlet {

    private static final String ACTION_REPORT = "REPORT";
    private static final String ACTION_PARAM = "actionType";

    // TODO add user permissions here OM-29006
    private final ReportingServiceBlockingStub reportingService;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs the servlet.
     *
     * @param reportingService reporting gRPC service to query
     */
    public ReportCgiServlet(@Nonnull ReportingServiceBlockingStub reportingService) {
        this.reportingService = Objects.requireNonNull(reportingService);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (ACTION_REPORT.equals(req.getParameter(ACTION_PARAM))) {
            final String reportId = req.getParameter("output");
            if (StringUtils.isNumeric(reportId)) {
                final ReportInstanceId reportInstanceId =
                        ReportInstanceId.newBuilder().setId(Long.valueOf(reportId)).build();
                final ReportData report;
                try {
                    report = reportingService.getReportData(reportInstanceId);
                } catch (StatusRuntimeException e) {
                    logger.info("Error retrieving report with id " + reportId, e);
                    resp.sendError(grpc2httpStatusCode(e.getStatus().getCode()), e.getMessage());
                    return;
                }
                final ReportOutputFormat format = ReportOutputFormat.get(report.getFormat());
                if (format != null) {
                    resp.setHeader("Content-Type",
                            "application/" + format.getLiteral().toLowerCase());
                    resp.setHeader("Content-Disposition",
                            "inline; filename=\"" + Long.toString(reportInstanceId.getId()) +
                                    format.getOutputExtension() + '\"');
                    try (final OutputStream os = resp.getOutputStream()) {
                        os.write(report.getData().toByteArray());
                    }
                } else {
                    logger.warn("Could not determine format {} for report {}", reportId,
                            report.getFormat());
                    resp.sendError(501, "Could not determine report format " + report.getFormat());
                }
            } else {
                resp.sendError(400, "Report Id is not a numeric value: " + reportId);
            }
        } else {
            logger.warn("Unknown request arrived with parameters: " + req.getParameterMap());
            resp.sendError(501, "Not implemented");
        }
    }

    /**
     * Performs mapping from GRPC status codes into HTTP status codes.
     *
     * @param code GRPC status code
     * @return HTTP status code
     */
    private static int grpc2httpStatusCode(@Nonnull final Code code) {
        switch (code) {
            case NOT_FOUND:
                return 404;
            default:
                return 501;
        }
    }
}
