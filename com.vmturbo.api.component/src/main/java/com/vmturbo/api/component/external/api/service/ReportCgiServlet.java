package com.vmturbo.api.component.external.api.service;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Objects;

import javax.annotation.Nonnull;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.reporting.api.protobuf.Reporting.ReportData;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportInstanceId;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;

/**
 * A servlet to substitute cgi-scripts in Legacy. Should be reworked to be an ordinal REST call
 * later.
 */
public class ReportCgiServlet extends HttpServlet {

    // TODO add user permissions here OM-29006
    private final ReportingServiceBlockingStub reportingService;
    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Constructs the servlet.
     *
     * @param reportingService reporting GRPC service to query
     */
    public ReportCgiServlet(@Nonnull ReportingServiceBlockingStub reportingService) {
        this.reportingService = Objects.requireNonNull(reportingService);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException {
        if (req.getParameter("actionType").equals("REPORT")) {
            final String reportId = req.getParameter("output");
            if (StringUtils.isNumeric(reportId)) {
                final ReportInstanceId reportInstanceId =
                        ReportInstanceId.newBuilder().setId(Long.valueOf(reportId)).build();
                final ReportData report = reportingService.getReportData(reportInstanceId);
                try (final OutputStream os = resp.getOutputStream()) {
                    os.write(report.getData().toByteArray());
                }
                resp.setHeader("Content-Type", "application/" + report.getFormat());
            } else {
                resp.sendError(400,
                        "Report Id is not a numeric value: " + reportId);
            }
        } else {
            logger.warn("Unknown request arrived with parameters: " + req.getParameterMap());
            resp.sendError(501, "Not implemented");
        }
    }
}
