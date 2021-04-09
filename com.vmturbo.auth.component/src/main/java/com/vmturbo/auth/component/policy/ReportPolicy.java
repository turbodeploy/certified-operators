package com.vmturbo.auth.component.policy;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.component.licensing.LicenseCheckService;
import com.vmturbo.common.protobuf.licensing.Licensing.LicenseSummary;

/**
 * Report related policies.
 */
public class ReportPolicy {

    private final Logger logger = LogManager.getLogger(getClass());
    private final LicenseCheckService licenseCheckService;
    private boolean reportingEnabled;

    /**
     * Constructor.
     *
     * @param licenseCheckService the license check service.
     * @param reportingEnabled is reporting component enabled?
     */
    public ReportPolicy(@Nonnull final LicenseCheckService licenseCheckService, final boolean reportingEnabled) {
        this.licenseCheckService = licenseCheckService;
        this.reportingEnabled = reportingEnabled;
    }

    /**
     * Get the max count of allowed report editors.
     * If there is an external grafana licence which has information about max count of editors
     * then return it.
     * If an external grafana licence don't have information about max count of editors or there
     * is no grafana license then return predefined default max count of report editors.
     *
     * @return the max count of allowed reports editors.
     */
    public int getAllowedMaximumEditors() {
        LicenseSummary lastSummary = licenseCheckService.getLastSummary();
        if (lastSummary != null) {
            return lastSummary.getMaxReportEditorsCount();
        } else {
            final int defaultEditorCount = LicenseSummary.getDefaultInstance().getMaxReportEditorsCount();
            logger.warn("There is no available license summary. Using default instance with"
                    + "editor count {}", defaultEditorCount);
            return defaultEditorCount;
        }
    }

    public boolean isReportingEnabled() {
        return reportingEnabled;
    }
}
