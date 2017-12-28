package com.vmturbo.reporting.api;

import javax.annotation.Nonnull;

/**
 * Listener to accept reporting notifications.
 */
public interface ReportListener {

    /**
     * Fired, when a report generation has been successfully finished.
     *
     * @param reportId id of the report, finished generation
     */
    void onReportGenerated(long reportId);

    /**
     * Fired, when a report generation failed.
     *
     * @param reportId report id
     * @param failureDescription reason of report generation failure
     */
    void onReportFailed(long reportId, @Nonnull String failureDescription);
}
