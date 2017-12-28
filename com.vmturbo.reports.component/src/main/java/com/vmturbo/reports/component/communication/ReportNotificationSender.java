package com.vmturbo.reports.component.communication;

import javax.annotation.Nonnull;

import com.vmturbo.communication.CommunicationException;

/**
 * Represents an instance, usable to broadcast report updates notifications.
 */
public interface ReportNotificationSender {

    /**
     * Broadcast notification about a report successfully finished generation.
     *
     * @param reportId id of the report
     * @throws InterruptedException if thread has been interrupted during sending
     * @throws CommunicationException if persistent communication errors occurred.
     */
    void notifyReportGenerated(long reportId) throws InterruptedException, CommunicationException;

    /**
     * Broadcast notification about a report failed generation.
     *
     * @param reportId id of the report
     * @param failureMessage message explaining the failure reason
     * @throws InterruptedException if thread has been interrupted during sending
     * @throws CommunicationException if persistent communication errors occurred.
     */
    void notifyReportGenerationFailed(long reportId, @Nonnull String failureMessage)
            throws InterruptedException, CommunicationException;
}
