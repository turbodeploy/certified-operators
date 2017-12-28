package com.vmturbo.reports.component.communication;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.server.ComponentNotificationSender;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.reporting.api.protobuf.Reporting.Empty;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportNotification;

/**
 * Implementation of report notification sender.
 */
public class ReportNotificationSenderImpl extends
        ComponentNotificationSender<ReportNotification> implements ReportNotificationSender {

    private final IMessageSender<ReportNotification> messageSender;

    /**
     * Constructs notification sender.
     *
     * @param messageSender message sender of the underlying communication layer.
     */
    public ReportNotificationSenderImpl(@Nonnull IMessageSender<ReportNotification> messageSender) {
        this.messageSender = Objects.requireNonNull(messageSender);
    }

    @Override
    public void notifyReportGenerated(long reportId)
            throws InterruptedException, CommunicationException {
        sendMessage(messageSender, ReportNotification.newBuilder()
                .setReportId(reportId)
                .setGenerated(Empty.getDefaultInstance())
                .build());
    }

    @Override
    public void notifyReportGenerationFailed(long reportId, @Nonnull String failureMessage)
            throws InterruptedException, CommunicationException {
        Objects.requireNonNull(failureMessage);
        sendMessage(messageSender, ReportNotification.newBuilder()
                .setReportId(reportId)
                .setFailed(failureMessage)
                .build());
    }

    @Override
    protected String describeMessage(@Nonnull ReportNotification reportNotification) {
        return "Notification " + reportNotification.getNotificationCase() + " for report " +
                reportNotification.getReportId();
    }
}
