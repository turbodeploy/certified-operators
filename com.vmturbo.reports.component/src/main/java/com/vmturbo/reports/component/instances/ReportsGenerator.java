package com.vmturbo.reports.component.instances;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.collect.ImmutableList;

import org.apache.commons.mail.EmailException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.mail.MailConfigException;
import com.vmturbo.components.common.mail.MailException;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reports.component.ComponentReportRunner;
import com.vmturbo.reports.component.EmailAddressValidator;
import com.vmturbo.reports.component.ReportRequest;
import com.vmturbo.reports.component.ReportingException;
import com.vmturbo.reports.component.communication.ReportNotificationSender;
import com.vmturbo.reports.component.templates.TemplateWrapper;
import com.vmturbo.reports.component.templates.TemplatesOrganizer;
import com.vmturbo.sql.utils.DbException;

/**
 * Class to generate reports.
 */
@ThreadSafe
public class ReportsGenerator {

    private static final String MAIL_SUBJECT = "Turbonomic generated report";
    private static final String MAIL_CONTENT = "Attached report was generated by Turbonomic.";

    private final Logger logger = LogManager.getLogger();

    /**
     * Report runner to use for reports generation.
     */
    private final ComponentReportRunner reportRunner;

    private final TemplatesOrganizer templatesOrganizer;
    private final ReportInstanceDao reportInstanceDao;
    private final File outputDirectory;
    private final Executor executor;
    private final ReportNotificationSender notificationSender;
    private final MailManager mailManager;

    /**
     * Creates instance of reports generator.
     *
     * @param reportRunner to create report
     * @param templatesOrganizer to use appropriate templates dao and get data from storage
     * @param reportInstanceDao to get instance data from storage
     * @param outputDirectory to save generated reports in
     * @param executor to execute generation or reports in separate threads
     * @param notificationSender to send notifications if generation was success or failed
     */
    public ReportsGenerator(@Nonnull ComponentReportRunner reportRunner,
                    @Nonnull TemplatesOrganizer templatesOrganizer,
                    @Nonnull ReportInstanceDao reportInstanceDao, @Nonnull File outputDirectory,
                    @Nonnull Executor executor, @Nonnull ReportNotificationSender notificationSender,
                    @Nonnull MailManager mailManager) {
        this.reportRunner = Objects.requireNonNull(reportRunner);
        this.templatesOrganizer = Objects.requireNonNull(templatesOrganizer);
        this.reportInstanceDao = Objects.requireNonNull(reportInstanceDao);
        this.outputDirectory = Objects.requireNonNull(outputDirectory);
        this.executor = Objects.requireNonNull(executor);
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.mailManager = Objects.requireNonNull(mailManager);
    }

    /**
     * Triggeres generating a report and returns report id. Report generation will be executed in a
     * separate thread, notifications will be sent on finish using {@link #notificationSender}.
     * Report instance data (generated reports) are stored in the filesystem in
     * {@link #outputDirectory} and have associated DB record in {@code report_instances} table.
     * Name of the file to store report data is a report instance Id (converted to {@link String})
     *
     * @param request report generation request
     */
    @Nonnull
    public Reporting.ReportInstanceId generateReport(@Nonnull Reporting.GenerateReportRequest request)
                    throws DbException, ReportingException, EmailException {
        EmailAddressValidator.validateAddresses(request.getSubcribersEmailsList());
        final Reporting.ReportTemplateId templateId = request.getTemplate();
        final ReportType reportType = ReportType.get(templateId.getReportType());
        final TemplateWrapper template =
                        templatesOrganizer.getTemplateById(reportType, templateId.getId())
                                        .orElseThrow(() -> new ReportingException(
                                                        "Could not find report template by id " +
                                                                        templateId.getId()));
        final ReportOutputFormat format =
                        Objects.requireNonNull(ReportOutputFormat.get(request.getFormat()));
        final ReportInstanceRecord reportInstance =
                        reportInstanceDao.createInstanceRecord(reportType, templateId.getId(), format);
        final ReportRequest report = new ReportRequest(template.getTemplateFile(), format,
                        request.getParametersMap());
        final File file = new File(outputDirectory, Long.toString(reportInstance.getId()));
        executor.execute(() -> generateReportInternal(report, file, reportInstance,
                        request.getSubcribersEmailsList()));
        final Reporting.ReportInstanceId.Builder resultBuilder = Reporting.ReportInstanceId.newBuilder();
        resultBuilder.setId(reportInstance.getId());
        return resultBuilder.build();
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
                    @Nonnull ReportInstanceRecord reportInstance, @Nonnull List<String> emailAddresses) {
        try {
            try {
                try {
                    reportRunner.createReport(reportRequest, outputFile);
                    reportInstance.commit();
                    if (!CollectionUtils.isEmpty(emailAddresses)) {
                        mailManager.sendMail(emailAddresses, MAIL_SUBJECT, MAIL_CONTENT,
                                        ImmutableList.of(outputFile.getAbsolutePath()));
                    }
                    notificationSender.notifyReportGenerated(reportInstance.getId());
                } catch (ReportingException e) {
                    logger.warn("Error generating a report {}. Removing report record from the DB...",
                                    reportInstance.getId());
                    reportInstance.rollback();
                    throw e;
                } catch (MailException | MailConfigException mailEx) {
                    logger.error(String.format("Failed to send generated report %s by email to %s",
                                    outputFile, emailAddresses), mailEx);
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
}
