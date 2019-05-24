package com.vmturbo.reports.component.instances;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.mail.EmailException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.google.common.collect.ImmutableList;

import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.common.mail.MailConfigException;
import com.vmturbo.components.common.mail.MailException;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.reporting.api.ReportingConstants;
import com.vmturbo.reporting.api.protobuf.Reporting;
import com.vmturbo.reports.component.ComponentReportRunner;
import com.vmturbo.reports.component.EmailAddressValidator;
import com.vmturbo.reports.component.ReportRequest;
import com.vmturbo.reports.component.ReportingException;
import com.vmturbo.reports.component.communication.ReportNotificationSender;
import com.vmturbo.reports.component.data.ReportDataUtils.MetaGroup;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataGenerator;
import com.vmturbo.reports.component.entities.EntitiesDao;
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
    private static final String VM_GROUP_NAME = "vm_group_name";

    private final Logger logger = LogManager.getLogger();

    /**
     * Report runner to use for reports generation.
     */
    private final ComponentReportRunner reportRunner;

    private final TemplatesOrganizer templatesOrganizer;
    private final ReportInstanceDao reportInstanceDao;
    private final EntitiesDao entitiesDao;
    private final File outputDirectory;
    private final Executor executor;
    private final ReportNotificationSender notificationSender;
    private final MailManager mailManager;
    private final ReportsDataGenerator reportsDataGenerator;

    /**
     * Creates instance of reports generator.
     * @param reportRunner to create report
     * @param templatesOrganizer to use appropriate templates dao and get data from storage
     * @param reportInstanceDao to get instance data from storage
     * @param entitiesDao DAO providing entity names by oids
     * @param outputDirectory to save generated reports in
     * @param executor to execute generation or reports in separate threads
     * @param notificationSender to send notifications if generation was success or failed
     * @param mailManager mail manager
     */
    public ReportsGenerator(@Nonnull final ComponentReportRunner reportRunner,
                            @Nonnull final TemplatesOrganizer templatesOrganizer,
                            @Nonnull final ReportInstanceDao reportInstanceDao, @Nonnull EntitiesDao entitiesDao,
                            @Nonnull final File outputDirectory, @Nonnull Executor executor,
                            @Nonnull final ReportNotificationSender notificationSender,
                            @Nonnull final MailManager mailManager,
                            @Nonnull final ReportsDataGenerator reportsDataGenerator) {
        this.reportRunner = Objects.requireNonNull(reportRunner);
        this.templatesOrganizer = Objects.requireNonNull(templatesOrganizer);
        this.reportInstanceDao = Objects.requireNonNull(reportInstanceDao);
        this.entitiesDao = Objects.requireNonNull(entitiesDao);
        this.outputDirectory = Objects.requireNonNull(outputDirectory);
        this.executor = Objects.requireNonNull(executor);
        this.notificationSender = Objects.requireNonNull(notificationSender);
        this.mailManager = Objects.requireNonNull(mailManager);
        this.reportsDataGenerator = Objects.requireNonNull(reportsDataGenerator);
    }

    /**
     * Triggeres generating a report and returns report id. Report generation will be executed in a
     * separate thread, notifications will be sent on finish using {@link #notificationSender}.
     * Report instance data (generated reports) are stored in the filesystem in
     * {@link #outputDirectory} and have associated DB record in {@code report_instances} table.
     * Name of the file to store report data is a report instance Id (converted to {@link String})
     *
     * @param request report generation request
     * @return report instance id
     * @throws DbException if and external storage error occurs
     * @throws ReportingException if error occurred generating the report
     * @throws EmailException if error occurred sending emails
     */
    @Nonnull
    public Reporting.ReportInstanceId generateReport(@Nonnull Reporting.GenerateReportRequest request)
                    throws DbException, ReportingException, EmailException {
        EmailAddressValidator.validateAddresses(request.getSubscribersEmailsList());
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
        final Map<String, String> reportAttributes = prepareReportAttributes(request.getParametersMap());
        final File file = new File(outputDirectory, Long.toString(reportInstance.getId()));
        executor.execute(() -> {
            final Optional<String> id = reportsDataGenerator.generateDataByRequest(request);
            // only some reports require updating report attributes, e.g. on-demand report.
            id.ifPresent(generatedGroupId -> {
                reportAttributes.put(ReportingConstants.ITEM_UUID_PROPERTY, String.valueOf(generatedGroupId));
                populateDisplayName(reportAttributes, generatedGroupId);
            });

            final ReportRequest report =
                new ReportRequest(template.getTemplateFile(), format, reportAttributes);
            generateReportInternal(report, file, reportInstance, request.getSubscribersEmailsList());
        });

        final Reporting.ReportInstanceId.Builder resultBuilder = Reporting.ReportInstanceId.newBuilder();
        resultBuilder.setId(reportInstance.getId());
        return resultBuilder.build();
    }



    private Map<String, String> prepareReportAttributes(Map<String, String> parametersMap)
            throws DbException {
        final Map<String, String> result = new HashMap<>();
        result.putAll(parametersMap);
        final String oidString = parametersMap.get(ReportingConstants.ITEM_UUID_PROPERTY);
        if (!StringUtils.isEmpty(oidString)) {
            populateDisplayName(result, oidString);
        }
        // Some reports require this group to retrieve child elements. Currently we will use
        // "fake_vm_group" as group name.
        result.put(VM_GROUP_NAME, MetaGroup.FAKE_VM_GROUP.name().toLowerCase());
        return result;
    }

    private void populateDisplayName(@Nonnull final Map<String, String> result,
                                     @Nonnull final String oidString) {
        try {
            if (oidString != null) {
                final Long oid = Long.valueOf(oidString);
                final Optional<String> objectName = entitiesDao.getEntityName(oid);
                if (objectName.isPresent()) {
                    result.put("selected_item_name", objectName.get());
                } else {
                    logger.warn("Could not find entity name for oid {}", oid);
                }
            }
        } catch (DbException e) {
           logger.error("Failed to get entity name for oid: {}", oidString);
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
                logger.warn("Error occurred while rendering report " + reportInstance.getId() +
                        " from file " + reportRequest.getRptDesign(), e);
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

    public Map<Long, ReportTemplate> getSupportedReport() {
        return reportsDataGenerator.getSupportedReport();

    }
}
