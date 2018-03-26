package com.vmturbo.reports.component;

import java.io.File;
import java.time.Clock;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import javax.annotation.Nonnull;

import org.eclipse.birt.core.exception.BirtException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.servlet.DispatcherServlet;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.schema.abstraction.tables.records.OnDemandReportsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.StandardReportsRecord;
import com.vmturbo.reporting.api.ReportingNotificationReceiver;
import com.vmturbo.reporting.api.protobuf.ReportingREST.ReportingServiceController;
import com.vmturbo.reports.component.communication.ReportNotificationSenderImpl;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.instances.ReportInstanceDaoImpl;
import com.vmturbo.reports.component.instances.ReportsGenerator;
import com.vmturbo.reports.component.schedules.ScheduleDAO;
import com.vmturbo.reports.component.schedules.ScheduleDAOimpl;
import com.vmturbo.reports.component.schedules.Scheduler;
import com.vmturbo.reports.component.templates.OnDemandTemplatesDao;
import com.vmturbo.reports.component.templates.StandardTemplatesDaoImpl;
import com.vmturbo.reports.component.templates.TemplatesDao;
import com.vmturbo.reports.component.templates.TemplatesOrganizer;

/**
 * Spring beans configuration for running reporting.
 */
@Configuration
@Import({ReportingDbConfig.class, BaseKafkaProducerConfig.class, GroupClientConfig.class})
public class ReportingConfig {

    @Autowired
    private ReportingDbConfig dbConfig;

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Value("${report.files.output.dir}")
    private File reportOutputDir;

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    /**
     * Time when scheduled reports should be generated.
     */
    @Value("${scheduledReportsGenerationTime}")
    private int scheduledReportsGenerationTime;

    @Nonnull
    public ReportingDbConfig dbConfig() {
        return dbConfig;
    }

    @Bean
    public ComponentReportRunner componentReportRunner() {
        try {
            return new ComponentReportRunner(dbConfig.reportingDatasource());
        } catch (BirtException e) {
            throw new BeanCreationException("Could not create component report runner", e);
        }
    }

    @Bean(destroyMethod = "shutdownNow")
    public ExecutorService threadPool() {
        final ThreadFactory tf = new ThreadFactoryBuilder().setNameFormat("reporting-%d").build();
        return Executors.newCachedThreadPool(tf);
    }

    @Bean
    public ReportingServiceRpc reportingService() {
        IdentityGenerator.initPrefix(identityGeneratorPrefix);
        return new ReportingServiceRpc(templatesOrganizer(),
                reportInstanceDao(), reportOutputDir, reportsGenerator(), scheduler());
    }

    @Bean
    public ReportsGenerator reportsGenerator() {
        return new ReportsGenerator(componentReportRunner(), templatesOrganizer(), reportInstanceDao(),
                        reportOutputDir, threadPool(), notificationSender(), mailManager());
    }

    @Bean
    public MailManager mailManager() {
        return new MailManager(SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }

    @Bean
    public Scheduler scheduler() {
        return new Scheduler(reportsGenerator(), scheduleDAO(), scheduledReportsGenerationTime,
                        Clock.systemDefaultZone(), new Timer("scheduledReportsGeneration"));
    }

    @Bean
    public ReportNotificationSenderImpl notificationSender() {
        return new ReportNotificationSenderImpl(baseKafkaProducerConfig.kafkaMessageSender()
                .messageSender(ReportingNotificationReceiver.REPORT_GENERATED_TOPIC));
    }

    @Bean
    public TemplatesOrganizer templatesOrganizer() {
        return new TemplatesOrganizer(standardTemplatesDao(), onDemandReportsTemplatesDao());
    }

    @Bean
    public TemplatesDao standardTemplatesDao() {
        return new StandardTemplatesDaoImpl(dbConfig.dsl());
    }

    @Bean
    public TemplatesDao onDemandReportsTemplatesDao() {
        return new OnDemandTemplatesDao(dbConfig.dsl());
    }

    @Bean
    public ReportInstanceDao reportInstanceDao() {
        return new ReportInstanceDaoImpl(dbConfig.dsl());
    }

    @Bean
    public ScheduleDAO scheduleDAO() {
        return new ScheduleDAOimpl(dbConfig.dsl());
    }

    @Bean
    public KafkaProducerHealthMonitor kafkaHealthMonitor() {
        return new KafkaProducerHealthMonitor(baseKafkaProducerConfig.kafkaMessageSender());
    }

    @Bean
    public ReportingServiceController reportingServiceController() {
        return new ReportingServiceController(reportingService());
    }

    /**
     * Creates bean to support all the HTTP requests.
     *
     * @return dispatcher servlet bean.
     */
    @Bean
    public DispatcherServlet dispatcherServlet() {
        return new DispatcherServlet();
    }
}
