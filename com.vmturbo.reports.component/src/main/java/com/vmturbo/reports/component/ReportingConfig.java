package com.vmturbo.reports.component;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.eclipse.birt.core.exception.BirtException;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;
import com.vmturbo.reporting.api.ReportingNotificationReceiver;
import com.vmturbo.reports.component.communication.ReportNotificationSenderImpl;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.instances.ReportInstanceDaoImpl;
import com.vmturbo.reports.component.schedules.ScheduleDAO;
import com.vmturbo.reports.component.schedules.ScheduleDAOimpl;
import com.vmturbo.reports.component.templates.OnDemandTemplatesDao;
import com.vmturbo.reports.component.templates.StandardTemplatesDaoImpl;
import com.vmturbo.reports.component.templates.TemplatesDao;
import com.vmturbo.reports.component.templates.TemplatesOrganizer;
import com.vmturbo.reports.db.abstraction.tables.records.OnDemandReportsRecord;
import com.vmturbo.reports.db.abstraction.tables.records.StandardReportsRecord;

/**
 * Spring beans configuration for running reporting.
 */
@Configuration
@Import({ReportingDbConfig.class, BaseKafkaProducerConfig.class})
public class ReportingConfig {

    @Autowired
    private ReportingDbConfig dbConfig;

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Value("${report.files.output.dir}")
    private File reportOutputDir;

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

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
        return new ReportingServiceRpc(componentReportRunner(), templatesOrganizer(),
                reportInstanceDao(), scheduleDAO(), reportOutputDir, threadPool(),
                notificationSender());
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
    public TemplatesDao<StandardReportsRecord> standardTemplatesDao() {
        return new StandardTemplatesDaoImpl(dbConfig.dsl());
    }

    @Bean
    public TemplatesDao<OnDemandReportsRecord> onDemandReportsTemplatesDao() {
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

}
