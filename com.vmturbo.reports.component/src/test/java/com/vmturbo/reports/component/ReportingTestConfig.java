package com.vmturbo.reports.component;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.eclipse.birt.core.exception.BirtException;
import org.flywaydb.core.Flyway;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.ShortcutTransport;
import com.vmturbo.components.api.server.IMessageSender;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.reporting.api.ReportingNotificationReceiver;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportNotification;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceImplBase;
import com.vmturbo.reports.component.communication.ReportNotificationSender;
import com.vmturbo.reports.component.communication.ReportNotificationSenderImpl;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.instances.ReportInstanceDao;
import com.vmturbo.reports.component.instances.ReportInstanceDaoImpl;
import com.vmturbo.reports.component.templates.TemplatesDao;
import com.vmturbo.reports.component.templates.TemplatesDaoImpl;
import com.vmturbo.sql.utils.FlywayMigrator;

/**
 * Test configuration to run reporting tests, related to the DB.
 */
@Configuration
@EnableTransactionManagement
@Import({ReportingTestDbConfig.class})
public class ReportingTestConfig {

    private static final String REPORTING_SCHEMA = "reporting_test";

    @Autowired
    private ReportingTestDbConfig dbConfig;

    @Bean
    protected GrpcTestServer planGrpcServer() throws IOException {
        final GrpcTestServer server = GrpcTestServer.newServer(reportingService());
        server.start();
        return server;
    }

    @Bean
    protected ComponentReportRunner reportRunner() {
        try {
            return new ComponentReportRunner(dbConfig.reportingDatasource());
        } catch (BirtException e) {
            throw new BeanCreationException("Could not create report runner", e);
        }
    }

    @Bean
    public File reportsOutputDir() {
        try {
            return temporaryFolder().newFolder();
        } catch (IOException e) {
            throw new BeanCreationException("Failed to create reports output directory", e);
        }
    }

    @Bean(destroyMethod = "delete")
    public TemporaryFolder temporaryFolder() throws IOException {
        final TemporaryFolder tmpFolder = new TemporaryFolder();
        tmpFolder.create();
        return tmpFolder;
    }

    @Bean
    protected ReportingServiceImplBase reportingService() {
        return new ReportingServiceRpc(reportRunner(), templatesDao(), reportInstanceDao(),
                reportsOutputDir(), Executors.newCachedThreadPool(), notificationSender());
    }

    @Bean
    public ReportNotificationSender notificationSender() {
        return new ReportNotificationSenderImpl(messageChannel());
    }

    @Bean
    public SenderReceiverPair<ReportNotification> messageChannel() {
        return new SenderReceiverPair<>();
    }

    @Bean
    public ReportingNotificationReceiver notificationReceiver() {
        return new ReportingNotificationReceiver(messageChannel(), threadPool());
    }

    @Bean
    public ExecutorService threadPool() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    public TemplatesDao templatesDao() {
        return new TemplatesDaoImpl(dbConfig.dsl());
    }

    @Bean
    public ReportInstanceDao reportInstanceDao() {
        return new ReportInstanceDaoImpl(dbConfig.dsl());
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer properties() {
        final PropertySourcesPlaceholderConfigurer pspc =
                new PropertySourcesPlaceholderConfigurer();
        final Properties properties = new Properties();
        properties.setProperty("originalSchemaName", "vmtdb");
        pspc.setProperties(properties);
        return pspc;
    }

    @PostConstruct
    public void init() {
        IdentityGenerator.initPrefix(0);
        dbConfig.flyway().clean();
        dbConfig.flyway().migrate();
    }

    @Bean
    public Flyway localFlyway() {
        return new FlywayMigrator(Duration.ofMinutes(1), Duration.ofSeconds(5), () -> {
            final Flyway flyway = new Flyway();
            flyway.setDataSource(dbConfig.dataSource());
            flyway.setSchemas(REPORTING_SCHEMA);
            flyway.setLocations(ReportingDbConfig.MIGRATIONS_LOCATION);
            return flyway;
        }).migrate();
    }
}
