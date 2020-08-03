package com.vmturbo.reports.component;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.annotation.PostConstruct;

import org.eclipse.birt.core.exception.BirtException;
import org.flywaydb.core.Flyway;
import org.jooq.DSLContext;
import org.junit.rules.TemporaryFolder;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.transaction.annotation.EnableTransactionManagement;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.test.GrpcTestServer;
import com.vmturbo.components.api.test.SenderReceiverPair;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.reporting.api.ReportingNotificationReceiver;
import com.vmturbo.reporting.api.protobuf.Reporting.ReportNotification;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceImplBase;
import com.vmturbo.reports.component.communication.ReportNotificationSender;
import com.vmturbo.reports.component.communication.ReportNotificationSenderImpl;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.ReportDBDataWriter;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.reports.component.data.ReportsDataGenerator;
import com.vmturbo.reports.component.data.pm.Daily_cluster_30_days_avg_stats_vs_thresholds_grid;
import com.vmturbo.reports.component.data.pm.NullReportTemplate;
import com.vmturbo.reports.component.data.pm.PM_group_daily_pm_vm_utilization_heatmap_grid;
import com.vmturbo.reports.component.data.pm.PM_group_hosting;
import com.vmturbo.reports.component.data.pm.PM_group_profile;
import com.vmturbo.reports.component.data.pm.PM_profile;
import com.vmturbo.reports.component.data.vm.Daily_vm_over_under_prov_grid;
import com.vmturbo.reports.component.data.vm.Daily_vm_over_under_prov_grid_30_days;
import com.vmturbo.reports.component.data.vm.Daily_vm_rightsizing_advice_grid;
import com.vmturbo.reports.component.data.vm.VM_group_30_days_vm_top_bottom_capacity_grid;
import com.vmturbo.reports.component.data.vm.VM_group_daily_over_under_prov_grid_30_days;
import com.vmturbo.reports.component.data.vm.VM_group_individual_monthly_summary;
import com.vmturbo.reports.component.data.vm.VM_group_profile;
import com.vmturbo.reports.component.data.vm.VM_group_profile_physical_resources;
import com.vmturbo.reports.component.data.vm.VM_group_rightsizing_advice_grid;
import com.vmturbo.reports.component.data.vm.VM_profile;
import com.vmturbo.reports.component.entities.EntitiesDao;
import com.vmturbo.reports.component.entities.EntitiesDaoImpl;
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
    public GrpcTestServer settingsGrpcServer() throws IOException {
        final GrpcTestServer server = GrpcTestServer.newServer(settingsService());
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
    protected ReportingServiceImplBase reportingService() throws IOException {
        return new ReportingServiceRpc(templatesOrganizer(), reportInstanceDao(),
                reportsOutputDir(), reportsGenerator(), scheduler());
    }

    @Bean
    SettingServiceGrpc.SettingServiceImplBase settingsService() {
        return new TestSetttingsService();
    }

    @Bean
    public ReportsGenerator reportsGenerator() throws IOException {
        final ReportsDataContext context = mock(ReportsDataContext.class);
        final ReportDBDataWriter reportDBDataWriter = mock(ReportDBDataWriter.class);
        when(context.getReportDataWriter()).thenReturn(reportDBDataWriter);
        return new ReportsGenerator(reportRunner(), templatesOrganizer(), reportInstanceDao(),
                entitiesDao(), reportsOutputDir(), threadPool(), notificationSender(),
                mailManager(), new ReportsDataGenerator(context, getReportMap()));
    }

    private Map<Long, ReportTemplate> getReportMap(){
        final GroupGeneratorDelegate delegate = new GroupGeneratorDelegate(60,10000);
        return ImmutableMap.<Long, ReportTemplate>builder().
            // standard reports
            // daily_infra_30_days_avg_stats_vs_thresholds_grid
                put(140L, new NullReportTemplate(delegate)).
            // require stats in cluster_members table
            //    put(146L, new Monthly_cluster_summary(delegate)).
            //    put(147L, new Monthly_summary(delegate)).
                put(148L, new Daily_vm_over_under_prov_grid(delegate)).
                put(150L, new Daily_vm_rightsizing_advice_grid(delegate)).
            // daily_pm_vm_top_bottom_utilized_grid
                put(155L, new NullReportTemplate(delegate)).
            // daily_pm_vm_utilization_heatmap_grid
                put(156L, new NullReportTemplate(delegate)).
            // daily_pm_top_bottom_capacity_grid
                put(165L, new NullReportTemplate(delegate)).
            // daily_vm_top_bottom_capacity_grid
                put(166L, new NullReportTemplate(delegate)).
            // daily_pm_top_cpu_ready_queue_grid
                put(168L, new NullReportTemplate(delegate)).
            // daily_pm_top_resource_utilization_bar
                put(169L, new NullReportTemplate(delegate)).
                put(170L, new Daily_cluster_30_days_avg_stats_vs_thresholds_grid(delegate)).
            // daily_potential_storage_waste_grid
            //    put(172L, new NullReportTemplate(delegate)).
            // monthly_individual_vm_summary
                put(181L, new NullReportTemplate(delegate)).
            // monthly_30_days_pm_top_bottom_capacity_grid
                put(182L, new NullReportTemplate(delegate)).
            // monthly_30_days_vm_top_bottom_capacity_grid
                put(183L, new NullReportTemplate(delegate)).
                put(184L, new Daily_vm_over_under_prov_grid_30_days(delegate)).
            // weekly_socket_audit_report
                put(189L, new NullReportTemplate(delegate)).

            // on demand report
                put(1L, new PM_group_profile(delegate)).
                put(2L, new PM_profile(delegate)).
                put(5L, new VM_group_profile(delegate)).
                put(6L, new VM_profile(delegate)).
                put(8L, new PM_group_hosting(delegate)).
                put(9L, new VM_group_profile_physical_resources(delegate)).
                put(10L, new VM_group_individual_monthly_summary(delegate)).
                put(11L, new VM_group_daily_over_under_prov_grid_30_days(delegate)).
                put(12L, new VM_group_30_days_vm_top_bottom_capacity_grid(delegate)).
            // vm_group_daily_over_under_prov_grid_given_days
                put(13L, new NullReportTemplate(delegate)).
            // require stats in cluster_members table
            //    put(14L, new PM_group_monthly_individual_cluster_summary(delegate)).
            //    put(15L, new PM_group_pm_top_bottom_capacity_grid_per_cluster(delegate)).
                put(16L, new PM_group_daily_pm_vm_utilization_heatmap_grid(delegate)).
                put(17L, new VM_group_rightsizing_advice_grid(delegate))
            .build();
    }

    @Bean
    public MailManager mailManager() throws IOException {
        return new MailManager(SettingServiceGrpc.newBlockingStub(settingsGrpcServer().getChannel()));
    }

    @Bean public Scheduler scheduler() throws IOException {
        return new Scheduler(reportsGenerator(), scheduleDAO(), 1,
                        Clock.systemDefaultZone(), new Timer("scheduledReportsGeneration"));
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
        return new ReportingNotificationReceiver(messageChannel(), threadPool(), 0);
    }

    @Bean
    public ExecutorService threadPool() {
        return Executors.newCachedThreadPool();
    }

    @Bean
    public TemplatesOrganizer templatesOrganizer() {
        return new TemplatesOrganizer(standardTemplatesDao(), onDemandTemplatesDao());
    }

    @Bean
    public TemplatesDao standardTemplatesDao() {
        return new StandardTemplatesDaoImpl(dbConfig.dsl());
    }

    @Bean
    public TemplatesDao onDemandTemplatesDao() {
        return new OnDemandTemplatesDao(dbConfig.dsl());
    }

    @Bean
    public ScheduleDAO scheduleDAO() { return new ScheduleDAOimpl(dbConfig.dsl()); }

    @Bean
    public ReportInstanceDao reportInstanceDao() {
        return new ReportInstanceDaoImpl(dbConfig.dsl());
    }

    @Bean
    public EntitiesDao entitiesDao() {
        return new EntitiesDaoImpl(dbConfig.dsl());
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
        localFlyway().clean();
        localFlyway().migrate();
    }

    @Bean
    public Flyway localFlyway() {
        return new FlywayMigrator(Duration.ofMinutes(1), Duration.ofSeconds(5), () -> {
            final Flyway flyway = new Flyway();
            flyway.setDataSource(dbConfig.flyway().getDataSource());
            flyway.setSchemas(REPORTING_SCHEMA);
            flyway.setLocations(ReportingDbConfig.MIGRATIONS_LOCATION);
            return flyway;
        }).migrate();
    }

    @Bean
    public DSLContext dslContext() {
        return dbConfig.dsl();
    }
}
