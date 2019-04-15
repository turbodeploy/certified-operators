package com.vmturbo.reports.component;

import java.io.File;
import java.time.Clock;
import java.util.Map;
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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.components.api.server.BaseKafkaProducerConfig;
import com.vmturbo.components.common.health.KafkaProducerHealthMonitor;
import com.vmturbo.components.common.mail.MailManager;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.reporting.api.ReportingNotificationReceiver;
import com.vmturbo.reporting.api.protobuf.ReportingREST.ReportingServiceController;
import com.vmturbo.reports.component.communication.ReportNotificationSenderImpl;
import com.vmturbo.reports.component.communication.ReportingServiceRpc;
import com.vmturbo.reports.component.data.GroupGeneratorDelegate;
import com.vmturbo.reports.component.data.ReportDBDataWriter;
import com.vmturbo.reports.component.data.ReportTemplate;
import com.vmturbo.reports.component.data.ReportsDataContext;
import com.vmturbo.reports.component.data.ReportsDataGenerator;
import com.vmturbo.reports.component.data.pm.Daily_cluster_30_days_avg_stats_vs_thresholds_grid;
import com.vmturbo.reports.component.data.pm.Monthly_cluster_summary;
import com.vmturbo.reports.component.data.pm.Monthly_summary;
import com.vmturbo.reports.component.data.pm.PM_group_daily_pm_vm_utilization_heatmap_grid;
import com.vmturbo.reports.component.data.pm.PM_group_hosting;
import com.vmturbo.reports.component.data.pm.PM_group_monthly_individual_cluster_summary;
import com.vmturbo.reports.component.data.pm.PM_group_pm_top_bottom_capacity_grid_per_cluster;
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
import com.vmturbo.repository.api.impl.RepositoryClientConfig;

/**
 * Spring beans configuration for running reporting.
 */
@Configuration
@Import({ReportingDbConfig.class, BaseKafkaProducerConfig.class, GroupClientConfig.class,
    RepositoryClientConfig.class, ActionOrchestratorClientConfig.class})
public class ReportingConfig {

    @Autowired
    private ReportingDbConfig dbConfig;

    @Autowired
    private BaseKafkaProducerConfig baseKafkaProducerConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private  ActionOrchestratorClientConfig actionOrchestratorClientConfig;

    @Value("${report.files.output.dir}")
    private File reportOutputDir;

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    private final GroupGeneratorDelegate delegate = new GroupGeneratorDelegate();

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
        return new ReportsGenerator(componentReportRunner(), templatesOrganizer(),
                reportInstanceDao(), entitiesDao(), reportOutputDir, threadPool(),
                notificationSender(), mailManager(), reportsDataGenerator());
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
    public EntitiesDao entitiesDao() {
        return new EntitiesDaoImpl(dbConfig.dsl());
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
    public GroupServiceBlockingStub groupRpcService() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public ReportingServiceController reportingServiceController() {
        return new ReportingServiceController(reportingService());
    }

    @Bean
    public ReportDBDataWriter reportDataWriter() {
        return new ReportDBDataWriter(dbConfig.dsl());
    }

    @Bean
    public Channel repositoryChannel() {
        return repositoryClientConfig.repositoryChannel();
    }

    @Bean
    public ActionsServiceBlockingStub actionsRpcService() {
        return ActionsServiceGrpc.newBlockingStub(actionOrchestratorClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public ReportsDataGenerator reportsDataGenerator() {
        return new ReportsDataGenerator(new ReportsDataContext(groupRpcService(),
            reportDataWriter(), repositoryChannel(), actionsRpcService(), realtimeTopologyContextId)
            , getReportMap());
    }

    private Map<Long, ReportTemplate> getReportMap() {
        return ImmutableMap.<Long, ReportTemplate>builder().
            // VM related reports
                put(150L, new Daily_vm_rightsizing_advice_grid(delegate)).
                put(184L, new Daily_vm_over_under_prov_grid_30_days(delegate)).
                put(148L, new Daily_vm_over_under_prov_grid(delegate)).

            // PM related reports
                put(146L, new Monthly_cluster_summary(delegate)).

                put(147L, new Monthly_summary(delegate)).
                put(14L, new PM_group_monthly_individual_cluster_summary(delegate)).

            // both VM and PM
                put(170L, new Daily_cluster_30_days_avg_stats_vs_thresholds_grid(delegate)).

            // on demand reprot
                put(1L, new PM_group_profile(delegate)).
                put(2L, new PM_profile(delegate)).
                put(5L, new VM_group_profile(delegate)).
                put(8L, new PM_group_hosting(delegate)).
                put(9L, new VM_group_profile_physical_resources(delegate)).
                put(10L, new VM_group_individual_monthly_summary(delegate)).
                put(11L, new VM_group_daily_over_under_prov_grid_30_days(delegate)).
                put(12L, new VM_group_30_days_vm_top_bottom_capacity_grid(delegate)).
                put(15L, new PM_group_pm_top_bottom_capacity_grid_per_cluster(delegate)).
                put(16L, new PM_group_daily_pm_vm_utilization_heatmap_grid(delegate)).
                put(17L, new VM_group_rightsizing_advice_grid(delegate))

            .build();
    }
}
