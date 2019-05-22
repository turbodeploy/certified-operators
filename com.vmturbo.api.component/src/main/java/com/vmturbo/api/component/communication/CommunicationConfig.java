package com.vmturbo.api.component.communication;

import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;

import io.grpc.Channel;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.api.ReportNotificationDTO.ReportNotification;
import com.vmturbo.api.ReportNotificationDTO.ReportStatusNotification;
import com.vmturbo.api.ReportNotificationDTO.ReportStatusNotification.ReportStatus;
import com.vmturbo.api.component.external.api.mapper.SeverityPopulator;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcherFactory;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtClientInterceptor;
import com.vmturbo.auth.api.widgets.AuthClientConfig;
import com.vmturbo.clustermgr.api.ClusterMgrClient;
import com.vmturbo.clustermgr.api.ClusterMgrRestClient;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc.ActionsServiceBlockingStub;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc.EntitySeverityServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc;
import com.vmturbo.common.protobuf.cost.CostServiceGrpc.CostServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceBlockingStub;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc;
import com.vmturbo.common.protobuf.cpucapacity.CpuCapacityServiceGrpc.CpuCapacityServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseCheckServiceGrpc.LicenseCheckServiceBlockingStub;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc;
import com.vmturbo.common.protobuf.licensing.LicenseManagerServiceGrpc.LicenseManagerServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceFutureStub;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc;
import com.vmturbo.common.protobuf.plan.ReservationServiceGrpc.ReservationServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateServiceGrpc.TemplateServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc;
import com.vmturbo.common.protobuf.plan.TemplateSpecServiceGrpc.TemplateSpecServiceBlockingStub;
import com.vmturbo.common.protobuf.probe.ProbeRpcServiceGrpc;
import com.vmturbo.common.protobuf.probe.ProbeRpcServiceGrpc.ProbeRpcServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc;
import com.vmturbo.common.protobuf.stats.StatsHistoryServiceGrpc.StatsHistoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc;
import com.vmturbo.common.protobuf.topology.TopologyServiceGrpc.TopologyServiceBlockingStub;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc;
import com.vmturbo.common.protobuf.widgets.WidgetsetsServiceGrpc.WidgetsetsServiceBlockingStub;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc;
import com.vmturbo.common.protobuf.workflow.WorkflowServiceGrpc.WorkflowServiceBlockingStub;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.ComponentRestTemplate;
import com.vmturbo.components.api.client.ComponentApiConnectionConfig;
import com.vmturbo.cost.api.CostClientConfig;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.history.component.api.impl.HistoryClientConfig;
import com.vmturbo.notification.NotificationInMemoryStore;
import com.vmturbo.notification.NotificationStore;
import com.vmturbo.notification.api.impl.NotificationClientConfig;
import com.vmturbo.plan.orchestrator.api.PlanOrchestrator;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.reporting.api.ReportListener;
import com.vmturbo.reporting.api.ReportingClientConfig;
import com.vmturbo.reporting.api.ReportingNotificationReceiver;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig.Subscription;

/**
 * Configuration for the communication between the API component
 * and the rest of the components in the system.
 */
@Configuration
@Import({TopologyProcessorClientConfig.class,
        ActionOrchestratorClientConfig.class, PlanOrchestratorClientConfig.class,
        GroupClientConfig.class, HistoryClientConfig.class, NotificationClientConfig.class,
        RepositoryClientConfig.class, ReportingClientConfig.class, AuthClientConfig.class,
        CostClientConfig.class})
public class CommunicationConfig {

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;
    @Autowired
    private ActionOrchestratorClientConfig aoClientConfig;
    @Autowired
    private PlanOrchestratorClientConfig planClientConfig;
    @Autowired
    private GroupClientConfig groupClientConfig;
    @Autowired
    private HistoryClientConfig historyClientConfig;
    @Autowired
    private NotificationClientConfig notificationClientConfig;
    @Autowired
    private RepositoryClientConfig repositoryClientConfig;
    @Autowired
    private ReportingClientConfig reportingClientConfig;
    @Autowired
    private AuthClientConfig authClientConfig;
    @Autowired
    private CostClientConfig costClientConfig;
    @Value("${clustermgr_host}")
    private String clusterMgrHost;
    @Value("${clustermgr_port}")
    private int clusterMgrPort;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Autowired
    private ApiWebsocketConfig websocketConfig;

    public long getRealtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

    @Bean
    public RestTemplate serviceRestTemplate() {
        return ComponentRestTemplate.create();
    }

    @Bean
    public ApiComponentActionListener apiComponentActionListener()
        throws CommunicationException, InterruptedException, URISyntaxException {
        final ApiComponentActionListener actionsListener =
            new ApiComponentActionListener(websocketConfig.websocketHandler());
        aoClientConfig.actionOrchestratorClient().addActionsListener(actionsListener);
        return actionsListener;
    }

    @Bean
    public JwtClientInterceptor jwtClientInterceptor() {
        return new JwtClientInterceptor();
    }

    @Bean
    public ActionsServiceBlockingStub actionsRpcService() {
        return ActionsServiceGrpc.newBlockingStub(aoClientConfig.actionOrchestratorChannel())
                // Intercept client call and add JWT token to the metadata
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public TopologyServiceBlockingStub topologyService() {
        return TopologyServiceGrpc.newBlockingStub(tpClientConfig.topologyProcessorChannel());
    }

    @Bean
    public PolicyServiceBlockingStub policyRpcService() {
        return PolicyServiceGrpc.newBlockingStub(groupChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public Channel groupChannel() {
        return groupClientConfig.groupChannel();
    }

    @Bean
    public PlanServiceBlockingStub planRpcService() {
        return PlanServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public PlanServiceFutureStub planRpcServiceFuture() {
        return PlanServiceGrpc.newFutureStub(planClientConfig.planOrchestratorChannel());
    }

    @Bean
    public EntitySeverityServiceBlockingStub entitySeverityService() {
        return EntitySeverityServiceGrpc.newBlockingStub(
                aoClientConfig.actionOrchestratorChannel());
    }

    @Bean
    public RepositoryApi repositoryApi() {
        return new RepositoryApi(repositoryClientConfig.getRepositoryHost(),
                repositoryClientConfig.getRepositoryPort(), serviceRestTemplate(),
                severityPopulator(), getRealtimeTopologyContextId());
    }

    @Bean
    public RepositoryServiceBlockingStub repositoryRpcService() {
        return RepositoryServiceGrpc.newBlockingStub(repositoryChannel());
    }

    @Bean
    public WorkflowServiceBlockingStub fetchWorkflowRpcService() {
        return WorkflowServiceGrpc.newBlockingStub(actionOrchestratorChannel());
    }

    @Bean
    public ClusterMgrRestClient clusterMgr() {
        return ClusterMgrClient.createClient(ComponentApiConnectionConfig.newBuilder()
                .setHostAndPort(clusterMgrHost, clusterMgrPort)
                .build());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        return tpClientConfig.topologyProcessorRpcOnly();
    }

    @Bean
    public ProbeRpcServiceBlockingStub probeRpcService() {
        return
            ProbeRpcServiceGrpc.newBlockingStub(
                tpClientConfig.topologyProcessorChannel());
    }

    @Bean
    public ApiComponentPlanListener apiComponentPlanListener() {
        // TODO (roman, Dec 20 2016): It's kind of ugly to have the communication config
        // depend on the external API package. It may be worth it to have the
        // ApiWebsocketHandler/ApiWebsocketConfig register listeners instead.
        return new ApiComponentPlanListener(websocketConfig.websocketHandler());
    }

    @Bean
    public PlanOrchestrator planOrchestrator() {
        final PlanOrchestrator planOrchestrator = planClientConfig.planOrchestrator();
        planOrchestrator.addPlanListener(apiComponentPlanListener());
        return planOrchestrator;
    }

    @Bean
    public GroupServiceBlockingStub groupRpcService() {
        return GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public SettingServiceBlockingStub settingRpcService() {
        return SettingServiceGrpc.newBlockingStub(groupClientConfig.groupChannel());
    }

    @Bean
    public Channel historyChannel() {
        return historyClientConfig.historyChannel();
    }

    @Bean
    public StatsHistoryServiceBlockingStub historyRpcService() {
        return StatsHistoryServiceGrpc.newBlockingStub(historyChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public ReservationServiceBlockingStub reservationServiceBlockingStub() {
        return ReservationServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }

    @Bean
    public Channel repositoryChannel() {
        return repositoryClientConfig.repositoryChannel();
    }

    @Bean
    public SearchServiceGrpc.SearchServiceBlockingStub searchServiceBlockingStub() {
        return SearchServiceGrpc.newBlockingStub(repositoryChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public SupplyChainServiceBlockingStub supplyChainRpcService() {
        return SupplyChainServiceGrpc.newBlockingStub(repositoryChannel());
    }

    @Bean
    public TemplateServiceBlockingStub templateServiceBlockingStub() {
        return TemplateServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }

    @Bean
    public TemplateSpecServiceBlockingStub templateSpecServiceBlockingStub() {
        return TemplateSpecServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }

    @Bean
    public CpuCapacityServiceBlockingStub cpuCapacityServiceBlockingStub() {
        return CpuCapacityServiceGrpc.newBlockingStub(planClientConfig.planOrchestratorChannel());
    }

    @Bean
    public ReservedInstanceBoughtServiceBlockingStub reservedInstanceBoughtServiceBlockingStub() {
        return ReservedInstanceBoughtServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public ReservedInstanceSpecServiceBlockingStub reservedInstanceSpecServiceBlockingStub() {
        return ReservedInstanceSpecServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public CostServiceBlockingStub costServiceBlockingStub() {
        return CostServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public ReservedInstanceUtilizationCoverageServiceBlockingStub
            reservedInstanceUtilizationCoverageServiceBlockingStub() {
        return ReservedInstanceUtilizationCoverageServiceGrpc.newBlockingStub(costClientConfig.costChannel());
    }

    @Bean
    public Channel planOrchestratorChannel() {
        return planClientConfig.planOrchestratorChannel();
    }

    @Bean
    public Channel actionOrchestratorChannel() {
        return aoClientConfig.actionOrchestratorChannel();
    }

    @Bean
    public WidgetsetsServiceBlockingStub widgetsetsServiceBlockingStub() {
        return WidgetsetsServiceGrpc.newBlockingStub(authClientConfig.authClientChannel())
        // Intercept client call and add JWT token to the metadata
                .withInterceptors(jwtClientInterceptor());

    }

    @Bean
    public LicenseManagerServiceBlockingStub licenseManagerStub() {
        return LicenseManagerServiceGrpc.newBlockingStub(authClientConfig.authClientChannel())
                .withInterceptors(jwtClientInterceptor());
    }

    @Bean
    public LicenseCheckServiceBlockingStub licenseCheckServiceStub() {
        return LicenseCheckServiceGrpc.newBlockingStub(authClientConfig.authClientChannel());
    }

    @Bean
    public SupplyChainFetcherFactory supplyChainFetcher() {
        return new SupplyChainFetcherFactory(repositoryChannel(),
                actionOrchestratorChannel(),
                repositoryApi(),
                groupExpander(),
                realtimeTopologyContextId);
    }

    @Bean
    public GroupExpander groupExpander() {
        return new GroupExpander(groupRpcService());
    }

    @Bean
    public SeverityPopulator severityPopulator() {
        return new SeverityPopulator(entitySeverityService());
    }

    @Bean
    public ReportingNotificationReceiver reportingNotificationReceiver() {
        final ReportingNotificationReceiver receiver =
                reportingClientConfig.reportingNotificationReceiver();
        receiver.addListener(new ReportListener() {
            @Override
            public void onReportGenerated(long reportId) {
                final String reportIdStr = Long.toString(reportId);
                websocketConfig.websocketHandler()
                        .broadcastReportNotification(ReportNotification.newBuilder()
                                .setReportId(reportIdStr)
                                .setReportStatusNotification(ReportStatusNotification.newBuilder()
                                        .setDescription(reportIdStr)
                                        .setStatus(ReportStatus.GENERATED))
                                .build());
            }

            @Override
            public void onReportFailed(long reportId, @Nonnull String failureDescription) {
                // TODO add some specific websocket notifications
                final String reportIdStr = Long.toString(reportId);
                websocketConfig.websocketHandler()
                        .broadcastReportNotification(ReportNotification.newBuilder()
                                .setReportId(reportIdStr)
                                .setReportStatusNotification(ReportStatusNotification.newBuilder()
                                        .setDescription(reportIdStr)
                                        .setStatus(ReportStatus.GENERATED))
                                .build());
            }
        });
        return receiver;
    }

    @Bean
    public NotificationStore notificationStore() {
        // Setting the maximum notification size to 1000, since current notification center UI doesn't
        // support pagination, size of 50 is considered huge for a page, so size of 1000 should be safe
        // even when pagination is enabled.
        return new NotificationInMemoryStore(1000L, 1, TimeUnit.DAYS);
    }

    @Bean
    public ApiComponentTargetListener apiComponentTargetListener() {
        final ApiComponentTargetListener apiComponentTargetListener =
                new ApiComponentTargetListener(topologyService(), websocketConfig.websocketHandler());
        historyClientConfig.historyComponent().addStatsListener(apiComponentTargetListener);
        repositoryClientConfig.repository().addListener(apiComponentTargetListener);
        tpClientConfig.topologyProcessor(EnumSet.of(Subscription.Notifications))
                .addTargetListener(apiComponentTargetListener);
        return apiComponentTargetListener;
    }

    @Bean
    public ApiComponentNotificationListener apiComponentNotificationListener() {
        final ApiComponentNotificationListener listener =
                new ApiComponentNotificationListener(notificationStore());
        notificationClientConfig.systemNotificationListener().addNotificationListener(listener);
        return listener;
    }
}
