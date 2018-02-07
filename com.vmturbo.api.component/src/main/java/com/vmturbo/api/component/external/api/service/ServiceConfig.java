package com.vmturbo.api.component.external.api.service;

import java.time.Clock;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.mapper.MapperConfig;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.reporting.api.ReportingClientConfig;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;


/**
 * Spring Configuration that initializes all the services.
 * <p>
 * For readability purposes, the services should appear in alphabetical order.
 */
@Configuration
@Import({SpringSecurityConfig.class, MapperConfig.class, CommunicationConfig.class,
        ApiWebsocketConfig.class, RepositoryClientConfig.class, ReportingClientConfig.class})
public class ServiceConfig {

    @Value("${targetValidationTimeoutSeconds}")
    private Long targetValidationTimeoutSeconds;

    @Value("${targetValidationPollIntervalSeconds}")
    private Long targetValidationPollIntervalSeconds;
    @Value("${initialPlacementTimeoutSeconds}")
    private  Long initialPlacementTimeoutSeconds;

    @Value("${supplyChainFetcherTimeoutSeconds}")
    private Long supplyChainFetcherTimeoutSeconds;

    /**
     * We allow autowiring between different configuration objects, but not for a bean.
     */
    @Autowired
    private SpringSecurityConfig securityConfig;

    @Autowired
    private CommunicationConfig communicationConfig;

    @Autowired
    private RepositoryClientConfig repositoryClientConfig;

    @Autowired
    private MapperConfig mapperConfig;

    @Autowired
    private ApiWebsocketConfig websocketConfig;

    @Autowired
    private ReportingClientConfig reportingClientConfig;

    @Bean
    public ActionsService actionsService() {
        return new ActionsService(communicationConfig.actionsRpcService(),
                                  mapperConfig.actionSpecMapper(),
                                  communicationConfig.repositoryApi(),
                                  communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public AdminService adminService() {
        return new AdminService();
    }

    @Bean
    public AuthenticationService authenticationService() {
        return new AuthenticationService(communicationConfig.getAuthHost(),
                                         communicationConfig.getHttpPort(),
                                         securityConfig.verifier(),
                                         communicationConfig.serviceRestTemplate());
    }

    @Bean
    public BusinessUnitsService businessUnitsService() {
        return new BusinessUnitsService();
    }

    @Bean
    public ClusterService clusterService() {
        return new ClusterService(communicationConfig.clusterMgr());
    }

    @Bean
    public DeploymentProfilesService deploymentProfilesService() {
        return new DeploymentProfilesService();
    }

    @Bean
    public EntitiesService entitiesService() throws Exception {
        return new EntitiesService(
                communicationConfig.actionsRpcService(),
                mapperConfig.actionSpecMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.getRealtimeTopologyContextId(),
                communicationConfig.supplyChainFetcher()
        );
    }

    @Bean
    public GroupsService groupsService() {
        return new GroupsService(
                communicationConfig.actionsRpcService(),
                communicationConfig.groupRpcService(),
                mapperConfig.actionSpecMapper(),
                mapperConfig.groupMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.getRealtimeTopologyContextId(),
                mapperConfig.settingManagerMappingLoader().getMapping(),
                communicationConfig.templateServiceBlockingStub());
    }

    @Bean
    public LicenseService licenseService() {
        return new LicenseService(communicationConfig.getAuthHost(),
                communicationConfig.getHttpPort(),
                communicationConfig.serviceRestTemplate());
    }

    @Bean
    public LogsService logsService() {
        return new LogsService();
    }

    @Bean
    public MarketsService marketsService() {
        return new MarketsService(mapperConfig.actionSpecMapper(),
              mapperConfig.uuidMapper(),
              communicationConfig.actionsRpcService(),
              communicationConfig.policyRpcService(),
              communicationConfig.planRpcService(),
              mapperConfig.policyMapper(),
              mapperConfig.marketMapper(),
              communicationConfig.groupRpcService(),
              websocketConfig.websocketHandler());
    }

    @Bean
    public NotificationService notificationService() {
        return new NotificationService();
    }

    @Bean
    public PoliciesService policiesService() {
        return new PoliciesService(
                communicationConfig.policyRpcService(),
                communicationConfig.groupRpcService(), mapperConfig.policyMapper());
    }

    @Bean
    public ReservedInstancesService reservedInstancesService() {
        return new ReservedInstancesService();
    }

    @Bean
    public ReportsService reportsService() {
        return new ReportsService(
                ReportingServiceGrpc.newBlockingStub(reportingClientConfig.reportingChannel()),
                        groupsService());
    }

    @Bean
    public ReportCgiServlet reportServlet() {
        return new ReportCgiServlet(
                ReportingServiceGrpc.newBlockingStub(reportingClientConfig.reportingChannel()));
    }

    @Bean
    public ServletRegistrationBean servletRegistrationBean() {
        return new ServletRegistrationBean(reportServlet(), "/cgi-bin/vmtreport.cgi");
    }

    @Bean
    public ReservationsService reservationsService() {
        return new ReservationsService(
                communicationConfig.reservationServiceBlockingStub(),
                mapperConfig.reservationMapper(),
                initialPlacementTimeoutSeconds,
                communicationConfig.planRpcService(),
                communicationConfig.planRpcServiceFuture(),
                communicationConfig.actionsRpcService());
    }

    @Bean
    public RolesService rolesService() {
        return new RolesService();
    }

    @Bean
    public NotificationSettingsService notificationSettingsService() {
        return new NotificationSettingsService();
    }

    @Bean
    public ScenariosService scenariosService() {
        return new ScenariosService(communicationConfig.planOrchestratorChannel(),
                mapperConfig.scenarioMapper());
    }

    @Bean
    public SearchService searchService() {
        return new SearchService(
                communicationConfig.repositoryApi(),
                marketsService(),
                groupsService(),
                targetService(),
                communicationConfig.searchServiceBlockingStub(),
                communicationConfig.groupExpander(),
                communicationConfig.supplyChainFetcher(),
                mapperConfig.groupMapper(),
                mapperConfig.groupUseCaseParser(),
                mapperConfig.uuidMapper());
    }

    @Bean
    public SettingsService settingsService() {
        return new SettingsService(communicationConfig.settingRpcService(),
                communicationConfig.historyRpcService(),
                mapperConfig.settingsMapper(),
                mapperConfig.settingManagerMappingLoader().getMapping());
    }

    @Bean
    public SettingsPoliciesService settingsPoliciesService() {
        return new SettingsPoliciesService(mapperConfig.settingsMapper(),
                communicationConfig.groupChannel());
    }

    @Bean
    public StatsService statsService() {
        return new StatsService(communicationConfig.historyRpcService(),
                communicationConfig.planRpcService(),
                communicationConfig.repositoryApi(),
                repositoryClientConfig.repositoryClient(),
                communicationConfig.groupExpander(),
                Clock.systemUTC(),
                targetService(),
                communicationConfig.groupRpcService());
    }

    @Bean
    public SupplyChainsService supplyChainService() {
        return new SupplyChainsService(communicationConfig.supplyChainFetcher(),
                communicationConfig.planRpcService(),
                communicationConfig.getRealtimeTopologyContextId(),
                communicationConfig.groupExpander());
    }

    @Bean
    public TargetsService targetService() {
        return new TargetsService(communicationConfig.topologyProcessor(),
                Duration.ofSeconds(targetValidationTimeoutSeconds),
                Duration.ofSeconds(targetValidationPollIntervalSeconds));
    }

    @Bean
    public TemplatesService templatesService() {
        return new TemplatesService(communicationConfig.templateServiceBlockingStub(),
                                    mapperConfig.templateMapper(),
                                    communicationConfig.templateSpecServiceBlockingStub());
    }

    @Bean
    public UsersService usersService() {
        return new UsersService(communicationConfig.getAuthHost(),
                                communicationConfig.getHttpPort(),
                                communicationConfig.serviceRestTemplate());
    }

    @Bean
    public WidgetSetsService widgetSetsService() {
        return new WidgetSetsService();
    }
}
