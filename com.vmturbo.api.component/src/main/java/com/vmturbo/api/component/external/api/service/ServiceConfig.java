package com.vmturbo.api.component.external.api.service;

import java.time.Clock;
import java.time.Duration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.mapper.MapperConfig;
import com.vmturbo.api.component.external.api.util.GroupExpander;
import com.vmturbo.api.component.external.api.util.SupplyChainFetcher;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.auth.api.SpringSecurityConfig;

/**
 * Spring Configuration that initializes all the services.
 * <p>
 * For readability purposes, the services should appear in alphabetical order.
 */
@Configuration
@Import({SpringSecurityConfig.class, MapperConfig.class,
        CommunicationConfig.class, ApiWebsocketConfig.class})
public class ServiceConfig {

    @Value("${targetValidationTimeoutSeconds}")
    private Long targetValidationTimeoutSeconds;

    @Value("${targetValidationPollIntervalSeconds}")
    private Long targetValidationPollIntervalSeconds;

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
    private MapperConfig mapperConfig;

    @Autowired
    private ApiWebsocketConfig websocketConfig;

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
                communicationConfig.getRealtimeTopologyContextId()
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
                communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public LicenseService licenseService() {
        return new LicenseService();
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
    public ReportsService reportsService() {
        return new ReportsService();
    }

    @Bean
    public ReservationsService reservationsService() {
        return new ReservationsService();
    }

    @Bean
    public RolesService rolesService() {
        return new RolesService();
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
                groupExpander(),
                supplyChainFetcher(),
                mapperConfig.groupMapper(),
                mapperConfig.groupUseCaseParser(),
                mapperConfig.uuidMapper());
    }

    @Bean
    public SettingsService settingsService() {
        return new SettingsService(communicationConfig.settingRpcService(),
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
                communicationConfig.repositoryApi(),
                groupExpander(),
                Clock.systemUTC(),
                targetService());
    }

    @Bean
    public SupplyChainsService supplyChainService() {
        return new SupplyChainsService(supplyChainFetcher(),
                communicationConfig.getRealtimeTopologyContextId(),
                groupExpander());
    }

    @Bean
    public SupplyChainFetcher supplyChainFetcher() {
        return new SupplyChainFetcher(communicationConfig.repositoryChannel(),
                communicationConfig.actionOrchestratorChannel(),
                communicationConfig.repositoryApi(),
                groupExpander(),
                Duration.ofSeconds(supplyChainFetcherTimeoutSeconds));
    }

    @Bean
    public GroupExpander groupExpander() {
        return new GroupExpander(communicationConfig.groupRpcService());
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
