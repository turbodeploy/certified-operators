package com.vmturbo.api.component.external.api.service;

import java.time.Clock;
import java.time.Duration;

import javax.servlet.Servlet;
import javax.servlet.ServletContext;
import javax.servlet.ServletRegistration;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;
import org.springframework.security.saml.userdetails.SAMLUserDetailsService;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.external.api.SAML.SAMLCondition;
import com.vmturbo.api.component.external.api.SAML.SAMLUserDetailsServiceImpl;
import com.vmturbo.api.component.external.api.mapper.CpuInfoMapper;
import com.vmturbo.api.component.external.api.mapper.MapperConfig;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.api.serviceinterfaces.ISAMLService;
import com.vmturbo.api.serviceinterfaces.IWorkflowsService;
import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.kvstore.ComponentJwtStore;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.auth.api.widgets.AuthClientConfig;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc;
import com.vmturbo.common.protobuf.plan.ScenarioServiceGrpc.ScenarioServiceBlockingStub;
import com.vmturbo.kvstore.PublicKeyStoreConfig;
import com.vmturbo.kvstore.SAMLConfigurationStoreConfig;
import com.vmturbo.reporting.api.ReportingClientConfig;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc;
import com.vmturbo.reporting.api.protobuf.ReportingServiceGrpc.ReportingServiceBlockingStub;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;


/**
 * Spring Configuration that initializes all the services.
 * <p>
 * For readability purposes, the services should appear in alphabetical order.
 */
@Configuration
@Import({SpringSecurityConfig.class,
        MapperConfig.class,
        CommunicationConfig.class,
        RepositoryClientConfig.class,
        ReportingClientConfig.class,
        PublicKeyStoreConfig.class,
        SAMLConfigurationStoreConfig.class,
        LicenseCheckClientConfig.class})
@PropertySource("classpath:api-component.properties")
public class ServiceConfig {

    @Autowired
    private AuthClientConfig authConfig;
    /**
     * A path prefix to access reports data (implemented in Legacy as CGI-script) using
     * {@link ReportCgiServlet}.
     */
    public static final String REPORT_CGI_PATH = "/cgi-bin/vmtreport.cgi";

    @Value("${targetValidationTimeoutSeconds}")
    private Long targetValidationTimeoutSeconds;

    @Value("${targetValidationPollIntervalSeconds}")
    private Long targetValidationPollIntervalSeconds;

    @Value("${targetDiscoveryTimeoutSeconds}")
    private Long targetDiscoveryTimeoutSeconds;

    @Value("${targetDiscoveryPollIntervalSeconds}")
    private Long targetDiscoveryPollIntervalSeconds;

    @Value("${initialPlacementTimeoutSeconds}")
    private  Long initialPlacementTimeoutSeconds;

    @Value("${supplyChainFetcherTimeoutSeconds}")
    private Long supplyChainFetcherTimeoutSeconds;

    @Value("${liveStatsRetrievalWindowSeconds}")
    private long liveStatsRetrievalWindowSeconds;

    @Value("${samlEnabled:false}")
    private boolean samlEnabled;

    @Value("${samlIdpMetadata:samlIdpMetadata}")
    private String samlIdpMetadata;

    @Value("${cpuInfoCacheLifetimeHours}")
    private int cpuCatalogLifeHours;

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

    @Autowired
    private ServletContext servletContext;

    @Autowired
    private PublicKeyStoreConfig publicKeyStoreConfig;

    @Autowired
    private SAMLConfigurationStoreConfig samlConfigurationStoreConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Bean
    public ActionsService actionsService() {
        return new ActionsService(communicationConfig.actionsRpcService(),
                                  mapperConfig.actionSpecMapper(),
                                  communicationConfig.repositoryApi(),
                                  communicationConfig.getRealtimeTopologyContextId(),
                                  communicationConfig.groupExpander());
    }

    @Bean
    public AdminService adminService() {
        return new AdminService(clusterService());
    }

    @Bean
    public AuthenticationService authenticationService() {
        return new AuthenticationService(authConfig.getAuthHost(), authConfig.getAuthPort(),
                securityConfig.verifier(), communicationConfig.serviceRestTemplate(), targetStore());
    }

    @Bean
    public ISAMLService samlService() {
        return new SAMLService(samlConfigurationStoreConfig.samlConfigurationStore());
    }

    @Bean
    @Conditional(SAMLCondition.class)
    public SAMLUserDetailsService samlUserDetailsService() {
        return new SAMLUserDetailsServiceImpl();
    }

    @Bean
    public BusinessUnitsService businessUnitsService() {
        return new BusinessUnitsService(
                repositoryClientConfig.repositoryClient(),
                communicationConfig.costServiceBlockingStub(),
                mapperConfig.businessUnitMapper(),
                searchService(),
                targetService()
        );
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
    public EntitiesService entitiesService() {
        return new EntitiesService(
                communicationConfig.actionsRpcService(),
                mapperConfig.actionSpecMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.getRealtimeTopologyContextId(),
                communicationConfig.supplyChainFetcher(),
                mapperConfig.paginationMapper(),
                communicationConfig.searchServiceBlockingStub(),
                mapperConfig.entityAspectMapper()
        );
    }

    @Bean
    public GroupsService groupsService() {
        return new GroupsService(
                communicationConfig.actionsRpcService(),
                communicationConfig.groupRpcService(),
                mapperConfig.actionSpecMapper(),
                mapperConfig.groupMapper(),
                mapperConfig.paginationMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.getRealtimeTopologyContextId(),
                mapperConfig.settingManagerMappingLoader().getMapping(),
                communicationConfig.templateServiceBlockingStub(),
                mapperConfig.entityAspectMapper(),
                communicationConfig.searchServiceBlockingStub());
    }

    @Bean
    public LicenseService licenseService() {
        return new LicenseService(authConfig.getAuthHost(),
                authConfig.getAuthPort(),
                communicationConfig.serviceRestTemplate(),
                communicationConfig.licenseManagerStub(),
                communicationConfig.licenseCheckServiceStub());
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
                policiesService(),
                communicationConfig.policyRpcService(),
                communicationConfig.planRpcService(),
                scenarioServiceClient(),
                mapperConfig.policyMapper(),
                mapperConfig.marketMapper(),
                mapperConfig.statsMapper(),
                mapperConfig.paginationMapper(),
                communicationConfig.groupRpcService(),
                communicationConfig.repositoryRpcService(),
                websocketConfig.websocketHandler(),
                communicationConfig.getRealtimeTopologyContextId());
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
        return new ReservedInstancesService(
                communicationConfig.reservedInstanceBoughtServiceBlockingStub(),
                communicationConfig.reservedInstanceSpecServiceBlockingStub(),
                communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
                mapperConfig.reservedInstanceMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.groupExpander());
    }

    @Bean
    public ReportsService reportsService() {
        return new ReportsService(reportingRpcService(), groupsService());
    }

    @Bean
    public ReportingServiceBlockingStub reportingRpcService() {
        return ReportingServiceGrpc.newBlockingStub(reportingClientConfig.reportingChannel());
    }

    @Bean
    public Servlet reportServlet() {
        final Servlet servlet = new ReportCgiServlet(reportingRpcService());
        final ServletRegistration.Dynamic registration =
                servletContext.addServlet("reports-cgi-servlet", servlet);
        registration.setLoadOnStartup(1);
        registration.addMapping(REPORT_CGI_PATH);
        return servlet;
    }

    @Bean
    public ReservationsService reservationsService() {
        return new ReservationsService(
                communicationConfig.reservationServiceBlockingStub(),
                mapperConfig.reservationMapper(),
                initialPlacementTimeoutSeconds,
                communicationConfig.planRpcService(),
                communicationConfig.planRpcServiceFuture(),
                communicationConfig.actionsRpcService(),
                communicationConfig.templateServiceBlockingStub());
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
    public ScenarioServiceBlockingStub scenarioServiceClient() {
        return ScenarioServiceGrpc.newBlockingStub(
                communicationConfig.planOrchestratorChannel());
    }

    @Bean
    public ScenariosService scenariosService() {
        return new ScenariosService(
                scenarioServiceClient(),
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
                communicationConfig.entitySeverityService(),
                communicationConfig.historyRpcService(),
                communicationConfig.groupExpander(),
                communicationConfig.supplyChainFetcher(),
                mapperConfig.groupMapper(),
                mapperConfig.paginationMapper(),
                mapperConfig.groupUseCaseParser(),
                mapperConfig.uuidMapper(),
                tagsService(),
                communicationConfig.getRealtimeTopologyContextId());
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
                communicationConfig.repositoryRpcService(),
                communicationConfig.searchServiceBlockingStub(),
                communicationConfig.supplyChainFetcher(),
                mapperConfig.statsMapper(),
                communicationConfig.groupExpander(),
                Clock.systemUTC(),
                targetService(),
                communicationConfig.groupRpcService(),
                Duration.ofSeconds(liveStatsRetrievalWindowSeconds),
                communicationConfig.costServiceBlockingStub(),
                searchService(),
                communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
                mapperConfig.reservedInstanceMapper(),
                magicScopeGateway());
    }

    @Bean
    public SupplyChainsService supplyChainService() {
        return new SupplyChainsService(communicationConfig.supplyChainFetcher(),
                communicationConfig.planRpcService(),
                communicationConfig.getRealtimeTopologyContextId(),
                communicationConfig.groupExpander());
    }

    @Bean
    public TagsService tagsService() {
        return new TagsService(
                communicationConfig.searchServiceBlockingStub(),
                communicationConfig.groupExpander());
    }

    @Bean
    public TargetsService targetService() {
        return new TargetsService(communicationConfig.topologyProcessor(),
                Duration.ofSeconds(targetValidationTimeoutSeconds),
                Duration.ofSeconds(targetValidationPollIntervalSeconds),
                Duration.ofSeconds(targetDiscoveryTimeoutSeconds),
                Duration.ofSeconds(targetDiscoveryPollIntervalSeconds),
                licenseCheckClientConfig.licenseCheckClient(),
                communicationConfig.apiComponentTargetListener());
    }

    @Bean
    public TemplatesService templatesService() {
        return new TemplatesService(communicationConfig.templateServiceBlockingStub(),
            mapperConfig.templateMapper(), communicationConfig.templateSpecServiceBlockingStub(),
            communicationConfig.cpuCapacityServiceBlockingStub(), cpuInfoMapper(), cpuCatalogLifeHours);
    }

    @Bean
    public CpuInfoMapper cpuInfoMapper() {
        return new CpuInfoMapper();
    }

    @Bean
    public UsersService usersService() {
        return new UsersService(authConfig.getAuthHost(),
                                authConfig.getAuthPort(),
                                communicationConfig.serviceRestTemplate(),
                                samlIdpMetadata,
                                samlEnabled);
    }

    @Bean
    public WidgetSetsService widgetSetsService() {
        return new WidgetSetsService(communicationConfig.widgetsetsServiceBlockingStub());
    }

    @Bean
    public IWorkflowsService workflowService() {
        return new WorkflowsService(communicationConfig.fetchWorkflowRpcService(),
                targetService(), mapperConfig.workflowMapper());
    }

    @Bean
    public ComponentJwtStore targetStore() {
        return new ComponentJwtStore(publicKeyStoreConfig.publicKeyStore());
    }

    @Bean
    public MagicScopeGateway magicScopeGateway() {
        final MagicScopeGateway gateway = new MagicScopeGateway(groupsService(),
            communicationConfig.groupRpcService(),
            communicationConfig.getRealtimeTopologyContextId());
        repositoryClientConfig.repository().addListener(gateway);
        return gateway;
    }
}
