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
import com.vmturbo.api.component.external.api.serviceinterfaces.IProbesService;
import com.vmturbo.api.component.external.api.util.MagicScopeGateway;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.api.serviceinterfaces.ISAMLService;
import com.vmturbo.api.serviceinterfaces.IWorkflowsService;
import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.kvstore.ComponentJwtStore;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.auth.api.widgets.AuthClientConfig;
import com.vmturbo.kvstore.KeyValueStoreConfig;
import com.vmturbo.kvstore.PublicKeyStoreConfig;
import com.vmturbo.kvstore.SAMLConfigurationStoreConfig;
import com.vmturbo.notification.api.impl.NotificationClientConfig;
import com.vmturbo.reporting.api.ReportingClientConfig;
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
        LicenseCheckClientConfig.class,
        NotificationClientConfig.class,
        UserSessionConfig.class,
        KeyValueStoreConfig.class})
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

    @Value("${liveStatsRetrievalWindowSeconds}")
    private long liveStatsRetrievalWindowSeconds;

    @Value("${samlEnabled:false}")
    private boolean samlEnabled;

    @Value("${samlIdpMetadata:samlIdpMetadata}")
    private String samlIdpMetadata;

    @Value("${cpuInfoCacheLifetimeHours}")
    private int cpuCatalogLifeHours;

    @Value("${sessionTimeoutSeconds}")
    private int sessionTimeoutSeconds;


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

    @Autowired
    private NotificationClientConfig notificationClientConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private KeyValueStoreConfig keyValueStoreConfig;

    @Bean
    public ActionsService actionsService() {
        return new ActionsService(communicationConfig.actionsRpcService(),
                                  mapperConfig.actionSpecMapper(),
                                  communicationConfig.repositoryApi(),
                                  communicationConfig.getRealtimeTopologyContextId(),
                                  actionStatsQueryExecutor(),
                                  mapperConfig.uuidMapper());
    }

    @Bean
    public AdminService adminService() {
        return new AdminService(clusterService(), keyValueStoreConfig.keyValueStore(),
            communicationConfig.clusterMgr(), communicationConfig.serviceRestTemplate());
    }

    @Bean
    public AuthenticationService authenticationService() {
        return new AuthenticationService(authConfig.getAuthHost(),
                authConfig.getAuthPort(),
                securityConfig.verifier(),
                communicationConfig.serviceRestTemplate(),
                targetStore(),
                sessionTimeoutSeconds);
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
                communicationConfig.getRealtimeTopologyContextId(),
                communicationConfig.supplyChainFetcher(),
                communicationConfig.groupRpcService(),
                mapperConfig.entityAspectMapper(),
                communicationConfig.severityPopulator(),
                statsService(),
                actionStatsQueryExecutor(),
                mapperConfig.uuidMapper(),
                communicationConfig.historyRpcService(),
                communicationConfig.settingPolicyRpcService(),
                communicationConfig.settingRpcService(),
                mapperConfig.settingsMapper(),
                actionSearchUtil(),
                communicationConfig.repositoryApi());
    }

    @Bean
    public GroupsService groupsService() {
        return new GroupsService(
                communicationConfig.actionsRpcService(),
                communicationConfig.groupRpcService(),
                mapperConfig.groupMapper(),
                communicationConfig.groupExpander(),
                mapperConfig.uuidMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.getRealtimeTopologyContextId(),
                mapperConfig.settingManagerMappingLoader().getMapping(),
                communicationConfig.templateServiceBlockingStub(),
                mapperConfig.entityAspectMapper(),
                actionStatsQueryExecutor(),
                communicationConfig.severityPopulator(),
                communicationConfig.supplyChainFetcher(),
                actionSearchUtil(),
                communicationConfig.settingPolicyRpcService(),
                mapperConfig.settingsMapper(),
                targetService());
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
                communicationConfig.scenarioRpcService(),
                mapperConfig.policyMapper(),
                mapperConfig.marketMapper(),
                mapperConfig.statsMapper(),
                mapperConfig.paginationMapper(),
                communicationConfig.groupRpcService(),
                communicationConfig.repositoryRpcService(),
                userSessionContext(),
                websocketConfig.websocketHandler(),
                actionStatsQueryExecutor(),
                communicationConfig.topologyProcessor(),
                communicationConfig.entitySeverityService(),
                communicationConfig.historyRpcService(),
                statsService(),
                communicationConfig.repositoryApi(),
                communicationConfig.serviceEntityMapper(),
                communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public NotificationService notificationService() {
        return new NotificationService(communicationConfig.notificationStore());
    }

    @Bean
    public PoliciesService policiesService() {
        return new PoliciesService(
                communicationConfig.policyRpcService(),
                communicationConfig.groupRpcService(), mapperConfig.policyMapper());
    }

    @Bean
    public IProbesService probeService() {
        return new ProbesService(communicationConfig.probeRpcService());
    }

    @Bean
    public ReservedInstancesService reservedInstancesService() {
        return new ReservedInstancesService(
                communicationConfig.reservedInstanceBoughtServiceBlockingStub(),
                communicationConfig.reservedInstanceSpecServiceBlockingStub(),
                communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
                mapperConfig.reservedInstanceMapper(),
                communicationConfig.repositoryApi(),
                communicationConfig.groupExpander(),
                communicationConfig.planRpcService(),
                communicationConfig.costServiceBlockingStub(),
                communicationConfig.supplyChainFetcher(),
                communicationConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public ReportsService reportsService() {
        return new ReportsService(communicationConfig.reportingRpcService(), groupsService());
    }

    @Bean
    public Servlet reportServlet() {
        final Servlet servlet = new ReportCgiServlet(communicationConfig.reportingRpcService());
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
    public SchedulesService schedulesService() {
        return new SchedulesService();
    }

    @Bean
    public PlanDestinationService planDestinationService() {
        return new PlanDestinationService();
    }

    @Bean
    public ScenariosService scenariosService() {
        return new ScenariosService(communicationConfig.scenarioRpcService(),
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
                communicationConfig.severityPopulator(),
                communicationConfig.historyRpcService(),
                communicationConfig.groupExpander(),
                communicationConfig.supplyChainFetcher(),
                mapperConfig.groupMapper(),
                mapperConfig.paginationMapper(),
                mapperConfig.groupUseCaseParser(),
                mapperConfig.uuidMapper(),
                tagsService(),
                repositoryClientConfig.repositoryClient(),
                mapperConfig.businessUnitMapper(),
                communicationConfig.getRealtimeTopologyContextId(),
                userSessionContext(),
                communicationConfig.groupRpcService(),
                communicationConfig.serviceEntityMapper());
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
        return new SettingsPoliciesService(communicationConfig.settingRpcService(),
            mapperConfig.settingsMapper(), communicationConfig.settingPolicyRpcService());
    }

    @Bean
    public StatsService statsService() {
        final StatsService statsService =
            new StatsService(
                communicationConfig.historyRpcService(),
                communicationConfig.planRpcService(),
                communicationConfig.repositoryApi(),
                communicationConfig.repositoryRpcService(),
                communicationConfig.supplyChainFetcher(),
                mapperConfig.statsMapper(),
                communicationConfig.groupExpander(),
                Clock.systemUTC(),
                targetService(),
                communicationConfig.groupRpcService(),
                Duration.ofSeconds(liveStatsRetrievalWindowSeconds),
                communicationConfig.costServiceBlockingStub(),
                magicScopeGateway(),
                userSessionContext(),
                reservedInstancesService(),
                communicationConfig.serviceEntityMapper(),
                communicationConfig.getRealtimeTopologyContextId());
        groupsService().setStatsService(statsService);
        //marketsService().setStatsService(statsService);
        return statsService;
    }

    @Bean
    public SupplyChainsService supplyChainService() {
        return new SupplyChainsService(communicationConfig.supplyChainFetcher(),
            communicationConfig.planRpcService(),
            mapperConfig.actionSpecMapper(),
            communicationConfig.actionsRpcService(),
            communicationConfig.getRealtimeTopologyContextId(),
            communicationConfig.groupExpander(),
            userSessionConfig.userSessionContext());
    }

    @Bean
    public TagsService tagsService() {
        return new TagsService(
                communicationConfig.searchServiceBlockingStub(),
                communicationConfig.repositoryApi(),
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
                communicationConfig.apiComponentTargetListener(),
                communicationConfig.repositoryApi(),
                mapperConfig.actionSpecMapper(),
                communicationConfig.actionsRpcService(),
                communicationConfig.getRealtimeTopologyContextId(),
                websocketConfig.websocketHandler());
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
                                samlEnabled,
                                groupsService(),
                                widgetSetsService());
    }

    @Bean
    public UserSessionContext userSessionContext() {
        return userSessionConfig.userSessionContext();
    }

    @Bean
    public WidgetSetsService widgetSetsService() {
        return new WidgetSetsService(communicationConfig.widgetsetsServiceBlockingStub(),
            mapperConfig.widgetsetMapper());
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

    @Bean
    public ActionStatsQueryExecutor actionStatsQueryExecutor() {
        return new ActionStatsQueryExecutor(Clock.systemUTC(),
            communicationConfig.actionsRpcService(),
            mapperConfig.actionSpecMapper(),
            mapperConfig.uuidMapper(),
            communicationConfig.groupExpander(),
            communicationConfig.supplyChainFetcher(),
            userSessionContext(),
            communicationConfig.repositoryApi());
    }

    @Bean
    public ActionSearchUtil actionSearchUtil() {
        return new ActionSearchUtil(
            communicationConfig.actionsRpcService(),
            mapperConfig.actionSpecMapper(),
            mapperConfig.paginationMapper(),
            communicationConfig.supplyChainFetcher(),
            communicationConfig.getRealtimeTopologyContextId());
    }
}
