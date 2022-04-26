package com.vmturbo.api.component.external.api.service;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContext;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.PropertySource;

import com.vmturbo.api.component.communication.CommunicationConfig;
import com.vmturbo.api.component.communication.HeaderAuthenticationProvider;
import com.vmturbo.api.component.communication.SamlAuthenticationProvider;
import com.vmturbo.api.component.external.api.CustomRequestAwareAuthenticationSuccessHandler;
import com.vmturbo.api.component.external.api.health.ApiComponentRestartHelper;
import com.vmturbo.api.component.external.api.listener.HttpSessionListener;
import com.vmturbo.api.component.external.api.logging.GlobalExceptionHandler;
import com.vmturbo.api.component.external.api.mapper.CloudTypeMapper;
import com.vmturbo.api.component.external.api.mapper.CpuInfoMapper;
import com.vmturbo.api.component.external.api.mapper.MapperConfig;
import com.vmturbo.api.component.external.api.mapper.TargetDetailsMapper;
import com.vmturbo.api.component.external.api.service.util.HealthDataAggregator;
import com.vmturbo.api.component.external.api.service.util.SearchServiceFilterResolver;
import com.vmturbo.api.component.external.api.util.BusinessAccountRetriever;
import com.vmturbo.api.component.external.api.util.ReportingUserCalculator;
import com.vmturbo.api.component.external.api.util.action.ActionSearchUtil;
import com.vmturbo.api.component.external.api.util.action.ActionStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.cost.CostStatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.setting.EntitySettingQueryExecutor;
import com.vmturbo.api.component.external.api.util.stats.PaginatedStatsExecutor;
import com.vmturbo.api.component.external.api.util.stats.PlanEntityStatsFetcher;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryContextFactory;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryExecutor;
import com.vmturbo.api.component.external.api.util.stats.StatsQueryScopeExpander;
import com.vmturbo.api.component.external.api.util.stats.query.impl.CloudCommitmentStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.CloudCostsStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.CloudPlanNumEntitiesByTierSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.ClusterStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.EntitySavingsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.HistoricalCommodityStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.NumClustersStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.PlanCommodityStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.PlanRIStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.ProjectedCommodityStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.RIStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.ScopedUserCountStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.StorageStatsSubQuery;
import com.vmturbo.api.component.external.api.util.stats.query.impl.TemplateCostStatsSubQuery;
import com.vmturbo.api.component.external.api.websocket.ApiWebsocketConfig;
import com.vmturbo.api.component.security.HeaderAuthenticationCondition;
import com.vmturbo.api.component.security.IntersightIdTokenVerifier;
import com.vmturbo.api.component.security.OpenIdAuthenticationCondition;
import com.vmturbo.api.component.security.SamlAuthenticationCondition;
import com.vmturbo.api.enums.DeploymentMode;
import com.vmturbo.api.serviceinterfaces.IClassicMigrationService;
import com.vmturbo.api.serviceinterfaces.IProbesService;
import com.vmturbo.api.serviceinterfaces.IWorkflowsService;
import com.vmturbo.auth.api.AuthClientConfig;
import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.keyprovider.EncryptionKeyProvider;
import com.vmturbo.auth.api.authorization.keyprovider.KVKeyProvider;
import com.vmturbo.auth.api.authorization.keyprovider.KeyProvider;
import com.vmturbo.auth.api.authorization.keyprovider.MasterKeyReader;
import com.vmturbo.auth.api.authorization.keyprovider.PersistentVolumeKeyProvider;
import com.vmturbo.auth.api.authorization.kvstore.ComponentJwtStore;
import com.vmturbo.auth.api.licensing.LicenseCheckClientConfig;
import com.vmturbo.auth.api.securestorage.SecureStorageClient;
import com.vmturbo.common.api.crypto.CryptoFacility;
import com.vmturbo.common.api.utils.EnvironmentUtils;
import com.vmturbo.common.protobuf.search.SearchFilterResolver;
import com.vmturbo.components.common.BaseVmtComponentConfig;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory.DefaultEntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.components.common.pagination.EntityStatsPaginator.SortCommodityValueGetter;
import com.vmturbo.components.common.utils.BuildProperties;
import com.vmturbo.kvstore.KeyValueStoreConfig;
import com.vmturbo.kvstore.PublicKeyStoreConfig;
import com.vmturbo.repository.api.RepositoryClient;
import com.vmturbo.repository.api.impl.RepositoryClientConfig;
import com.vmturbo.search.SearchDBConfig;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;

/**
 * Spring Configuration that initializes all the services.
 *
 * <p>For readability purposes, the services should appear in alphabetical order.
 */
@Configuration
@SuppressFBWarnings
@Import({SpringSecurityConfig.class,
        MapperConfig.class,
        CommunicationConfig.class,
        RepositoryClientConfig.class,
        TopologyProcessorClientConfig.class,
        PublicKeyStoreConfig.class,
        LicenseCheckClientConfig.class,
        UserSessionConfig.class,
        KeyValueStoreConfig.class,
        SearchDBConfig.class
})
@PropertySource("classpath:api-component.properties")
public class ServiceConfig {

    @Value("${targetValidationTimeoutSeconds:120}")
    private Long targetValidationTimeoutSeconds;

    @Value("${targetValidationPollIntervalSeconds:2}")
    private Long targetValidationPollIntervalSeconds;

    @Value("${targetDiscoveryTimeoutSeconds:120}")
    private Long targetDiscoveryTimeoutSeconds;

    @Value("${targetDiscoveryPollIntervalSeconds:2}")
    private Long targetDiscoveryPollIntervalSeconds;

    @Value("${initialPlacementTimeoutSeconds:600}")
    private  Long initialPlacementTimeoutSeconds;

    @Value("${liveStatsRetrievalWindowSeconds:60}")
    private long liveStatsRetrievalWindowSeconds;

    @Value("${samlEnabled:false}")
    private boolean samlEnabled;

    @Value("${samlRegistrationId:simplesamlphp}")
    private String samlRegistrationId;

    @Value("${externalGroupTag:group}")
    private String externalGroupTag;

    @Value("${cpuInfoCacheLifetimeHours:8}")
    private int cpuCatalogLifeHours;

    @Value("${sessionTimeoutSeconds:1800}")
    private int sessionTimeoutSeconds;

    @Value("${componentType:api}")
    private String apiComponentType;

    @Value("${com.vmturbo.kvdir:/home/turbonomic/data/kv}")
    private String keyDir;

    /**
     * If true, use Kubernetes secrets to read in the sensitive Auth data (like encryption keys and
     * private/public key pairs). If false, this data will be read from (legacy) persistent volumes.
     */
    @Value("${enableExternalSecrets:false}")
    private boolean enableExternalSecrets;

    @Value("${identityGeneratorPrefix}")
    private long identityGeneratorPrefix;

    // 10 minutes default skew period.
    @Value("${clockSkewSecond:600}")
    private String clockSkewSecond;

    // permission tag
    @Value("${permissionTag:roles}")
    private String permissionTag;

    // maximum placement count.
    @Value("${maximumPlacementCount:100}")
    private int maximumPlacementCount;

    /**
     * Deployment configuration used to expose areas of the application front or backend.
     */
    @Value("${deploymentMode:SERVER}")
    private DeploymentMode deploymentMode;

    /**
     * Configuration used to enable/disable ui reporting section.
     */
    @Value("${enableReporting:false}")
    private boolean enableReporting;

    /**
     * Configuration used to enable/disable search api.
     */
    @Value("${enableSearchApi:false}")
    private boolean enableSearchApi;

    /**
     * The name to use for the REPORT_EDITOR user to log into grafana.
     * Note: Needs to be in sync with extractor component.
     */
    @Value("${grafanaEditorUsername:turbo-report-editor}")
    private String grafanaEditorUsername;

    @Value("${apiPaginationDefaultLimit:100}")
    private int apiPaginationDefaultLimit;

    @Value("${apiPaginationMaxLimit:500}")
    private int apiPaginationMaxLimit;

    @Value("${apiPaginationDefaultSortCommodity:priceIndex}")
    private String apiPaginationDefaultSortCommodity;

    // Default to 6 hours
    @Value("${failureHoursBeforeRestart:6}")
    private Long failureHoursBeforeRestart;

    /**
     * Allow target management in integration mode? False by default.
     */
    @Value("${allowTargetManagementInIntegrationMode:false}")
    private boolean allowTargetManagementInIntegrationMode;

    @Value("${enableEntitySavings:true}")
    private boolean enableEntitySavings;

    @Value("${realtimeTopologyContextId}")
    private Long realtimeTopologyContextId;

    @Value("${openIdExternalGroupTag:groups}")
    private String openIdExternalGroupTag;

    /**
     * We allow autowiring between different configuration objects, but not for a bean.
     */
    @Autowired
    private AuthClientConfig authConfig;

    @Autowired
    private SpringSecurityConfig securityConfig;

    @Autowired
    private CommunicationConfig communicationConfig;

    @Autowired
    private MapperConfig mapperConfig;

    @Autowired
    private ApiWebsocketConfig websocketConfig;

    @Autowired
    private ServletContext servletContext;

    @Autowired
    private PublicKeyStoreConfig publicKeyStoreConfig;

    @Autowired
    private LicenseCheckClientConfig licenseCheckClientConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private KeyValueStoreConfig keyValueStoreConfig;

    @Autowired
    private SearchDBConfig searchDBConfig;

    @Autowired
    private RepositoryClient repositoryClient;


    @Bean
    public ActionsService actionsService() {
        return ActionsService.newBuilder()
                .withActionOrchestratorRpc(communicationConfig.actionsRpcService())
                .withActionSpecMapper(mapperConfig.actionSpecMapper())
                .withRealtimeTopologyContextId(communicationConfig.getRealtimeTopologyContextId())
                .withActionStatsQueryExecutor(actionStatsQueryExecutor())
                .withUuidMapper(mapperConfig.uuidMapper())
                .withServiceProviderExpander(communicationConfig.serviceProviderExpander())
                .withActionSearchUtil(actionSearchUtil())
                .withMarketsService(marketsService())
                .withSupplyChainFetcherFactory(communicationConfig.supplyChainFetcher())
                .withRepositoryService(communicationConfig.repositoryRpcService())
                .withPolicyRpcService(communicationConfig.policyRpcService())
                .withPolicyMapper(mapperConfig.policyMapper())
                .withSettingPolicyServiceBlockingStub(communicationConfig.settingPolicyRpcService())
                .withSettingsMapper(mapperConfig.settingsMapper())
                .withGroupService(communicationConfig.groupRpcService())
                .withApiPaginationMaxLimit(apiPaginationMaxLimit)
                .build();
    }

    @Bean
    public AdminService adminService() {
        return new AdminService(clusterService(),
                        keyValueStoreConfig.keyValueStore(),
                        communicationConfig.clusterMgr(),
                        communicationConfig.serviceRestTemplate(),
                        websocketConfig.websocketHandler(),
                        BuildProperties.get(),
                        deploymentMode,
                        enableReporting,
                        settingsService(),
                        enableSearchApi,
                        healthDataAggregator());
    }

    @Bean
    public AuthenticationService authenticationService() {
        return new AuthenticationService(authConfig.getAuthHost(),
                authConfig.getAuthPort(),
                authConfig.getAuthRoute(),
                securityConfig.verifier(),
                communicationConfig.serviceRestTemplate(),
                targetStore(),
                sessionTimeoutSeconds);
    }

    @Bean
    public SecureStorageClient secureStorageClient() {
        return new SecureStorageClient(authConfig.getAuthHost(),
                authConfig.getAuthPort(),
                authConfig.getAuthRoute(),
                targetStore());
    }

    /**
     * Header security bean.
     *
     * @return header security bean.
     */
    @Bean
    @Conditional(HeaderAuthenticationCondition.class)
    public HeaderAuthenticationProvider headerAuthorizationProvider() {
        return new HeaderAuthenticationProvider(authConfig.getAuthHost(), authConfig.getAuthPort(),
                authConfig.getAuthRoute(), communicationConfig.serviceRestTemplate(),
                securityConfig.verifier(), targetStore(), new IntersightIdTokenVerifier(permissionTag),
                Integer.parseInt(clockSkewSecond));
    }

    /**
     * SAML authentication provider bean.
     *
     * @return SAML authentication provider bean.
     */
    @Bean
    @Conditional(SamlAuthenticationCondition.class)
    public SamlAuthenticationProvider samlAuthenticationProvider() {
        return new SamlAuthenticationProvider(authConfig.getAuthHost(), authConfig.getAuthPort(),
                authConfig.getAuthRoute(), communicationConfig.serviceRestTemplate(),
                securityConfig.verifier(), targetStore(), externalGroupTag);
    }

    @Bean
    public BusinessAccountRetriever businessAccountRetriever() {
        return new BusinessAccountRetriever(communicationConfig.repositoryApi(),
                communicationConfig.groupExpander(), communicationConfig.thinTargetCache(),
                mapperConfig.entityFilterMapper(), communicationConfig.businessAccountMapper(),
                searchFilterResolver());
    }

    /**
     * The {@link BusinessUnitsService} bean.
     *
     * @return The {@link BusinessUnitsService}.
     */
    @Bean
    public BusinessUnitsService businessUnitsService() {
        return new BusinessUnitsService(
            communicationConfig.costServiceBlockingStub(),
            mapperConfig.discountMapper(),
            communicationConfig.thinTargetCache(),
            communicationConfig.getRealtimeTopologyContextId(),
            mapperConfig.uuidMapper(),
            entitiesService(),
            communicationConfig.supplyChainFetcher(),
            communicationConfig.repositoryApi(),
            businessAccountRetriever(),
            cloudTypeMapper());
    }

    @Bean
    public CloudTypeMapper cloudTypeMapper() {
        return new CloudTypeMapper();
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
                communicationConfig.priceIndexPopulator(),
                statsService(),
                actionStatsQueryExecutor(),
                mapperConfig.uuidMapper(),
                communicationConfig.historyRpcService(),
                communicationConfig.settingPolicyRpcService(),
                mapperConfig.settingsMapper(),
                actionSearchUtil(),
                communicationConfig.repositoryApi(),
                entitySettingQueryExecutor(),
                communicationConfig.entityConstraintRpcService(),
                communicationConfig.policyRpcService(),
                communicationConfig.thinTargetCache(),
                mapperConfig.paginationMapper(),
                communicationConfig.serviceEntityMapper(),
                mapperConfig.settingManagerMappingLoader().getMapping(),
                communicationConfig.entityCustomTagsServiceBlockingStub(),
                costStatsQueryExecutor());
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
                communicationConfig.supplyChainFetcher(),
                actionSearchUtil(),
                communicationConfig.settingPolicyRpcService(),
                mapperConfig.settingsMapper(),
                communicationConfig.thinTargetCache(),
                entitySettingQueryExecutor(),
                mapperConfig.groupFilterMapper(),
                businessAccountRetriever(),
                communicationConfig.serviceProviderExpander(),
                mapperConfig.paginationMapper(),
                userSessionConfig.userSessionContext(),
                costStatsQueryExecutor());
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
                policiesService(),
                communicationConfig.policyRpcService(),
                communicationConfig.planRpcService(),
                communicationConfig.planProjectRpcService(),
                communicationConfig.scenarioRpcService(),
                mapperConfig.policyMapper(),
                mapperConfig.marketMapper(),
                mapperConfig.paginationMapper(),
                communicationConfig.groupRpcService(),
                communicationConfig.repositoryRpcService(),
                websocketConfig.websocketHandler(),
                actionStatsQueryExecutor(),
                communicationConfig.thinTargetCache(),
                statsService(),
                communicationConfig.repositoryApi(),
                communicationConfig.serviceEntityMapper(),
                communicationConfig.severityPopulator(),
                communicationConfig.priceIndexPopulator(),
                communicationConfig.actionsRpcService(),
                planEntityStatsFetcher(),
                communicationConfig.searchServiceBlockingStub(),
                actionSearchUtil(),
                entitySettingQueryExecutor(),
                licenseCheckClientConfig.licenseCheckClient(),
                mapperConfig.entityAspectMapper(),
                costStatsQueryExecutor(),
                userSessionContext(),
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
        return new ProbesService(communicationConfig.probeRpcService(), communicationConfig.topologyProcessor());
    }

    @Bean
    public ReservedInstancesService reservedInstancesService() {
        return new ReservedInstancesService(
            communicationConfig.reservedInstanceBoughtServiceBlockingStub(),
            communicationConfig.planReservedInstanceServiceBlockingStub(),
            communicationConfig.reservedInstanceSpecServiceBlockingStub(),
            communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
            mapperConfig.reservedInstanceMapper(),
            communicationConfig.repositoryApi(),
            communicationConfig.groupExpander(),
            statsQueryExecutor(),
            mapperConfig.uuidMapper(),
            communicationConfig.groupRpcService());
    }

    @Bean
    public ReportsService reportsService() {
        return new ReportsService();
    }

    @Bean
    public ReservationsService reservationsService() {
        return new ReservationsService(
                communicationConfig.reservationServiceBlockingStub(),
                mapperConfig.reservationMapper(),
                maximumPlacementCount,
                statsService(),
                communicationConfig.groupRpcService(),
                mapperConfig.uuidMapper());
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
        return new SchedulesService(communicationConfig.scheduleRpcService(),
            communicationConfig.settingPolicyRpcService(),
            mapperConfig.scheduleMapper(),
            mapperConfig.settingsMapper(),
                actionSearchUtil());
    }

    @Bean
    public PlanDestinationService planDestinationService() {
        return new PlanDestinationService(communicationConfig.planExportServiceBlockingRpcService(),
            mapperConfig.uuidMapper(),
            mapperConfig.planDestinationMapper());
    }

    @Bean
    public ScenariosService scenariosService() {
        return new ScenariosService(communicationConfig.scenarioRpcService(),
            mapperConfig.scenarioMapper());
    }

    @Bean
    public SearchFilterResolver searchFilterResolver() {
        return new SearchServiceFilterResolver(communicationConfig.targetsService(),
                communicationConfig.groupExpander());
    }

    @Bean
    public SearchQueryService searchQueryService() throws UnsupportedDialectException {
        return new SearchQueryService(this.searchDBConfig.apiQueryEngine(), userSessionContext());
    }

    /**
     * Search service fulfilling requests from search REST controller.
     *
     * @return search service.
     */
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
                mapperConfig.paginationMapper(),
                mapperConfig.groupUseCaseParser(),
                tagsService(),
                businessAccountRetriever(),
                communicationConfig.getRealtimeTopologyContextId(),
                userSessionContext(),
                communicationConfig.groupRpcService(),
                communicationConfig.serviceEntityMapper(),
                mapperConfig.entityFilterMapper(),
                mapperConfig.entityAspectMapper(),
                searchFilterResolver(),
                communicationConfig.priceIndexPopulator(),
                communicationConfig.thinTargetCache());
    }

    @Bean
    public SettingsService settingsService() {
        return new SettingsService(communicationConfig.settingRpcService(),
                communicationConfig.historyRpcService(),
                mapperConfig.settingsMapper(),
                mapperConfig.settingManagerMappingLoader().getMapping(),
                settingsPoliciesService(),
                communicationConfig.extractorSettingService());
    }

    @Bean
    public SettingsPoliciesService settingsPoliciesService() {
        return new SettingsPoliciesService(communicationConfig.settingRpcService(),
            mapperConfig.settingsMapper(), communicationConfig.settingPolicyRpcService());
    }

    @Bean
    public StatsService statsService() {
        final StatsService statsService = new StatsService(
            communicationConfig.historyRpcService(),
            communicationConfig.planRpcService(),
            mapperConfig.statsMapper(),
            communicationConfig.groupRpcService(),
            mapperConfig.magicScopeGateway(),
            userSessionContext(),
            mapperConfig.uuidMapper(),
            statsQueryExecutor(),
            planEntityStatsFetcher(),
            paginatedStatsExecutor(),
            Duration.ofSeconds(liveStatsRetrievalWindowSeconds),
            communicationConfig.groupExpander(),
            Clock.systemUTC());
        groupsService().setStatsService(statsService);
        return statsService;
    }

    @Bean
    public PaginatedStatsExecutor paginatedStatsExecutor() {
        return new PaginatedStatsExecutor(
                mapperConfig.statsMapper(),
                mapperConfig.uuidMapper(),
                Clock.systemUTC(),
                communicationConfig.repositoryApi(),
                communicationConfig.historyRpcService(),
                communicationConfig.supplyChainFetcher(),
                userSessionContext(),
                communicationConfig.groupExpander(),
                entityStatsPaginator(),
                paginationParamsFactory(),
                mapperConfig.paginationMapper(),
                communicationConfig.costServiceBlockingStub(),
                statsQueryExecutor()
                );
    }

    /**
     * A utility class to do in-memory pagination of entities given a way to access their
     * sort commodity (via a {@link SortCommodityValueGetter}.
     *
     * @return {link EntityStatsPaginator}
     */
    @Bean
    public EntityStatsPaginator entityStatsPaginator() {
        return new EntityStatsPaginator();
    }

    /**
     * Default factory of EntityStatsPaginationParamsFactory.
     *
     * @return {@link EntityStatsPaginationParamsFactory} for creating paginating entityStats
     */
    @Bean
    public EntityStatsPaginationParamsFactory paginationParamsFactory() {
        return new DefaultEntityStatsPaginationParamsFactory(
                apiPaginationDefaultLimit,
                apiPaginationMaxLimit,
                apiPaginationDefaultSortCommodity);

    }

    @Bean
    public SupplyChainsService supplyChainService() {
        return new SupplyChainsService(communicationConfig.supplyChainFetcher(),
            communicationConfig.planRpcService(),
            communicationConfig.getRealtimeTopologyContextId(),
            communicationConfig.groupExpander(),
            mapperConfig.entityAspectMapper(),
            licenseCheckClientConfig.licenseCheckClient(),
            Clock.systemUTC());
    }

    @Bean
    public TagsService tagsService() {
        return new TagsService(
                communicationConfig.searchServiceBlockingStub(),
                communicationConfig.repositoryApi(),
                communicationConfig.groupExpander(),
                mapperConfig.paginationMapper(),
                mapperConfig.tagsPaginationMapper());
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
                actionSearchUtil(),
                websocketConfig.websocketHandler(),
                communicationConfig.targetsService(),
                mapperConfig.paginationMapper(),
                targetDetailsMapper(),
                allowTargetManagement(),
                apiPaginationDefaultLimit);
    }

    // Target management is always allowed from REST API, unless in integration mode.
    // In integration mode, targets are managed from other sources, and thus REST API is NOT allowed.
    private boolean allowTargetManagement() {
        /*
         Integration mode requires Header authentication.
        */
        if (EnvironmentUtils.parseBooleanFromEnv(HeaderAuthenticationCondition.ENABLED)
                && !allowTargetManagementInIntegrationMode) {
            return false;
        }
        return true;
    }

    @Bean
    public TemplatesService templatesService() {
        return new TemplatesService(communicationConfig.templateServiceBlockingStub(),
            mapperConfig.templateMapper(), communicationConfig.templateSpecServiceBlockingStub(),
            communicationConfig.cpuCapacityServiceBlockingStub(), cpuInfoMapper(),
            mapperConfig.templatesUtils(),
            cpuCatalogLifeHours);
    }

    /**
     * Bean for {@link TopologyDataDefinitionService}.
     *
     * @return topology data definition service
     */
    @Bean
    public TopologyDataDefinitionService topologyDataDefinitionService() {
        return new TopologyDataDefinitionService(communicationConfig.topologyDataDefinitionServiceBlockingStub(),
                mapperConfig.topologyDataDefinitionMapper());
    }

    @Bean
    public CpuInfoMapper cpuInfoMapper() {
        return new CpuInfoMapper();
    }

    /**
     * User for calculating the reporting user.
     *
     * @return {@link ReportingUserCalculator} to use.
     */
    @Bean
    public ReportingUserCalculator reportingUserCalculator() {
        return new ReportingUserCalculator(enableReporting, grafanaEditorUsername,
                licenseCheckClientConfig.licenseCheckClient());
    }

    /**
     * {@link UsersService}.
     *
     * @return The {@link UsersService}.
     */
    @Bean
    public UsersService usersService() {
        return new UsersService(authConfig.getAuthHost(),
            authConfig.getAuthPort(),
            authConfig.getAuthRoute(),
            mapperConfig.uuidMapper(),
            communicationConfig.serviceRestTemplate(),
            samlRegistrationId,
            samlEnabled,
            groupsService(),
            widgetSetsService(),
            reportingUserCalculator(),
            licenseService());
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
                targetService(), mapperConfig.workflowMapper(), communicationConfig.settingPolicyRpcService(),
                secureStorageClient(), actionsService());
    }

    @Bean
    public IClassicMigrationService classicMigrationService() {
        return new ClassicMigrationService(targetService());
    }

    /**
     * If true, use Kubernetes secrets to read in a master encryption key which is used to encrypt
     * and decrypt the internal, component-specific encryption keys.
     * If false, this data will be read from (legacy) persistent volumes.
     *
     * <p>Note: This feature flag is exposed in a static way to avoid having to refactor the
     * many static methods that already exist in {@link CryptoFacility}. This is expected to be a
     * short-lived situation, until enabling external secrets becomes the default.</p>
     */
    @Value("${" + BaseVmtComponentConfig.ENABLE_EXTERNAL_SECRETS_FLAG + ":false}")
    public void setKeyProviderStatic(boolean enableExternalSecrets){
        CryptoFacility.enableExternalSecrets = enableExternalSecrets;
        if (enableExternalSecrets) {
            CryptoFacility.encryptionKeyProvider =
                    new EncryptionKeyProvider(keyValueStoreConfig.keyValueStore(), new MasterKeyReader());
        }
    }

    /**
     * Create a provider for private/public key pairs.
     *
     * @return the key provider
     */
    @Bean
    public KeyProvider keyProvider() {
        // Feature flag
        if (enableExternalSecrets) {
            return new KVKeyProvider(
                publicKeyStoreConfig.publicKeyStore(),
                keyValueStoreConfig.keyValueStore());
        }
        // Legacy behavior
        return new PersistentVolumeKeyProvider(publicKeyStoreConfig.publicKeyStore(), keyDir);
    }

    @Bean
    public ComponentJwtStore targetStore() {
        return new ComponentJwtStore(publicKeyStoreConfig.publicKeyStore(),
            identityGeneratorPrefix,
            keyProvider());
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
            communicationConfig.repositoryApi(),
            mapperConfig.buyRiScopeHandler(),
            communicationConfig.thinTargetCache());
    }

    @Bean
    public CloudCostsStatsSubQuery cloudCostsStatsSubQuery() {
        final CloudCostsStatsSubQuery cloudCostsStatsQuery =
            new CloudCostsStatsSubQuery(communicationConfig.repositoryApi(),
                communicationConfig.costServiceBlockingStub(),
                communicationConfig.supplyChainFetcher(),
                communicationConfig.thinTargetCache(),
                mapperConfig.buyRiScopeHandler(),
                storageStatsSubQuery(),
                userSessionContext());
        statsQueryExecutor().addSubquery(cloudCostsStatsQuery);
        return cloudCostsStatsQuery;
    }

    /**
     * Bean for {@link EntitySavingsSubQuery}.
     *
     * @return entity savings sub-query
     */
    @Bean
    public EntitySavingsSubQuery entitySavingsSubQuery() {
        final EntitySavingsSubQuery entitySavingsSubQuery =
                new EntitySavingsSubQuery(communicationConfig.costServiceBlockingStub(),
                        communicationConfig.groupExpander(), repositoryClient);
        if (enableEntitySavings) {
            statsQueryExecutor().addSubquery(entitySavingsSubQuery);
        }
        return entitySavingsSubQuery;
    }

    @Bean
    public StorageStatsSubQuery storageStatsSubQuery() {
        final StorageStatsSubQuery storageStatsSubQuery =
            new StorageStatsSubQuery(communicationConfig.repositoryApi(), userSessionContext());
        statsQueryExecutor().addSubquery(storageStatsSubQuery);
        return storageStatsSubQuery;
    }

    @Bean
    public CloudPlanNumEntitiesByTierSubQuery cloudPlanNumEntitiesByTierSubQuery() {
        final CloudPlanNumEntitiesByTierSubQuery cloudPlanNumEntitiesByTierQuery =
            new CloudPlanNumEntitiesByTierSubQuery(communicationConfig.repositoryApi(),
                communicationConfig.supplyChainFetcher(),
                    communicationConfig.actionsRpcService());
        statsQueryExecutor().addSubquery(cloudPlanNumEntitiesByTierQuery);
        return cloudPlanNumEntitiesByTierQuery;
    }

    @Bean
    public ClusterStatsSubQuery clusterStatsSubQuery() {
        final ClusterStatsSubQuery clusterStatsQuery =
            new ClusterStatsSubQuery(mapperConfig.statsMapper(), communicationConfig.historyRpcService());
        statsQueryExecutor().addSubquery(clusterStatsQuery);
        return clusterStatsQuery;
    }

    @Bean
    public HistoricalCommodityStatsSubQuery historicalCommodityStatsSubQuery() {
        final HistoricalCommodityStatsSubQuery historicalStatsQuery =
            new HistoricalCommodityStatsSubQuery(mapperConfig.statsMapper(),
                communicationConfig.historyRpcService(), userSessionContext(),
                communicationConfig.repositoryApi());
        statsQueryExecutor().addSubquery(historicalStatsQuery);
        return historicalStatsQuery;
    }

    @Bean
    public ProjectedCommodityStatsSubQuery projectedCommodityStatsSubQuery() {
        final ProjectedCommodityStatsSubQuery projectedStatsQuery =
            new ProjectedCommodityStatsSubQuery(Duration.ofSeconds(liveStatsRetrievalWindowSeconds),
                mapperConfig.statsMapper(), communicationConfig.historyRpcService());
        statsQueryExecutor().addSubquery(projectedStatsQuery);
        return projectedStatsQuery;
    }

    @Bean
    public RIStatsSubQuery riStatsSubQuery() {
        final RIStatsSubQuery riStatsQuery =
                new RIStatsSubQuery(
                        communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
                        communicationConfig.reservedInstanceBoughtServiceBlockingStub(),
                        communicationConfig.repositoryApi(),
                        communicationConfig.reservedInstanceCostServiceBlockingStub(),
                        mapperConfig.buyRiScopeHandler(),
                        userSessionContext());
        statsQueryExecutor().addSubquery(riStatsQuery);
        return riStatsQuery;
    }

    @Bean
    public CloudCommitmentStatsSubQuery cloudCommitmentStatsSubQuery() {
        final CloudCommitmentStatsSubQuery cloudCommitmentStatsSubQuery = new CloudCommitmentStatsSubQuery(communicationConfig.cloudCommitmentStatsServiceBlockingStub());
        statsQueryExecutor().addSubquery(cloudCommitmentStatsSubQuery);
        return cloudCommitmentStatsSubQuery;
    }

    /**
     * Plan RI stats sub-query.
     *
     * @return The {@link PlanRIStatsSubQuery}.
     */
    @Bean
    public PlanRIStatsSubQuery planRIStatsSubQuery() {
        final PlanRIStatsSubQuery planRIStatsQuery =
                new PlanRIStatsSubQuery(
                        communicationConfig.repositoryApi(),
                        communicationConfig.planReservedInstanceServiceBlockingStub(),
                        communicationConfig.reservedInstanceUtilizationCoverageServiceBlockingStub(),
                        mapperConfig.buyRiScopeHandler());
        statsQueryExecutor().addSubquery(planRIStatsQuery);
        return planRIStatsQuery;
    }

    @Bean
    public NumClustersStatsSubQuery numClustersStatsSubQuery() {
        final NumClustersStatsSubQuery numClustersStatsQuery =
            new NumClustersStatsSubQuery(communicationConfig.groupRpcService());
        statsQueryExecutor().addSubquery(numClustersStatsQuery);
        return numClustersStatsQuery;
    }

    @Bean
    public PlanCommodityStatsSubQuery planCommodityStatsSubQuery() {
        final PlanCommodityStatsSubQuery planCommodityStatsSubQuery =
            new PlanCommodityStatsSubQuery(mapperConfig.statsMapper(), communicationConfig.historyRpcService());
        statsQueryExecutor().addSubquery(planCommodityStatsSubQuery);
        return planCommodityStatsSubQuery;
    }

    @Bean
    public ScopedUserCountStatsSubQuery scopedUserCountStatsSubQuery() {
        final ScopedUserCountStatsSubQuery scopedUserCountStatsSubQuery =
            new ScopedUserCountStatsSubQuery(Duration.ofSeconds(liveStatsRetrievalWindowSeconds),
                userSessionContext(),
                communicationConfig.repositoryApi());
        statsQueryExecutor().addSubquery(scopedUserCountStatsSubQuery);
        return scopedUserCountStatsSubQuery;
    }

    /**
     * Template costs sub-query.
     *
     * @return the {@link TemplateCostStatsSubQuery}
     */
    @Bean
    public TemplateCostStatsSubQuery templateCostsStatsSubQuery() {
        final TemplateCostStatsSubQuery templateCostsStatsSubQuery =
            new TemplateCostStatsSubQuery(mapperConfig.templatesUtils());
        statsQueryExecutor().addSubquery(templateCostsStatsSubQuery);
        return templateCostsStatsSubQuery;
    }

    @Bean
    public StatsQueryExecutor statsQueryExecutor() {
        return new StatsQueryExecutor(statsQueryContextFactory(), scopeExpander(),
            communicationConfig.repositoryApi(), mapperConfig.uuidMapper(),
                licenseCheckClientConfig.licenseCheckClient());
    }

    @Bean
    public StatsQueryScopeExpander scopeExpander() {
        return new StatsQueryScopeExpander(communicationConfig.supplyChainFetcher(),
            userSessionContext());
    }

    @Bean
    public StatsQueryContextFactory statsQueryContextFactory() {
        return new StatsQueryContextFactory(Duration.ofSeconds(liveStatsRetrievalWindowSeconds),
            userSessionContext(), Clock.systemUTC(), communicationConfig.thinTargetCache());
    }

    /**
     * A utility class to retrieve plan stats.
     *
     * @return a utility class to retrieve plan stats
     */
    @Bean
    public PlanEntityStatsFetcher planEntityStatsFetcher() {
        return new PlanEntityStatsFetcher(mapperConfig.statsMapper(),
            communicationConfig.serviceEntityMapper(),
            communicationConfig.repositoryRpcService());
    }

    @Bean
    public ActionSearchUtil actionSearchUtil() {
        return new ActionSearchUtil(
            communicationConfig.actionsRpcService(),
            mapperConfig.actionSpecMapper(),
            mapperConfig.paginationMapper(),
            communicationConfig.supplyChainFetcher(),
            communicationConfig.groupExpander(),
            communicationConfig.serviceProviderExpander(),
            communicationConfig.getRealtimeTopologyContextId(),
            communicationConfig.useStableActionIdAsUuid());
    }

    @Bean
    public EntitySettingQueryExecutor entitySettingQueryExecutor() {
        return new EntitySettingQueryExecutor(communicationConfig.settingPolicyRpcService(),
            communicationConfig.settingRpcService(),
            communicationConfig.groupExpander(),
            mapperConfig.settingsMapper(),
            mapperConfig.settingManagerMappingLoader().getMapping(),
            userSessionContext());
    }

    /**
     * Returns the HttpSessionListener with access to the websocketHandler.
     *
     * @return an {@link HttpSessionListener} to capture session events.
     */
    @Bean
    public HttpSessionListener httpSessionListener() {
        return new HttpSessionListener(
            websocketConfig.websocketHandler()
        );
    }

    /**
     * OpenID authentication provider bean.
     *
     * @return OpenID authentication provider bean.
     */
    @Bean
    @Conditional(OpenIdAuthenticationCondition.class)
    public CustomRequestAwareAuthenticationSuccessHandler customRequestAwareAuthenticationSuccessHandler() {
        return new CustomRequestAwareAuthenticationSuccessHandler(authConfig.getAuthHost(), authConfig.getAuthPort(),
                authConfig.getAuthRoute(), communicationConfig.serviceRestTemplate(),
                securityConfig.verifier(), targetStore(), openIdExternalGroupTag);
    }

    /**
     * Provides {@link HealthDataAggregator} bean
     *
     * @return HealthDataAggregator bean
     */
    @Bean
    public HealthDataAggregator healthDataAggregator() {
        return new HealthDataAggregator(communicationConfig.targetsService(), communicationConfig.analysisStateService(), realtimeTopologyContextId);
    }

    @Bean
    public TargetDetailsMapper targetDetailsMapper() {
        return new TargetDetailsMapper();
    };

    @Bean
    public CostStatsQueryExecutor costStatsQueryExecutor() {
        return new CostStatsQueryExecutor(
                communicationConfig.costServiceBlockingStub(),
                mapperConfig.statsMapper());
    }

    /**
     * global exception handler.
     *
     * @return global exception handler object
     */
    @Bean
    public GlobalExceptionHandler exceptionHandler() {
        return new GlobalExceptionHandler();
    }

    /**
     * API component helper class to restart a component
     *
     * @return API GRPC health monitor object.
     */
    @Bean
    public ApiComponentRestartHelper apiGrpcHealthMonitor() {
        final ApiComponentRestartHelper apiComponentRestartHelper = new ApiComponentRestartHelper(exceptionHandler(),
                licenseService(), failureHoursBeforeRestart);
        int initialDelayMinutes = 1;
        apiComponentHealthManagerExecutor().scheduleAtFixedRate(
                apiComponentRestartHelper::execute, initialDelayMinutes, 10, TimeUnit.MINUTES);
        return apiComponentRestartHelper;
    }

    /**
     * Setup and return a ScheduledExecutorService for the running of recurrent tasks.
     *
     * @return a new single threaded scheduled executor service with the thread factory configured.
     */
    @Bean(destroyMethod = "shutdownNow")
    public ScheduledExecutorService apiComponentHealthManagerExecutor() {
        final ThreadFactory threadFactory = new ThreadFactoryBuilder().setNameFormat(
                "API-Component-Health-Manager").build();
        return Executors.newScheduledThreadPool(1, threadFactory);
    }
}
