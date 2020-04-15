package com.vmturbo.repository;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.SortedMap;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import javaslang.circuitbreaker.CircuitBreakerConfig;
import javaslang.circuitbreaker.CircuitBreakerRegistry;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTOREST.RepositoryServiceController;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoREST.SupplyChainServiceController;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.common.protobuf.search.SearchREST.SearchServiceController;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceImplBase;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.migration.Migration;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory.DefaultEntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.repository.controller.RepositorySecurityConfig;
import com.vmturbo.repository.diagnostics.RepositoryDiagnosticsConfig;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.listener.MarketTopologyListener;
import com.vmturbo.repository.listener.TopologyEntitiesListener;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.migration.RepositoryMigrationsLibrary;
import com.vmturbo.repository.search.SearchHandler;
import com.vmturbo.repository.service.ArangoRepositoryRpcService;
import com.vmturbo.repository.service.ArangoSupplyChainRpcService;
import com.vmturbo.repository.service.LiveTopologyPaginator;
import com.vmturbo.repository.service.PartialEntityConverter;
import com.vmturbo.repository.service.PlanStatsService;
import com.vmturbo.repository.service.SupplyChainStatistician;
import com.vmturbo.repository.service.TopologyGraphRepositoryRpcService;
import com.vmturbo.repository.service.TopologyGraphSearchRpcService;
import com.vmturbo.repository.service.TopologyGraphSupplyChainRpcService;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorSubscription;

@Configuration("theComponent")
@Import({
    RepositoryApiConfig.class,
    TopologyProcessorClientConfig.class,
    ActionOrchestratorClientConfig.class,
    GroupClientConfig.class,
    MarketClientConfig.class,
    PlanOrchestratorClientConfig.class,
    RepositorySecurityConfig.class,
    RepositoryProperties.class,
    SpringSecurityConfig.class,
    UserSessionConfig.class,
        RepositoryProperties.class,
        RepositoryComponentConfig.class,
        RepositoryDiagnosticsConfig.class
})
public class RepositoryComponent extends BaseVmtComponent {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryComponent.class);

    @Autowired
    private RepositoryApiConfig apiConfig;

    @Autowired
    private RepositoryComponentConfig repositoryComponentConfig;

    @Autowired
    private ActionOrchestratorClientConfig actionOrchestratorClientConfig;

    @Autowired
    private GroupClientConfig groupClientConfig;

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Autowired
    private MarketClientConfig marketClientConfig;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private SpringSecurityConfig securityConfig;

    @Autowired
    private RepositoryProperties repositoryProperties;

    @Autowired
    private RepositoryDiagnosticsConfig repositoryDiagnosticsConfig;

    @Value("${repositoryEntityStatsPaginationDefaultLimit}")
    private int repositoryEntityStatsPaginationDefaultLimit;

    @Value("${repositoryEntityStatsPaginationMaxLimit}")
    private int repositoryEntityStatsPaginationMaxLimit;

    @Value("${repositoryEntityStatsPaginationDefaultSortCommodity}")
    private String repositoryEntityStatsPaginationDefaultSortCommodity;

    @Value("${repositorySearchPaginationDefaultLimit}")
    private int repositorySearchPaginationDefaultLimit;

    @Value("${repositorySearchPaginationMaxLimit}")
    private int repositorySearchPaginationMaxLimit;

    @Value("${repositoryMaxEntitiesPerChunk:5}")
    private int maxEntitiesPerChunk;

    @PostConstruct
    private void setup() {
        getHealthMonitor().addHealthCheck(apiConfig.kafkaHealthMonitor());
        // Temporarily force all Repository migrations to retry, in order to address some
        // observed issues with V_01_00_00__PURGE_ALL_LEGACY_PLANS not running successfully in
        // previous versions.
        setForceRetryMigrations(true);
    }

    @Bean
    public EntityStatsPaginator entityStatsPaginator() {
        return new EntityStatsPaginator();
    }

    @Bean
    public EntityStatsPaginationParamsFactory paginationParamsFactory() {
        return new DefaultEntityStatsPaginationParamsFactory(
                repositoryEntityStatsPaginationDefaultLimit,
                repositoryEntityStatsPaginationMaxLimit,
                repositoryEntityStatsPaginationDefaultSortCommodity);
    }

    @Bean
    public PartialEntityConverter partialEntityConverter() {
        return new PartialEntityConverter();
    }

    /**
     * Create a service for retrieving plan entity stats.
     *
     * @return a service for retrieving plan entity stats.
     */
    @Bean
    public PlanStatsService planStatsService() {
        return new PlanStatsService(paginationParamsFactory(),
            entityStatsPaginator(),
            partialEntityConverter(),
            maxEntitiesPerChunk);
    }

    @Bean
    public RepositoryServiceImplBase repositoryRpcService() {
        final ArangoRepositoryRpcService arangoRpcService =
                new ArangoRepositoryRpcService(repositoryComponentConfig.topologyManager(),
                        repositoryComponentConfig.topologyProtobufsManager(),
                        repositoryComponentConfig.graphDBService(),
            planStatsService(),
            partialEntityConverter(),
            maxEntitiesPerChunk);

        // Return a topology-graph backed rpc service, which will fall back to arango for
        // non-realtime queries.
        return new TopologyGraphRepositoryRpcService(repositoryComponentConfig.liveTopologyStore(),
            arangoRpcService,
            partialEntityConverter(),
                repositoryComponentConfig.getRealtimeTopologyContextId(),
            maxEntitiesPerChunk,
            userSessionConfig.userSessionContext());
    }

    @Bean
    public RepositoryServiceController repositoryServiceController() throws GraphDatabaseException,
            InterruptedException, URISyntaxException, CommunicationException{
        return new RepositoryServiceController(repositoryRpcService());
    }

    @Bean
    public LiveTopologyPaginator liveTopologyPaginator() {
        return new LiveTopologyPaginator(repositorySearchPaginationDefaultLimit,
            repositorySearchPaginationMaxLimit);
    }

    @Bean
    public SearchServiceImplBase searchRpcService() {
        return new TopologyGraphSearchRpcService(repositoryComponentConfig.liveTopologyStore(),
            searchResolver(), liveTopologyPaginator(),
            partialEntityConverter(),
            userSessionConfig.userSessionContext(),
            maxEntitiesPerChunk);
    }

    @Bean
    public SearchResolver<RepoGraphEntity> searchResolver() {
        return new SearchResolver<>(new TopologyFilterFactory<RepoGraphEntity>());
    }

    @Bean
    public SupplyChainCalculator supplyChainCalculator() {
        return new SupplyChainCalculator();
    }

    @Bean
    public SupplyChainServiceImplBase supplyChainRpcService()
            throws InterruptedException, CommunicationException, URISyntaxException {
        // We always create the arango one, because we always use it for plans.
        final ArangoSupplyChainRpcService arangoService = new ArangoSupplyChainRpcService(
                repositoryComponentConfig.graphDBService(),
                repositoryComponentConfig.supplyChainService(),
            userSessionConfig.userSessionContext(),
                repositoryComponentConfig.getRealtimeTopologyContextId());
       return new TopologyGraphSupplyChainRpcService(userSessionConfig.userSessionContext(),
               repositoryComponentConfig.liveTopologyStore(),
                arangoService,
                supplyChainStatistician(),
                supplyChainCalculator(),
               repositoryComponentConfig.getRealtimeTopologyContextId());
    }

    @Bean
    public SupplyChainStatistician supplyChainStatistician() {
        return new SupplyChainStatistician(
            EntitySeverityServiceGrpc.newBlockingStub(
                actionOrchestratorClientConfig.actionOrchestratorChannel()),
            ActionsServiceGrpc.newBlockingStub(
                actionOrchestratorClientConfig.actionOrchestratorChannel()),
            GroupServiceGrpc.newBlockingStub(groupClientConfig.groupChannel()));
    }



    @Bean
    public SupplyChainServiceController supplyChainServiceController()
            throws InterruptedException, CommunicationException, URISyntaxException {
        return new SupplyChainServiceController(supplyChainRpcService());
    }

    @Bean
    public SearchHandler searchHandler() {
        return new SearchHandler(repositoryComponentConfig.graphDefinition(),
                repositoryComponentConfig.arangoDatabaseFactory(),
                repositoryComponentConfig.arangoDBExecutor());
    }

    // The controller generated with gRPC service
    @Bean
    public SearchServiceController searchServiceController() throws InterruptedException, URISyntaxException, CommunicationException {
        return new SearchServiceController(searchRpcService());
    }



    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        return new TopologyEntitiesListener(repositoryComponentConfig.topologyManager(),
                                            apiConfig.repositoryNotificationSender());
    }

    @Bean
    public MarketTopologyListener marketTopologyListener() {
        return new MarketTopologyListener(
                apiConfig.repositoryNotificationSender(),
                repositoryComponentConfig.topologyManager());
    }

    @Bean
    public TopologyProcessor topologyProcessor() {
        final TopologyProcessor topologyProcessor;
        // If using the in-memory graph, we want to read from the beginning on restart
        // so that we can populate the graph without waiting for the next broadcast.
        topologyProcessor = tpClientConfig.topologyProcessor(
            TopologyProcessorSubscription.forTopicWithStartFrom(
                TopologyProcessorSubscription.Topic.LiveTopologies, StartFrom.BEGINNING),
            TopologyProcessorSubscription.forTopicWithStartFrom(
                TopologyProcessorSubscription.Topic.TopologySummaries, StartFrom.BEGINNING));
        topologyProcessor.addLiveTopologyListener(topologyEntitiesListener());
        topologyProcessor.addTopologySummaryListener(topologyEntitiesListener());
        return topologyProcessor;
    }

    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent(
            // Read the most recent projected topologies instead of reading from the beginning
            // on restart to avoid writing stale plan data to ArangoDB.
            MarketSubscription.forTopic(MarketSubscription.Topic.ProjectedTopologies),
            MarketSubscription.forTopicWithStartFrom(
                MarketSubscription.Topic.AnalysisSummary, StartFrom.BEGINNING),
            // Plan analysis (source) topologies are always persisted, so there is no need to
            // read this topic from the beginning.
            MarketSubscription.forTopic(MarketSubscription.Topic.PlanAnalysisTopologies));
        market.addProjectedTopologyListener(marketTopologyListener());
        market.addAnalysisSummaryListener(marketTopologyListener());
        market.addPlanAnalysisTopologyListener(marketTopologyListener());
        return market;
    }

    @Bean
    public CircuitBreakerRegistry circuitBreakerRegistry() {
        final CircuitBreakerConfig circuitBreakerConfig = CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .ringBufferSizeInClosedState(3)
                .ringBufferSizeInHalfOpenState(3)
                .build();

        return CircuitBreakerRegistry.of(circuitBreakerConfig);
    }

    /**
     * Manages all the migrations in the Repository.
     *
     * @return an instance of the RepositoryMigrationsLibrary
     */
    @Bean
    public RepositoryMigrationsLibrary repositoryMigrationsLibrary() {
        return new RepositoryMigrationsLibrary(repositoryComponentConfig.arangoDatabaseFactory());
    }

    @Nonnull
    @Override
    public List<BindableService> getGrpcServices() {
        try {
            return Arrays.asList(repositoryRpcService(),
                searchRpcService(),
                supplyChainRpcService());
        } catch (InterruptedException | CommunicationException | URISyntaxException e) {
            logger.error("Failed to start gRPC services due to exception.", e);
            return Collections.emptyList();
        }
    }

    @Nonnull
    @Override
    public List<ServerInterceptor> getServerInterceptors() {
        final JwtServerInterceptor jwtInterceptor = new JwtServerInterceptor(securityConfig.apiAuthKVStore());
        return Collections.singletonList(jwtInterceptor);
    }

    @Nonnull
    @Override
    protected SortedMap<String, Migration> getMigrations() {
        return repositoryMigrationsLibrary().getMigrations();
    }

    /**
     * Starts the component.
     *
     * @param args The mandatory arguments.
     */
    public static void main(String[] args) {
        startContext(RepositoryComponent.class);
    }

    @Override
    protected void onDumpDiags(@Nonnull final ZipOutputStream diagnosticZip) {
        repositoryDiagnosticsConfig.repositoryDiagnosticsHandler().dump(diagnosticZip);
    }
}
