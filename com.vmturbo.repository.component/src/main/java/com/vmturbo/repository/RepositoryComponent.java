package com.vmturbo.repository;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import com.arangodb.ArangoDB;
import com.arangodb.velocypack.VPackDeserializer;
import com.arangodb.velocypack.VPackSerializer;
import com.arangodb.velocypack.ValueType;
import com.arangodb.velocypack.exception.VPackBuilderException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.BindableService;
import io.grpc.ServerInterceptor;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.web.client.RestTemplate;

import javaslang.circuitbreaker.CircuitBreakerConfig;
import javaslang.circuitbreaker.CircuitBreakerRegistry;

import com.vmturbo.action.orchestrator.api.impl.ActionOrchestratorClientConfig;
import com.vmturbo.arangodb.tool.ArangoDump;
import com.vmturbo.arangodb.tool.ArangoRestore;
import com.vmturbo.auth.api.SpringSecurityConfig;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.authorization.jwt.JwtServerInterceptor;
import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.action.ActionsServiceGrpc;
import com.vmturbo.common.protobuf.action.EntitySeverityServiceGrpc;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc;
import com.vmturbo.common.protobuf.repository.RepositoryDTOREST.RepositoryServiceController;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.repository.SupplyChainProtoREST.SupplyChainServiceController;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceImplBase;
import com.vmturbo.common.protobuf.search.SearchREST.SearchServiceController;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.ComponentRestTemplate;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.api.client.KafkaMessageConsumer.TopicSettings.StartFrom;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.OsCommandProcessRunner;
import com.vmturbo.components.common.diagnostics.DiagnosticsWriter;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory;
import com.vmturbo.components.common.diagnostics.DiagsZipReaderFactory.DefaultDiagsZipReader;
import com.vmturbo.components.common.diagnostics.FileFolderZipper;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParamsFactory.DefaultEntityStatsPaginationParamsFactory;
import com.vmturbo.components.common.pagination.EntityStatsPaginator;
import com.vmturbo.group.api.GroupClientConfig;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.market.component.api.impl.MarketSubscription;
import com.vmturbo.repository.controller.RepositoryDiagnosticController;
import com.vmturbo.repository.controller.RepositorySecurityConfig;
import com.vmturbo.repository.exception.GraphDatabaseExceptions.GraphDatabaseException;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseDriverBuilder;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.graph.executor.ArangoDBExecutor;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.executor.ReactiveArangoDBExecutor;
import com.vmturbo.repository.graph.executor.ReactiveGraphDBExecutor;
import com.vmturbo.repository.listener.MarketTopologyListener;
import com.vmturbo.repository.listener.TopologyEntitiesListener;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.search.SearchHandler;
import com.vmturbo.repository.service.ArangoRepositoryRpcService;
import com.vmturbo.repository.service.ArangoSupplyChainRpcService;
import com.vmturbo.repository.service.GraphDBService;
import com.vmturbo.repository.service.LiveTopologyPaginator;
import com.vmturbo.repository.service.PartialEntityConverter;
import com.vmturbo.repository.service.PlanStatsService;
import com.vmturbo.repository.service.SupplyChainService;
import com.vmturbo.repository.service.SupplyChainStatistician;
import com.vmturbo.repository.service.TopologyGraphRepositoryRpcService;
import com.vmturbo.repository.service.TopologyGraphSearchRpcService;
import com.vmturbo.repository.service.TopologyGraphSupplyChainRpcService;
import com.vmturbo.repository.topology.GlobalSupplyChainManager;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyIDFactory;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;
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
    RepositorySecurityConfig.class,
    RepositoryProperties.class,
    SpringSecurityConfig.class,
    UserSessionConfig.class
})
public class RepositoryComponent extends BaseVmtComponent {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryComponent.class);
    private static final String DOCUMENT_KEY_FIELD = "_key";
    private static final String TOPOLOGY_PROTO_FIELD = "topology_proto_field";
    private static final String GRAPH_NAME = "seGraph";
    // svc : ServiceEntity vertex collection.
    // Using abbreviated name to consume less space as the
    // name is referenced in the edges in the edgeCollection
    private static final String VERTEX_COLLECTION_NAME = "svc";
    private static final String EDGE_COLLECTION_NAME = "seProviderEdgeCollection";
    private static final String TOPOLOGY_PROTO_COLLECTION_NAME = "topology_proto";

    @Value("${actionOrchestratorHost}")
    private String actionOrchestratorHost;

    @Autowired
    RepositoryApiConfig apiConfig;

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

    RepositoryProperties repositoryProperties;

    FileFolderZipper fileFolderZipper;

    OsCommandProcessRunner osCommandProcessRunner;

    @Value("${arangoDumpRestorePort:8599}")
    private int arangoDumpRestorePort;

    @Value("${arangodbHealthCheckIntervalSeconds:60}")
    private int arangoHealthCheckIntervalSeconds;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${authHost}")
    private String authHost;

    @Value("${authRoute:}")
    private String authRoute;

    @Value("${serverHttpPort}")
    private int authPort;

    @Value("${authRetryDelaySecs}")
    private int authRetryDelaySecs;

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

    @Value("${repositoryRealtimeTopologyDropDelaySecs}")
    private int repositoryRealtimeTopologyDropDelaySecs;

    @Value("${numberOfExpectedRealtimeSourceDB}")
    private int numberOfExpectedRealtimeSourceDB;

    @Value("${numberOfExpectedRealtimeProjectedDB}")
    private int numberOfExpectedRealtimeProjectedDB;

    @Value("${repositoryMaxEntitiesPerChunk:5}")
    private int maxEntitiesPerChunk;

    private final SetOnce<ArangoDB> arangoDB = new SetOnce<>();

    private final com.vmturbo.repository.RepositoryProperties.ArangoDB arangoProps;

    public RepositoryComponent(final RepositoryProperties repositoryProperties,
                               final FileFolderZipper fileFolderZipper,
                               final OsCommandProcessRunner osCommandProcessRunner) {

        this.repositoryProperties = repositoryProperties;
        this.fileFolderZipper = fileFolderZipper;
        this.osCommandProcessRunner = osCommandProcessRunner;
        this.arangoProps = repositoryProperties.getArangodb();
    }

    @PostConstruct
    private void setup() {
        getHealthMonitor().addHealthCheck(apiConfig.kafkaHealthMonitor());
    }

    /**
     * Utility to retrieve the secret root database password from the auth component.
     *
     * @return The {@link DBPasswordUtil} instance.
     */
    @Bean
    public DBPasswordUtil dbPasswordUtil() {
        return new DBPasswordUtil(authHost, authPort, authRoute, authRetryDelaySecs);
    }

    /**
     * Store an object of type {@link TopologyDTO.Topology} into ArangoDB.
     *
     * A {@link TopologyDTO.Topology} object will be stored as an Arango document which
     * contains one field called {@link #TOPOLOGY_PROTO_FIELD} and the value will the binary version
     * of the {@link TopologyDTO.Topology}.
     */
    private static VPackSerializer<TopologyDTO.Topology> TOPOLOGY_VPACK_SERIALIZER =
            (builder, attribute, topology, context) -> {
                builder.add(attribute, ValueType.OBJECT);
                builder.add(DOCUMENT_KEY_FIELD, Long.toString(topology.getTopologyId()));
                builder.add(TOPOLOGY_PROTO_FIELD, topology.toByteArray());

                builder.close();
            };

    /**
     * Retrieve the topology protobuf from ArangoDB.
     *
     * Read the {@link #TOPOLOGY_PROTO_FIELD} field from the document as an array of bytes.
     * Then convert the array of bytes into an object of type {@link TopologyDTO.Topology}.
     */
    private static VPackDeserializer<TopologyDTO.Topology> TOPOLOGY_VPACK_DESERIALIZER =
            (parent, vpack, context) -> {
                try {
                    final byte[] bytes = vpack.get(TOPOLOGY_PROTO_FIELD).getAsBinary();
                    return TopologyDTO.Topology.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new VPackBuilderException(e);
                }
            };

    @Primary
    @Bean
    ObjectMapper objectMapper() {
        final ObjectMapper om = new ObjectMapper()
                .registerModule(new GuavaModule());

        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return om;
    }

    @Bean
    public ArangoDatabaseFactory arangoDatabaseFactory() {
        return () -> {
            return this.arangoDB.ensureSet(() -> {
                return new ArangoDB.Builder()
                    .host(arangoProps.getHost(), arangoProps.getPort())
                    .registerSerializer(TopologyDTO.Topology.class, TOPOLOGY_VPACK_SERIALIZER)
                    .registerDeserializer(TopologyDTO.Topology.class, TOPOLOGY_VPACK_DESERIALIZER)
                    .password(getArangoDBPassword())
                    .user(arangoProps.getUsername())
                    .maxConnections(arangoProps.getMaxConnections())
                    .build();
            });
        };
    }

    @Bean
    public TopologyLifecycleManager topologyManager() {
        return new TopologyLifecycleManager(graphDatabaseDriverBuilder(), graphDefinition(),
                topologyProtobufsManager(), realtimeTopologyContextId,
                new ScheduledThreadPoolExecutor(1),
                liveTopologyStore(),
                repositoryRealtimeTopologyDropDelaySecs,
                numberOfExpectedRealtimeSourceDB,
                numberOfExpectedRealtimeProjectedDB,
                globalSupplyChainManager(),
                arangoDBExecutor());
    }

    @Bean
    public GraphDefinition graphDefinition() {
        return new GraphDefinition.Builder()
                .setGraphName(GRAPH_NAME)
                .setServiceEntityVertex(VERTEX_COLLECTION_NAME)
                .setProviderRelationship(EDGE_COLLECTION_NAME)
                .setTopologyProtoCollection(TOPOLOGY_PROTO_COLLECTION_NAME)
                .createGraphDefinition();
    }

    @Bean
    public GraphDBExecutor arangoDBExecutor() {
        return new ArangoDBExecutor(arangoDatabaseFactory());
    }

    @Bean
    public ReactiveGraphDBExecutor arangoReactiveDBExecutor() {
        return new ReactiveArangoDBExecutor(arangoDatabaseFactory(), objectMapper());
    }

    @Bean
    public ArangoDump arangoDump() {
        final String dumpEndPoint = String.format("http://%s:%d/dump/",
                repositoryProperties.getArangodb().getHost(),
                arangoDumpRestorePort);

        return new ArangoDump.Builder()
                .endpoint(dumpEndPoint)
                .outputDir(repositoryProperties.getArangodb().getArangoDumpOutputDir())
                .build();
    }

    @Bean
    public ArangoRestore arangoRestore() {
        final String restoreEndpoint = String.format("http://%s:%d/restore/",
                repositoryProperties.getArangodb().getHost(),
                arangoDumpRestorePort);

        return new ArangoRestore.Builder()
                .endpoint(restoreEndpoint)
                .baseDir(repositoryProperties.getArangodb().getArangoRestoreBaseDir())
                .inputDir(repositoryProperties.getArangodb().getArangoRestoreInputDir())
                .build();
    }

    @Bean
    public RepositoryDiagnosticsHandler repositoryDiagnosticsHandler() {
        return new RepositoryDiagnosticsHandler(arangoDump(),
                arangoRestore(),
                globalSupplyChainManager(),
                topologyManager(),
                liveTopologyStore(),
                arangoDBExecutor(),
                restTemplate(),
                recursiveZipReaderFactory(),
                diagnosticsWriter());
    }

    @Bean
    public DiagnosticsWriter diagnosticsWriter() {
        return new DiagnosticsWriter();
    }

    @Bean
    public DiagsZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultDiagsZipReader();
    }

    @Bean
    public GraphDBService graphDBService() {
        return new GraphDBService(arangoDBExecutor(),
                                  graphDefinition(),
                                  topologyManager());
    }

    @Bean
    public GlobalSupplyChainManager globalSupplyChainManager() {
        return new GlobalSupplyChainManager(arangoDBExecutor());
    }

    @Bean
    public SupplyChainService supplyChainService() throws InterruptedException, URISyntaxException, CommunicationException {
        return new SupplyChainService(arangoReactiveDBExecutor(),
                                      graphDBService(),
                                      graphDefinition(),
                                      topologyManager(),
                                      globalSupplyChainManager(),
                                      userSessionConfig.userSessionContext());
    }

    @Bean
    public RestTemplate restTemplate() {
        return ComponentRestTemplate.create();
    }

    @Bean
    public TopologyProtobufsManager topologyProtobufsManager() {
        return new TopologyProtobufsManager(arangoDatabaseFactory(), getArangoDBNamespacePrefix());
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
        final ArangoRepositoryRpcService arangoRpcService = new ArangoRepositoryRpcService(topologyManager(),
            topologyProtobufsManager(),
            graphDBService(),
            planStatsService(),
            partialEntityConverter(),
            maxEntitiesPerChunk,
            topologyIDFactory());

        // Return a topology-graph backed rpc service, which will fall back to arango for
        // non-realtime queries.
        return new TopologyGraphRepositoryRpcService(liveTopologyStore(),
            arangoRpcService,
            partialEntityConverter(),
            realtimeTopologyContextId,
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
        return new TopologyGraphSearchRpcService(liveTopologyStore(),
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
            graphDBService(),
            supplyChainService(),
            userSessionConfig.userSessionContext(),
            realtimeTopologyContextId);
       return new TopologyGraphSupplyChainRpcService(userSessionConfig.userSessionContext(),
                liveTopologyStore(),
                arangoService,
                supplyChainStatistician(),
                supplyChainCalculator(),
                realtimeTopologyContextId);
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
    public GlobalSupplyChainCalculator globalSupplyChainCalculator() {
        return new GlobalSupplyChainCalculator();
    }

    @Bean
    public LiveTopologyStore liveTopologyStore() {
        return new LiveTopologyStore(globalSupplyChainCalculator());
    }

    @Bean
    public SupplyChainServiceController supplyChainServiceController()
            throws InterruptedException, CommunicationException, URISyntaxException {
        return new SupplyChainServiceController(supplyChainRpcService());
    }

    @Bean
    public SearchHandler searchHandler() {
        return new SearchHandler(graphDefinition(),
                                 arangoDatabaseFactory(),
                                 arangoDBExecutor());
    }

    // The controller generated with gRPC service
    @Bean
    public SearchServiceController searchServiceController() throws InterruptedException, URISyntaxException, CommunicationException {
        return new SearchServiceController(searchRpcService());
    }

    @Bean
    public RepositoryDiagnosticController repositoryDiagnosticController() {
        return new RepositoryDiagnosticController(repositoryDiagnosticsHandler());
    }

    @Bean
    public GraphDatabaseDriverBuilder graphDatabaseDriverBuilder() {
        return new ArangoDatabaseDriverBuilder(arangoDatabaseFactory());
    }

    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() {
        return new TopologyEntitiesListener(topologyManager(),
                                            apiConfig.repositoryNotificationSender(), topologyIDFactory());
    }

    @Bean
    public MarketTopologyListener marketTopologyListener() {
        return new MarketTopologyListener(
                apiConfig.repositoryNotificationSender(),
                topologyManager(), topologyIDFactory());
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
            // If using the in-memory graph, we want to read from the beginning on restart
            // so that we can populate the graph without waiting for the next broadcast.
            MarketSubscription.forTopicWithStartFrom(
                MarketSubscription.Topic.ProjectedTopologies, StartFrom.BEGINNING),
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
     * The {@link TopologyIDFactory} used to create {@link TopologyID}.
     *
     * @return {@link TopologyIDFactory}.
     */
    @Bean
    public TopologyIDFactory topologyIDFactory() {
        return new TopologyIDFactory(getArangoDBNamespacePrefix());
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
        repositoryDiagnosticsHandler().dump(diagnosticZip);
    }

    @PreDestroy
    private void destroy() {
        if (arangoDB != null) {
            logger.info("Closing all arangodb client connections");
            arangoDB.getValue().ifPresent(ArangoDB::shutdown);
        }
    }

    /**
     * Return password if specified, else return default ArangoDB root password.
     *
     * @return ArangoDB password.
     */
    private String getArangoDBPassword() {
        return !StringUtils.isEmpty(arangoProps.getPassword()) ? arangoProps.getPassword() :
            dbPasswordUtil().getArangoDbRootPassword();
    }

    /**
     * Construct ArangoDB namespace prefix to be prepended to database names. For example, if ArangoDB
     * namespace is "turbonomic", then the constructed arangoDBNamespacePrefix is "turbonomic-".
     *
     * @return Constructed ArangoDB namespace prefix to be prepended to database names.
     */
    private String getArangoDBNamespacePrefix() {
        return StringUtils.isEmpty(arangoProps.getNamespace()) ? StringUtils.EMPTY : arangoProps.getNamespace() + "-";
    }
}
