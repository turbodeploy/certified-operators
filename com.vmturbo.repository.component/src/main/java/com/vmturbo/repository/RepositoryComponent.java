package com.vmturbo.repository;

import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Optional;
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

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.ServerInterceptors;

import me.dinowernli.grpc.prometheus.MonitoringServerInterceptor;

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

import com.vmturbo.arangodb.ArangoHealthMonitor;
import com.vmturbo.arangodb.tool.ArangoDump;
import com.vmturbo.arangodb.tool.ArangoRestore;
import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTOREST.RepositoryServiceController;
import com.vmturbo.common.protobuf.search.SearchREST.SearchServiceController;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.components.api.ComponentRestTemplate;
import com.vmturbo.components.common.BaseVmtComponent;
import com.vmturbo.components.common.DiagnosticsWriter;
import com.vmturbo.components.common.FileFolderZipper;
import com.vmturbo.components.common.OsCommandProcessRunner;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory;
import com.vmturbo.components.common.diagnostics.RecursiveZipReaderFactory.DefaultRecursiveZipReaderFactory;
import com.vmturbo.market.component.api.MarketComponent;
import com.vmturbo.market.component.api.impl.MarketClientConfig;
import com.vmturbo.repository.controller.GraphServiceEntityController;
import com.vmturbo.repository.controller.GraphTopologyController;
import com.vmturbo.repository.controller.RepositoryDiagnosticController;
import com.vmturbo.repository.controller.RepositorySecurityConfig;
import com.vmturbo.repository.controller.SearchController;
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
import com.vmturbo.repository.search.SearchHandler;
import com.vmturbo.repository.service.GraphDBService;
import com.vmturbo.repository.service.GraphTopologyService;
import com.vmturbo.repository.service.RepositoryRpcService;
import com.vmturbo.repository.service.SearchService;
import com.vmturbo.repository.service.SupplyChainRpcService;
import com.vmturbo.repository.service.SupplyChainService;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyRelationshipRecorder;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;
import com.vmturbo.topology.processor.api.TopologyProcessor;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig;
import com.vmturbo.topology.processor.api.impl.TopologyProcessorClientConfig.Subscription;

@Configuration("theComponent")
@Import({
    RepositoryApiConfig.class,
    TopologyProcessorClientConfig.class,
    MarketClientConfig.class,
    RepositorySecurityConfig.class,
    RepositoryProperties.class
})
public class RepositoryComponent extends BaseVmtComponent {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryComponent.class);
    private static final String DOCUMENT_KEY_FIELD = "_key";
    private static final String TOPOLOGY_PROTO_FIELD = "topology_proto_field";

    @Value("${actionOrchestratorHost}")
    private String actionOrchestratorHost;

    @Autowired
    RepositoryApiConfig apiConfig;

    @Autowired
    private TopologyProcessorClientConfig tpClientConfig;

    @Autowired
    private  MarketClientConfig marketClientConfig;

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

    @Value("${authPort}")
    private int authPort;

    @Value("${authRetryDelaySecs}")
    private int authRetryDelaySecs;

    private ArangoDB arangoDB;

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
        logger.info("Setting up connection to ArangoDB...");
        final String arangoDbPassword = new DBPasswordUtil(authHost, authPort, authRetryDelaySecs)
            .getArangoDbRootPassword();

        this.arangoDB =
            new ArangoDB.Builder()
                .host(arangoProps.getHost(), arangoProps.getPort())
                .registerSerializer(TopologyDTO.Topology.class, TOPOLOGY_VPACK_SERIALIZER)
                .registerDeserializer(TopologyDTO.Topology.class, TOPOLOGY_VPACK_DESERIALIZER)
                .password(arangoDbPassword)
                .user(arangoProps.getUsername())
                .maxConnections(arangoProps.getMaxConnections())
                .build();

        logger.info("Adding ArangoDB health check to the component health monitor.");
        // add a health monitor for Arango
        getHealthMonitor().addHealthCheck(
                new ArangoHealthMonitor(arangoHealthCheckIntervalSeconds, arangoDatabaseFactory()::getArangoDriver));
        getHealthMonitor().addHealthCheck(apiConfig.kafkaHealthMonitor());
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
        return () -> arangoDB;
    }

    @Bean
    public TopologyLifecycleManager topologyManager() {
        return new TopologyLifecycleManager(graphDatabaseDriverBuilder(), graphDefinition(),
                topologyProtobufsManager(), realtimeTopologyContextId);
    }

    @Bean
    public GraphDefinition graphDefinition() {
        return new GraphDefinition.Builder()
                .setGraphName("seGraph")
                .setServiceEntityVertex("seVertexCollection")
                .setProviderRelationship("seProviderEdgeCollection")
                .setTopologyProtoCollection("topology_proto")
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
                topologyRelationshipRecorder(),
                topologyManager(),
                restTemplate(),
                recursiveZipReaderFactory(),
                diagnosticsWriter());
    }

    @Bean
    public DiagnosticsWriter diagnosticsWriter() {
        return new DiagnosticsWriter();
    }

    @Bean
    public RecursiveZipReaderFactory recursiveZipReaderFactory() {
        return new DefaultRecursiveZipReaderFactory();
    }

    @Bean
    public GraphDBService graphDBService() throws InterruptedException, URISyntaxException, CommunicationException {
        return new GraphDBService(arangoDBExecutor(),
                                  graphDefinition(),
                                  topologyManager());
    }

    @Bean
    public SupplyChainService supplyChainService() throws InterruptedException, URISyntaxException, CommunicationException {
        return new SupplyChainService(arangoReactiveDBExecutor(),
                                      graphDBService(),
                                      graphDefinition(),
                                      topologyRelationshipRecorder(),
                                      topologyManager());
    }

    @Bean
    public RestTemplate restTemplate() {
        return ComponentRestTemplate.create();
    }

    @Bean
    public TopologyProtobufsManager topologyProtobufsManager() {
        return new TopologyProtobufsManager(arangoDatabaseFactory());
    }

    @Bean
    public GraphTopologyService graphTopologyService() {
        return new GraphTopologyService(
                arangoDatabaseFactory(),
                graphDefinition());
    }

    @Bean
    public RepositoryRpcService repositoryRpcService() throws GraphDatabaseException,
            InterruptedException, URISyntaxException, CommunicationException{
        return new RepositoryRpcService(topologyManager(), topologyProtobufsManager(), graphDBService());
    }

    @Bean
    public RepositoryServiceController repositoryServiceController() throws GraphDatabaseException,
            InterruptedException, URISyntaxException, CommunicationException{
        return new RepositoryServiceController(repositoryRpcService());
    }

    @Bean
    public SearchService searchService() throws InterruptedException, CommunicationException, URISyntaxException {
        return new SearchService(supplyChainService(),
                                 topologyManager(),
                                 searchHandler());
    }

    @Bean
    public SupplyChainRpcService supplyChainRpcService()
        throws InterruptedException, CommunicationException, URISyntaxException {
        return new SupplyChainRpcService(graphDBService(), supplyChainService());
    }

    @Bean
    public SearchHandler searchHandler() {
        return new SearchHandler(graphDefinition(),
                                 arangoDatabaseFactory(),
                                 arangoDBExecutor());
    }

    // The controller generated with gRPC service
    @Bean
    public SearchServiceController searchServiceController(final SearchService searchService) {
        return new SearchServiceController(searchService);
    }

    // The regular REST controller
    @Bean
    public SearchController searchController(final SearchService searchService) {
        return new SearchController(searchService);
    }

    @Bean
    public GraphTopologyController graphTopologyController(final GraphTopologyService graphTopologyService) {
        return new GraphTopologyController(graphTopologyService);
    }

    @Bean
    public RepositoryDiagnosticController repositoryDiagnosticController() {
        return new RepositoryDiagnosticController(repositoryDiagnosticsHandler());
    }

    /**
     * Constructs the SE graph controller, which handles REST requests.
     *
     * @param graphDBService The graph DB service.
     * @return The SE controller with the name of graphServiceEntityController.
     */
    @Bean
    public GraphServiceEntityController graphServiceEntityController(final GraphDBService graphDBService) {
        return new GraphServiceEntityController(graphDBService);
    }

    @Bean
    public GraphDatabaseDriverBuilder graphDatabaseDriverBuilder() {
        return new ArangoDatabaseDriverBuilder(arangoDatabaseFactory());
    }

    @Bean
    public TopologyRelationshipRecorder topologyRelationshipRecorder() {
        return new TopologyRelationshipRecorder();
    }

    @Bean
    public TopologyEntitiesListener topologyEntitiesListener() throws GraphDatabaseException {
        return new TopologyEntitiesListener(topologyManager(),
                                            topologyRelationshipRecorder(),
                                            apiConfig.repositoryNotificationSender());
    }

    @Bean
    public MarketTopologyListener marketTopologyListener() {
        return new MarketTopologyListener(
                apiConfig.repositoryNotificationSender(),
                topologyManager());
    }

    @Bean
    public ComponentStartUpManager componentStartUpManager() throws GraphDatabaseException {
        ComponentStartUpManager componentStartUpManager =
                        new ComponentStartUpManager(graphDatabaseDriverBuilder());
        componentStartUpManager.startup();
        return componentStartUpManager;
    }

    @Bean
    public TopologyProcessor topologyProcessor() throws GraphDatabaseException {
        final TopologyProcessor topologyProcessor =
                tpClientConfig.topologyProcessor(EnumSet.of(Subscription.LiveTopologies));
        topologyProcessor.addLiveTopologyListener(topologyEntitiesListener());
        return topologyProcessor;
    }

    @Bean
    public MarketComponent marketComponent() {
        final MarketComponent market = marketClientConfig.marketComponent(
                EnumSet.of(MarketClientConfig.Subscription.ProjectedTopologies));
        market.addProjectedTopologyListener(marketTopologyListener());
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

    @Override
    @Nonnull
    protected Optional<Server> buildGrpcServer(@Nonnull final ServerBuilder builder) {
        try {
            // Monitor for server metrics with prometheus.
            final MonitoringServerInterceptor monitoringInterceptor =
                MonitoringServerInterceptor.create(me.dinowernli.grpc.prometheus.Configuration.allMetrics());

            return Optional.of(builder
                .addService(ServerInterceptors.intercept(repositoryRpcService(), monitoringInterceptor))
                .addService(ServerInterceptors.intercept(searchService(), monitoringInterceptor))
                .addService(ServerInterceptors.intercept(supplyChainRpcService(), monitoringInterceptor))
                .build());
        } catch (InterruptedException | CommunicationException
                | URISyntaxException | GraphDatabaseException e) {
            logger.error("Failed building grpc server", e);
            return Optional.empty();
        }
    }

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
            arangoDB.shutdown();
        }
    }
}
