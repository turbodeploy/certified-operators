package com.vmturbo.repository;

import java.util.concurrent.ScheduledThreadPoolExecutor;

import javax.annotation.PreDestroy;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.Protocol;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.velocypack.VPackDeserializer;
import com.arangodb.velocypack.VPackSerializer;
import com.arangodb.velocypack.ValueType;
import com.arangodb.velocypack.exception.VPackBuilderException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.protobuf.InvalidProtocolBufferException;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.vmturbo.arangodb.ArangoError;
import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.Topology;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.repository.graph.GraphDefinition;
import com.vmturbo.repository.graph.driver.ArangoDatabaseDriverBuilder;
import com.vmturbo.repository.graph.driver.ArangoDatabaseFactory;
import com.vmturbo.repository.graph.driver.GraphDatabaseDriverBuilder;
import com.vmturbo.repository.graph.executor.ArangoDBExecutor;
import com.vmturbo.repository.graph.executor.GraphDBExecutor;
import com.vmturbo.repository.graph.executor.ReactiveArangoDBExecutor;
import com.vmturbo.repository.graph.executor.ReactiveGraphDBExecutor;
import com.vmturbo.repository.listener.RepositoryPlanGarbageCollector;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.service.GraphDBService;
import com.vmturbo.repository.service.SupplyChainService;
import com.vmturbo.repository.topology.GlobalSupplyChainManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;

/**
 * Spring configuration for repository component.
 */
@Configuration
@Import({RepositoryProperties.class, UserSessionConfig.class, PlanOrchestratorClientConfig.class})
public class RepositoryComponentConfig {

    private static final String DOCUMENT_KEY_FIELD = "_key";
    private static final String TOPOLOGY_PROTO_FIELD = "topology_proto_field";

    private static final String GRAPH_NAME = "seGraph";
    // svc : ServiceEntity vertex collection.
    // Using abbreviated name to consume less space as the
    // name is referenced in the edges in the edgeCollection
    private static final String VERTEX_COLLECTION_NAME = "svc";
    private static final String EDGE_COLLECTION_NAME = "seProviderEdgeCol";
    private static final String TOPOLOGY_PROTO_COLLECTION_NAME = "topology_proto";
    /**
     * Database name prefix used to construct Arango database name to make sure db name starts with
     * a letter.
     * https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-database-names.html
     */
    public static final String DATABASE_NAME_PREFIX = "T";

    private final Logger logger = LogManager.getLogger(getClass());

    @Value("${authHost}")
    private String authHost;

    @Value("${authRoute:}")
    private String authRoute;

    @Value("${serverHttpPort}")
    private int authPort;

    @Value("${authRetryDelaySecs}")
    private int authRetryDelaySecs;

    @Value("${realtimeTopologyContextId}")
    private long realtimeTopologyContextId;

    @Value("${repositoryRealtimeTopologyDropDelaySecs}")
    private int repositoryRealtimeTopologyDropDelaySecs;

    @Value("${numberOfExpectedRealtimeSourceDB}")
    private int numberOfExpectedRealtimeSourceDB;

    @Value("${numberOfExpectedRealtimeProjectedDB}")
    private int numberOfExpectedRealtimeProjectedDB;

    @Value("${collectionReplicaCount:1}")
    private int collectionReplicaCount;

    @Value("${collectionNumShards:1}")
    private int collectionNumShards;

    @Value("${collectionWaitForSync:true}")
    private boolean collectionWaitForSync;

    @Autowired
    private RepositoryProperties repositoryProperties;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    private final SetOnce<ArangoDB> arangoDB = new SetOnce<>();

    /**
     * Store an object of type {@link TopologyDTO.Topology} into ArangoDB.
     *
     * <p>A {@link TopologyDTO.Topology} object will be stored as an Arango document which
     * contains one field called {@link #TOPOLOGY_PROTO_FIELD} and the value will the binary version
     * of the {@link TopologyDTO.Topology}.
     */
    private static final VPackSerializer<Topology> TOPOLOGY_VPACK_SERIALIZER =
            (builder, attribute, topology, context) -> {
                builder.add(attribute, ValueType.OBJECT);
                builder.add(DOCUMENT_KEY_FIELD, Long.toString(topology.getTopologyId()));
                builder.add(TOPOLOGY_PROTO_FIELD, topology.toByteArray());

                builder.close();
            };

    /**
     * Retrieve the topology protobuf from ArangoDB.
     *
     * <p>Read the {@link #TOPOLOGY_PROTO_FIELD} field from the document as an array of bytes.
     * Then convert the array of bytes into an object of type {@link TopologyDTO.Topology}.
     */
    private static final VPackDeserializer<Topology> TOPOLOGY_VPACK_DESERIALIZER =
            (parent, vpack, context) -> {
                try {
                    final byte[] bytes = vpack.get(TOPOLOGY_PROTO_FIELD).getAsBinary();
                    return TopologyDTO.Topology.parseFrom(bytes);
                } catch (InvalidProtocolBufferException e) {
                    throw new VPackBuilderException(e);
                }
            };

    /**
     * Global supply chain calculator.
     *
     * @return global supply chain calculator
     */
    @Bean
    public GlobalSupplyChainCalculator globalSupplyChainCalculator() {
        return new GlobalSupplyChainCalculator();
    }

    /**
     * Live topology store.
     *
     * @return live topology store
     */
    @Bean
    public LiveTopologyStore liveTopologyStore() {
        return new LiveTopologyStore(globalSupplyChainCalculator(), searchResolver());
    }

    @Bean
    public SearchResolver<RepoGraphEntity> searchResolver() {
        return new SearchResolver<>(new TopologyFilterFactory<RepoGraphEntity>());
    }

    /**
     * Listener for plan deletion.
     *
     * @return The listener.
     */
    @Bean
    public PlanGarbageDetector repositoryPlanGarbageDetector() {
        final RepositoryPlanGarbageCollector collector = new RepositoryPlanGarbageCollector(topologyManager());
        return planOrchestratorClientConfig.newPlanGarbageDetector(collector);
    }

    /**
     * Topology lifecycle manager.
     *
     * @return topology manager
     */
    @Bean
    public TopologyLifecycleManager topologyManager() {
        if (collectionReplicaCount != 1) {
            logger.info("Using collection replica count of {} instead of default (1).", collectionReplicaCount);
        }
        return new TopologyLifecycleManager(graphDatabaseDriverBuilder(), graphDefinition(),
                topologyProtobufsManager(), realtimeTopologyContextId,
                new ScheduledThreadPoolExecutor(1), liveTopologyStore(),
                repositoryRealtimeTopologyDropDelaySecs, numberOfExpectedRealtimeSourceDB,
                numberOfExpectedRealtimeProjectedDB, collectionReplicaCount, globalSupplyChainManager(),
                arangoDBExecutor());
    }

    /**
     * Graph database driver builder.
     *
     * @return graph database driver builder
     */
    @Bean
    public GraphDatabaseDriverBuilder graphDatabaseDriverBuilder() {
        return new ArangoDatabaseDriverBuilder(arangoDatabaseFactory());
    }

    /**
     * Arango database factory.
     *
     * @return arango database factory
     */
    @Bean
    public ArangoDatabaseFactory arangoDatabaseFactory() {
        return () -> {
            ArangoDB driver = this.arangoDB.ensureSet(() -> {
                return new ArangoDB.Builder().host(repositoryProperties.getArangodb().getHost(),
                        repositoryProperties.getArangodb().getPort())
                        .registerSerializer(TopologyDTO.Topology.class, TOPOLOGY_VPACK_SERIALIZER)
                        .registerDeserializer(TopologyDTO.Topology.class,
                                TOPOLOGY_VPACK_DESERIALIZER)
                        .password(getArangoDBPassword())
                        .user(repositoryProperties.getArangodb().getUsername())
                        .maxConnections(repositoryProperties.getArangodb().getMaxConnections())
                        .useProtocol(Protocol.HTTP_VPACK)
                        .build();
            });

            // If we are not able to connect to arango, db.exists() returns false. We will try to create the
            // database, but it's ok since we cannot connect to arango anyway.
            // Note: arango currently don't support checking if Database(Connection) exists
            // https://github.com/arangodb/arangodb-java-driver/issues/254
            final ArangoDatabase db = driver.db(getArangoDatabaseName());
            // If it does not exist, create it.
            if (!db.exists()) {
                logger.info("Arango DB {} does not exist. Creating database.", getArangoDatabaseName());
                createDatabase(driver);
            }
            // TODO: Ideally it would be good to return the created database from here instead of the driver.
            // But since that is quite a big refactor, we are not doing it at the moment.
            return driver;
        };
    }

    /**
     * Create an {@link ArangoDatabase}.
     * 
     * @param driver the Arango driver.
     */
    private void createDatabase(ArangoDB driver) {
        try {
            if (driver.createDatabase(getArangoDatabaseName())) {
                logger.info("Database {} successfully created.", getArangoDatabaseName());
            } else {
                throw new IllegalStateException("Could not create Arango DB. Arango createDatabase returned false.");
            }
        } catch (ArangoDBException adbe) {
            // We will treat "duplicate name" errors as harmless -- this means someone
            // else may have already created our database.
            // Cast ERROR_ARANGO_DUPLICATE_NAME primitive int to Integer because we have seen cases
            // where the error number of the ArangoDBException can be a null Integer and comparison
            // with a primitive int will then throw NPE.
            if (adbe.getErrorNum() == (Integer)ArangoError.ERROR_ARANGO_DUPLICATE_NAME) {
                logger.info("Database {} already created.", getArangoDatabaseName());
            } else {
                // we'll re-throw the other errors.
                logger.error("Error when creating database {}.", getArangoDatabaseName());
                throw adbe;
            }
        }
    }

    /**
     * A set of default collection creation options that can be used as a template for creating new
     * collections.
     *
     * <p>NOTE: this is not being used everywhere we create collections at this time. But
     * since we are likely to add more collection creation params if/when we horizontally scale the
     * arangodb cluster, we can start to propagate this object instead of the individual default params we
     * will use.
     *
     * @return the default collection creation options.
     */
    @Bean
    public CollectionCreateOptions defaultCollectionOptions() {
        CollectionCreateOptions defaultOptions = new CollectionCreateOptions()
                .waitForSync(collectionWaitForSync)
                .replicationFactor(collectionReplicaCount)
                .numberOfShards(collectionNumShards);
        return defaultOptions;
    }

    /**
     * Topology protobufs manager.
     *
     * @return Topology protobufs manager.
     */
    @Bean
    public TopologyProtobufsManager topologyProtobufsManager() {
        return new TopologyProtobufsManager(arangoDatabaseFactory(), getArangoDatabaseName(), defaultCollectionOptions());
    }

    /**
     * Graph definition.
     *
     * @return graph definition
     */
    @Bean
    public GraphDefinition graphDefinition() {
        return new GraphDefinition.Builder().setGraphName(GRAPH_NAME)
                .setServiceEntityVertex(VERTEX_COLLECTION_NAME)
                .setProviderRelationship(EDGE_COLLECTION_NAME)
                .setTopologyProtoCollection(TOPOLOGY_PROTO_COLLECTION_NAME)
                .createGraphDefinition();
    }

    /**
     * Construct Arango database name based on arangoDBNamespace and starting with letter "T", which
     * stands for Topology, because Arango database (except "_system") name must always start with a
     * letter: https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-database-names.html
     * For each namespace, we create only one database to store plan data. For example, if
     * arangoDBNamespace is "turbonomic", then the constructed database name is "Tturbonomic".
     *
     * @return Constructed ArangoDB database name.
     */
    public String getArangoDatabaseName() {
        return DATABASE_NAME_PREFIX + repositoryProperties.getArangodb().getNamespace();
    }

    /**
     * Global supply chain manager.
     *
     * @return global supply chain manager
     */
    @Bean
    public GlobalSupplyChainManager globalSupplyChainManager() {
        return new GlobalSupplyChainManager(arangoDBExecutor());
    }

    /**
     * Object mapper.
     *
     * @return object mapper
     */
    @Primary
    @Bean
    public ObjectMapper objectMapper() {
        final ObjectMapper om = new ObjectMapper().registerModule(new GuavaModule());

        om.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        return om;
    }

    /**
     * Arango DB executor.
     *
     * @return arango DB executor
     */
    @Bean
    public GraphDBExecutor arangoDBExecutor() {
        return new ArangoDBExecutor(arangoDatabaseFactory(), getArangoDatabaseName());
    }

    /**
     * Arango reactive DB executor.
     *
     * @return Arango DB executor
     */
    @Bean
    public ReactiveGraphDBExecutor arangoReactiveDBExecutor() {
        return new ReactiveArangoDBExecutor(arangoDatabaseFactory(), objectMapper());
    }

    /**
     * Graph DB service.
     *
     * @return graph DB service
     */
    @Bean
    public GraphDBService graphDBService() {
        return new GraphDBService(arangoDBExecutor(), graphDefinition(), topologyManager());
    }

    /**
     * Supply chain service.
     *
     * @return Supply chain service
     */
    @Bean
    public SupplyChainService supplyChainService() {
        return new SupplyChainService(topologyManager(), globalSupplyChainManager(),
                userSessionConfig.userSessionContext());
    }

    /**
     * Return password if specified, else return default ArangoDB root password.
     *
     * @return ArangoDB password.
     */
    private String getArangoDBPassword() {
        return !StringUtils.isEmpty(repositoryProperties.getArangodb().getPassword()) ? repositoryProperties.getArangodb().getPassword() :
                dbPasswordUtil().getArangoDbRootPassword();
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

    public long getRealtimeTopologyContextId() {
        return realtimeTopologyContextId;
    }

    @PreDestroy
    private void destroy() {
        logger.info("Closing all arangodb client connections");
        arangoDB.getValue().ifPresent(ArangoDB::shutdown);
    }
}
