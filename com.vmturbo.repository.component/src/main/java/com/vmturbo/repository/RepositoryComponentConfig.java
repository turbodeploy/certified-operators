package com.vmturbo.repository;

import java.sql.SQLException;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import com.vmturbo.auth.api.authorization.UserSessionConfig;
import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.plan.orchestrator.api.impl.PlanGarbageDetector;
import com.vmturbo.plan.orchestrator.api.impl.PlanOrchestratorClientConfig;
import com.vmturbo.repository.listener.RepositoryPlanGarbageCollector;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.plan.db.DbAccessConfig;
import com.vmturbo.repository.plan.db.SQLPlanEntityStore;
import com.vmturbo.repository.service.PartialEntityConverter;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.search.SearchResolver;
import com.vmturbo.topology.graph.search.filter.TopologyFilterFactory;
import com.vmturbo.topology.graph.supplychain.GlobalSupplyChainCalculator;
import com.vmturbo.topology.graph.supplychain.SupplyChainCalculator;

/**
 * Spring configuration for repository component.
 */
@Configuration
@Import({UserSessionConfig.class, PlanOrchestratorClientConfig.class})
public class RepositoryComponentConfig {

    private static final String GRAPH_NAME = "seGraph";
    // svc : ServiceEntity vertex collection.
    // Using abbreviated name to consume less space as the
    // name is referenced in the edges in the edgeCollection
    private static final String VERTEX_COLLECTION_NAME = "svc";
    private static final String EDGE_COLLECTION_NAME = "seProviderEdgeCol";
    private static final String TOPOLOGY_PROTO_COLLECTION_NAME = "topology_proto";

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

    @Value("${repositoryRealtimeTopologyDropDelaySecs:30}")
    private int repositoryRealtimeTopologyDropDelaySecs;

    @Value("${numberOfExpectedRealtimeSourceDB:2}")
    private int numberOfExpectedRealtimeSourceDB;

    @Value("${numberOfExpectedRealtimeProjectedDB:2}")
    private int numberOfExpectedRealtimeProjectedDB;

    @Value("${collectionReplicaCount:1}")
    private int collectionReplicaCount;

    @Value("${collectionNumShards:1}")
    private int collectionNumShards;

    @Value("${collectionWaitForSync:true}")
    private boolean collectionWaitForSync;

    /**
     * If true, use SQL backend for plan data storage.
     */
    @Value("${useSqlForPlans:true}")
    private boolean useSqlForPLans;

    /**
     * When inserting plan data we insert this many rows in one call.
     */
    @Value("${plan.sqlInsertionChunkSize:5000}")
    private int sqlInsertionChunkSize;

    /**
     * When deleting plan data we delete this many rows in one call.
     */
    @Value("${plan.sqlDeletionChunkSize:10000}")
    private int sqlDeletionChunkSize;

    @Autowired
    private UserSessionConfig userSessionConfig;

    @Autowired
    private PlanOrchestratorClientConfig planOrchestratorClientConfig;

    @Autowired
    private DbAccessConfig dbAccessConfig;

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

    /**
     * Resolves searches against a topology graph.
     *
     * @return The {@link SearchResolver}.
     */
    @Bean
    public SearchResolver<RepoGraphEntity> searchResolver() {
        return new SearchResolver<>(new TopologyFilterFactory<RepoGraphEntity>());
    }

    /**
     * Stores and allows queries on information about entities in plans.
     *
     * @return The {@link SQLPlanEntityStore}.
     */
    @Bean
    public SQLPlanEntityStore sqlPlanEntityStore() {
        try {
            return new SQLPlanEntityStore(dbAccessConfig.dsl(), partialEntityConverter(), new SupplyChainCalculator(), sqlInsertionChunkSize, sqlDeletionChunkSize,
                    userSessionConfig.userSessionContext());
        } catch (SQLException | UnsupportedDialectException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new BeanCreationException("Failed to create SQLPlanEntityStore", e);
        }
    }

    /**
     * Converter for entities into {@link com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity}
     * format for gRPC outputs.
     *
     * @return The {@link PartialEntityConverter}.
     */
    @Bean
    public PartialEntityConverter partialEntityConverter() {
        return new PartialEntityConverter(liveTopologyStore());
    }


    /**
     * Listener for plan deletion.
     *
     * @return The listener.
     */
    @Bean
    public PlanGarbageDetector repositoryPlanGarbageDetector() {
        final RepositoryPlanGarbageCollector collector =
            new RepositoryPlanGarbageCollector(topologyManager(), sqlPlanEntityStore());
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
        return new TopologyLifecycleManager(realtimeTopologyContextId,
                new ScheduledThreadPoolExecutor(1), liveTopologyStore(),
                repositoryRealtimeTopologyDropDelaySecs, numberOfExpectedRealtimeSourceDB,
                numberOfExpectedRealtimeProjectedDB, collectionReplicaCount,
                sqlPlanEntityStore(), useSqlForPLans);
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
}
