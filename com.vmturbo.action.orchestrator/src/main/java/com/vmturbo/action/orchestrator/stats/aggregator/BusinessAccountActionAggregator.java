package com.vmturbo.action.orchestrator.stats.aggregator;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.StatusRuntimeException;

import com.vmturbo.action.orchestrator.stats.ManagementUnitType;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.EntityWithConnections;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.proactivesupport.DataMetricCounter;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.topology.graph.OwnershipGraph;

/**
 * Aggregates action stats for business accounts in the cloud.
 */
public class BusinessAccountActionAggregator extends CloudActionAggregator {

    private final RepositoryServiceBlockingStub repositoryService;

    /**
     * We use the ownership graph to accelerate lookups of business account parents of entities
     * involved in actions.
     */
    private OwnershipGraph<EntityWithConnections> ownershipGraph = OwnershipGraph.empty();

    protected BusinessAccountActionAggregator(@Nonnull final LocalDateTime snapshotTime,
                                              @Nonnull final RepositoryServiceBlockingStub repositoryService) {
        super(snapshotTime);
        this.repositoryService = Objects.requireNonNull(repositoryService);
    }

    @Override
    public void start() {
        try (DataMetricTimer ignored = Metrics.INIT_TIME_SECONDS.startTimer()) {
            ownershipGraph = retrieveOwnershipGraph();
            if (ownershipGraph.size() > 0) {
                logger.info("Retrieved ownership graph of size {}", ownershipGraph.size());
            }
        }
    }

    @Override
    protected Optional<Long> getAggregationEntity(long entityOid) {
        return ownershipGraph.getOwners(entityOid).stream()
            .filter(e -> e.getEntityType() == ApiEntityType.BUSINESS_ACCOUNT.typeNumber())
            .map(EntityWithConnections::getOid)
            .findFirst();
    }

    @Override
    protected void incrementEntityWithMissedAggregationEntity() {
        Metrics.MISSING_OWNERS_COUNTER.increment();
    }

    @Nonnull
    @Override
    protected ManagementUnitType getManagementUnitType() {
        return ManagementUnitType.BUSINESS_ACCOUNT;
    }

    @Nonnull
    private OwnershipGraph<EntityWithConnections> retrieveOwnershipGraph() {
        final OwnershipGraph.Builder<EntityWithConnections> graphBuilder =
            OwnershipGraph.newBuilder(EntityWithConnections::getOid);

        final RetrieveTopologyEntitiesRequest.Builder entitiesReqBldr = RetrieveTopologyEntitiesRequest.newBuilder()
            .setReturnType(Type.WITH_CONNECTIONS)
            .addEntityType(ApiEntityType.BUSINESS_ACCOUNT.typeNumber());


        // Get all the business accounts and add them to the ownership graph.
        try {
            RepositoryDTOUtil.topologyEntityStream(
                repositoryService.retrieveTopologyEntities(entitiesReqBldr.build()))
                .map(PartialEntity::getWithConnections)
                .forEach(ba -> ba.getConnectedEntitiesList().stream()
                    // Get the entities owned by the business account and add them to the graph.
                    .filter(connectedEntity -> connectedEntity.getConnectionType() == ConnectionType.OWNS_CONNECTION)
                    .forEach(relevantEntity -> graphBuilder.addOwner(ba, relevantEntity.getConnectedEntityId())));
            return graphBuilder.build();
        } catch (StatusRuntimeException e) {
            logger.error("Failed to retrieve ownership graph entities due to repository error: {}",
                e.getMessage());
            return OwnershipGraph.empty();
        }
    }

    /**
     * Metrics for {@link BusinessAccountActionAggregator}.
     */
    private static class Metrics {

        private static final DataMetricSummary INIT_TIME_SECONDS = DataMetricSummary.builder()
            .withName("ao_action_ba_agg_init_seconds")
            .withHelp("Information about how long it took to initialize the business account aggregator.")
            .build()
            .register();

        private static final DataMetricCounter MISSING_OWNERS_COUNTER = DataMetricCounter.builder()
            .withName("ao_action_ba_agg_missing_owners_count")
            .withHelp("Count of cloud/hybrid entities with missing business account owners.")
            .build()
            .register();

    }

    /**
     * Factory class for {@link BusinessAccountActionAggregator}s.
     */
    public static class BusinessAccountActionAggregatorFactory implements ActionAggregatorFactory<BusinessAccountActionAggregator> {
        private final RepositoryServiceBlockingStub repositoryServiceBlockingStub;

        /**
         * Constructor for the aggregator factory.
         *
         * @param repositoryServiceBlockingStub Stub to access the repository.
         */
        public BusinessAccountActionAggregatorFactory(@Nonnull final RepositoryServiceBlockingStub repositoryServiceBlockingStub) {
            this.repositoryServiceBlockingStub = Objects.requireNonNull(repositoryServiceBlockingStub);
        }

        @Override
        public BusinessAccountActionAggregator newAggregator(@Nonnull final LocalDateTime snapshotTime) {
            return new BusinessAccountActionAggregator(snapshotTime, repositoryServiceBlockingStub);
        }
    }
}
