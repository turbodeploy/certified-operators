package com.vmturbo.repository.service;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityBatch;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.topology.TopologyID.TopologyType;

/**
 * An implementation of {@link RepositoryServiceImplBase} (see RepositoryDTO.proto) that uses
 * the in-memory topology for resolving queries on the realtime topology (source or projected).
 */
public class TopologyGraphRepositoryRpcService extends RepositoryServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ArangoRepositoryRpcService arangoRepoRpcService;

    private final int maxEntitiesPerChunk; // the max number of entities to send in a single message

    private final long realtimeTopologyContextId;

    private final LiveTopologyStore liveTopologyStore;

    public TopologyGraphRepositoryRpcService(@Nonnull final LiveTopologyStore liveTopologyStore,
                                      @Nonnull final ArangoRepositoryRpcService arangoRepoRpcService,
                                      final long realtimeTopologyContextId,
                                      final int maxEntitiesPerChunk) {
        this.arangoRepoRpcService = arangoRepoRpcService;
        this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.maxEntitiesPerChunk = maxEntitiesPerChunk;
    }

    @Override
    public void deleteTopology(DeleteTopologyRequest request,
            StreamObserver<RepositoryOperationResponse> responseObserver) {
        arangoRepoRpcService.deleteTopology(request, responseObserver);
    }

    @Override
    public void retrieveTopology(final RepositoryDTO.RetrieveTopologyRequest topologyRequest,
                                 final StreamObserver<RepositoryDTO.RetrieveTopologyResponse> responseObserver) {
        // TODO (roman, May 22 2019): We could check for the realtime topology here, but realistically
        // we only use this endpoint for plans, so it's probably not necessary.
        arangoRepoRpcService.retrieveTopology(topologyRequest, responseObserver);
    }

    @Override
    public void retrieveTopologyEntities(RetrieveTopologyEntitiesRequest request,
                                         StreamObserver<EntityBatch> responseObserver) {
        final TopologyType topologyType = (request.getTopologyType() ==
            RetrieveTopologyEntitiesRequest.TopologyType.PROJECTED) ? TopologyType.PROJECTED :
            TopologyType.SOURCE;
        final boolean realtime = !request.hasTopologyContextId()  ||
            request.getTopologyContextId() == realtimeTopologyContextId;
        if (realtime) {
            final Stream<TopologyEntityDTO> filteredEntities;
            if (topologyType == TopologyType.SOURCE) {
                final Optional<SourceRealtimeTopology> realtimeTopology = liveTopologyStore.getSourceTopology();

                filteredEntities = realtimeTopology
                    .map(topo -> filterEntityByType(request, topo.entityGraph()
                        .getEntities(ImmutableSet.copyOf(request.getEntityOidsList()))
                        .map(RepoGraphEntity::getTopologyEntity)))
                    .orElse(Stream.empty());
            } else {
                final Optional<ProjectedRealtimeTopology> projectedTopology = liveTopologyStore.getProjectedTopology();
                filteredEntities = projectedTopology
                    .map(projectedTopo -> projectedTopo.getEntities(
                        ImmutableSet.copyOf(request.getEntityOidsList()),
                        ImmutableSet.copyOf(request.getEntityTypeList())))
                    .orElse(Stream.empty());
            }

            // send the results in batches, if needed
            Iterators.partition(filteredEntities.iterator(), maxEntitiesPerChunk).forEachRemaining(chunk -> {
                EntityBatch batch = EntityBatch.newBuilder()
                    .addAllEntities(chunk)
                    .build();
                logger.debug("Sending entity batch of {} items ({} bytes)", batch.getEntitiesCount(), batch.getSerializedSize());
                responseObserver.onNext(batch);
            });

            responseObserver.onCompleted();
        } else {
            arangoRepoRpcService.retrieveTopologyEntities(request, responseObserver);
        }
    }

    private Stream<TopologyEntityDTO> filterEntityByType(RetrieveTopologyEntitiesRequest request,
                                                         Stream<TopologyEntityDTO> entities) {
        if (!request.getEntityTypeList().isEmpty()) {
            return entities
                    .filter(e -> request.getEntityTypeList().contains(e.getEntityType()));
        } else {
            return entities;
        }
    }
    /**
     * Fetch the stats related to a Plan topology. Depending on the 'startTime' of the
     * request: if there is a 'startTime' and it is in the future, then this request is
     * satisfied from the projected plan topology. If there is no 'startTime' or in the past, then
     * this request is to be to satisfied from the plan input topology (not yet implemented).
     *
     * @param request the parameters for this request, including the plan topology id and a StatsFilter
     *                object describing which stats to include in the result
     * @param responseObserver observer for the PlanTopologyResponse created here
     */
    @Override
    public void getPlanTopologyStats(@Nonnull final PlanTopologyStatsRequest request,
                      @Nonnull final StreamObserver<PlanTopologyStatsResponse> responseObserver) {
        arangoRepoRpcService.getPlanTopologyStats(request, responseObserver);
    }
}
