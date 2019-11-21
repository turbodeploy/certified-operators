package com.vmturbo.repository.service;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.protobuf.util.JsonFormat;

import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.ProjectedRealtimeTopology;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
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

    private final PartialEntityConverter partialEntityConverter;

    private final UserSessionContext userSessionContext;

    public TopologyGraphRepositoryRpcService(@Nonnull final LiveTopologyStore liveTopologyStore,
                                      @Nonnull final ArangoRepositoryRpcService arangoRepoRpcService,
                                      @Nonnull final PartialEntityConverter partialEntityConverter,
                                      final long realtimeTopologyContextId,
                                      final int maxEntitiesPerChunk,
                                      final UserSessionContext userSessionContext) {
        this.arangoRepoRpcService = arangoRepoRpcService;
        this.liveTopologyStore = Objects.requireNonNull(liveTopologyStore);
        this.partialEntityConverter = Objects.requireNonNull(partialEntityConverter);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.maxEntitiesPerChunk = maxEntitiesPerChunk;
        this.userSessionContext = userSessionContext;
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
                                         StreamObserver<PartialEntityBatch> responseObserver) {
        final TopologyType topologyType = TopologyType.mapTopologyType(request.getTopologyType());
        final boolean realtime = !request.hasTopologyContextId()  ||
            request.getTopologyContextId() == realtimeTopologyContextId;
        Tracing.log(() -> "Getting topology entities. Request: " + JsonFormat.printer().print(request));
        if (realtime) {
            final Stream<PartialEntity> filteredEntities;
            if (topologyType == TopologyType.SOURCE) {
                final Optional<SourceRealtimeTopology> realtimeTopology = liveTopologyStore.getSourceTopology();

                filteredEntities = realtimeTopology
                    .map(topo -> filterEntityByType(request, topo.entityGraph()
                            .getEntities(ImmutableSet.copyOf(request.getEntityOidsList())))
                                .map(repoGraphEntity -> partialEntityConverter.createPartialEntity(
                                    repoGraphEntity, request.getReturnType())))
                    .orElse(Stream.empty());
            } else {
                final Optional<ProjectedRealtimeTopology> projectedTopology = liveTopologyStore.getProjectedTopology();
                filteredEntities = projectedTopology
                    .map(projectedTopo -> projectedTopo.getEntities(
                            ImmutableSet.copyOf(request.getEntityOidsList()),
                            ImmutableSet.copyOf(request.getEntityTypeList()))
                        .map(topoEntity -> partialEntityConverter.createPartialEntity(topoEntity, request.getReturnType())))
                    .orElse(Stream.empty());
            }

            // if the user is scoped, attach a filter to the matching entities.
            Predicate<PartialEntity> accessFilter = userSessionContext.isUserScoped()
                    ? e -> userSessionContext.getUserAccessScope().contains(TopologyDTOUtil.getOid(e))
                    : e -> true;

            // send the results in batches, if needed
            Iterators.partition(filteredEntities.filter(accessFilter).iterator(), maxEntitiesPerChunk)
                .forEachRemaining(chunk -> {
                    PartialEntityBatch batch = PartialEntityBatch.newBuilder()
                        .addAllEntities(chunk)
                        .build();
                    Tracing.log(() -> "Sending chunk of " + batch.getEntitiesCount() + " entities.");
                    logger.debug("Sending entity batch of {} items ({} bytes)", batch.getEntitiesCount(), batch.getSerializedSize());
                    responseObserver.onNext(batch);
                });

            responseObserver.onCompleted();
        } else {
            arangoRepoRpcService.retrieveTopologyEntities(request, responseObserver);
        }
    }

    private Stream<RepoGraphEntity> filterEntityByType(RetrieveTopologyEntitiesRequest request,
                                                         Stream<RepoGraphEntity> entities) {
        if (!request.getEntityTypeList().isEmpty()) {
            return entities
                    .filter(e -> request.getEntityTypeList().contains(e.getEntityType()));
        } else {
            return entities;
        }
    }

    @Override
    public void getPlanTopologyStats(@Nonnull final PlanTopologyStatsRequest request,
                                     @Nonnull final StreamObserver<PlanTopologyStatsResponse> responseObserver) {
        arangoRepoRpcService.getPlanTopologyStats(request, responseObserver);
    }

    @Override
    public void getPlanCombinedStats(@Nonnull final PlanCombinedStatsRequest request,
                                     @Nonnull final StreamObserver<PlanCombinedStatsResponse> responseObserver) {
        arangoRepoRpcService.getPlanCombinedStats(request, responseObserver);
    }
}
