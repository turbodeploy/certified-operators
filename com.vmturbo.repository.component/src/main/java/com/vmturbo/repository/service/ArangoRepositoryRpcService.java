package com.vmturbo.repository.service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.repository.RepositoryDTO;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.DeleteTopologyRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.EntityFilter;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanCombinedStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.PlanTopologyStatsResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RepositoryOperationResponseCode;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RequestDetails;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyIDFactory;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufReader;
import com.vmturbo.repository.topology.protobufs.TopologyProtobufsManager;

/**
 * Implementation of RepositoryService that uses ArangoDB for both plan and realtime.
 *
 * <p>Note that this is currently only used for plans. It is still capable of handling realtime
 * topologies, but for such topologies it has been replaced by the
 * {@link TopologyGraphRepositoryRpcService} for efficiency reasons.</p>
 */
public class ArangoRepositoryRpcService extends RepositoryServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyLifecycleManager topologyLifecycleManager;

    private final TopologyProtobufsManager topologyProtobufsManager;

    private final GraphDBService graphDBService;

    /**
     * A service for retrieving plan entity stats.
     */
    private final PlanStatsService planStatsService;

    private final PartialEntityConverter partialEntityConverter;

    private final int maxEntitiesPerChunk; // the max number of entities to send in a single message

    private final TopologyIDFactory topologyIDFactory;

    public ArangoRepositoryRpcService(@Nonnull final TopologyLifecycleManager topologyLifecycleManager,
                                      @Nonnull final TopologyProtobufsManager topologyProtobufsManager,
                                      @Nonnull final GraphDBService graphDBService,
                                      @Nonnull final PlanStatsService planStatsService,
                                      @Nonnull final PartialEntityConverter partialEntityConverter,
                                      final int maxEntitiesPerChunk,
                                      @Nonnull final TopologyIDFactory topologyIDFactory) {
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.topologyProtobufsManager = Objects.requireNonNull(topologyProtobufsManager);
        this.graphDBService = Objects.requireNonNull(graphDBService);
        this.planStatsService = Objects.requireNonNull(planStatsService);
        this.partialEntityConverter = partialEntityConverter;
        this.maxEntitiesPerChunk = maxEntitiesPerChunk;
        this.topologyIDFactory = topologyIDFactory;
    }

    private boolean validateDeleteTopologyRequest(DeleteTopologyRequest request,
            StreamObserver<RepositoryOperationResponse> responseObserver) {

        if (!request.hasTopologyId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology ID missing")
                .asException());
            return false;
        }

        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology Context ID missing")
                .asException());
            return false;
        }

        if (!request.hasTopologyType()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology Type missing")
                .asException());
            return false;
        }

        return true;
    }

    @Override
    public void deleteTopology(DeleteTopologyRequest request,
            StreamObserver<RepositoryOperationResponse> responseObserver) {
        if (!validateDeleteTopologyRequest(request, responseObserver)) {
            return;
        }

        // Map the topology type from the request to the enum used by TopologyId
        final TopologyType topologyType = TopologyType.mapTopologyType(request.getTopologyType());
        logger.debug("Deleting topology with id:{}, contextId:{} and type:{}.",
                request.getTopologyId(), request.getTopologyContextId(), topologyType);
        try {
            topologyLifecycleManager.deleteTopology(
                topologyIDFactory.createTopologyID(request.getTopologyContextId(),
                            request.getTopologyId(),
                            topologyType));
            final RepositoryOperationResponse responseBuilder =
                    RepositoryOperationResponse.newBuilder()
                        .setResponseCode(RepositoryOperationResponseCode.OK)
                        .build();
            responseObserver.onNext(responseBuilder);
            responseObserver.onCompleted();

        } catch (TopologyDeletionException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }

    }

    @Override
    public void retrieveTopology(final RepositoryDTO.RetrieveTopologyRequest topologyRequest,
                                 final StreamObserver<RepositoryDTO.RetrieveTopologyResponse> responseObserver) {
        final long topologyID = topologyRequest.getTopologyId();

        try {
            logger.debug("Retrieving topology for {} with filter {}", topologyID,
                    topologyRequest.getEntityFilter());
            final TopologyProtobufReader reader =
                    topologyProtobufsManager.createTopologyProtobufReader(
                        topologyID,
                        topologyRequest.hasEntityFilter() ?
                                Optional.of(topologyRequest.getEntityFilter()) : Optional.empty());
            while (reader.hasNext()) {
                for (List<ProjectedTopologyEntity> chunk :
                    Lists.partition(reader.nextChunk(), maxEntitiesPerChunk)) {
                    final RepositoryDTO.RetrieveTopologyResponse responseChunk =
                        RepositoryDTO.RetrieveTopologyResponse.newBuilder()
                            .addAllEntities(Collections2.transform(chunk, ProjectedTopologyEntity::getEntity))
                            .build();
                    responseObserver.onNext(responseChunk);
                }
            }
            responseObserver.onCompleted();
        } catch (NoSuchElementException nse) {
            responseObserver.onError(Status
                .NOT_FOUND
                .withDescription(String.format("Cannot find topology with ID %s", topologyID))
                .asException());
        } catch (RuntimeException e) {
            responseObserver.onError(Status.UNKNOWN.withCause(e)
                                                   .withDescription(e.getMessage())
                                                   .asException());
        }
    }

    @Override
    public void retrieveTopologyEntities(RetrieveTopologyEntitiesRequest request,
                                         StreamObserver<PartialEntityBatch> responseObserver) {

        if (!request.hasTopologyContextId() || !request.hasTopologyType()) {
            logger.error("Missing parameters for retrieve topology entities: " + request);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Missing parameters for retrieve topology entities")
                    .asException());
            return;
        }

        final TopologyType topologyType = TopologyType.mapTopologyType(request.getTopologyType());

        Optional<TopologyID> topologyIdOpt =
            topologyLifecycleManager.getTopologyId(request.getTopologyContextId(), topologyType);

        if (!topologyIdOpt.isPresent()) {
            logger.warn("No topology exists with context {} and type {}. Returning empty results.",
                request.getTopologyContextId(), topologyType);
            responseObserver.onCompleted();
            return;
        }

        final TopologyID topologyID = topologyIdOpt.get();
        final Either<String, Collection<TopologyEntityDTO>> result =
            graphDBService.retrieveTopologyEntities(topologyID,
                ImmutableSet.copyOf(request.getEntityOidsList()));

        Collection<TopologyEntityDTO> filteredEntities = result.isRight()
                ? filterEntityByType(request, result.get())
                : Collections.emptyList();
        // send the results in batches, if needed
        Iterators.partition(filteredEntities.iterator(), maxEntitiesPerChunk).forEachRemaining(chunk -> {
            PartialEntityBatch.Builder batch = PartialEntityBatch.newBuilder();
            chunk.forEach(e -> batch.addEntities(
                partialEntityConverter.createPartialEntity(e, request.getReturnType())));

            logger.debug("Sending entity batch of {} items", batch.getEntitiesCount());
            responseObserver.onNext(batch.build());
        });

        responseObserver.onCompleted();
    }

    private Collection<TopologyEntityDTO> filterEntityByType (RetrieveTopologyEntitiesRequest request,
                                                              Collection<TopologyEntityDTO> entities) {
        if (!request.getEntityTypeList().isEmpty()) {
            return entities.stream()
                    .filter(e -> request.getEntityTypeList().contains(e.getEntityType()))
                    .collect(Collectors.toList());
        } else {
            return entities;
        }
    }

    /**
     * Fetch stats from the requested Plan Topology.
     *
     * <p>This topology may be either plan source or plan projected (i.e. the output of Market Analysis).
     * This data is taken from the Arango DB "raw TopologyApiDTO" storage. The reason for using
     * this rather than the ArangoDB graph representation of the topology appears to be that the
     * graph representations of the entities does not contain all of the commodity information
     * needed to calculate these stats.</p>
     *
     * @param request the topologyId, time, entityType, and commodity filter to retrieve stats for
     * @param responseObserver the sync for entity stats constructed here and returned to caller
     */
    @Override
    public void getPlanTopologyStats(@Nonnull PlanTopologyStatsRequest request,
                          @Nonnull StreamObserver<PlanTopologyStatsResponse> responseObserver) {
        final long topologyId = request.getTopologyId();
        logger.debug("Retrieving plan stats for topology {}.", topologyId);
        // create a filter on relatedEntityType
        final StatsFilter statsFilter = request.getRequestDetails().getFilter();
        final Predicate<TopologyEntityDTO> entityPredicate = newEntityMatcher(request);

        // Create a reader to read the entities for the provided topology
        final TopologyProtobufReader reader;
        try {
            reader = topologyProtobufsManager.createTopologyProtobufReader(topologyId, Optional.empty());
        } catch (NoSuchElementException e) {
            // If the topology couldn't be found, notify the responseObserver and return.
            final String errorMessage = "Topology with ID: " + topologyId + "not found.";
            logger.error(errorMessage, e.getMessage());
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(errorMessage)
                .asException());
            return;
        }

        // Lookup the topology type so that the StatEpoch will be set correctly
        StatEpoch statEpoch = topologyLifecycleManager.getTopologyId(topologyId)
            .map(TopologyID::getType)
            .map(topologyType -> topologyType == TopologyType.SOURCE ? StatEpoch.PLAN_SOURCE
                : StatEpoch.PLAN_PROJECTED)
            .orElse(null);

        final PaginationParameters paginationParams = request.getRequestDetails().getPaginationParams();
        final Type entityReturnType = request.getRequestDetails().getReturnType();
        // Let the plan stats service do the rest of the heavy lifting.
        // The response will be sent through the responseObserver. Streaming in this way may perform
        // better than returning the entire result here and subsequently streaming.
        planStatsService.getPlanTopologyStats(reader, statEpoch, statsFilter, entityPredicate,
            paginationParams, entityReturnType, responseObserver);
    }

    /**
     * Fetch the combined stats (both source and projected) related to a given plan execution
     *
     * <p>The response will contain a paginated list containing both the entities and their
     * associated stats, representing both the source and projected plan topologies.</p>
     *
     * <p>The request includes a field indicating which (source or projected) plan topology to
     * sort on.</p>
     *
     * <p>The stats in each StatSnapshot returned will be restricted to commodities in the
     * commodity_name list in the StatsFilter parameter, if any. Otherwise, all stats will be
     * returned.</p>
     *
     * <p>The stats in each StatSnapshot returned will be restricted to the entity type specified
     * in the StatsFilter related_entity_type, if any. Otherwise, stats from entities of all types
     * will be returned.</p>
     *
     * <p>Returns (through the responseObserver) a sequence of PlanEntityStats, containing entities
     * and their associated stats derived from both the source and projected plan topologies</p>
     *
     * @param request a {@link PlanCombinedStatsRequest} representing the contextId, entityType,
     *                and commodity filter for which to retrieve stats
     * @param responseObserver stream for entity stats constructed here to be returned to the caller
     */
    @Override
    public void getPlanCombinedStats(@Nonnull PlanCombinedStatsRequest request,
            @Nonnull StreamObserver<PlanCombinedStatsResponse> responseObserver) {
        // Check that all required fields are set on the request
        if (!validatePlanCombinedStatsRequest(request, responseObserver)) {
            return;
        }
        final long contextId = request.getTopologyContextId();
        logger.debug("Retrieving plan combined stats for context {}.", contextId);
        // Lookup the plan source and projected topology IDs, based on the contextId
        final Optional<TopologyID> sourceTopologyIdOpt =
            lookupTopologyId(contextId, TopologyType.SOURCE);

        final Optional<TopologyID> projectedTopologyIdOpt =
            lookupTopologyId(contextId, TopologyType.PROJECTED);

        if (!sourceTopologyIdOpt.isPresent() || !projectedTopologyIdOpt.isPresent()) {
            responseObserver.onCompleted();
            return;
        }

        final TopologyID sourceTopologyID = sourceTopologyIdOpt.get();
        final TopologyID projectedTopologyID = projectedTopologyIdOpt.get();

        // create a filter on relatedEntityType
        final StatsFilter statsFilter = request.getRequestDetails().getFilter();
        final Predicate<TopologyEntityDTO> entityPredicate = newEntityMatcher(request);

        // Create a reader to read the entities for each of source and projected topologies
        final TopologyProtobufReader sourceReader;
        final TopologyProtobufReader projectedReader;
        try {
            sourceReader = topologyProtobufsManager
                .createTopologyProtobufReader(sourceTopologyID.getTopologyId(), Optional.empty());
            projectedReader = topologyProtobufsManager
                .createTopologyProtobufReader(projectedTopologyID.getTopologyId(), Optional.empty());
        } catch (NoSuchElementException e) {
            // If either topology couldn't be found, notify the responseObserver and return.
            final String errorMessage = "Topology for plan ID: " + contextId + "not found.";
            logger.error(errorMessage, e.getMessage());
            responseObserver.onError(Status.NOT_FOUND
                .withDescription(errorMessage)
                .asException());
            return;
        }

        // Determine which topology to sort on
        // Retrieve from the request the primary topology type for this request, which will be used
        // for sorting
        final TopologyType topologyToSortOn = TopologyType.mapTopologyType(request.getTopologyToSortOn());
        final PaginationParameters paginationParams = request.getRequestDetails().getPaginationParams();
        final Type entityReturnType = request.getRequestDetails().getReturnType();
        // Let the plan stats service do the rest of the heavy lifting.
        // The response will be sent through the responseObserver. Streaming in this way may perform
        // better than returning the entire result here and subsequently streaming.
        planStatsService.getPlanCombinedStats(sourceReader, projectedReader, statsFilter,
            entityPredicate, topologyToSortOn, paginationParams, entityReturnType, responseObserver);
    }

    private boolean validatePlanCombinedStatsRequest(PlanCombinedStatsRequest request,
                            StreamObserver<PlanCombinedStatsResponse> responseObserver) {

        if (!request.hasTopologyContextId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology Context ID missing")
                .asException());
            return false;
        }

        if (!request.hasTopologyToSortOn()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Topology to sort on missing")
                .asException());
            return false;
        }

        return true;
    }

    private Optional<TopologyID> lookupTopologyId(long topologyContextId, TopologyType topologyType) {
        Optional<TopologyID> topologyIdOpt =
            topologyLifecycleManager.getTopologyId(topologyContextId, topologyType);

        if (!topologyIdOpt.isPresent()) {
            logger.warn("No topology exists with context {} and type {}. Returning empty results.",
                topologyContextId, topologyType);
        }

        return topologyIdOpt;
    }

    /**
     * A predicate over a TopologyEntityDTO that will return true if the entity matches the
     * filters in the request.
     *
     * TODO: implement the filtering in the PropertyValueFilter component of the request - OM-33678
     *
     * @param request the External REST API version of the entity type to select
     * @return true if the entity type of the given TopologyEntityDTO matches the given relatedEntityType
     */
    @Nonnull
    private static Predicate<TopologyEntityDTO> newEntityMatcher(
        @Nonnull final PlanTopologyStatsRequest request) {
        final RequestDetails details = request.getRequestDetails();
        EntityFilter entityFilter = details.hasEntityFilter() ? details.getEntityFilter() : null;
        String relatedEntityType = details.hasRelatedEntityType() ? details.getRelatedEntityType() : null;
        return newEntityMatcher(entityFilter, relatedEntityType);
    }

    /**
     * A predicate over a TopologyEntityDTO that will return true if the entity matches the
     * filters in the request.
     *
     * @param request the External REST API version of the entity type to select
     * @return true if the entity type of the given TopologyEntityDTO matches the given relatedEntityType
     */
    @Nonnull
    private static Predicate<TopologyEntityDTO> newEntityMatcher(
        @Nonnull final PlanCombinedStatsRequest request) {
        final RequestDetails details = request.getRequestDetails();
        EntityFilter entityFilter = details.hasEntityFilter() ? details.getEntityFilter() : null;
        String relatedEntityType = details.hasRelatedEntityType() ? details.getRelatedEntityType() : null;
        return newEntityMatcher(entityFilter, relatedEntityType);
    }

    /**
     * A predicate over a TopologyEntityDTO that will return true if the entity matches the
     * filters in the request.
     *
     * @param entityFilter a list of entities to match
     * @param relatedEntityType a type of entities to match
     * @return true if the entity type of the given TopologyEntityDTO matches the given relatedEntityType
     */
    @Nonnull
    private static Predicate<TopologyEntityDTO> newEntityMatcher(
        @Nullable final EntityFilter entityFilter,
        @Nullable final String relatedEntityType) {
        logger.debug("Creating entity matcher using relatedType: {}, entity filter: {}.",
            relatedEntityType, entityFilter);
        final Optional<Set<Long>> requestedEntities = entityFilter != null ?
            Optional.of(Sets.newHashSet(entityFilter.getEntityIdsList())) :
            Optional.empty();
        final Predicate<TopologyEntityDTO> entityTypePredicate = relatedEntityType != null ?
            matchEntityType(relatedEntityType) : noFilterPredicate();
        return (entity) -> {
            if (requestedEntities.isPresent() && !requestedEntities.get().contains(entity.getOid())) {
                return false;
            } else {
                return entityTypePredicate.test(entity);
            }
        };
    }

    /**
     * A predicate over a TopologyEntityDTO that will return true if the entity type matches
     * the String version of entity type passed from the External REST API.
     *
     * @param relatedEntityType the External REST API version of the entity type to select
     * @return true if the entity type of the given TopologyEntityDTO matches the given relatedEntityType
     */
    private static Predicate<TopologyEntityDTO> matchEntityType(String relatedEntityType) {
        return (entity) ->
            UIEntityType.fromString(relatedEntityType) == UIEntityType.fromEntity(entity);
    }

    private static Predicate<TopologyEntityDTO> noFilterPredicate() {
        return (entity) -> true;
    }


}
