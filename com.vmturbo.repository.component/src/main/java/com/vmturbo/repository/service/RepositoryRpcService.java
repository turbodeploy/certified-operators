package com.vmturbo.repository.service;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.UserSessionContext;
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
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceImplBase;
import com.vmturbo.common.protobuf.stats.Stats.StatEpoch;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.TopologyDTO.ProjectedTopologyEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.api.tracing.Tracing;
import com.vmturbo.repository.plan.db.PlanEntityFilter;
import com.vmturbo.repository.plan.db.PlanEntityFilter.PlanEntityFilterConverter;
import com.vmturbo.repository.plan.db.PlanEntityStore;
import com.vmturbo.repository.plan.db.TopologyNotFoundException;
import com.vmturbo.repository.plan.db.TopologySelection;
import com.vmturbo.repository.topology.TopologyID;
import com.vmturbo.repository.topology.TopologyID.TopologyType;
import com.vmturbo.repository.topology.TopologyLifecycleManager;
import com.vmturbo.repository.topology.TopologyLifecycleManager.TopologyDeletionException;

/**
 * Implementation of RepositoryService for plans.
 *
 * <p>Note that this is currently only used for plans. It is still capable of handling realtime
 * topologies, but for such topologies it has been replaced by the
 * {@link TopologyGraphRepositoryRpcService} for efficiency reasons.</p>
 */
public class RepositoryRpcService extends RepositoryServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyLifecycleManager topologyLifecycleManager;


    /**
     * A service for retrieving plan entity stats.
     */
    private final PlanStatsService planStatsService;

    private final PartialEntityConverter partialEntityConverter;

    private final int maxEntitiesPerChunk; // the max number of entities to send in a single message

    private final PlanEntityStore planEntityStore;

    private final PlanEntityFilterConverter planEntityFilterConverter;

    private final UserSessionContext userSessionContext;

    /**
     * Repository Service.
     */
    public RepositoryRpcService(@Nonnull final TopologyLifecycleManager topologyLifecycleManager,
                                      @Nonnull final PlanStatsService planStatsService,
                                      @Nonnull final PartialEntityConverter partialEntityConverter,
                                      final int maxEntitiesPerChunk,
                                      @Nonnull final PlanEntityStore planEntityStore,
                                      @Nonnull final PlanEntityFilterConverter planEntityFilterConverter,
                                      @Nonnull final UserSessionContext userSessionContext) {
        this.topologyLifecycleManager = Objects.requireNonNull(topologyLifecycleManager);
        this.planStatsService = Objects.requireNonNull(planStatsService);
        this.partialEntityConverter = partialEntityConverter;
        this.maxEntitiesPerChunk = maxEntitiesPerChunk;
        this.planEntityStore = planEntityStore;
        this.planEntityFilterConverter = planEntityFilterConverter;
        this.userSessionContext = userSessionContext;
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
                new TopologyID(request.getTopologyContextId(),
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

    private boolean retrieveTopologyFromSql(final RepositoryDTO.RetrieveTopologyRequest topologyRequest,
                                 final StreamObserver<RepositoryDTO.RetrieveTopologyResponse> responseObserver) {
        try {
            TopologySelection topologySelection = planEntityStore.getTopologySelection(topologyRequest.getTopologyId());
            PlanEntityFilter planEntityFilter = planEntityFilterConverter.newPlanFilter(topologyRequest.getEntityFilter());
            Iterator<PartialEntity> retIt = planEntityStore.getPlanEntities(topologySelection, planEntityFilter, topologyRequest.getReturnType()).iterator();
            Iterators.partition(retIt, maxEntitiesPerChunk)
                .forEachRemaining(chunk -> {
                    RetrieveTopologyResponse batch = RetrieveTopologyResponse.newBuilder()
                        .addAllEntities(chunk)
                        .build();
                    Tracing.log(() -> "Returning chunk of " + batch.getEntitiesCount() + " entities.");
                    logger.debug("Returning topology batch of {} items ({} bytes)", batch.getEntitiesCount(), batch.getSerializedSize());
                    responseObserver.onNext(batch);
                });
            responseObserver.onCompleted();
            return true;
        } catch (TopologyNotFoundException e) {
            logger.warn("Topology not found in SQL database: {}", e.toString());
            return false;
        }
    }

    @Override
    public void retrieveTopology(final RepositoryDTO.RetrieveTopologyRequest topologyRequest,
                                 final StreamObserver<RepositoryDTO.RetrieveTopologyResponse> responseObserver) {

        if (retrieveTopologyFromSql(topologyRequest, responseObserver)) {
            return;
        } else {
            final long topologyID = topologyRequest.getTopologyId();
            responseObserver.onError(Status
                    .NOT_FOUND
                    .withDescription(String.format("Cannot find topology with ID %s", topologyID))
                    .asException());
        }
    }

    private boolean retrieveTopologyEntitiesFromSql(
            RetrieveTopologyEntitiesRequest request,
            StreamObserver<PartialEntityBatch> responseObserver) {
        // TODO - what do we do with scoping for plans?
        try {
            final TopologySelection topologySelection;
            if (request.hasTopologyId()) {
                topologySelection = planEntityStore.getTopologySelection(request.getTopologyId());
            } else {
                topologySelection = planEntityStore.getTopologySelection(request.getTopologyContextId(), request.getTopologyType());
            }
            Iterator<PartialEntity> retIt = planEntityStore.getPlanEntities(topologySelection,
                    planEntityFilterConverter.newPlanFilter(request), request.getReturnType()).iterator();
            Iterators.partition(retIt, maxEntitiesPerChunk)
                .forEachRemaining(chunk -> {
                    PartialEntityBatch batch = PartialEntityBatch.newBuilder()
                        .addAllEntities(chunk)
                        .build();
                    Tracing.log(() -> "Sending chunk of " + batch.getEntitiesCount() + " entities.");
                    logger.debug("Sending entity batch of {} items ({} bytes)", batch.getEntitiesCount(), batch.getSerializedSize());
                    responseObserver.onNext(batch);
                });
            responseObserver.onCompleted();
            return true;
        } catch (TopologyNotFoundException e) {
            logger.warn("Topology not found in SQL database: {}", e.toString());
            return false;
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

        if (retrieveTopologyEntitiesFromSql(request, responseObserver)) {
            // Fully handled by SQL, yay.
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

        responseObserver.onCompleted();
    }

    private boolean getPlanTopologyStatsFromSql(@Nonnull PlanTopologyStatsRequest request,
                                                @Nonnull StreamObserver<PlanTopologyStatsResponse> responseObserver) {
        try {
            TopologySelection topologySelection = planEntityStore.getTopologySelection(request.getTopologyId());
            PlanEntityFilter filter = planEntityFilterConverter.newPlanFilter(request.getRequestDetails());
            final String relatedEntityType = request.getRequestDetails().hasRelatedEntityType()
                ? request.getRequestDetails().getRelatedEntityType() : null;
            StatEpoch epoch = topologySelection.getTopologyType() == RepositoryDTO.TopologyType.SOURCE ? StatEpoch.PLAN_SOURCE : StatEpoch.PLAN_PROJECTED;
            Iterator<List<ProjectedTopologyEntity>> entities =
                    planEntityStore.getHackyStatsEntities(topologySelection, filter);
            Predicate<TopologyEntityDTO> entityPredicate = RepositoryRpcService.newEntityMatcher(request);

            planStatsService.getPlanTopologyStats(entities, epoch, request.getRequestDetails().getFilter(),
                entityPredicate,
                request.getRequestDetails().getPaginationParams(),
                request.getRequestDetails().getReturnType(),
                responseObserver,
                relatedEntityType);
            return true;
        } catch (TopologyNotFoundException e) {
            // Move up to warn when we deprecate Arango.
            logger.debug("Topology not found in SQL database: {}", e.toString());
            return false;
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
        if (getPlanTopologyStatsFromSql(request, responseObserver)) {
            // Served the request from SQL database.
            return;
        } else {
            final long topologyId = request.getTopologyId();
            final String errorMessage = "Topology with ID: " + topologyId + "not found.";
            logger.error(errorMessage);
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription(errorMessage)
                    .asException());
            return;
        }
    }

    private boolean getPlanCombinedStatsFromSql(PlanCombinedStatsRequest request,
                    StreamObserver<PlanCombinedStatsResponse> responseObserver) {
        try {
            final TopologySelection srcSelection = planEntityStore.getTopologySelection(request.getTopologyContextId(), RepositoryDTO.TopologyType.SOURCE);
            final TopologySelection projSelection = planEntityStore.getTopologySelection(request.getTopologyContextId(), RepositoryDTO.TopologyType.PROJECTED);
            PlanEntityFilter sourcePlanEntityFilter = planEntityFilterConverter.newPlanFilter(request.getRequestDetails());
            PlanEntityFilter projectedPlanEntityFilter = planEntityFilterConverter.newPlanFilter(request.getRequestDetails());
            Iterator<List<ProjectedTopologyEntity>> source = planEntityStore.getHackyStatsEntities(srcSelection, sourcePlanEntityFilter);
            Iterator<List<ProjectedTopologyEntity>> projected = planEntityStore.getHackyStatsEntities(projSelection, projectedPlanEntityFilter);

            final TopologyType topologyToSortOn = TopologyType.mapTopologyType(request.getTopologyToSortOn());
            final PaginationParameters paginationParams = request.getRequestDetails().getPaginationParams();
            final Type entityReturnType = request.getRequestDetails().getReturnType();
            final String relatedEntityType = request.getRequestDetails().hasRelatedEntityType()
                ? request.getRequestDetails().getRelatedEntityType() : null;
            Predicate<TopologyEntityDTO> entityPredicate = RepositoryRpcService.newEntityMatcher(request);
            planStatsService.getPlanCombinedStats(source,
                projected,
                request.getRequestDetails().getFilter(),
                // The entity predicate is handled at the database level.
                entityPredicate,
                topologyToSortOn,
                paginationParams,
                entityReturnType,
                responseObserver,
                relatedEntityType);
            return true;
        } catch (TopologyNotFoundException e) {
            logger.warn("Topology not found in SQL database: {}", e.toString());
            return false;
        }
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

        if (getPlanCombinedStatsFromSql(request, responseObserver)) {
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
     * TODO: implement the filtering in the PropertyValueFilter component of the request - OM-33678
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
        final Optional<Set<Long>> requestedEntities = entityFilter != null
                ? Optional.of(Sets.newHashSet(entityFilter.getEntityIdsList())) : Optional.empty();
        final Predicate<TopologyEntityDTO> entityTypePredicate = relatedEntityType != null
                ? matchEntityType(relatedEntityType) : noFilterPredicate();
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
            ApiEntityType.fromString(relatedEntityType) == ApiEntityType.fromEntity(entity);
    }

    private static Predicate<TopologyEntityDTO> noFilterPredicate() {
        return (entity) -> true;
    }

}
