package com.vmturbo.repository.service;

import static com.vmturbo.common.protobuf.PaginationProtoUtil.DEFAULT_PAGINATION;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.PaginationProtoUtil;
import com.vmturbo.common.protobuf.PaginationProtoUtil.PaginatedResults;
import com.vmturbo.common.protobuf.common.Pagination.PaginationParameters;
import com.vmturbo.common.protobuf.repository.EntityConstraints.CurrentPlacement;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraint;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraintsRequest;
import com.vmturbo.common.protobuf.repository.EntityConstraints.EntityConstraintsResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacements;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacementsRequest;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacementsResponse;
import com.vmturbo.common.protobuf.repository.EntityConstraints.PotentialPlacementsResponse.MatchedEntity;
import com.vmturbo.common.protobuf.repository.EntityConstraints.RelationType;
import com.vmturbo.common.protobuf.repository.EntityConstraintsServiceGrpc.EntityConstraintsServiceImplBase;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.proactivesupport.DataMetricSummary;
import com.vmturbo.proactivesupport.DataMetricTimer;
import com.vmturbo.repository.listener.realtime.LiveTopologyStore;
import com.vmturbo.repository.listener.realtime.RepoGraphEntity;
import com.vmturbo.repository.listener.realtime.SourceRealtimeTopology;
import com.vmturbo.repository.service.ConstraintsCalculator.ConstraintGrouping;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Service to calculate the constraints of an entity. This is used in the constraints view.
 */
public class EntityConstraintsRpcService extends EntityConstraintsServiceImplBase {

    private final Logger logger = LogManager.getLogger();
    private final LiveTopologyStore liveTopologyStore;
    private static final DataMetricSummary CALCULATE_CONSTRAINTS_DURATION_SUMMARY =
        DataMetricSummary.builder()
            .withName("repo_calculate_constraints_duration_seconds")
            .withHelp("Duration in seconds it takes repository to construct constraints for an entity.")
            .build()
            .register();
    private static final DataMetricSummary CALCULATE_POTENTIAL_PLACEMENTS_DURATION_SUMMARY =
        DataMetricSummary.builder()
            .withName("repo_calculate_potential_placements_duration_seconds")
            .withHelp("Duration in seconds it takes repository to calculate potential placements for a set of constraints.")
            .build()
            .register();
    private final ConstraintsCalculator constraintsCalculator;

    /**
     * Entity Constraints RPC service. We calculate the constraints only for real-time entities.
     *
     * @param liveTopologyStore live topology store
     * @param constraintsCalculator this is used to calculate the constraints
     */
    public EntityConstraintsRpcService(@Nonnull LiveTopologyStore liveTopologyStore,
                                       @Nonnull ConstraintsCalculator constraintsCalculator) {
        this.liveTopologyStore = liveTopologyStore;
        this.constraintsCalculator = constraintsCalculator;
    }

    /**
     * A service that is used to get constraints by entity oid.
     *
     * @param request an entity constraint request that contains an entity oid
     * @param responseObserver An observer on which to write entity constraints.
     */
    @Override
    public void getConstraints(@Nonnull final EntityConstraintsRequest request,
                               @Nonnull final StreamObserver<EntityConstraintsResponse> responseObserver) {
        Optional<SourceRealtimeTopology> realTimeTopology = liveTopologyStore.getSourceTopology();
        if (!realTimeTopology.isPresent()) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("No real time source topology present")
                .asException());
            return;
        }
        TopologyGraph<RepoGraphEntity> entityGraph = realTimeTopology.get().entityGraph();
        Optional<RepoGraphEntity> entity = entityGraph.getEntity(request.getOid());
        if (!entity.isPresent()) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("Oid " + request.getOid() + " not found in source topology")
                .asException());
            return;
        }
        final DataMetricTimer timer = CALCULATE_CONSTRAINTS_DURATION_SUMMARY.startTimer();
        RepoGraphEntity repoEntity = entity.get();
        Map<Integer, List<ConstraintGrouping>> constraintGroupingsByProviderEntityType =
            constraintsCalculator.calculateConstraints(repoEntity, entityGraph);

        EntityConstraintsResponse response = buildEntityConstraintsResponse(
            constraintGroupingsByProviderEntityType, entityGraph, repoEntity.getDiscoveringTargetIds());
        responseObserver.onNext(response);
        responseObserver.onCompleted();
        timer.observe();
        logger.info("Calculated constraints for oid {} in {} seconds", request.getOid(), timer.getTimeElapsedSecs());
    }

    /**
     * Converts the constraintGroupings to EntityConstraintsResponse.
     *
     * @param constraintGroupings constraintGroupings for the requested entity
     * @param entityGraph entity graph
     * @param discoveringTargetIds discovering target ids of entity
     * @return EntityConstraintsResponse
     */
    private EntityConstraintsResponse buildEntityConstraintsResponse(
        @Nonnull final Map<Integer, List<ConstraintGrouping>> constraintGroupings,
        @Nonnull final TopologyGraph<RepoGraphEntity> entityGraph,
        @Nonnull final Stream<Long> discoveringTargetIds) {
        // Convert the constraintGroupings map to protobuf structures
        final EntityConstraintsResponse.Builder response = EntityConstraintsResponse.newBuilder();
        constraintGroupings.forEach((providerEntityType, constraintGroupingsForProviderEntityType) -> {
            constraintGroupingsForProviderEntityType.forEach(constraintGrouping -> {
                // We create an EntityConstraint for each constraintGrouping
                final EntityConstraint.Builder entityConstraint = EntityConstraint.newBuilder();
                entityConstraint.setRelationType(RelationType.BOUGHT);
                // set the current placement
                constraintGrouping.getCurrentPlacement().ifPresent(currPlacement -> {
                    entityGraph.getEntity(currPlacement).ifPresent(curPlacementEntity -> {
                        entityConstraint.setCurrentPlacement(CurrentPlacement.newBuilder()
                            .setOid(curPlacementEntity.getOid())
                            .setDisplayName(curPlacementEntity.getDisplayName())
                            .build());
                        entityConstraint.setEntityType(curPlacementEntity.getEntityType());
                    });
                });
                // Overall potential placements should be the intersection of the
                // potential placements for all of the commodity types
                final Set<Long> overallPotentialPlacements = Sets.newHashSet();
                if (!constraintGrouping.getPotentialSellersByCommodityType().isEmpty()) {
                    // Initialize the overallPotentialPlacements to the first set of sellers
                    overallPotentialPlacements.addAll(
                        constraintGrouping.getPotentialSellersByCommodityType().values().iterator().next().getSellers());
                }
                constraintGrouping.getPotentialSellersByCommodityType().forEach((commType, potentialSellers) -> {
                    entityConstraint.addPotentialPlacements(
                        PotentialPlacements.newBuilder()
                            .setCommodityType(commType)
                            .setNumPotentialPlacements(potentialSellers.getSellers().size())
                            .setScopeDisplayName(potentialSellers.getScopeDisplayName()));
                    overallPotentialPlacements.retainAll(potentialSellers.getSellers());
                });
                entityConstraint.setNumPotentialPlacements(overallPotentialPlacements.size());
                response.addEntityConstraint(entityConstraint);
            });
        });
        response.addAllDiscoveringTargetIds(discoveringTargetIds.collect(Collectors.toList()));
        return response.build();
    }

    /**
     * A service that is used to get the potential placements.
     * Given a set of constraints, this method will return the set of sellers which can satisfy all
     * the constraints.
     *
     * @param request a potential placements request that contains an entity oid
     * @param responseObserver An observer on which to write entity constraints.
     */
    @Override
    public void getPotentialPlacements(PotentialPlacementsRequest request,
                                       StreamObserver<PotentialPlacementsResponse> responseObserver) {
        Optional<SourceRealtimeTopology> realTimeTopology = liveTopologyStore.getSourceTopology();
        if (!realTimeTopology.isPresent()) {
            responseObserver.onError(Status.NOT_FOUND
                .withDescription("No real time source topology present")
                .asException());
            return;
        }

        final DataMetricTimer timer = CALCULATE_POTENTIAL_PLACEMENTS_DURATION_SUMMARY.startTimer();

        final PaginationParameters paginationParameters = request.hasPaginationParams()
            ? request.getPaginationParams() : DEFAULT_PAGINATION;

        try {
            PaginationProtoUtil.validatePaginationParams(paginationParameters);
        } catch (IllegalArgumentException e) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription(e.getMessage()).asException());
            return;
        }

        TopologyGraph<RepoGraphEntity> entityGraph = realTimeTopology.get().entityGraph();
        Set<Integer> potentialEntityTypes = Sets.newHashSet(request.getPotentialEntityTypesList());
        Set<CommodityType> constraints = Sets.newHashSet(request.getCommodityTypeList());
        final PaginatedResults<MatchedEntity> paginatedResults = constraintsCalculator.calculatePotentialPlacements(
            potentialEntityTypes, constraints, entityGraph, paginationParameters,
            realTimeTopology.get().topologyInfo().getTopologyContextId());

        responseObserver.onNext(PotentialPlacementsResponse.newBuilder()
            .addAllEntities(paginatedResults.nextPageEntities())
            .setPaginationResponse(paginatedResults.paginationResponse())
            .build());
        responseObserver.onCompleted();

        timer.observe();
        logger.info("Calculated potential placements for oid {} with constraints {} in {} seconds",
            request.getOid(), constraintsCalculator.getPrintableConstraints(constraints), timer.getTimeElapsedSecs());
    }
}