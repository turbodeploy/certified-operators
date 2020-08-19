package com.vmturbo.cost.component.reserved.instance;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoveredEntitiesResponse.EntitiesCoveredByReservedInstance;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceImplBase;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.cost.component.reserved.instance.filter.EntityReservedInstanceMappingFilter;
import com.vmturbo.cost.component.reserved.instance.filter.PlanProjectedEntityReservedInstanceMappingFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;

/**
 * A RPC service for getting reserved instance utilization and coverage statistics.
 */
public class ReservedInstanceUtilizationCoverageRpcService extends ReservedInstanceUtilizationCoverageServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    private final ProjectedRICoverageAndUtilStore projectedRICoverageStore;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final AccountRIMappingStore accountRIMappingStore;

    private final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore;

    private final TimeFrameCalculator timeFrameCalculator;

    private final long realtimeTopologyContextId;

    /**
     * Constructor for ReservedInstanceUtilizationCoverageRpcService. The parameters are the shared
     * data structures or unique instances created at startup.
     *
     * @param reservedInstanceUtilizationStore
     *     The instance of ReservedInstanceUtilizationStore
     * @param reservedInstanceCoverageStore
     *     The instance of ReservedInstanceCoverageStore
     * @param projectedRICoverageStore
     *     The instance of ProjectedRICoverageStore
     * @param entityReservedInstanceMappingStore
     *     The instance of EntityReservedInstanceMappingStore
     * @param accountRIMappingStore
     *      The instance of AccountRIMappingStore
     * @param planProjectedRICoverageAndUtilStore
     *     The instance of PlanProjectedRICoverageAndUtilStore
     * @param timeFrameCalculator
     *     The instance of TimeFrameCalculator
     * @param realtimeTopologyContextId realtime topology context ID
     */
    public ReservedInstanceUtilizationCoverageRpcService(
            @Nonnull final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore,
            @Nonnull final ReservedInstanceCoverageStore reservedInstanceCoverageStore,
            @Nonnull final ProjectedRICoverageAndUtilStore projectedRICoverageStore,
            @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final AccountRIMappingStore accountRIMappingStore,
            @Nonnull final PlanProjectedRICoverageAndUtilStore planProjectedRICoverageAndUtilStore,
            @Nonnull final TimeFrameCalculator timeFrameCalculator,
            final long realtimeTopologyContextId) {
        this.reservedInstanceUtilizationStore = reservedInstanceUtilizationStore;
        this.reservedInstanceCoverageStore = reservedInstanceCoverageStore;
        this.projectedRICoverageStore = projectedRICoverageStore;
        this.entityReservedInstanceMappingStore =
                        Objects.requireNonNull(entityReservedInstanceMappingStore);
        this.accountRIMappingStore = accountRIMappingStore;
        this.planProjectedRICoverageAndUtilStore = Objects.requireNonNull(planProjectedRICoverageAndUtilStore);
        this.timeFrameCalculator = timeFrameCalculator;
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void getReservedInstanceUtilizationStats(
            GetReservedInstanceUtilizationStatsRequest request,
            StreamObserver<GetReservedInstanceUtilizationStatsResponse> responseObserver) {
        if (request.hasStartDate() != request.hasEndDate()) {
            logger.error("Missing start date and end date for query reserved instance utilization stats!");
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Must provide start date " +
                    "and end date for query reserved instance utilization stats").asException());
            return;
        }

        try {
            final ReservedInstanceUtilizationFilter filter =
                    createReservedInstanceUtilizationFilter(request);
            final List<ReservedInstanceStatsRecord> statRecords =
                    reservedInstanceUtilizationStore.getReservedInstanceUtilizationStatsRecords(filter);

            final long currentTime = System.currentTimeMillis();
            if (shouldAddLatestStats(currentTime, request.getStartDate(), request.getEndDate())) {
                final Collection<ReservedInstanceStatsRecord> latestStats =
                        reservedInstanceUtilizationStore.getReservedInstanceUtilizationStatsRecords(
                                filter.toLatestFilter());
                statRecords.addAll(latestStats);
                logger.trace("Adding latest RI utilization stats: {}", () -> latestStats);
            }

            if (request.hasTopologyContextId() && request.getTopologyContextId() != realtimeTopologyContextId) {
                final List<ReservedInstanceStatsRecord> statsRecords = planProjectedRICoverageAndUtilStore
                                .getPlanReservedInstanceUtilizationStatsRecords(request.getTopologyContextId(),
                                        filter.getRegionFilter().getRegionIdList());
                if (!CollectionUtils.isEmpty(statsRecords)) {
                    statRecords.add(statsRecords.get(0));
                }
            } else {
                // Add projected RI Utilization point
                        projectedRICoverageStore.getReservedInstanceUtilizationStats(
                                filter, request.getIncludeBuyRiUtilization(), request.getEndDate())
                                .ifPresent(statRecords::add);

            }
            final GetReservedInstanceUtilizationStatsResponse response =
                    GetReservedInstanceUtilizationStatsResponse.newBuilder()
                            .addAllReservedInstanceStatsRecords(statRecords)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance coverage stats.")
                    .asException());
        }
    }

    @Override
    public void getReservedInstanceCoverageStats(
            GetReservedInstanceCoverageStatsRequest request,
            StreamObserver<GetReservedInstanceCoverageStatsResponse> responseObserver) {
        // The start and end date need to both be set, or both be unset.
        // Both unset means "look for most recent stats".
        if (request.hasStartDate() != request.hasEndDate()) {
            logger.error("Missing start date and end date for query reserved instance coverage stats!");
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Must provide start date " +
                    " and end date for query reserved instance coverage stats").asException());
            return;
        }
        try {
            final ReservedInstanceCoverageFilter filter =
                    createReservedInstanceCoverageFilter(request);
            final List<ReservedInstanceStatsRecord> statRecords = reservedInstanceCoverageStore
                .getReservedInstanceCoverageStatsRecords(filter);

            // Add latest RI Coverage point
            final long currentTime = System.currentTimeMillis();
            if (shouldAddLatestStats(currentTime, request.getStartDate(), request.getEndDate())) {
                final Collection<ReservedInstanceStatsRecord> latestStats =
                        reservedInstanceCoverageStore.getReservedInstanceCoverageStatsRecords(
                                filter.toLatestFilter());
                statRecords.addAll(latestStats);
                logger.trace("Adding latest RI coverage stats: {}", () -> latestStats);
            }

            if (request.hasTopologyContextId() && request.getTopologyContextId() != realtimeTopologyContextId) {
                final List<ReservedInstanceStatsRecord> statsRecords = planProjectedRICoverageAndUtilStore
                                .getPlanReservedInstanceCoverageStatsRecords(request.getTopologyContextId(),
                                        filter.getRegionFilter().getRegionIdList());
                if (!CollectionUtils.isEmpty(statsRecords)) {
                    statRecords.add(statsRecords.get(0));
                }
            } else {
                // Add projected RI Coverage point
                projectedRICoverageStore.getReservedInstanceCoverageStats(
                        filter, request.getIncludeBuyRiCoverage(), request.getEndDate())
                        .ifPresent(statRecords::add);
            }

            final GetReservedInstanceCoverageStatsResponse response =
                    GetReservedInstanceCoverageStatsResponse.newBuilder()
                            .addAllReservedInstanceStatsRecords(statRecords)
                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance coverage stats.")
                    .asException());
        }
    }

    private boolean shouldAddLatestStats(final long currentTime, final long startTime,
                                         final long endTime) {
        return currentTime >= startTime && currentTime <= endTime;
    }

    @Override
    public void getEntityReservedInstanceCoverage(GetEntityReservedInstanceCoverageRequest request,
                    StreamObserver<GetEntityReservedInstanceCoverageResponse> responseObserver) {
        try {
            logger.debug("Request for Entity RI coverage: {}", request);
            EntityReservedInstanceMappingFilter filter = EntityReservedInstanceMappingFilter
                    .newBuilder()
                    .entityFilter(request.getEntityFilter())
                    .build();

            ReservedInstanceCoverageFilter reservedInstanceCoverageFilter = ReservedInstanceCoverageFilter
                    .newBuilder()
                    .entityFilter(request.getEntityFilter())
                    .build();

            final Map<Long, Set<Coverage>> riCoverageByEntity = entityReservedInstanceMappingStore
                    .getRICoverageByEntity(filter);

            final Map<Long, Double> entitiesCouponCapacity = reservedInstanceCoverageStore
                    .getEntitiesCouponCapacity(reservedInstanceCoverageFilter);

            final Map<Long, EntityReservedInstanceCoverage> retCoverage = entitiesCouponCapacity.entrySet()
                    .stream()
                    .map(capacityEntry -> {
                        long entityOid = capacityEntry.getKey();
                        int capacity = capacityEntry.getValue().intValue();

                        final Map<Long, Double> riCoverage =
                                riCoverageByEntity.getOrDefault(entityOid, Collections.emptySet())
                                        .stream()
                                        .collect(ImmutableMap.toImmutableMap(
                                                Coverage::getReservedInstanceId,
                                                Coverage::getCoveredCoupons,
                                                Double::sum));

                        return EntityReservedInstanceCoverage.newBuilder()
                                .setEntityId(entityOid)
                                .setEntityCouponCapacity(capacity)
                                .putAllCouponsCoveredByRi(riCoverage)
                                .build();
                    }).collect(ImmutableMap.toImmutableMap(
                            EntityReservedInstanceCoverage::getEntityId,
                            Function.identity()));

            logger.debug("Retrieved and returning RI coverage for {} entities.",
                            retCoverage.size());
            responseObserver.onNext(GetEntityReservedInstanceCoverageResponse.newBuilder()
                            .putAllCoverageByEntityId(retCoverage).build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL.withDescription(
                            "Failed to retrieve RI coverage from DB: " + e.getLocalizedMessage())
                            .asException());
        }
    }

    /**
     * Create {@link ReservedInstanceUtilizationFilter} based on input different filters and
     * timestamp.
     *
     * @param request The {@link GetReservedInstanceUtilizationStatsRequest}.
     * @return a {@link ReservedInstanceUtilizationFilter}.
     */
    private ReservedInstanceUtilizationFilter createReservedInstanceUtilizationFilter(
            @Nonnull final GetReservedInstanceUtilizationStatsRequest request) {

        final ReservedInstanceUtilizationFilter.Builder filterBuilder = ReservedInstanceUtilizationFilter.newBuilder()
                .regionFilter(request.getRegionFilter())
                .accountFilter(request.getAccountFilter())
                .availabilityZoneFilter(request.getAvailabilityZoneFilter());

        if (request.hasStartDate()) {
            filterBuilder.startDate(Instant.ofEpochMilli(request.getStartDate()));

            final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
            filterBuilder.timeFrame(timeFrame);
        }

        if (request.hasEndDate()) {
            filterBuilder.endDate(Instant.ofEpochMilli(request.getEndDate()));
        }
        return filterBuilder.build();
    }

    /**
     * Create {@link ReservedInstanceCoverageFilter} based on input different filters and
     * timestamp.
     *
     * @param request The {@link GetReservedInstanceCoverageStatsRequest}.
     * @return a {@link ReservedInstanceCoverageFilter}.
     */
    private ReservedInstanceCoverageFilter createReservedInstanceCoverageFilter(GetReservedInstanceCoverageStatsRequest request) {
        final ReservedInstanceCoverageFilter.Builder filterBuilder = ReservedInstanceCoverageFilter.newBuilder()
                .regionFilter(request.getRegionFilter())
                .accountFilter(request.getAccountFilter())
                .availabilityZoneFilter(request.getAvailabilityZoneFilter())
                .entityFilter(request.getEntityFilter());

        if (request.hasStartDate()) {
            filterBuilder.startDate(Instant.ofEpochMilli(request.getStartDate()));

            final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
            filterBuilder.timeFrame(timeFrame);

        }

        if (request.hasEndDate()) {
            filterBuilder.endDate(Instant.ofEpochMilli(request.getEndDate()));

        }

        return filterBuilder.build();
    }

    @Override
    public void getProjectedEntityReservedInstanceCoverageStats(
                    Cost.GetProjectedEntityReservedInstanceCoverageRequest request,
                    StreamObserver<Cost.GetProjectedEntityReservedInstanceCoverageResponse> responseObserver) {
        final ReservedInstanceCoverageFilter filter = ReservedInstanceCoverageFilter.newBuilder()
                .entityFilter(request.getEntityFilter())
                .topologyContextId(request.getTopologyContextId())
                .build();
        Long topologyContextId = request.getTopologyContextId();
        final Map<Long, EntityReservedInstanceCoverage> retCoverage;
        if (!Objects.equals(realtimeTopologyContextId, topologyContextId)) {
            PlanProjectedEntityReservedInstanceMappingFilter planProjectedRICoverageByEntityFilter = PlanProjectedEntityReservedInstanceMappingFilter
                .newBuilder()
                .entityFilter(request.getEntityFilter())
                .topologyContextId(topologyContextId)
                .build();
            retCoverage = planProjectedRICoverageAndUtilStore.getPlanProjectedRiCoverage(
                topologyContextId,
                planProjectedRICoverageByEntityFilter);
        } else {
            retCoverage = projectedRICoverageStore.getScopedProjectedEntitiesRICoverages(filter);
        }
        final Cost.GetProjectedEntityReservedInstanceCoverageResponse response =
                        Cost.GetProjectedEntityReservedInstanceCoverageResponse.newBuilder()
                                        .putAllCoverageByEntityId(retCoverage).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void getReservedInstanceCoveredEntities(
            final GetReservedInstanceCoveredEntitiesRequest request,
            final StreamObserver<GetReservedInstanceCoveredEntitiesResponse> responseObserver) {
        final GetReservedInstanceCoveredEntitiesResponse.Builder response =
                GetReservedInstanceCoveredEntitiesResponse.newBuilder();
        final Map<Long, EntitiesCoveredByReservedInstance.Builder> riToCoveredEntities =
                new HashMap<>();
        entityReservedInstanceMappingStore.getEntitiesCoveredByReservedInstances(
                request.getReservedInstanceIdList()).forEach(
                (reservedInstanceId, coveredEntities) -> riToCoveredEntities.computeIfAbsent(
                        reservedInstanceId, id -> EntitiesCoveredByReservedInstance.newBuilder())
                        .addAllCoveredEntityId(coveredEntities));
        accountRIMappingStore.getUndiscoveredAccountsCoveredByReservedInstances(
                request.getReservedInstanceIdList()).forEach(
                (reservedInstanceId, coveredUndiscoveredAccounts) -> riToCoveredEntities.computeIfAbsent(
                        reservedInstanceId, id -> EntitiesCoveredByReservedInstance.newBuilder())
                        .addAllCoveredUndiscoveredAccountId(coveredUndiscoveredAccounts));
        riToCoveredEntities.forEach(
                (reservedInstanceId, coveredEntities) -> response.putEntitiesCoveredByReservedInstances(
                        reservedInstanceId, coveredEntities.build()));
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }
}
