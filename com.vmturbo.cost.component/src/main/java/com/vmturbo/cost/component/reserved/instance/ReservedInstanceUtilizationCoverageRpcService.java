package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.google.common.collect.ImmutableMap;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest.EntityRICoverageUpload.Coverage;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceImplBase;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.reserved.instance.filter.EntityReservedInstanceMappingFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A RPC service for getting reserved instance utilization and coverage statistics.
 */
public class ReservedInstanceUtilizationCoverageRpcService extends ReservedInstanceUtilizationCoverageServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    private final ProjectedRICoverageAndUtilStore projectedRICoverageStore;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final TimeFrameCalculator timeFrameCalculator;

    private final Clock clock;

    private static final int PROJECTED_STATS_TIME_IN_FUTURE_HOURS = 1;

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
     * @param timeFrameCalculator
     *     The instance of TimeFrameCalculator
     * @param clock
     *     The instance of Clock
     */
    public ReservedInstanceUtilizationCoverageRpcService(
            @Nonnull final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore,
            @Nonnull final ReservedInstanceCoverageStore reservedInstanceCoverageStore,
            @Nonnull final ProjectedRICoverageAndUtilStore projectedRICoverageStore,
                    @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
            @Nonnull final TimeFrameCalculator timeFrameCalculator,
            @Nonnull final Clock clock) {
        this.reservedInstanceUtilizationStore = reservedInstanceUtilizationStore;
        this.reservedInstanceCoverageStore = reservedInstanceCoverageStore;
        this.projectedRICoverageStore = projectedRICoverageStore;
        this.entityReservedInstanceMappingStore =
                        Objects.requireNonNull(entityReservedInstanceMappingStore);
        this.timeFrameCalculator = timeFrameCalculator;
        this.clock = clock;
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
            // Add projected RI Utilization point
            // TODO (Alexey, Oct 24 2019): Respect input filter passed in the request.
            // TODO (Alexey, Oct 24 2019): Currently we use the same method as for RI Coverage.
            //  It looks incorrect. E.g. it doesn't take into account recommended RI purchases.
            statRecords.add(createProjectedRICoverageStats(filter));
            final long currentTime = System.currentTimeMillis();
            if (shouldAddLatestStats(currentTime, request.getStartDate(), request.getEndDate())) {
                final Collection<ReservedInstanceStatsRecord> latestStats =
                        reservedInstanceUtilizationStore.getLatestReservedInstanceStatsRecords(
                                createRIUtilizationLatestStatFilter(request));
                statRecords.addAll(latestStats);
                logger.trace("Adding latest RI utilization stats: {}", () -> latestStats);
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
            // Add projected RI Coverage point
            statRecords.add(createProjectedRICoverageStats(filter));
            // Add latest RI Coverage point
            final long currentTime = System.currentTimeMillis();
            if (shouldAddLatestStats(currentTime, request.getStartDate(), request.getEndDate())) {
                final Collection<ReservedInstanceStatsRecord> latestStats =
                        reservedInstanceCoverageStore.getLatestReservedInstanceStatsRecords(
                                createRICoverageLatestStatFilter(request));
                statRecords.addAll(latestStats);
                logger.trace("Adding latest RI coverage stats: {}", () -> latestStats);
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
                    .addAllScopeId(request.getEntityFilter().getEntityIdList())
                    .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
                    .build();

            ReservedInstanceCoverageFilter reservedInstanceCoverageFilter = ReservedInstanceCoverageFilter
                    .newBuilder()
                    .addAllScopeId(request.getEntityFilter().getEntityIdList())
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
        // Get all business accounts based on scope ID's and scope type.
        final ReservedInstanceUtilizationFilter.Builder filterBuilder =
                addRIUtilizationFilterScope(request,
                        ReservedInstanceUtilizationFilter.newBuilder());
        filterBuilder.setStartDateMillis(request.getStartDate());
        filterBuilder.setEndDateMillis(request.getEndDate());
        final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
        filterBuilder.setTimeFrame(timeFrame);
        return filterBuilder.build();
    }

    /**
     * Create {@link ReservedInstanceCoverageFilter} based on input different filters and
     * timestamp.
     *
     * @param request The {@link GetReservedInstanceCoverageStatsRequest}.
     * @return a {@link ReservedInstanceCoverageFilter}.
     */
    private ReservedInstanceCoverageFilter createReservedInstanceCoverageFilter(
            GetReservedInstanceCoverageStatsRequest request) {
        final ReservedInstanceCoverageFilter.Builder filterBuilder =
                addRICoverageFilterScope(request, ReservedInstanceCoverageFilter.newBuilder());
        filterBuilder.setStartDateMillis(request.getStartDate());
        filterBuilder.setEndDateMillis(request.getEndDate());
        filterBuilder.setTimeFrame(request.hasStartDate() ?
            timeFrameCalculator.millis2TimeFrame(request.getStartDate()) : TimeFrame.LATEST);
        if (request.hasStartDate() && request.hasEndDate()) {
            filterBuilder.setStartDateMillis(request.getStartDate())
                .setEndDateMillis(request.getEndDate());
        } else {
            // Look for last half hour.
            // TODO (roman, Oct 11 2019): This is kind of a hack - what we should do is find the
            //  most recent snapshot in the LATEST timeframe and use that.
            filterBuilder.setStartDateMillis(clock.instant().minus(30, ChronoUnit.MINUTES).toEpochMilli())
                .setEndDateMillis(clock.millis());
        }
        return filterBuilder.build();
    }

    private ReservedInstanceCoverageFilter createRICoverageLatestStatFilter(
            GetReservedInstanceCoverageStatsRequest request) {
        final ReservedInstanceCoverageFilter.Builder filterBuilder =
                addRICoverageFilterScope(request, ReservedInstanceCoverageFilter.newBuilder());
        filterBuilder.setTimeFrame(TimeFrame.LATEST);
        return filterBuilder.build();
    }

    private ReservedInstanceUtilizationFilter createRIUtilizationLatestStatFilter(
            GetReservedInstanceUtilizationStatsRequest request) {
        final ReservedInstanceUtilizationFilter.Builder filterBuilder =
                addRIUtilizationFilterScope(request,
                        ReservedInstanceUtilizationFilter.newBuilder());
        filterBuilder.setTimeFrame(TimeFrame.LATEST);
        return filterBuilder.build();
    }

    private ReservedInstanceUtilizationFilter.Builder addRIUtilizationFilterScope(
            GetReservedInstanceUtilizationStatsRequest request,
            ReservedInstanceUtilizationFilter.Builder filterBuilder) {
        // Get all business accounts based on scope ID's and scope type.
        if (request.hasRegionFilter()) {
            filterBuilder.addAllScopeId(request.getRegionFilter().getRegionIdList())
                    .setScopeEntityType(Optional.of(EntityType.REGION_VALUE));
        } else if (request.hasAvailabilityZoneFilter()) {
            filterBuilder.addAllScopeId(request.getAvailabilityZoneFilter()
                    .getAvailabilityZoneIdList())
                    .setScopeEntityType(Optional.of(EntityType.AVAILABILITY_ZONE_VALUE));
        } else if (request.hasAccountFilter()) {
            filterBuilder.addAllScopeId(request.getAccountFilter().getAccountIdList())
                    .setScopeEntityType(Optional.of(EntityType.BUSINESS_ACCOUNT_VALUE));
        }
        return filterBuilder;
    }

    private ReservedInstanceCoverageFilter.Builder addRICoverageFilterScope(
            GetReservedInstanceCoverageStatsRequest request,
            ReservedInstanceCoverageFilter.Builder filterBuilder) {
        if (request.hasRegionFilter()) {
            filterBuilder.addAllScopeId(request.getRegionFilter().getRegionIdList())
                    .setScopeEntityType(EntityType.REGION_VALUE);
        } else if (request.hasAvailabilityZoneFilter()) {
            filterBuilder.addAllScopeId(request.getAvailabilityZoneFilter()
                    .getAvailabilityZoneIdList())
                    .setScopeEntityType(EntityType.AVAILABILITY_ZONE_VALUE);
        } else if (request.hasAccountFilter()) {
            filterBuilder.addAllScopeId(request.getAccountFilter().getAccountIdList())
                    .setScopeEntityType(EntityType.BUSINESS_ACCOUNT_VALUE);
        } else if (request.hasEntityFilter()) {
            filterBuilder.addAllScopeId(request.getEntityFilter().getEntityIdList());
            // No entity type.
        }
        return filterBuilder;
    }

    private ReservedInstanceStatsRecord createProjectedRICoverageStats(
                        @Nonnull final ReservedInstanceFilter filter) {
        final Map<Long, EntityReservedInstanceCoverage> projectedEntitiesRICoverages =
                projectedRICoverageStore.getScopedProjectedEntitiesRICoverages(filter);
        double usedCouponsTotal = 0d;
        double entityCouponsCapTotal = 0d;
        for (EntityReservedInstanceCoverage entityRICoverage : projectedEntitiesRICoverages.values()) {
            final Map<Long, Double> riMap = entityRICoverage.getCouponsCoveredByRiMap();
            if (riMap != null) {
                usedCouponsTotal += riMap.values().stream().mapToDouble(Double::doubleValue).sum();
            }
            entityCouponsCapTotal += entityRICoverage.getEntityCouponCapacity();
        }
        final long projectedTime = clock.instant()
            .plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();
        return ReservedInstanceUtil.createRIStatsRecord((float)entityCouponsCapTotal,
                (float)usedCouponsTotal, projectedTime);
    }

    @Override
    public void getProjectedEntityReservedInstanceCoverageStats(
                    Cost.GetProjectedEntityReservedInstanceCoverageRequest request,
                    StreamObserver<Cost.GetProjectedEntityReservedInstanceCoverageResponse> responseObserver) {
        final ReservedInstanceCoverageFilter filter = createProjectedEntityFilter(request);
        final Map<Long, EntityReservedInstanceCoverage> retCoverage = projectedRICoverageStore
                .getScopedProjectedEntitiesRICoverages(filter);
        final Cost.GetProjectedEntityReservedInstanceCoverageResponse response =
                        Cost.GetProjectedEntityReservedInstanceCoverageResponse.newBuilder()
                                        .putAllCoverageByEntityId(retCoverage).build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Nonnull
    private ReservedInstanceCoverageFilter createProjectedEntityFilter(
                    @Nonnull Cost.GetProjectedEntityReservedInstanceCoverageRequest request) {
        final ReservedInstanceCoverageFilter.Builder filterBuilder =
                        ReservedInstanceCoverageFilter.newBuilder();
        if (request.hasEntityFilter()) {
            filterBuilder.addAllScopeId(request.getEntityFilter().getEntityIdList());
            // No entity type.
        }
        return filterBuilder.build();
    }
}
