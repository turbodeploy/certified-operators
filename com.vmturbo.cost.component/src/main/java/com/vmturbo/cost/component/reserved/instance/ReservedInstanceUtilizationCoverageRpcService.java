package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.List;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceImplBase;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * A rpc service for get reserved instance utilization and coverage stats.
 */
public class ReservedInstanceUtilizationCoverageRpcService extends ReservedInstanceUtilizationCoverageServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    private final ProjectedRICoverageAndUtilStore projectedRICoverageStore;

    private final TimeFrameCalculator timeFrameCalculator;

    private final Clock clock;

    private static final int PROJECTED_STATS_TIME_IN_FUTURE_HOURS = 1;

    public ReservedInstanceUtilizationCoverageRpcService(
            @Nonnull final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore,
            @Nonnull final ReservedInstanceCoverageStore reservedInstanceCoverageStore,
            @Nonnull final ProjectedRICoverageAndUtilStore projectedRICoverageStore,
            @Nonnull final TimeFrameCalculator timeFrameCalculator,
            @Nonnull final Clock clock) {
        this.reservedInstanceUtilizationStore = reservedInstanceUtilizationStore;
        this.reservedInstanceCoverageStore = reservedInstanceCoverageStore;
        this.projectedRICoverageStore = projectedRICoverageStore;
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
            statRecords.add(createProjectedRICoverageStats(statRecords, filter));
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
            statRecords.add(createProjectedRICoverageStats(statRecords, filter));
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
        final ReservedInstanceUtilizationFilter.Builder filterBuilder = ReservedInstanceUtilizationFilter.newBuilder();
        if (request.hasRegionFilter()) {
            filterBuilder.addAllScopeId(request.getRegionFilter().getRegionIdList())
                       .setScopeEntityType(EntityType.REGION_VALUE);
        } else if (request.hasAvailabilityZoneFilter()) {
            filterBuilder.addAllScopeId(request.getAvailabilityZoneFilter().getAvailabilityZoneIdList())
                .setScopeEntityType(EntityType.AVAILABILITY_ZONE_VALUE);
        } else if (request.hasAccountFilter()) {
            filterBuilder.addAllScopeId(request.getAccountFilter().getAccountIdList())
                .setScopeEntityType(EntityType.BUSINESS_ACCOUNT_VALUE);
        }
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
    private ReservedInstanceCoverageFilter createReservedInstanceCoverageFilter(GetReservedInstanceCoverageStatsRequest request) {
        final ReservedInstanceCoverageFilter.Builder filterBuilder = ReservedInstanceCoverageFilter.newBuilder();
        if (request.hasRegionFilter()) {
            filterBuilder.addAllScopeId(request.getRegionFilter().getRegionIdList())
                .setScopeEntityType(EntityType.REGION_VALUE);
        } else if (request.hasAvailabilityZoneFilter()) {
            filterBuilder.addAllScopeId(request.getAvailabilityZoneFilter().getAvailabilityZoneIdList())
                .setScopeEntityType(EntityType.AVAILABILITY_ZONE_VALUE);
        } else if (request.hasAccountFilter()) {
            filterBuilder.addAllScopeId(request.getAccountFilter().getAccountIdList())
                .setScopeEntityType(EntityType.BUSINESS_ACCOUNT_VALUE);
        } else if (request.hasEntityFilter()) {
            filterBuilder.addAllScopeId(request.getEntityFilter().getEntityIdList());
            // No entity type.
        }
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

    private ReservedInstanceStatsRecord createProjectedRICoverageStats(
                    @Nonnull final List<ReservedInstanceStatsRecord> currentStatRecords,
                    @Nonnull final ReservedInstanceFilter filter) {
        final float usedCouponsTotal = getProjectedRICoverageCouponTotal(filter);
        final long projectedTime = clock.instant()
            .plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();
        // TODO (Alexey, Oct 24 2019): Instead of again computing the total capacity stats for the
        //  projected stats, we use the one from the last record. This is wrong since capacity may
        //  have changed in the projected state. Also if currentStatRecords is empty we use used
        //  coupons for capacity which is also incorrect.
        final float capacity = currentStatRecords.isEmpty()
            ? usedCouponsTotal
            : currentStatRecords.get(currentStatRecords.size() - 1).getCapacity().getTotal();
        return ReservedInstanceUtil.createRIStatsRecord(capacity, usedCouponsTotal, projectedTime);
    }

    private float getProjectedRICoverageCouponTotal(
                    @Nonnull final ReservedInstanceFilter filter) {
        return (float)projectedRICoverageStore.getScopedProjectedEntitiesRICoverages(filter)
            .values().stream()
            .flatMap(map -> map.values().stream())
            .mapToDouble(i -> i)
            .sum();
    }
}
