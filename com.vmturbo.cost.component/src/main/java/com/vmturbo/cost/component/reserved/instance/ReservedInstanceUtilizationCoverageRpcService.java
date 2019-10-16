package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceImplBase;
import com.vmturbo.components.common.utils.TimeFrameCalculator;
import com.vmturbo.components.common.utils.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
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
        if (!request.hasStartDate() || !request.hasEndDate()) {
            logger.error("Missing start date and end date for query reserved instance utilization stats!");
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Must provide start date " +
                    "and end date for query reserved instance utilization stats").asException());
        }

        try {
            final Optional<RegionFilter> regionFilter = request.hasRegionFilter()
                    ? Optional.of(request.getRegionFilter())
                    : Optional.empty();
            final Optional<AvailabilityZoneFilter> azFilter = request.hasRegionFilter()
                    ? Optional.of(request.getAvailabilityZoneFilter())
                    : Optional.empty();
            final Optional<AccountFilter> accountFilter = request.hasRegionFilter()
                    ? Optional.of(request.getAccountFilter())
                    : Optional.empty();
            final TimeFrame timeFrame = timeFrameCalculator.millis2TimeFrame(request.getStartDate());
            final ReservedInstanceUtilizationFilter filter =
                    createReservedInstanceUtilizationFilter(regionFilter, azFilter, accountFilter,
                            request.getStartDate(), request.getEndDate(), timeFrame);
            final List<ReservedInstanceStatsRecord> statRecords =
                    reservedInstanceUtilizationStore.getReservedInstanceUtilizationStatsRecords(filter);
            float usedCouponsTotal = (float)getProjectedRICoverageCouponTotal();
            statRecords.add(ReservedInstanceUtil.createRIStatsRecord(
                        statRecords.isEmpty() ? usedCouponsTotal : statRecords.get(statRecords.size()-1).getCapacity().getTotal(),
                        usedCouponsTotal,
                        request.getEndDate() + TimeUnit.HOURS.toMillis(PROJECTED_STATS_TIME_IN_FUTURE_HOURS)));
            GetReservedInstanceUtilizationStatsResponse response =
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
        }
        try {
            final ReservedInstanceCoverageFilter filter =
                    createReservedInstanceCoverageFilter(request);
            final List<ReservedInstanceStatsRecord> statRecords =
                    reservedInstanceCoverageStore.getReservedInstanceCoverageStatsRecords(filter);
            float usedCouponsTotal = (float)getProjectedRICoverageCouponTotal();
            // Instead of again computing the total capacity stats for the projected stats, we use the one from the last record
            // as it should be the same.
            statRecords.add(ReservedInstanceUtil.createRIStatsRecord(
                        statRecords.isEmpty() ? usedCouponsTotal : statRecords.get(statRecords.size()-1).getCapacity().getTotal(),
                        usedCouponsTotal,
                        request.getEndDate() + TimeUnit.HOURS.toMillis(PROJECTED_STATS_TIME_IN_FUTURE_HOURS)));
            GetReservedInstanceCoverageStatsResponse response =
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
     * @param regionFilter region id filters.
     * @param azFilter availability zone id filters.
     * @param accountFilter account id filters.
     * @param startDateMillis start date timestamp.
     * @param endDateMillis end date timestamp.
     * @return a {@link ReservedInstanceUtilizationFilter}.
     */
    private ReservedInstanceUtilizationFilter createReservedInstanceUtilizationFilter(
            @Nonnull final Optional<RegionFilter> regionFilter,
            @Nonnull final Optional<AvailabilityZoneFilter> azFilter,
            @Nonnull final Optional<AccountFilter> accountFilter,
            final long startDateMillis,
            final long endDateMillis,
            final TimeFrame timeFrame) {
        // Get all business accounts based on scope ID's and scope type.
        final ReservedInstanceUtilizationFilter.Builder filterBuilder = ReservedInstanceUtilizationFilter.newBuilder();
        if (regionFilter.isPresent()) {
            filterBuilder.addAllScopeId(regionFilter.get().getRegionIdList())
                       .setScopeEntityType(EntityType.REGION_VALUE);
        } else if (azFilter.isPresent()) {
            filterBuilder.addAllScopeId(azFilter.get().getAvailabilityZoneIdList())
                        .setScopeEntityType(EntityType.AVAILABILITY_ZONE_VALUE);
        } else if (accountFilter.isPresent()) {
            filterBuilder.addAllScopeId(accountFilter.get().getAccountIdList())
                        .setScopeEntityType(EntityType.BUSINESS_ACCOUNT_VALUE);
        }
        filterBuilder.setStartDateMillis(startDateMillis);
        filterBuilder.setEndDateMillis(endDateMillis);
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

    /**
     * Add one extra {@link ReservedInstanceStatsRecord} into list, and its stats value is exactly
     * same as the latest reserved instance stats record value, but its snapshot time should be
     * the current timestamp.
     *
     * @param records a list of {@link ReservedInstanceStatsRecord}.
     * @return a list of {@link ReservedInstanceStatsRecord} contains one extra stats record.
     */
    private List<ReservedInstanceStatsRecord> addLatestRIStats(
            @Nonnull final List<ReservedInstanceStatsRecord> records) {
        if (records.isEmpty()) {
            return Collections.emptyList();
        }
        records.sort(Comparator.comparingLong(ReservedInstanceStatsRecord::getSnapshotDate));
        final long currentTimeMillis = Instant.now().toEpochMilli();
        ReservedInstanceStatsRecord lastRIStatsRecord = records.get(records.size() - 1);
        if (lastRIStatsRecord.getSnapshotDate() != currentTimeMillis) {
            final ReservedInstanceStatsRecord newCurrentRIStatsRecord =
                    ReservedInstanceStatsRecord.newBuilder(lastRIStatsRecord)
                            .setSnapshotDate(currentTimeMillis)
                            .build();
            records.add(newCurrentRIStatsRecord);
        }
        return records;
    }

    private double getProjectedRICoverageCouponTotal() {
        return projectedRICoverageStore.getAllProjectedEntitiesRICoverages()
            .values().stream()
            .flatMap(map -> map.values().stream())
            .mapToDouble(i -> i)
            .sum();
    }
}
