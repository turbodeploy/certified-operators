package com.vmturbo.cost.component.reserved.instance;

import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCoverageStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceStatsRecord;
import com.vmturbo.common.protobuf.cost.ReservedInstanceUtilizationCoverageServiceGrpc.ReservedInstanceUtilizationCoverageServiceImplBase;
import com.vmturbo.cost.component.reserved.instance.TimeFrameCalculator.TimeFrame;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCoverageFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceUtilizationFilter;

/**
 * A rpc service for get reserved instance utilization and coverage stats.
 */
public class ReservedInstanceUtilizationCoverageRpcService extends ReservedInstanceUtilizationCoverageServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore;

    private final ReservedInstanceCoverageStore reservedInstanceCoverageStore;

    private final TimeFrameCalculator timeFrameCalculator;

    public ReservedInstanceUtilizationCoverageRpcService(
            @Nonnull final ReservedInstanceUtilizationStore reservedInstanceUtilizationStore,
            @Nonnull final ReservedInstanceCoverageStore reservedInstanceCoverageStore,
            @Nonnull final TimeFrameCalculator timeFrameCalculator) {
        this.reservedInstanceUtilizationStore = reservedInstanceUtilizationStore;
        this.reservedInstanceCoverageStore = reservedInstanceCoverageStore;
        this.timeFrameCalculator = timeFrameCalculator;
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
            final List<ReservedInstanceStatsRecord> riStatsRecordWithLatestStats =
                    addLatestRIStats(statRecords);
            GetReservedInstanceUtilizationStatsResponse response =
                    GetReservedInstanceUtilizationStatsResponse.newBuilder()
                            .addAllReservedInstanceStatsRecords(riStatsRecordWithLatestStats)
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
        if (!request.hasStartDate() || !request.hasEndDate()) {
            logger.error("Missing start date and end date for query reserved instance coverage stats!");
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription("Must provide start date " +
                    " and end date for query reserved instance coverage stats").asException());
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
            final ReservedInstanceCoverageFilter filter =
                    createReservedInstanceCoverageFilter(regionFilter, azFilter, accountFilter,
                            request.getStartDate(), request.getEndDate(), timeFrame);
            final List<ReservedInstanceStatsRecord> statRecords =
                    reservedInstanceCoverageStore.getReservedInstanceCoverageStatsRecords(filter);
            final List<ReservedInstanceStatsRecord> riStatsRecordWithLatestStats =
                    addLatestRIStats(statRecords);
            GetReservedInstanceCoverageStatsResponse response =
                    GetReservedInstanceCoverageStatsResponse.newBuilder()
                            .addAllReservedInstanceStatsRecords(riStatsRecordWithLatestStats)
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
        final ReservedInstanceUtilizationFilter.Builder filterBuilder = ReservedInstanceUtilizationFilter.newBuilder();
        if (regionFilter.isPresent()) {
            regionFilter.get().getRegionIdList().forEach(filterBuilder::addRegionId);
        }
        if (azFilter.isPresent()) {
            azFilter.get().getAvailabilityZoneIdList().forEach(filterBuilder::addAvailabilityZoneId);
        }
        if (accountFilter.isPresent()) {
            accountFilter.get().getAccountIdList().forEach(filterBuilder::addBusinessAccountId);
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
     * @param regionFilter region id filters.
     * @param azFilter availability zone id filters.
     * @param accountFilter account id filters.
     * @param startDateMillis start date timestamp.
     * @param endDateMillis end date timestamp.
     * @return a {@link ReservedInstanceCoverageFilter}.
     */
    private ReservedInstanceCoverageFilter createReservedInstanceCoverageFilter(
            @Nonnull final Optional<RegionFilter> regionFilter,
            @Nonnull final Optional<AvailabilityZoneFilter> azFilter,
            @Nonnull final Optional<AccountFilter> accountFilter,
            final long startDateMillis,
            final long endDateMillis,
            final TimeFrame timeFrame) {
        final ReservedInstanceCoverageFilter.Builder filterBuilder = ReservedInstanceCoverageFilter.newBuilder();
        if (regionFilter.isPresent()) {
            regionFilter.get().getRegionIdList().forEach(filterBuilder::addRegionId);
        }
        if (azFilter.isPresent()) {
            azFilter.get().getAvailabilityZoneIdList().forEach(filterBuilder::addAvailabilityZoneId);
        }
        if (accountFilter.isPresent()) {
            accountFilter.get().getAccountIdList().forEach(filterBuilder::addBusinessAccountId);
        }
        filterBuilder.setStartDateMillis(startDateMillis);
        filterBuilder.setEndDateMillis(endDateMillis);
        filterBuilder.setTimeFrame(timeFrame);
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
}
