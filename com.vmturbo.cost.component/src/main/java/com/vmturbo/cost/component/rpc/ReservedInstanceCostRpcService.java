package com.vmturbo.cost.component.rpc;

import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.collections4.ListUtils;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.ReservedInstanceCostServiceGrpc;
import com.vmturbo.cost.component.reserved.instance.BuyReservedInstanceStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceCostFilter;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceCostFilter;

/**
 * Implements RPC calls to get the Reserved Instance Cost Stats (Current and Projected) from the cost component.
 */
public class ReservedInstanceCostRpcService extends
                ReservedInstanceCostServiceGrpc.ReservedInstanceCostServiceImplBase {

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;
    private final BuyReservedInstanceStore buyReservedInstanceStore;
    private final Clock clock;
    private static final int PROJECTED_STATS_TIME_IN_FUTURE_HOURS = 1;

    /**
     * Constructor for ReservedInstanceCostRpcService.
     *
     * @param reservedInstanceBoughtStore object of type ReservedInstanceBoughtStore.
     * @param buyReservedInstanceStore object of type BuyReservedInstanceStore.
     * @param clock object of type Clock.
     */
    public ReservedInstanceCostRpcService(@Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
                    @Nonnull final BuyReservedInstanceStore buyReservedInstanceStore, @Nonnull final Clock clock) {
        this.reservedInstanceBoughtStore = reservedInstanceBoughtStore;
        this.buyReservedInstanceStore = buyReservedInstanceStore;
        this.clock = clock;
    }

    @Override
    public void getReservedInstanceCostStats(Cost.GetReservedInstanceCostStatsRequest request,
                    io.grpc.stub.StreamObserver<Cost.GetReservedInstanceCostStatsResponse> responseObserver) {

        if (request.getTimeWindow().hasTimeWindow()) {
            throw new UnsupportedOperationException();
        }

        final ReservedInstanceCostFilter reservedInstanceCostFilter = buildReservedInstanceCostFilter(request);
        final BuyReservedInstanceCostFilter buyReservedInstanceCostFilter = buildBuyReservedInstanceCostFilter(request);

        final Cost.GetReservedInstanceCostStatsResponse.Builder responseBuilder =
                        Cost.GetReservedInstanceCostStatsResponse.newBuilder();
        List<Cost.ReservedInstanceCostStat> riCostStats = new ArrayList<>();
        // If query_latest is false / unset, return empty.
        if (request.getTimeWindow().hasQueryLatest() && request.getTimeWindow().getQueryLatest()) {
            // if query_latest = true -> return current inventory costs
            long currentSnapshotTime = clock.instant().toEpochMilli();
            riCostStats = reservedInstanceBoughtStore.queryReservedInstanceBoughtCostStats(reservedInstanceCostFilter);
            final List<Cost.ReservedInstanceCostStat> currentReservedInstanceCostStats =
                            updateSnapshotTime(riCostStats, currentSnapshotTime);
            responseBuilder.addAllStats(currentReservedInstanceCostStats);
        }

        if (request.getIncludeProjected()) {
            final long projectedTime = clock.instant()
                            .plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();
            // check if current data has already been queried.
            if (CollectionUtils.isEmpty(riCostStats)) {
                riCostStats = reservedInstanceBoughtStore.queryReservedInstanceBoughtCostStats(reservedInstanceCostFilter);
            }
            // if current data has already been queried, use that as part of projection data.
            List<Cost.ReservedInstanceCostStat> projectedRICostBuilders = riCostStats;
            //check if we should include buy RI data
            if (request.getIncludeBuyRi() && !request.hasAvailabilityZoneFilter()) {
                final List<Cost.ReservedInstanceCostStat> buyRICostStats = buyReservedInstanceStore.queryBuyReservedInstanceCostStats(buyReservedInstanceCostFilter);
                projectedRICostBuilders = unifyProjectedRICostStats(riCostStats, buyRICostStats,
                                request.getGroupBy());
            }

            final List<Cost.ReservedInstanceCostStat> projectedReservedInstanceCostStats =
                            updateSnapshotTime(projectedRICostBuilders, projectedTime);
            responseBuilder.addAllStats(projectedReservedInstanceCostStats);
        }

        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }

    @Nonnull
    private List<Cost.ReservedInstanceCostStat> unifyProjectedRICostStats(@Nonnull List<Cost.ReservedInstanceCostStat> boughtRICostStats,
                    @Nonnull List<Cost.ReservedInstanceCostStat> buyRICostStats, @Nonnull Cost.GetReservedInstanceCostStatsRequest.GroupBy requestGroupBy) {
        if (Cost.GetReservedInstanceCostStatsRequest.GroupBy.SNAPSHOT_TIME == requestGroupBy &&
                        !(CollectionUtils.isEmpty(boughtRICostStats)) && !(CollectionUtils.isEmpty(buyRICostStats))) {
            final Cost.ReservedInstanceCostStat boughtRICostStat = boughtRICostStats.get(0);
            final Cost.ReservedInstanceCostStat buyRICostStat = buyRICostStats.get(0);
            final Cost.ReservedInstanceCostStat projectedRICostStat =
                            Cost.ReservedInstanceCostStat.newBuilder()
                                            .setAmortizedCost(boughtRICostStat.getAmortizedCost()
                                                            + buyRICostStat
                                                            .getAmortizedCost())
                                            .setFixedCost(boughtRICostStat
                                                            .getFixedCost()
                                                            + buyRICostStat
                                                            .getFixedCost())
                                            .setRecurringCost(boughtRICostStat.getRecurringCost()
                                                            + buyRICostStat
                                                            .getRecurringCost())
                                            .setSnapshotTime(clock.instant().toEpochMilli()).build();
            return Collections.singletonList(projectedRICostStat);
        } else {
            return ListUtils.union(boughtRICostStats, buyRICostStats);
        }
    }

    @Nonnull
    private  List<Cost.ReservedInstanceCostStat> updateSnapshotTime(@Nonnull List<Cost.ReservedInstanceCostStat> reservedInstanceCostStats,
                    long snapshotTime) {
        final List<Cost.ReservedInstanceCostStat> riCostStats = reservedInstanceCostStats.stream().map(
                        Cost.ReservedInstanceCostStat::toBuilder)
                        .peek(riCostStatBuilder -> riCostStatBuilder.setSnapshotTime(snapshotTime)).map(
                                        Cost.ReservedInstanceCostStat.Builder::build)
                        .collect(Collectors.toList());
        return riCostStats;
    }

    private ReservedInstanceCostFilter buildReservedInstanceCostFilter(Cost.GetReservedInstanceCostStatsRequest request) {
        final ReservedInstanceCostFilter.Builder builder = ReservedInstanceCostFilter.newBuilder();
        if (request.hasAccountFilter() && request.getAccountFilter().getAccountIdCount() != 0) {
            final List<Long> accountIdsList = request.getAccountFilter().getAccountIdList();
            final Cost.AccountFilter accountFilter =
                            Cost.AccountFilter.newBuilder().addAllAccountId(accountIdsList).build();
            builder.accountFilter(accountFilter);
        }
        if (request.hasAvailabilityZoneFilter() && request.getAvailabilityZoneFilter().getAvailabilityZoneIdCount() != 0) {
            final List<Long> availabilityZoneIdList = request.getAvailabilityZoneFilter().getAvailabilityZoneIdList();
            final Cost.AvailabilityZoneFilter availabilityZoneFilter = Cost.AvailabilityZoneFilter.newBuilder()
                            .addAllAvailabilityZoneId(availabilityZoneIdList).build();
            builder.availabilityZoneFilter(availabilityZoneFilter);
        }
        if (request.hasRegionFilter() && request.getRegionFilter().getRegionIdCount() != 0) {
            final List<Long> regionIdList = request.getRegionFilter().getRegionIdList();
            final Cost.RegionFilter regionFilter =
                            Cost.RegionFilter.newBuilder().addAllRegionId(regionIdList).build();
            builder.regionFilter(regionFilter);
        }
        if (request.hasGroupBy()) {
            final Cost.GetReservedInstanceCostStatsRequest.GroupBy groupBy = request.getGroupBy();
            builder.addGroupBy(groupBy);
        }
        return builder.build();
    }

    private BuyReservedInstanceCostFilter buildBuyReservedInstanceCostFilter(Cost.GetReservedInstanceCostStatsRequest request) {
        final BuyReservedInstanceCostFilter.Builder builder = BuyReservedInstanceCostFilter.newBuilder();
        if (request.hasAccountFilter() && request.getAccountFilter().getAccountIdCount() != 0) {
            final List<Long> accountIdsList = request.getAccountFilter().getAccountIdList();
            builder.addAllAccountIdList(accountIdsList);
        }
        if (request.hasRegionFilter() && request.getRegionFilter().getRegionIdCount() != 0) {
            final List<Long> regionIdList = request.getRegionFilter().getRegionIdList();
            builder.addAllRegionIdList(regionIdList);
        }
        if (request.hasTopologyContextId() && request.getTopologyContextId() != 0) {
            builder.addTopologyContextId(request.getTopologyContextId());
        }
        if (request.hasGroupBy()) {
            final Cost.GetReservedInstanceCostStatsRequest.GroupBy groupBy = request.getGroupBy();
            builder.addGroupBy(groupBy);
        }
        return builder.build();
    }
}
