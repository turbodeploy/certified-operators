package com.vmturbo.cost.component.reserved.instance;

import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;
import org.springframework.util.CollectionUtils;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceCostStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceCostStatsRequest.GroupBy;
import com.vmturbo.common.protobuf.cost.Cost.DeletePlanReservedInstanceStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeletePlanReservedInstanceStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataRequest;
import com.vmturbo.common.protobuf.cost.Cost.UploadRIDataResponse;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceImplBase;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceCostFilter;

/**
 * Plan reserved instance service.
 */
public class PlanReservedInstanceRpcService extends PlanReservedInstanceServiceImplBase {
    private static final int PROJECTED_STATS_TIME_IN_FUTURE_HOURS = 1;

    private final Logger logger = LogManager.getLogger();

    private final PlanReservedInstanceStore planReservedInstanceStore;
    private final BuyReservedInstanceStore buyReservedInstanceStore;

    /**
     * Creates {@link PlanReservedInstanceRpcService} instance.
     *
     * @param planReservedInstanceStore plan RI store.
     * @param buyReservedInstanceStore buy RI Store.
     */
    public PlanReservedInstanceRpcService(
            @Nonnull final PlanReservedInstanceStore planReservedInstanceStore,
            @Nonnull final BuyReservedInstanceStore buyReservedInstanceStore) {
        this.planReservedInstanceStore = Objects.requireNonNull(planReservedInstanceStore);
        this.buyReservedInstanceStore = Objects.requireNonNull(buyReservedInstanceStore);
    }

    @Override
    public void getPlanReservedInstanceBoughtCountByTemplateType(GetPlanReservedInstanceBoughtCountRequest request,
        StreamObserver<GetPlanReservedInstanceBoughtCountByTemplateResponse> responseObserver) {
        try {
            final long planId = request.getPlanId();
            final Map<String, Long> riCountByRiSpecId = planReservedInstanceStore
                            .getPlanReservedInstanceCountByRISpecIdMap(planId);

            final GetPlanReservedInstanceBoughtCountByTemplateResponse response =
                            GetPlanReservedInstanceBoughtCountByTemplateResponse.newBuilder()
                                            .putAllReservedInstanceCountMap(riCountByRiSpecId)
                                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to get plan reserved instance count map.")
                            .asException());
        }
    }

    @Override
    public void getPlanReservedInstanceBought(GetPlanReservedInstanceBoughtRequest request,
        StreamObserver<GetReservedInstanceBoughtByFilterResponse> responseObserver) {
        try {
            final Long topologyContextId = request.getPlanId();
            final List<ReservedInstanceBought> reservedInstances =
                            planReservedInstanceStore.getReservedInstanceBoughtByPlanId(topologyContextId);
            final GetReservedInstanceBoughtByFilterResponse response =
                            GetReservedInstanceBoughtByFilterResponse.newBuilder()
                                            .addAllReservedInstanceBoughts(reservedInstances)
                                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to get plan reserved instances.")
                            .asException());
        }
    }

    @Override
    public void deletePlanReservedInstanceStats(DeletePlanReservedInstanceStatsRequest request,
        StreamObserver<DeletePlanReservedInstanceStatsResponse> responseObserver) {
        try {
            final Long topologyContextId = request.getTopologyContextId();
            final int rowsDeleted = planReservedInstanceStore.deletePlanReservedInstanceStats(topologyContextId);
            final DeletePlanReservedInstanceStatsResponse response =
                            DeletePlanReservedInstanceStatsResponse.newBuilder()
                                            .setDeleted(rowsDeleted > 0 ? true : false)
                                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to delete plan reserved instance stats.")
                            .asException());
        }
    }

    /**
     * Insert plan reserved instances bought (Included RIs).
     *
     * @param request  The request.
     * @param responseObserver The response observer.
     */
    @Override
    public void insertPlanReservedInstanceBought(UploadRIDataRequest request,
        StreamObserver<UploadRIDataResponse> responseObserver) {
        try {
            final Long planId = request.getTopologyId();
            planReservedInstanceStore.insertPlanReservedInstanceBought(request.getReservedInstanceBoughtList(),
                                                                       planId);
            responseObserver.onNext(UploadRIDataResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to insert plan bought reserved instance(s).")
                            .asException());
        }
    }

    @Override
    public void getPlanReservedInstanceCostStats(GetPlanReservedInstanceCostStatsRequest request,
        StreamObserver<GetPlanReservedInstanceCostStatsResponse> responseObserver) {
        try {
            final long planId = request.getPlanId();
            final Instant instant = Clock.systemUTC().instant();
            final long currentTime = instant.toEpochMilli();
            final long projectedTime = instant.plus(PROJECTED_STATS_TIME_IN_FUTURE_HOURS, ChronoUnit.HOURS).toEpochMilli();
            final Cost.ReservedInstanceCostStat riCostStats = planReservedInstanceStore.getPlanReservedInstanceAggregatedCosts(planId);
            // if current data has already been queried, use that as part of projection data.
            final List<Cost.ReservedInstanceCostStat> projectedRICostBuilders;
            //check if we should include buy RI data
            if (request.getIncludeBuyRi()) {
                final BuyReservedInstanceCostFilter buyReservedInstanceCostFilter =
                                BuyReservedInstanceCostFilter.newBuilder()
                                                .addTopologyContextId(planId)
                                                .addGroupBy(GroupBy.SNAPSHOT_TIME).build();
                final List<Cost.ReservedInstanceCostStat> buyRICostStats =
                                buyReservedInstanceStore.queryBuyReservedInstanceCostStats(buyReservedInstanceCostFilter);
                projectedRICostBuilders = unifyProjectedRICostStats(riCostStats, buyRICostStats);
            } else {
                projectedRICostBuilders = Collections.singletonList(riCostStats);
            }

            final List<Cost.ReservedInstanceCostStat> currentReservedInstanceCostStats =
                            updateSnapshotTime(Collections.singletonList(riCostStats), currentTime);
            final List<Cost.ReservedInstanceCostStat> projectedReservedInstanceCostStats =
                            updateSnapshotTime(projectedRICostBuilders, projectedTime);
            final GetPlanReservedInstanceCostStatsResponse response =
                            GetPlanReservedInstanceCostStatsResponse.newBuilder()
                            .addAllStats(currentReservedInstanceCostStats)
                            .addAllStats(projectedReservedInstanceCostStats)
                                            .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                            .withDescription("Failed to get plan reserved instances cost stats.")
                            .asException());
        }
    }

    @Nonnull
    private static List<Cost.ReservedInstanceCostStat> unifyProjectedRICostStats(@Nonnull Cost.ReservedInstanceCostStat boughtRICostStats,
                    @Nonnull List<Cost.ReservedInstanceCostStat> buyRICostStats) {
        if (CollectionUtils.isEmpty(buyRICostStats)) {
            return Collections.singletonList(boughtRICostStats);
        }
        final Cost.ReservedInstanceCostStat boughtRICostStat = boughtRICostStats;
        final Cost.ReservedInstanceCostStat buyRICostStat = buyRICostStats.get(0);
        final Cost.ReservedInstanceCostStat projectedRICostStat =
                        Cost.ReservedInstanceCostStat.newBuilder()
                                        .setAmortizedCost(boughtRICostStat.getAmortizedCost()
                                            + buyRICostStat.getAmortizedCost())
                                        .setFixedCost(boughtRICostStat.getFixedCost()
                                            + buyRICostStat.getFixedCost())
                                        .setRecurringCost(boughtRICostStat.getRecurringCost()
                                            + buyRICostStat.getRecurringCost())
                                        .setSnapshotTime(Clock.systemUTC().instant().toEpochMilli()).build();
        return Collections.singletonList(projectedRICostStat);
    }

    @Nonnull
    private static List<Cost.ReservedInstanceCostStat> updateSnapshotTime(
        @Nonnull List<Cost.ReservedInstanceCostStat> reservedInstanceCostStats, long snapshotTime) {
        final List<Cost.ReservedInstanceCostStat> riCostStats = reservedInstanceCostStats.stream()
                        .map(Cost.ReservedInstanceCostStat::toBuilder)
                        .peek(riCostStatBuilder -> riCostStatBuilder.setSnapshotTime(snapshotTime))
                        .map(Cost.ReservedInstanceCostStat.Builder::build)
                        .collect(Collectors.toList());
        return riCostStats;
    }

}
