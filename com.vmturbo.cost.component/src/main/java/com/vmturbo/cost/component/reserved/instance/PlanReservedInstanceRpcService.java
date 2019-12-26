package com.vmturbo.cost.component.reserved.instance;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.cost.Cost.DeletePlanReservedInstanceStatsRequest;
import com.vmturbo.common.protobuf.cost.Cost.DeletePlanReservedInstanceStatsResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountByTemplateResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetPlanReservedInstanceBoughtRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceImplBase;

/**
 * Plan reserved instance service.
 */
public class PlanReservedInstanceRpcService extends PlanReservedInstanceServiceImplBase {
    private final Logger logger = LogManager.getLogger();

    private final PlanReservedInstanceStore planReservedInstanceStore;

    /**
     * Creates {@link PlanReservedInstanceRpcService} instance.
     *
     * @param planReservedInstanceStore plan RI store.
     */
    public PlanReservedInstanceRpcService(
            @Nonnull final PlanReservedInstanceStore planReservedInstanceStore) {
        this.planReservedInstanceStore =
                Objects.requireNonNull(planReservedInstanceStore);
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
}
