package com.vmturbo.cost.component.reserved.instance;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountResponse;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceImplBase;

public class ReservedInstanceBoughtRpcService extends ReservedInstanceBoughtServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceBoughtStore reservedInstanceBoughtStore;

    private final DSLContext dsl;

    public ReservedInstanceBoughtRpcService(
            @Nonnull final ReservedInstanceBoughtStore reservedInstanceBoughtStore,
            @Nonnull final DSLContext dsl) {
        this.reservedInstanceBoughtStore = Objects.requireNonNull(reservedInstanceBoughtStore);
        this.dsl = Objects.requireNonNull(dsl);
    }

    @Override
    public void getReservedInstanceBoughtByFilter(
            GetReservedInstanceBoughtByFilterRequest request,
            StreamObserver<GetReservedInstanceBoughtByFilterResponse> responseObserver) {
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
            final ReservedInstanceBoughtFilter filter =
                    createReservedInstanceBoughtFilter(regionFilter, azFilter, accountFilter);
            final List<ReservedInstanceBought> reservedInstanceBoughts =
                    reservedInstanceBoughtStore.getReservedInstanceBoughtByFilter(filter);
            final GetReservedInstanceBoughtByFilterResponse.Builder responseBuilder =
                    GetReservedInstanceBoughtByFilterResponse.newBuilder();
            reservedInstanceBoughts.stream().forEach(responseBuilder::addReservedInstanceBoughts);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance by filter.")
                    .asException());
        }
    }

    @Override
    public void getReservedInstanceBoughtCount(
            GetReservedInstanceBoughtCountRequest request,
            StreamObserver<GetReservedInstanceBoughtCountResponse> responseObserver) {
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
            final ReservedInstanceBoughtFilter filter =
                    createReservedInstanceBoughtFilter(regionFilter, azFilter, accountFilter);
            final Map<Long, Long> reservedInstanceCountMap =
                    reservedInstanceBoughtStore.getReservedInstanceCountMap(filter);
            final GetReservedInstanceBoughtCountResponse response =
                    GetReservedInstanceBoughtCountResponse.newBuilder()
                        .putAllReservedInstanceCountMap(reservedInstanceCountMap)
                        .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();

        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance count map.")
                    .asException());
        }
    }

    private ReservedInstanceBoughtFilter createReservedInstanceBoughtFilter(
            @Nonnull final Optional<RegionFilter> regionFilter,
            @Nonnull final Optional<AvailabilityZoneFilter> azFilter,
            @Nonnull final Optional<AccountFilter> accountFilter) {
        final ReservedInstanceBoughtFilter.Builder filterBuilder = ReservedInstanceBoughtFilter.newBuilder();
        if (regionFilter.isPresent()) {
            regionFilter.get().getFilterIdList().forEach(filterBuilder::addRegionId);
            // Because region id is stored at RI spec table, it needs join operation.
            filterBuilder.setJoinWithSpecTable(true);
        }
        if (azFilter.isPresent()) {
            azFilter.get().getFilterIdList().forEach(filterBuilder::addAvailabilityZoneId);
        }
        if (accountFilter.isPresent()) {
            azFilter.get().getFilterIdList().forEach(filterBuilder::addBusinessAccountId);
        }
        return filterBuilder.build();
    }
}
