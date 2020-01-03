package com.vmturbo.cost.component.reserved.instance;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc.BuyReservedInstanceServiceImplBase;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.cost.component.reserved.instance.filter.BuyReservedInstanceFilter;

public class BuyReservedInstanceRpcService extends BuyReservedInstanceServiceImplBase {

    private final BuyReservedInstanceStore buyRiStore;

    public BuyReservedInstanceRpcService(
            @Nonnull final BuyReservedInstanceStore buyRiStore) {
        this.buyRiStore =
                Objects.requireNonNull(buyRiStore);
    }

    @Override
    public void getBuyReservedInstancesByFilter(
            GetBuyReservedInstancesByFilterRequest request,
            StreamObserver<GetBuyReservedInstancesByFilterResponse> responseObserver) {
        try {
            final BuyReservedInstanceFilter filter = constructBuyRIFilter(request);
            final Collection<ReservedInstanceBought> reservedInstanceBoughts =
                    buyRiStore.getBuyReservedInstances(filter);
            final GetBuyReservedInstancesByFilterResponse.Builder responseBuilder =
                    GetBuyReservedInstancesByFilterResponse.newBuilder();
            reservedInstanceBoughts.stream().forEach(responseBuilder::addReservedInstanceBoughts);
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get buy reserved instance by filter.")
                    .asException());
        }
    }

    @VisibleForTesting
    private static BuyReservedInstanceFilter constructBuyRIFilter(GetBuyReservedInstancesByFilterRequest request) {
        BuyReservedInstanceFilter.Builder filterBuilder = BuyReservedInstanceFilter.newBuilder()
                .setRegionFilter(request.getRegionFilter())
                .setAccountFilter(request.getAccountFilter());

        if (request.hasTopologyContextId()) {
            filterBuilder.addTopologyContextId(request.getTopologyContextId());
        }
        if (request.getBuyRiIdCount() > 0) {
            filterBuilder.addBuyRIIdList(request.getBuyRiIdList());
        }
        return filterBuilder.build();
    }
}
