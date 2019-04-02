package com.vmturbo.cost.component.reserved.instance;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.exception.DataAccessException;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.BuyReservedInstanceServiceGrpc.BuyReservedInstanceServiceImplBase;
import com.vmturbo.common.protobuf.cost.Cost.AccountFilter;
import com.vmturbo.common.protobuf.cost.Cost.AvailabilityZoneFilter;
import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetBuyReservedInstancesByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetEntityReservedInstanceCoverageResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtByFilterResponse;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceBoughtCountResponse;
import com.vmturbo.common.protobuf.cost.Cost.RegionFilter;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceImplBase;
import com.vmturbo.cost.component.db.tables.pojos.BuyReservedInstance;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;

public class BuyReservedInstanceRpcService extends BuyReservedInstanceServiceImplBase {

    private final Logger logger = LogManager.getLogger();

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
            final Collection<ReservedInstanceBought> reservedInstanceBoughts =
                    buyRiStore.getBuyReservedInstances(request);
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
}
