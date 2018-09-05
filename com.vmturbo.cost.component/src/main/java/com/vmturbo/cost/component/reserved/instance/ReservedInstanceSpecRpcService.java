package com.vmturbo.cost.component.reserved.instance;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;

import com.google.common.collect.Sets;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsRequest;
import com.vmturbo.common.protobuf.cost.Cost.GetReservedInstanceSpecByIdsResponse;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceSpec;
import com.vmturbo.common.protobuf.cost.ReservedInstanceSpecServiceGrpc.ReservedInstanceSpecServiceImplBase;

public class ReservedInstanceSpecRpcService extends ReservedInstanceSpecServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final ReservedInstanceSpecStore reservedInstanceSpecStore;

    private final DSLContext dsl;

    public ReservedInstanceSpecRpcService(@Nonnull final ReservedInstanceSpecStore reservedInstanceSpecStore,
                                          @Nonnull final DSLContext dsl) {
        this.reservedInstanceSpecStore = Objects.requireNonNull(reservedInstanceSpecStore);
        this.dsl = Objects.requireNonNull(dsl);
    }

    @Override
    public void getReservedInstanceSpecByIds(GetReservedInstanceSpecByIdsRequest request,
                                        StreamObserver<GetReservedInstanceSpecByIdsResponse> responseObserver) {
        try {
            final GetReservedInstanceSpecByIdsResponse.Builder riSpecResponseBuilder =
                    GetReservedInstanceSpecByIdsResponse.newBuilder();
            if (request.getReservedInstanceSpecIdsList().isEmpty()) {
                logger.debug("No spec ids are provided, return empty results.");
                responseObserver.onNext(riSpecResponseBuilder.build());
                responseObserver.onCompleted();
                return;
            }
            final List<ReservedInstanceSpec> reservedInstanceSpecs =
                    reservedInstanceSpecStore.getReservedInstanceSpecByIds(
                            Sets.newHashSet(request.getReservedInstanceSpecIdsList()));

            reservedInstanceSpecs.stream().forEach(riSpecResponseBuilder::addReservedInstanceSpec);
            responseObserver.onNext(riSpecResponseBuilder.build());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to get reserved instance spec by ids.")
                    .asException());
        }
    }
}
