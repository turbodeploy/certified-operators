package com.vmturbo.market.rpc;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.collect.Table;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementResponse;
import com.vmturbo.common.protobuf.market.InitialPlacement.InitialPlacementBuyerPlacementInfo;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceImplBase;
import com.vmturbo.market.reservations.InitialPlacementFinder;

/**
 * Implementation of gRpc service for Reservation.
 */
public class InitialPlacementRpcService extends InitialPlacementServiceImplBase {
    private final Logger logger = LogManager.getLogger();

    private final InitialPlacementFinder initPlacementFinder;

    /**
     * The grpc call to handle fast reservation.
     *
     * @param initialPlacementFinder {@link InitialPlacementFinder}
     */
    public InitialPlacementRpcService(@Nonnull final InitialPlacementFinder initialPlacementFinder) {
        this.initPlacementFinder = Objects.requireNonNull(initialPlacementFinder);
    }

    @Override
    public void findInitialPlacement(final FindInitialPlacementRequest request,
                                     final StreamObserver<FindInitialPlacementResponse> responseObserver) {
        logger.info("The number of workloads to find inital placement is " + request.getInitialPlacementBuyerList().size());
        Table<Long, Long, Long> result = initPlacementFinder.findPlacement(request.getInitialPlacementBuyerList());
        FindInitialPlacementResponse.Builder response = FindInitialPlacementResponse.newBuilder();
        for (Table.Cell<Long, Long, Long> triplet : result.cellSet()) {
            response.addInitialPlacementBuyerPlacementInfo(InitialPlacementBuyerPlacementInfo
                    .newBuilder()
                    .setBuyerId(triplet.getRowKey())
                    .setCommoditiesBoughtFromProviderId(triplet.getColumnKey())
                    .setProviderId(triplet.getValue())
                    .build());
        }
        try {
            responseObserver.onNext(response.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to create reservation.")
                    .asException());
        }
        return;
    }
}