package com.vmturbo.market.rpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementRequest;
import com.vmturbo.common.protobuf.market.InitialPlacement.FindInitialPlacementResponse;
import com.vmturbo.common.protobuf.market.InitialPlacementServiceGrpc.InitialPlacementServiceImplBase;

/**
 * Implementation of gRpc service for Reservation.
 */
public class InitialPlacementRpcService extends InitialPlacementServiceImplBase {
    private final Logger logger = LogManager.getLogger();

    @Override
    public void findInitialPlacement(final FindInitialPlacementRequest request,
                                     final StreamObserver<FindInitialPlacementResponse> responseObserver) {
        logger.info("The number of workloads to find inital placement is " + request.getInitialPlacementBuyerList().size());
        FindInitialPlacementResponse.Builder response = FindInitialPlacementResponse.newBuilder();
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