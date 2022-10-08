package com.vmturbo.cost.component.billed.cost;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.BilledCostServiceGrpc.BilledCostServiceImplBase;
import com.vmturbo.common.protobuf.cost.BilledCostServices.GetBilledCostStatsRequest;
import com.vmturbo.common.protobuf.cost.BilledCostServices.GetBilledCostStatsResponse;

/**
 * Billed cost RPC service for querying billed cost data.
 */
public class BilledCostRpcService extends BilledCostServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final CloudCostStore cloudCostStore;

    /**
     * Contructs a new {@link BilledCostRpcService} instance.
     * @param cloudCostStore The cloud cost (billed cost) store.
     */
    public BilledCostRpcService(@Nonnull CloudCostStore cloudCostStore) {
        this.cloudCostStore = Objects.requireNonNull(cloudCostStore);
    }

    @Override
    public void getBilledCostStats(GetBilledCostStatsRequest request,
                                   StreamObserver<GetBilledCostStatsResponse> responseObserver) {
        try {

            Preconditions.checkArgument(request.hasQuery(), "Billed cost request must contain a query");

            final Stopwatch stopwatch = Stopwatch.createStarted();
            responseObserver.onNext(GetBilledCostStatsResponse.newBuilder()
                    .addAllCostStats(cloudCostStore.getCostStats(request.getQuery()))
                    .build());

            logger.info("Responding to the following query in {}:\n{}", stopwatch, request);

            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }
}
