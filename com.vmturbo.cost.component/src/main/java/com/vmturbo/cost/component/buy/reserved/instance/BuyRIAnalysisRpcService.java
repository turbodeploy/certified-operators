package com.vmturbo.cost.component.buy.reserved.instance;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc.BuyRIAnalysisServiceImplBase;
import com.vmturbo.common.protobuf.cost.Cost.BuyRIAnalysisScheduleResponse;
import com.vmturbo.common.protobuf.cost.Cost.SetBuyRIAnalysisScheduleRequest;

/**
 * This class is a rpc service for buy reserved instance analysis.
 */
public class BuyRIAnalysisRpcService extends BuyRIAnalysisServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final BuyRIAnalysisScheduler buyRIAnalysisScheduler;

    public BuyRIAnalysisRpcService(
            @Nonnull final BuyRIAnalysisScheduler buyRIAnalysisScheduler) {
        this.buyRIAnalysisScheduler = Objects.requireNonNull(buyRIAnalysisScheduler);
    }

    /**
     * Set a new interval for buy reserved instance analysis scheduler. Note that after call
     * this function, it will trigger buy RI analysis immediately.
     *
     * @param request {@link SetBuyRIAnalysisScheduleRequest}.
     * @param responseObserver {@link StreamObserver}.
     */
    public void setBuyReservedInstanceSchedule(SetBuyRIAnalysisScheduleRequest request,
                                               StreamObserver<BuyRIAnalysisScheduleResponse> responseObserver) {
        if (!request.hasIntervalHours() || request.getIntervalHours() <= 0) {
            logger.error("Invalid interval hours parameter {}.", request.getIntervalHours());
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Must provide a new positive interval value.").asException());
        }
        final long scheduleIntervalHours = request.getIntervalHours();
        final long intervalValue = buyRIAnalysisScheduler.setBuyRIAnalysisSchedule(
                scheduleIntervalHours, TimeUnit.HOURS);
        BuyRIAnalysisScheduleResponse response = BuyRIAnalysisScheduleResponse.newBuilder()
                    .setIntervalHours(intervalValue)
                    .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }
}
