package com.vmturbo.cost.component.reserved.instance;

import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc.BuyRIAnalysisServiceImplBase;
import com.vmturbo.common.protobuf.cost.Cost.SetBuyRIAnalysisScheduleRequest;
import com.vmturbo.common.protobuf.cost.Cost.SetBuyRIAnalysisScheduleResponse;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisResponse;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisScope;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalyzer;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceHistoricalDemandDataType;

/**
 * This class is a rpc service for buy reserved instance analysis.
 */
public class BuyRIAnalysisRpcService extends BuyRIAnalysisServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final BuyRIAnalysisScheduler buyRIAnalysisScheduler;

    private final ReservedInstanceAnalyzer reservedInstanceAnalyzer;

    private final ExecutorService buyRIExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("buy-ri-algorithm-%d").build());

    public BuyRIAnalysisRpcService(
            @Nonnull final BuyRIAnalysisScheduler buyRIAnalysisScheduler,
            @Nonnull final ReservedInstanceAnalyzer reservedInstanceAnalyzer) {
        this.buyRIAnalysisScheduler = Objects.requireNonNull(buyRIAnalysisScheduler);
        this.reservedInstanceAnalyzer = Objects.requireNonNull(reservedInstanceAnalyzer);
    }

    /**
     * RI analysis task future.
     */
    private Future<?> buyRIFuture;

    /**
     * Set a new interval for buy reserved instance analysis scheduler. Note that after call
     * this function, it will trigger buy RI analysis immediately.
     *
     * @param request {@link SetBuyRIAnalysisScheduleRequest}.
     * @param responseObserver {@link StreamObserver}.
     */
    public void setBuyReservedInstanceSchedule(SetBuyRIAnalysisScheduleRequest request,
                                               StreamObserver<SetBuyRIAnalysisScheduleResponse> responseObserver) {
        if (!request.hasIntervalHours() || request.getIntervalHours() <= 0) {
            logger.error("Invalid interval hours parameter {}.", request.getIntervalHours());
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Must provide a new positive interval value.").asException());
        }
        final long scheduleIntervalHours = request.getIntervalHours();
        final long intervalValue = buyRIAnalysisScheduler.setBuyRIAnalysisSchedule(
                scheduleIntervalHours, TimeUnit.HOURS);
        SetBuyRIAnalysisScheduleResponse response = SetBuyRIAnalysisScheduleResponse.newBuilder()
                    .setIntervalHours(intervalValue)
                    .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Start the Buy RI Analysis algorithm
     *
     * @param request {@link StartBuyRIAnalysisRequest}.
     * @param responseObserver {@link StreamObserver}.
     */
    public void startBuyRIAnalysis(StartBuyRIAnalysisRequest request,
                                   StreamObserver<StartBuyRIAnalysisResponse> responseObserver) {

        // return if there is an outstanding buyRI algorithm execution.
        if (buyRIFuture != null && !buyRIFuture.isDone()) {
            logger.warn("A previous Buy RI algorithm execution is still in-progress.");
        }

        // We queue the requests. But there is only one RI analysis execution at a time.
        // Right now, the analysis is only for real-time. But when we add support for plans,
        // there could be multiple concurrent executions of the RI analysis algorithm.
        buyRIFuture = buyRIExecutor.submit( () -> {
            logger.info("Executing buy RI algorithm.");
            ReservedInstanceAnalysisScope reservedInstanceAnalysisScope =
                    new ReservedInstanceAnalysisScope(request);
            reservedInstanceAnalyzer.runRIAnalysisAndSendActions(reservedInstanceAnalysisScope,
                    ReservedInstanceHistoricalDemandDataType.CONSUMPTION);
            return;
        });
        responseObserver.onNext(StartBuyRIAnalysisResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }
}
