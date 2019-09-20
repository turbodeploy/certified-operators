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
import com.vmturbo.communication.CommunicationException;
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

    private final ComputeTierDemandStatsStore computeTierDemandStatsStore;

    private final long realtimeTopologyContextId;

    private final ExecutorService buyRIExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("buy-ri-algorithm-%d").build());

    public BuyRIAnalysisRpcService(
            @Nonnull final BuyRIAnalysisScheduler buyRIAnalysisScheduler,
            @Nonnull final ReservedInstanceAnalyzer reservedInstanceAnalyzer,
            @Nonnull final ComputeTierDemandStatsStore computeTierDemandStatsStore,
            final long realtimeTopologyContextId) {
        this.buyRIAnalysisScheduler = Objects.requireNonNull(buyRIAnalysisScheduler);
        this.reservedInstanceAnalyzer = Objects.requireNonNull(reservedInstanceAnalyzer);
        this.computeTierDemandStatsStore = Objects.requireNonNull(computeTierDemandStatsStore);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
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
        if (request.hasTopologyInfo() && request.getTopologyInfo().hasPlanInfo()) {
            runPlanBuyRIAnalysis(request, responseObserver);
        } else {
            runRealtimeBuyRIAnalysis(request, responseObserver);
        }
    }

    /**
     * Run buy ri analysis for plan and send buy ri action plan to action orchestrator.
     *
     * @param request StartBuyRIAnalysisRequest
     * @param responseObserver StartBuyRIAnalysisResponse
     */
    private void runPlanBuyRIAnalysis(StartBuyRIAnalysisRequest request,
                                      StreamObserver<StartBuyRIAnalysisResponse> responseObserver) {
        long planId = request.getTopologyInfo().getTopologyContextId();
        try {
            // failed the plan because not enough data to run buy ri
//            if (!computeTierDemandStatsStore.containsDataOverWeek()) {
//                String failedMsg = "Not enough compute tier data collected to run buy RI analysis";
//                logger.error(failedMsg);
//                responseObserver.onError(Status.FAILED_PRECONDITION.withDescription(failedMsg).asException());
//            } else {
                logger.info("Executing buy RI algorithm for plan {}.", planId);
                ReservedInstanceAnalysisScope reservedInstanceAnalysisScope =
                                new ReservedInstanceAnalysisScope(request);
                // TODO: ReservedInstanceHistoricalDemandDataType should be based on OCP planType
                reservedInstanceAnalyzer.runRIAnalysisAndSendActions(planId, reservedInstanceAnalysisScope,
                                                                     ReservedInstanceHistoricalDemandDataType.ALLOCATION);
           //}
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Failed to send buy RI action plan to action orchestrator for plan {}", planId);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        } catch (RuntimeException ex) {
            logger.error("Unexpected run time exception occurs in buy RI analysis for plan {}", planId);
            responseObserver.onError(Status.ABORTED.withDescription(ex.getMessage()).asException());
        }
    }

    /**
     * Run buy ri analysis in realtime and send buy ri action plan to action orchestrator.
     *
     * @param responseObserver StartBuyRIAnalysisResponse
     */
    private void runRealtimeBuyRIAnalysis(StartBuyRIAnalysisRequest request, StreamObserver<StartBuyRIAnalysisResponse> responseObserver) {
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
            try {
                reservedInstanceAnalyzer.runRIAnalysisAndSendActions(realtimeTopologyContextId,
                                                                     reservedInstanceAnalysisScope,
                        ReservedInstanceHistoricalDemandDataType.CONSUMPTION);
            } catch (InterruptedException ie) {
                logger.error("Interrupted publishing of buy RI actions", ie);
                Thread.currentThread().interrupt();
            } catch (CommunicationException ce) {
                logger.error("Exception while publishing buy RI actions", ce);
            }
            responseObserver.onNext(StartBuyRIAnalysisResponse.getDefaultInstance());
            responseObserver.onCompleted();
            return;
        });
    }
}
