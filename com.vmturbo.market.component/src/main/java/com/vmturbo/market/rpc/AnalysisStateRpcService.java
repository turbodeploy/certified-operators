package com.vmturbo.market.rpc;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.AnalysisStateServiceGrpc.AnalysisStateServiceImplBase;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.common.protobuf.market.MarketNotification.GetAnalysisStateRequest;
import com.vmturbo.common.protobuf.market.MarketNotification.GetAnalysisStateResponse;
import com.vmturbo.market.runner.MarketRunner;

/**
 * RPC service to get the analysis status notification.
 */
public class AnalysisStateRpcService extends AnalysisStateServiceImplBase {

    /**
     * Logger.
     */
    private final Logger logger = LogManager.getLogger();
    /**
     * The orchestrator managing analysis running.
     */
    private MarketRunner marketRunner;

    /**
     * Constructor.
     * @param marketRunner market running orchestrator.
     */
    public AnalysisStateRpcService(MarketRunner marketRunner) {
        this.marketRunner = marketRunner;
    }

    @Override
    public void getAnalysisState(final GetAnalysisStateRequest request,
            final StreamObserver<GetAnalysisStateResponse> responseObserver) {
        try {
            boolean isHealthy = marketRunner.getAnalysisHealthTracker().isAnalysisHealthy();
            GetAnalysisStateResponse response;
            if (isHealthy) {
                response = GetAnalysisStateResponse.newBuilder()
                        .setAnalysisState(AnalysisState.SUCCEEDED)
                        .build();
            } else {
                response = GetAnalysisStateResponse.newBuilder()
                        .setAnalysisState(AnalysisState.FAILED)
                        .setTimeOutSetting(marketRunner.getRtAnalysisTimeoutSecs())
                        .build();
            }
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription("Failed to query analysis state.")
                    .asException());
        }
        return;
    }
}
