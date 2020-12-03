package com.vmturbo.market.rpc;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.market.MarketDebug.AnalysisInput;
import com.vmturbo.common.protobuf.market.MarketDebug.AnalysisOutput;
import com.vmturbo.common.protobuf.market.MarketDebug.ControlAnalysisCollectionRequest;
import com.vmturbo.common.protobuf.market.MarketDebug.ControlAnalysisCollectionResponse;
import com.vmturbo.common.protobuf.market.MarketDebug.GetAnalysisInfoRequest;
import com.vmturbo.common.protobuf.market.MarketDebug.GetAnalysisInfoResponse;
import com.vmturbo.common.protobuf.market.MarketDebugServiceGrpc.MarketDebugServiceImplBase;
import com.vmturbo.common.protobuf.market.MarketNotification.AnalysisStatusNotification.AnalysisState;
import com.vmturbo.market.runner.Analysis;
import com.vmturbo.market.runner.AnalysisFactory.AnalysisConfig;

public class MarketDebugRpcService extends MarketDebugServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private volatile Optional<Analysis> latestRealtimeAnalysis = Optional.empty();

    private volatile Optional<Analysis> latestPlanAnalysis = Optional.empty();

    private final long realtimeTopologyContextId;

    private final AtomicBoolean analysisCollectionEnabled = new AtomicBoolean(false);

    public MarketDebugRpcService(final long realtimeTopologyContextId) {
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * Store a completed analysis in the service, making it accessible by external clients.
     *
     * @param analysis The {@link Analysis}.
     */
    public void recordCompletedAnalysis(@Nonnull final Analysis analysis) {
        if (analysisCollectionEnabled.get()) {
            if (!analysis.isDone()) {
                logger.error("Attempting to record incomplete analysis in context {}. Failure!",
                        analysis.getContextId());
            } else {
                if (analysis.getContextId() == realtimeTopologyContextId) {
                    logger.info("Replacing latest realtime analysis.");
                    latestRealtimeAnalysis = Optional.of(analysis);
                } else {
                    logger.info("Replacing latest plan analysis with plan {}.", analysis.getContextId());
                    latestPlanAnalysis = Optional.of(analysis);
                }
            }
        }
    }

    @Override
    public void controlAnalysisCollection(ControlAnalysisCollectionRequest request,
                                          StreamObserver<ControlAnalysisCollectionResponse> responseObserver) {
        analysisCollectionEnabled.set(request.getEnable());
        responseObserver.onNext(ControlAnalysisCollectionResponse.newBuilder()
                .setEnabled(analysisCollectionEnabled.get())
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void getAnalysisInfo(GetAnalysisInfoRequest request,
                                StreamObserver<GetAnalysisInfoResponse> responseObserver) {
        if (!analysisCollectionEnabled.get()) {
            logger.info("Request for analysis info when analysis collection disabled." +
                    "Enabling analysis collection.");
            // This is for convenience, so a user doesn't need to make a separate API call to
            // enable analysis collection.
            analysisCollectionEnabled.set(true);
            responseObserver.onError(Status.UNAVAILABLE
                .withDescription("Analysis collection was not enabled. It is now enabled, so " +
                    " try again after re-broadcasting the topolology. Don't forget to disable " +
                    "analysis collection once you no longer need it!").asException());
            return;
        }
        final Optional<Analysis> analysisOpt = request.getLatestRealtime() ? latestRealtimeAnalysis : latestPlanAnalysis;
        if (analysisOpt.isPresent()) {
            final Analysis analysis = analysisOpt.get();
            final AnalysisConfig analysisConfig = analysis.getConfig();
            final AnalysisInput.Builder analysisInputBuilder = AnalysisInput.newBuilder()
                    .setTopologyInfo(analysis.getTopologyInfo())
                    .addAllEntities(analysis.getTopology().values())
                    .setIncludeVdc(analysisConfig.getIncludeVdc())
                    .setUseQuoteCacheDuringSnm(analysisConfig.getUseQuoteCacheDuringSNM())
                    .setReplayProvisionsForRealTime(analysisConfig.getReplayProvisionsForRealTime())
                    .setRightSizeLowerWatermark(analysisConfig.getRightsizeLowerWatermark())
                    .setRightSizeUpperWatermark(analysisConfig.getRightsizeUpperWatermark())
                    .setQuoteFactor(analysisConfig.getQuoteFactor())
                    .putAllSettings(analysisConfig.getGlobalSettingMap());
            analysisConfig.getMaxPlacementsOverride().ifPresent(analysisInputBuilder::setMaxPlacementsOverride);

            final GetAnalysisInfoResponse.Builder respBuilder = GetAnalysisInfoResponse.newBuilder()
                    .setInput(analysisInputBuilder)
                    .setStartTime(analysis.getStartTime().toEpochMilli())
                    .setEndTime(analysis.getCompletionTime().toEpochMilli());

            // The state of a completed analysis may also be "failed".
            if (analysis.getState().equals(AnalysisState.SUCCEEDED)) {
                respBuilder.setOutput(AnalysisOutput.newBuilder()
                    .setActionPlan(analysis.getActionPlan().get())
                    .addAllProjectedEntities(analysis.getProjectedTopology().get()));
            }

            responseObserver.onNext(GetAnalysisInfoResponse.newBuilder()
                    .setInput(analysisInputBuilder)
                    .build());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND.withDescription("No completed " +
                    (request.getLatestRealtime() ? "realtime" : "plan") + " analysis available." +
                    " Try rebroadcasting.").asException());
        }
    }

}
