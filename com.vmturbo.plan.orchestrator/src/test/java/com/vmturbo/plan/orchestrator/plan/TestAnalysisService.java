package com.vmturbo.plan.orchestrator.plan;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceImplBase;

/**
 * Test implementation of analysis GRPC service.
 */
public class TestAnalysisService extends AnalysisServiceImplBase {

    @Override
    public void startAnalysis(StartAnalysisRequest request,
            StreamObserver<StartAnalysisResponse> responseObserver) {
        responseObserver.onNext(
                StartAnalysisResponse.newBuilder().setEntitiesBroadcast(10).build());
        responseObserver.onCompleted();
    }
}
