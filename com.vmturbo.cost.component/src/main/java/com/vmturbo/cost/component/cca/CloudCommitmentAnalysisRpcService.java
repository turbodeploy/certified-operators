package com.vmturbo.cost.component.cca;

import java.util.Objects;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import com.vmturbo.cloud.commitment.analysis.CloudCommitmentAnalysisManager;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cost.CloudCommitmentAnalysisServiceGrpc.CloudCommitmentAnalysisServiceImplBase;
import com.vmturbo.common.protobuf.cost.Cost.StartCloudCommitmentAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.StartCloudCommitmentAnalysisResponse;


public class CloudCommitmentAnalysisRpcService extends CloudCommitmentAnalysisServiceImplBase {

    private final CloudCommitmentAnalysisManager analysisManager;

    public CloudCommitmentAnalysisRpcService(@Nonnull CloudCommitmentAnalysisManager analysisManager) {
        this.analysisManager = Objects.requireNonNull(analysisManager);
    }

    @Override
    public void startCloudCommitmentAnalysis(final StartCloudCommitmentAnalysisRequest request,
                                             final StreamObserver<StartCloudCommitmentAnalysisResponse> responseObserver) {

        final CloudCommitmentAnalysisInfo analysisInfo = analysisManager.startAnalysis(request.getAnalysisConfig());

        responseObserver.onNext(StartCloudCommitmentAnalysisResponse.newBuilder()
                .setCloudCommitmentAnalysisInfo(analysisInfo)
                .build());
        responseObserver.onCompleted();

    }
}
