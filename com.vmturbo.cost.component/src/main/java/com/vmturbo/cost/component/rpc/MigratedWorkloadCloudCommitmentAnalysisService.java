package com.vmturbo.cost.component.rpc;

import com.vmturbo.common.protobuf.cost.MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.MigratedWorkloadCloudCommitmentAnalysisServiceImplBase;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisResponse;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Performs cloud commitment (Buy RI) analysis for migrated workloads, such as from a migrate-to-cloud plan
 */
public class MigratedWorkloadCloudCommitmentAnalysisService extends MigratedWorkloadCloudCommitmentAnalysisServiceImplBase {
    private static final Logger logger = LogManager.getLogger(MigratedWorkloadCloudCommitmentAnalysisService.class);

    @Override
    public void startAnalysis(MigratedWorkloadCloudCommitmentAnalysisRequest request,
                              StreamObserver<MigratedWorkloadCloudCommitmentAnalysisResponse> responseObserver) {
        logger.info("MigratedWorkloadCloudCommitmentAnalysisService::startAnalysis");
        responseObserver.onCompleted();
    }
}
