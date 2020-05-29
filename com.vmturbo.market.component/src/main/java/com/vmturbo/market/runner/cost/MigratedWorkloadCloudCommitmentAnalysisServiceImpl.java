package com.vmturbo.market.runner.cost;

import com.vmturbo.common.protobuf.cost.CostREST;
import com.vmturbo.common.protobuf.cost.MigratedWorkloadCloudCommitmentAnalysisServiceGrpc;
import io.grpc.Channel;

public class MigratedWorkloadCloudCommitmentAnalysisServiceImpl implements MigratedWorkloadCloudCommitmentAnalysisService {

    private MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.MigratedWorkloadCloudCommitmentAnalysisServiceBlockingStub client;

    public MigratedWorkloadCloudCommitmentAnalysisServiceImpl(Channel costChannel) {
        // Create an blocking stub to the MigratedWorkloadCloudCommitmentAnalysisService
        client = MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.newBlockingStub(costChannel);
    }

    @Override
    public void startAnalysis(String topology) {
        com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.Builder
                builder = com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest
                .newBuilder();
        client.startAnalysis(builder.build());
    }
}
