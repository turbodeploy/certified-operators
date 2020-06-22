package com.vmturbo.cost.component.rpc;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisResponse;
import com.vmturbo.common.protobuf.cost.MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.MigratedWorkloadCloudCommitmentAnalysisServiceImplBase;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.MigratedWorkloadCloudCommitmentAlgorithmStrategy;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Performs cloud commitment (Buy RI) analysis for migrated workloads, such as from a migrate-to-cloud plan
 */
@Service
public class MigratedWorkloadCloudCommitmentAnalysisService extends MigratedWorkloadCloudCommitmentAnalysisServiceImplBase {
    private static final Logger logger = LogManager.getLogger(MigratedWorkloadCloudCommitmentAnalysisService.class);

    /**
     * The strategy that implements our cloud commitment algorithm.
     */
    @Autowired
    private MigratedWorkloadCloudCommitmentAlgorithmStrategy migratedWorkloadCloudCommitmentAlgorithmStrategy;

    @Override
    public void startAnalysis(MigratedWorkloadCloudCommitmentAnalysisRequest request,
                              StreamObserver<MigratedWorkloadCloudCommitmentAnalysisResponse> responseObserver) {
        logger.info("MigratedWorkloadCloudCommitmentAnalysisService::startAnalysis");

        // Respond back to our client that we've received the message
        responseObserver.onNext(MigratedWorkloadCloudCommitmentAnalysisResponse.getDefaultInstance());
        responseObserver.onCompleted();

        // Analyze the topology
        List<ActionDTO.Action> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(request.getVirtualMachinesList());

        // TODO: Create Buy RI actions and send them to the action orchestrator

    }
}
