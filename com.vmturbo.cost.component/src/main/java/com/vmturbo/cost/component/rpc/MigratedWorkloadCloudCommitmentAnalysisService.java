package com.vmturbo.cost.component.rpc;

import java.util.List;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisResponse;
import com.vmturbo.common.protobuf.cost.MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.MigratedWorkloadCloudCommitmentAnalysisServiceImplBase;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSender;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.MigratedWorkloadCloudCommitmentAlgorithmStrategy;

/**
 * Performs cloud commitment (Buy RI) analysis for migrated workloads, such as from a migrate-to-cloud plan.
 */
@Service
public class MigratedWorkloadCloudCommitmentAnalysisService extends MigratedWorkloadCloudCommitmentAnalysisServiceImplBase {
    private static final Logger logger = LogManager.getLogger(MigratedWorkloadCloudCommitmentAnalysisService.class);

    private final ReservedInstanceActionsSender actionsSender;

    /**
     * Creates a new MigratedWorkloadCloudCommitmentAnalysisService.
     *
     * @param actionsSender The action sender that is used to publish our action plan to the action orchestrator
     */
    public MigratedWorkloadCloudCommitmentAnalysisService(ReservedInstanceActionsSender actionsSender) {
        this.actionsSender = actionsSender;
    }

    /**
     * The strategy that implements our cloud commitment algorithm.
     */
    @Autowired
    private MigratedWorkloadCloudCommitmentAlgorithmStrategy migratedWorkloadCloudCommitmentAlgorithmStrategy;

    /**
     * Starts the cloud commitment analysis.
     * @param request           The gRPC request
     * @param responseObserver  The gRPC response observer
     */
    @Override
    public void startAnalysis(MigratedWorkloadCloudCommitmentAnalysisRequest request,
                              StreamObserver<MigratedWorkloadCloudCommitmentAnalysisResponse> responseObserver) {
        logger.info("MigratedWorkloadCloudCommitmentAnalysisService::startAnalysis for topology: {}", request.getTopologyContextId());
        long analysisStartTime = System.currentTimeMillis();

        // Respond back to our client that we've received the message
        responseObserver.onNext(MigratedWorkloadCloudCommitmentAnalysisResponse.getDefaultInstance());
        responseObserver.onCompleted();

        // Analyze the topology
        List<ActionDTO.Action> actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(request.getVirtualMachinesList(),
                request.getBusinessAccount(),
                request.getMigrationProfile(),
                request.getTopologyContextId());

        // Create an ActionPlan
        ActionDTO.ActionPlan actionPlan = ActionDTO.ActionPlan.newBuilder()
                .setId(IdentityGenerator.next())
                .setAnalysisStartTimestamp(analysisStartTime)
                .setAnalysisCompleteTimestamp(System.currentTimeMillis())
                .setInfo(ActionDTO.ActionPlanInfo.newBuilder()
                        .setBuyRi(ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo.newBuilder()
                                .setTopologyContextId(request.getTopologyContextId())
                                .build())
                        .build())
                .addAllAction(actions)
                .build();

        try {
            // Publish the ActionPlan to Kafka
            actionsSender.notifyActionsRecommended(actionPlan);
        } catch (CommunicationException e) {
            logger.error("An error occurred while publishing ActionPlan: {}", e);
        } catch (InterruptedException e) {
            logger.error("An error occurred while publishing ActionPlan: {}", e);
        }
    }
}
