package com.vmturbo.market.runner.cost;

import java.util.List;

import io.grpc.Channel;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.MigratedWorkloadCloudCommitmentAnalysisServiceGrpc;
import com.vmturbo.platform.sdk.common.CloudCostDTO;

/**
 * Service implementation that interacts with the cost component's migrated workload cloud commitment service.
 */
public class MigratedWorkloadCloudCommitmentAnalysisServiceImpl implements MigratedWorkloadCloudCommitmentAnalysisService {

    /**
     * The gRPC service stub through which we send our requests.
     */
    private MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.MigratedWorkloadCloudCommitmentAnalysisServiceBlockingStub client;

    /**
     * Creates a new MigratedWorkloadCloudCommitmentAnalysisServiceImpl using the specified cost channel. The constructor
     * creates a new gRPC client stub through which to communicate with the cost component's migrated workload cloud commitment
     * service.
     *
     * @param costChannel   The gRPC channel through which we communicate with the cost component's migrated workload
     *                      cloud commitment service
     */
    public MigratedWorkloadCloudCommitmentAnalysisServiceImpl(Channel costChannel) {
        // Create an blocking stub to the MigratedWorkloadCloudCommitmentAnalysisService
        client = MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.newBlockingStub(costChannel);
    }

    /**
     * Starts a migrated workload cloud commitment analysis (Buy RI analysis).
     * @param topologyContextId         The plan topology ID
     * @param workloadPlacementList     A list of workload placements (VM, compute tier, and region)
     */
    @Override
    public void startAnalysis(long topologyContextId, List<Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement> workloadPlacementList) {
        Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.Builder builder = Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.newBuilder();

        builder.setTopologyContextId(topologyContextId);

        // TODO: this information needs to be extracted from the plan scenario; hardcoded for now
        builder.setMigrationProfile(Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigrationProfile.newBuilder()
                .setPreferredTerm(3)
                .setPreferredOfferingClass(CloudCostDTO.ReservedInstanceType.OfferingClass.STANDARD)
                .setPreferredPaymentOption(CloudCostDTO.ReservedInstanceType.PaymentOption.ALL_UPFRONT)
                .build());

        // Add the virtual machines
        builder.addAllVirtualMachines(workloadPlacementList);

        // Send the gRPC request to start the analysis
        client.startAnalysis(builder.build());
    }
}
