package com.vmturbo.market.runner.cost;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigrationProfile;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisResponse;
import com.vmturbo.common.protobuf.cost.MigratedWorkloadCloudCommitmentAnalysisServiceGrpc;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CommonCost.PaymentOption;

/**
 * Service implementation that interacts with the cost component's migrated workload cloud commitment service.
 */
public class MigratedWorkloadCloudCommitmentAnalysisServiceImpl implements MigratedWorkloadCloudCommitmentAnalysisService {
    private static final Logger logger = LogManager.getLogger(MigratedWorkloadCloudCommitmentAnalysisServiceImpl.class);

    // Maximum number of workloads to pass at a time to
    // MigratedWorkloadCloudCommitmentAnalysisService.startAnalysis in order to avoid gRPC buffer
    // overflow.
    private static final int WORKLOAD_CHUNK_SIZE = 800;

    /**
     * The gRPC service stub through which we send our requests.
     */
    private MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.MigratedWorkloadCloudCommitmentAnalysisServiceStub client;

    /**
     * Creates a new MigratedWorkloadCloudCommitmentAnalysisServiceImpl using the specified cost channel. The constructor
     * creates a new gRPC client stub through which to communicate with the cost component's migrated workload cloud commitment
     * service.
     *
     * @param costChannel The gRPC channel through which we communicate with the cost component's migrated workload
     *                    cloud commitment service
     */
    public MigratedWorkloadCloudCommitmentAnalysisServiceImpl(Channel costChannel) {
        // Create an blocking stub to the MigratedWorkloadCloudCommitmentAnalysisService
         client = MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.newStub(costChannel);
    }

    /**
     * Starts a migrated workload cloud commitment analysis (Buy RI analysis).
     *
     * @param topologyContextId     The plan topology ID
     * @param workloadPlacementList A list of workload placements (VM, compute tier, and region)
     */
    @Override
    public void startAnalysis(long topologyContextId, Optional<Long> businessAccountOid,
            List<Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement> workloadPlacementList) {
        // Streaming client calls must use an async request, so use a latch to block until the
        // request is complete.
        CountDownLatch latch = new CountDownLatch(1);
        // Create a response observer.  There are no responses, but we need to handle error and
        // completion events.
        StreamObserver<MigratedWorkloadCloudCommitmentAnalysisResponse> response =
                new StreamObserver<MigratedWorkloadCloudCommitmentAnalysisResponse>() {
                    @Override
                    public void onNext(MigratedWorkloadCloudCommitmentAnalysisResponse
                            migratedWorkloadCloudCommitmentAnalysisResponse) {
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        logger.error("Failed to start migrated workload cloud commitment analysis: {}",
                                throwable.getMessage());
                        latch.countDown();
                    }

                    @Override
                    public void onCompleted() {
                        latch.countDown();
                    }
                };

        // Send the gRPC request to start the analysis. Due to the potential size of the request,
        // break it up into chucks in order to avoid gRPC buffer overflow.
        logger.info("Starting analysis for topology: {}", topologyContextId);
        StreamObserver<MigratedWorkloadCloudCommitmentAnalysisRequest> request =
                client.startAnalysis(response);
        MigrationProfile migrationProfile = MigrationProfile.newBuilder()
                .setPreferredTerm(3)
                .setPreferredOfferingClass(OfferingClass.STANDARD)
                .setPreferredPaymentOption(PaymentOption.ALL_UPFRONT)
                .build();

        List<List<MigratedWorkloadPlacement>> chunks =
            Lists.partition(workloadPlacementList, WORKLOAD_CHUNK_SIZE);
        if (chunks.isEmpty()) {
            // Ensure we send at least one chunk, so that the server
            // gets the context information and can send an action plan
            // even if there's nothing to do.

            chunks = Collections.singletonList(Collections.emptyList());
        }

        for (List<Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement>
                chunk : chunks) {
            if (latch.getCount() == 0) {
                // For some reason, the server sent a premature response, most likely due to
                // an error.  Don't bother sending the rest of the request and report the error.
                logger.error("Failed to start migrated workload cloud commitment analysis: "
                        + "aborted at server");
            }
            Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.Builder builder =
                    Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.newBuilder()
                .setTopologyContextId(topologyContextId)
                // TODO: this information needs to be extracted from the plan scenario; hardcoded for now
                .setMigrationProfile(migrationProfile)
                // Add a batch of virtual machines
                .addAllVirtualMachines(chunk);
            businessAccountOid.ifPresent(ba -> builder.setBusinessAccount(ba));
            request.onNext(builder.build());
        }
        request.onCompleted();
        // Wait up to two minutes for completion.  Since we are not waiting for a result, it is
        // okay if we time out before the actual operation is complete.  This will wait up to
        // four minutes in the unlikely event that the thread is interrupted.
        long end = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(2);
        while (System.currentTimeMillis() < end) {
            try {
                latch.await(2, TimeUnit.MINUTES);
                break;
            } catch (InterruptedException e) {
                // Keep waiting
            }
        }
    }
}
