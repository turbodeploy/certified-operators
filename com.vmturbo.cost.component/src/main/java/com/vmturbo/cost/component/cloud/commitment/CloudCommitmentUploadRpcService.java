package com.vmturbo.cost.component.cloud.commitment;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.UploadCloudCommitmentDataRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.UploadCloudCommitmentDataResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentUploadServiceGrpc.CloudCommitmentUploadServiceImplBase;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;

/**
 * An RPC service for uploading cloud commitment data, including coverage and utilization stats.
 */
public class CloudCommitmentUploadRpcService extends CloudCommitmentUploadServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final CloudCommitmentUtilizationStore cloudCommitmentUtilizationStore;

    private final CloudCommitmentCoverageStore cloudCommitmentCoverageStore;

    /**
     * Constructs a new {@link CloudCommitmentUploadRpcService} instance.
     * @param cloudCommitmentCoverageStore The cloud commitment coverage store.
     * @param cloudCommitmentUtilizationStore The cloud commitment utilization store.
     */
    public CloudCommitmentUploadRpcService(@Nonnull CloudCommitmentUtilizationStore cloudCommitmentUtilizationStore,
                                           @Nonnull CloudCommitmentCoverageStore cloudCommitmentCoverageStore) {
        this.cloudCommitmentUtilizationStore = Objects.requireNonNull(cloudCommitmentUtilizationStore);
        this.cloudCommitmentCoverageStore = Objects.requireNonNull(cloudCommitmentCoverageStore);
    }

    /**
     * Receives and persists cloud commitment data, containing the coverage and utilization statistics.
     * @param request The {@link UploadCloudCommitmentDataRequest}.
     * @param responseObserver The {@link UploadCloudCommitmentDataResponse}.
     */
    @Override
    public void uploadCloudCommitmentData(final UploadCloudCommitmentDataRequest request,
                                          final StreamObserver<UploadCloudCommitmentDataResponse> responseObserver) {

        try {
            final Stopwatch stopwatch = Stopwatch.createStarted();

            try {
                cloudCommitmentUtilizationStore.persistUtilizationSamples(
                        request.getCloudCommitmentData().getUtilizationDataList());
            } catch (Exception e) {
                logger.error("Error in persisting cloud commitment utilization data", e);
            }

            try {
                cloudCommitmentCoverageStore.persistCoverageSamples(
                        request.getCloudCommitmentData().getCoverageDataList());
            } catch (Exception e) {
                logger.error("Error in persisting cloud commitment coverage data", e);
            }

            logger.info("Persisted cloud commitment data in {}", stopwatch);

            responseObserver.onNext(UploadCloudCommitmentDataResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error persisting cloud commitment data", e);
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }

    }
}
