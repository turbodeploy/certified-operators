package com.vmturbo.cost.component.cloud.commitment;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterators;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc.CloudCommitmentStatsServiceImplBase;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore.CloudCommitmentUtilizationStatsFilter;

/**
 * An RPC service for querying cloud commitment stats (e.g. coverage,utilization).
 */
public class CloudCommitmentStatsRpcService extends CloudCommitmentStatsServiceImplBase {

    private final CloudCommitmentUtilizationStore utilizationStore;

    private final int maxStatRecordsPerChunk;

    /**
     * Constructs a new {@link CloudCommitmentStatsRpcService} instance.
     * @param utilizationStore The cloud commitment utilization store.
     * @param maxStatRecordsPerChunk The max number of records to return per chunk in streaming
     *                               commitment stats.
     */
    public CloudCommitmentStatsRpcService(@Nonnull CloudCommitmentUtilizationStore utilizationStore,
                                          @Nonnull int maxStatRecordsPerChunk) {

        Preconditions.checkArgument(maxStatRecordsPerChunk > 0, "Max stat record per chunk must be positive");

        this.utilizationStore = Objects.requireNonNull(utilizationStore);
        this.maxStatRecordsPerChunk = maxStatRecordsPerChunk;
    }

    /**
     * Retrieves the historical utilization of filtered cloud commitments based on the request.
     * @param request The {@link GetHistoricalCloudCommitmentUtilizationRequest}.
     * @param responseObserver The {@link GetHistoricalCloudCommitmentUtilizationResponse}.
     */
    @Override
    public void getHistoricalCommitmentUtilization(final GetHistoricalCloudCommitmentUtilizationRequest request,
                                                   final StreamObserver<GetHistoricalCloudCommitmentUtilizationResponse> responseObserver) {

        try {
            final int requestedStatsPerChunk = Math.min(Math.max(request.getChunkSize(), 0), maxStatRecordsPerChunk);

            final CloudCommitmentUtilizationStatsFilter statsFilter = CloudCommitmentUtilizationStatsFilter.builder()
                    .startTime(request.hasStartTime()
                            ? Optional.of(Instant.ofEpochMilli(request.getStartTime()))
                            : Optional.empty())
                    .endTime(request.hasEndTime()
                            ? Optional.of(Instant.ofEpochMilli(request.getEndTime()))
                            : Optional.empty())
                    .granularity(request.getGranularity())
                    .regionFilter(request.getRegionFilter())
                    .accountFilter(request.getAccountFilter())
                    .cloudCommitmentFilter(request.getCloudCommitmentFilter())
                    .serviceProviderFilter(request.getServiceProviderFilter())
                    .groupByList(request.getGroupByList())
                    .build();

            final Stream<CloudCommitmentStatRecord> statRecordStream = utilizationStore.streamUtilizationStats(statsFilter);
            Iterators.partition(statRecordStream.iterator(), requestedStatsPerChunk)
                    .forEachRemaining(statRecordChunk ->
                            responseObserver.onNext(
                                    GetHistoricalCloudCommitmentUtilizationResponse.newBuilder()
                                            .addAllCommitmentStatRecordChunk(statRecordChunk)
                                            .build()));

            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }
}
