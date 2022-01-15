package com.vmturbo.cost.component.cloud.commitment;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.CloudCommitmentStatRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCloudCommitmentUtilizationResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetHistoricalCommitmentCoverageStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentCoverageStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentCoverageStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetTopologyCommitmentUtilizationStatsResponse.CloudCommitmentUtilizationRecord;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.TopologyType;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc.CloudCommitmentStatsServiceImplBase;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore.AccountCoverageStatsFilter;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore.CloudCommitmentUtilizationStatsFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SourceProjectedFieldsDataStore;

/**
 * An RPC service for querying cloud commitment stats (e.g. coverage,utilization).
 */
public class CloudCommitmentStatsRpcService extends CloudCommitmentStatsServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final CloudCommitmentCoverageStore coverageStore;

    private final CloudCommitmentUtilizationStore utilizationStore;

    private final int maxStatRecordsPerChunk;

    private final Map<TopologyType, Supplier<Optional<UtilizationInfo>>>
            topologyTypeToUtilizationGetter;

    private final Map<TopologyType, Supplier<Optional<CoverageInfo>>> topologyTypeToCoverageGetter;

    /**
     * Constructs a new {@link CloudCommitmentStatsRpcService} instance.
     * @param cloudCommitmentCoverageStore The cloud commitment coverage store.
     * @param utilizationStore The cloud commitment utilization store.
     * @param topologyCoverageStore The cloud commitment topology coverage store
     * @param topologyUtilizationStore The topology commitment utilization store.
     * @param maxStatRecordsPerChunk The max number of records to return per chunk in streaming
     *                               commitment stats.
     */
    public CloudCommitmentStatsRpcService(
            @Nonnull final CloudCommitmentCoverageStore cloudCommitmentCoverageStore,
            @Nonnull final CloudCommitmentUtilizationStore utilizationStore,
            @Nonnull final SourceProjectedFieldsDataStore<CoverageInfo> topologyCoverageStore,
            @Nonnull final SourceProjectedFieldsDataStore<UtilizationInfo> topologyUtilizationStore,
            final int maxStatRecordsPerChunk) {

        Preconditions.checkArgument(maxStatRecordsPerChunk > 0, "Max stat record per chunk must be positive");

        this.coverageStore = Objects.requireNonNull(cloudCommitmentCoverageStore);
        this.utilizationStore = Objects.requireNonNull(utilizationStore);
        this.maxStatRecordsPerChunk = maxStatRecordsPerChunk;
        this.topologyTypeToUtilizationGetter = createTopologyTypeToStoreMethod(
                topologyUtilizationStore);
        this.topologyTypeToCoverageGetter = createTopologyTypeToStoreMethod(topologyCoverageStore);
    }

    private <T> Map<TopologyType, Supplier<Optional<T>>> createTopologyTypeToStoreMethod(
            @Nonnull final SourceProjectedFieldsDataStore<T> store) {
        return ImmutableMap.of(TopologyType.TOPOLOGY_TYPE_SOURCE, store::getSourceData,
                TopologyType.TOPOLOGY_TYPE_PROJECTED, store::getProjectedData);
    }

    private int getRequestedChunkSize(final int chunkSize) {
        return Math.min(Math.max(chunkSize, 1), maxStatRecordsPerChunk);
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
            final int requestedStatsPerChunk = getRequestedChunkSize(request.getChunkSize());

            final CloudCommitmentUtilizationStatsFilter statsFilter = CloudCommitmentUtilizationStatsFilter.builder()
                    .startTime(request.hasStartTime()
                            ? Optional.of(Instant.ofEpochMilli(request.getStartTime()))
                            : Optional.empty())
                    .endTime(request.hasEndTime()
                            ? Optional.of(Instant.ofEpochMilli(request.getEndTime()))
                            : Optional.empty())
                    .granularity(request.hasGranularity()
                             ? Optional.of(request.getGranularity())
                             : Optional.empty())
                    .regionFilter(request.getRegionFilter())
                    .accountFilter(request.getAccountFilter())
                    .cloudCommitmentFilter(request.getCloudCommitmentFilter())
                    .serviceProviderFilter(request.getServiceProviderFilter())
                    .groupByList(request.getGroupByList())
                    .build();

            try (Stream<CloudCommitmentStatRecord> statRecordStream =
                         utilizationStore.streamUtilizationStats(statsFilter)) {

                Iterators.partition(statRecordStream.iterator(), requestedStatsPerChunk)
                        .forEachRemaining(statRecordChunk ->
                                responseObserver.onNext(
                                        GetHistoricalCloudCommitmentUtilizationResponse.newBuilder()
                                                .addAllCommitmentStatRecordChunk(statRecordChunk)
                                                .build()));
            }


            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    @Override
    public void getHistoricalCommitmentCoverageStats(final GetHistoricalCommitmentCoverageStatsRequest request,
                                                     final StreamObserver<GetHistoricalCommitmentCoverageStatsResponse> responseObserver) {
        try {

            logger.debug("Handling historical commitment coverage stats request: {}", request::toString);

            final int requestedStatsPerChunk = getRequestedChunkSize(request.getChunkSize());

            // As an initial pass, the assumption is that the request will always be for account aggregated
            // data. Once entity level granularity is supported, the request should be updated with a oneof
            // for AccountData (empty) and EntityData (entity level filter).
            final AccountCoverageStatsFilter statsFilter = AccountCoverageStatsFilter.builder()
                    .startTime(request.hasStartTime()
                            ? Optional.of(Instant.ofEpochMilli(request.getStartTime()))
                            : Optional.empty())
                    .endTime(request.hasEndTime()
                            ? Optional.of(Instant.ofEpochMilli(request.getEndTime()))
                            : Optional.empty())
                    .granularity(request.hasGranularity()
                            ? Optional.of(request.getGranularity())
                            : Optional.empty())
                    .regionFilter(request.getRegionFilter())
                    .accountFilter(request.getAccountFilter())
                    .serviceProviderFilter(request.getServiceProviderFilter())
                    .groupByList(request.getGroupByList())
                    .build();

            try (Stream<CloudCommitmentStatRecord> statRecordStream = coverageStore.streamCoverageStats(statsFilter)) {
                Iterators.partition(statRecordStream.iterator(), requestedStatsPerChunk)
                        .forEachRemaining(statRecordChunk ->
                                responseObserver.onNext(
                                        GetHistoricalCommitmentCoverageStatsResponse.newBuilder()
                                                .addAllCommitmentStatRecordChunk(statRecordChunk)
                                                .build()));
            }

            responseObserver.onCompleted();

        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    @Override
    public void getTopologyCommitmentCoverage(
            final GetTopologyCommitmentCoverageStatsRequest request,
            final StreamObserver<GetTopologyCommitmentCoverageStatsResponse> responseObserver) {
        final Supplier<Optional<CoverageInfo>> coverageGetter = topologyTypeToCoverageGetter.get(
                request.getTopologyType());
        if (coverageGetter != null) {
            coverageGetter.get().ifPresent(coverageInfo -> Iterators.partition(
                    coverageInfo.coverageDataPoints().iterator(),
                    getRequestedChunkSize(request.getChunkSize()))
                    .forEachRemaining(chunk -> responseObserver.onNext(
                            GetTopologyCommitmentCoverageStatsResponse.newBuilder()
                                    .addAllCommitmentCoverageStatChunk(chunk)
                                    .build())));
        }
        responseObserver.onCompleted();
    }

    @Override
    public void getTopologyCommitmentUtilization(
            @Nonnull final GetTopologyCommitmentUtilizationStatsRequest request,
            @Nonnull final StreamObserver<GetTopologyCommitmentUtilizationStatsResponse> responseObserver) {
        final Supplier<Optional<UtilizationInfo>> utilizationGetter =
                topologyTypeToUtilizationGetter.get(request.getTopologyType());
        if (utilizationGetter != null) {
            utilizationGetter.get().ifPresent(utilizationInfo -> Iterators.partition(
                    utilizationInfo.commitmentIdToUtilization().entrySet().iterator(),
                    getRequestedChunkSize(request.getChunkSize())).forEachRemaining(chunk -> {
                final GetTopologyCommitmentUtilizationStatsResponse.Builder nextResponse =
                        GetTopologyCommitmentUtilizationStatsResponse.newBuilder();
                chunk.forEach(u -> nextResponse.addCommitmentUtilizationRecordChunk(
                        CloudCommitmentUtilizationRecord.newBuilder()
                                .setCommitmentOid(u.getKey())
                                .setUtilization(u.getValue())));
                responseObserver.onNext(nextResponse.build());
            }));
        }
        responseObserver.onCompleted();
    }
}
