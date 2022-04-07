package com.vmturbo.cost.component.cloud.commitment;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
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
import com.vmturbo.common.protobuf.cloud.CloudCommitmentStatsServiceGrpc.CloudCommitmentStatsServiceImplBase;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore;
import com.vmturbo.cost.component.cloud.commitment.coverage.CloudCommitmentCoverageStore.AccountCoverageStatsFilter;
import com.vmturbo.cost.component.cloud.commitment.coverage.CoverageInfo;
import com.vmturbo.cost.component.cloud.commitment.coverage.TopologyEntityCoverageFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore;
import com.vmturbo.cost.component.cloud.commitment.utilization.CloudCommitmentUtilizationStore.CloudCommitmentUtilizationStatsFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.TopologyCommitmentUtilizationFilter;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SourceProjectedFieldsDataStore;

/**
 * An RPC service for querying cloud commitment stats (e.g. coverage,utilization).
 */
public class CloudCommitmentStatsRpcService extends CloudCommitmentStatsServiceImplBase {

    private static final Logger logger = LogManager.getLogger();

    private final CloudCommitmentCoverageStore coverageStore;

    private final CloudCommitmentUtilizationStore utilizationStore;

    private final CloudCommitmentStatsConverter statsConverter;

    private final int maxStatRecordsPerChunk;

    private final SourceProjectedFieldsDataStore<CoverageInfo, TopologyEntityCoverageFilter> topologyCoverageStore;

    private final SourceProjectedFieldsDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> topologyUtilizationStore;

    /**
     * Constructs a new {@link CloudCommitmentStatsRpcService} instance.
     * @param cloudCommitmentCoverageStore The cloud commitment coverage store.
     * @param utilizationStore The cloud commitment utilization store.
     * @param topologyCoverageStore The cloud commitment topology coverage store
     * @param topologyUtilizationStore The topology commitment utilization store.
     * @param statsConverter The converter of commitment data to stats records.
     * @param maxStatRecordsPerChunk The max number of records to return per chunk in streaming
     *                               commitment stats.
     */
    public CloudCommitmentStatsRpcService(
            @Nonnull final CloudCommitmentCoverageStore cloudCommitmentCoverageStore,
            @Nonnull final CloudCommitmentUtilizationStore utilizationStore,
            @Nonnull final SourceProjectedFieldsDataStore<CoverageInfo, TopologyEntityCoverageFilter> topologyCoverageStore,
            @Nonnull final SourceProjectedFieldsDataStore<UtilizationInfo, TopologyCommitmentUtilizationFilter> topologyUtilizationStore,
            @Nonnull CloudCommitmentStatsConverter statsConverter,
            final int maxStatRecordsPerChunk) {

        Preconditions.checkArgument(maxStatRecordsPerChunk > 0, "Max stat record per chunk must be positive");

        this.coverageStore = Objects.requireNonNull(cloudCommitmentCoverageStore);
        this.utilizationStore = Objects.requireNonNull(utilizationStore);
        this.maxStatRecordsPerChunk = maxStatRecordsPerChunk;
        this.topologyCoverageStore = Objects.requireNonNull(topologyCoverageStore);
        this.topologyUtilizationStore = Objects.requireNonNull(topologyUtilizationStore);
        this.statsConverter = Objects.requireNonNull(statsConverter);
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

            // If this is a request for entity coverage, an empty response should be returned. Entity level coverage stats
            // are currently not supported.
            if (!request.hasEntityFilter()) {
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

        final TopologyEntityCoverageFilter coverageFilter = TopologyEntityCoverageFilter.builder()
                .entityFilter(request.getEntityFilter())
                .accountFilter(request.getAccountFilter())
                .regionFilter(request.getRegionFilter())
                .serviceProviderFilter(request.getServiceProviderFilter())
                .build();

        topologyCoverageStore.filterData(request.getTopologyType(), coverageFilter)
                .map(coverageInfo -> statsConverter.convertToCoverageStats(coverageInfo, request.getGroupByList()))
                .ifPresent(coverageStats -> Iterables.partition(coverageStats, getRequestedChunkSize(request.getChunkSize()))
                        .forEach(statsChunk -> responseObserver.onNext(
                                GetTopologyCommitmentCoverageStatsResponse.newBuilder()
                                        .addAllCommitmentCoverageStatChunk(statsChunk)
                                        .build())));
        responseObserver.onCompleted();
    }

    @Override
    public void getTopologyCommitmentUtilization(
            @Nonnull final GetTopologyCommitmentUtilizationStatsRequest request,
            @Nonnull final StreamObserver<GetTopologyCommitmentUtilizationStatsResponse> responseObserver) {

        final TopologyCommitmentUtilizationFilter utilizationFilter = TopologyCommitmentUtilizationFilter.builder()
                .cloudCommitmentFilter(request.getCloudCommitmentFilter())
                .accountFilter(request.getAccountFilter())
                .regionFilter(request.getRegionFilter())
                .serviceProviderFilter(request.getServiceProviderFilter())
                .build();

        topologyUtilizationStore.filterData(request.getTopologyType(), utilizationFilter)
                .map(utilizationInfo -> statsConverter.convertToUtilizationStats(utilizationInfo, request.getGroupByList()))
                .ifPresent(utilizationStats -> Iterables.partition(utilizationStats, getRequestedChunkSize(request.getChunkSize()))
                        .forEach(statsChunk -> {
                            final GetTopologyCommitmentUtilizationStatsResponse.Builder nextResponse =
                                    GetTopologyCommitmentUtilizationStatsResponse.newBuilder()
                                            .addAllCommitmentUtilizationRecordChunk(statsChunk);
                            responseObserver.onNext(nextResponse.build());

                        }));
        responseObserver.onCompleted();
    }
}
