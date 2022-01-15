package com.vmturbo.cost.component.cloud.commitment;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.cloud.CloudCommitmentServiceGrpc.CloudCommitmentServiceImplBase;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetCloudCommitmentInfoForAnalysisRequest;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetCloudCommitmentInfoForAnalysisResponse;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetCloudCommitmentInfoForAnalysisResponse.CommitmentInfoBucket;
import com.vmturbo.common.protobuf.cloud.CloudCommitmentServices.GetCloudCommitmentInfoForAnalysisResponse.CommitmentInfoBucket.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.cost.component.cloud.commitment.mapping.MappingInfo;
import com.vmturbo.cost.component.cloud.commitment.utilization.UtilizationInfo;
import com.vmturbo.cost.component.stores.SingleFieldDataStore;

/**
 * An RPC service for querying cloud commitments.
 */
public class CloudCommitmentRpcService extends CloudCommitmentServiceImplBase {

    private final SingleFieldDataStore<MappingInfo> sourceTopologyCommitmentMappingStore;
    private final SingleFieldDataStore<UtilizationInfo> sourceTopologyCommitmentUtilizationStore;

    /**
     * Constructor.
     *
     * @param sourceTopologyCommitmentMappingStore the source topology commitment mapping
     *         store.
     * @param sourceTopologyCommitmentUtilizationStore the source topology commitment
     *         utilization store.
     */
    public CloudCommitmentRpcService(
            @Nonnull final SingleFieldDataStore<MappingInfo> sourceTopologyCommitmentMappingStore,
            @Nonnull final SingleFieldDataStore<UtilizationInfo> sourceTopologyCommitmentUtilizationStore) {
        this.sourceTopologyCommitmentMappingStore = sourceTopologyCommitmentMappingStore;
        this.sourceTopologyCommitmentUtilizationStore = sourceTopologyCommitmentUtilizationStore;
    }

    @Override
    public void getCloudCommitmentInfoForAnalysis(
            final GetCloudCommitmentInfoForAnalysisRequest request,
            final StreamObserver<GetCloudCommitmentInfoForAnalysisResponse> responseObserver) {
        final Map<Long, CommitmentInfoBucket.Builder> timestampToBucket = new HashMap<>();
        final Function<TopologyInfo, Builder> getOrCreateCommitmentInfoBucket =
                topology -> timestampToBucket.computeIfAbsent(
                        topology.hasCreationTime() ? topology.getCreationTime() : null,
                        CloudCommitmentRpcService::createCommitmentInfoBucket);
        sourceTopologyCommitmentMappingStore.getData().ifPresent(
                mappingInfo -> getOrCreateCommitmentInfoBucket.apply(mappingInfo.topologyInfo())
                        .addAllCloudCommitmentMapping(mappingInfo.cloudCommitmentMappings()));
        sourceTopologyCommitmentUtilizationStore.getData().ifPresent(
                utilizationInfo -> utilizationInfo.commitmentIdToUtilization()
                        .forEach((commitmentId, utilization) -> {
                            if (utilization.hasOverhead()) {
                                getOrCreateCommitmentInfoBucket.apply(
                                        utilizationInfo.topologyInfo()).putCloudCommitmentOverhead(
                                        commitmentId, utilization.getOverhead());
                            }
                        }));
        final GetCloudCommitmentInfoForAnalysisResponse.Builder response =
                GetCloudCommitmentInfoForAnalysisResponse.newBuilder();
        timestampToBucket.values().forEach(response::addCommitmentBucket);
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Nonnull
    private static CommitmentInfoBucket.Builder createCommitmentInfoBucket(
            @Nullable final Long timestamp) {
        final Builder bucket = CommitmentInfoBucket.newBuilder();
        if (timestamp != null) {
            bucket.setTimestampMillis(timestamp);
        }
        return bucket;
    }
}
