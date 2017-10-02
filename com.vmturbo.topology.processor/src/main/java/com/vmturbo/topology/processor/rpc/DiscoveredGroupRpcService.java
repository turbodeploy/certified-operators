package com.vmturbo.topology.processor.rpc;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.topology.DiscoveredGroup.GetDiscoveredGroupsRequest;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.GetDiscoveredGroupsResponse;
import com.vmturbo.common.protobuf.topology.DiscoveredGroup.TargetDiscoveredGroups;
import com.vmturbo.common.protobuf.topology.DiscoveredGroupServiceGrpc.DiscoveredGroupServiceImplBase;
import com.vmturbo.topology.processor.group.discovery.DiscoveredGroupUploader;

public class DiscoveredGroupRpcService extends DiscoveredGroupServiceImplBase {

    private final DiscoveredGroupUploader discoveredGroupUploader;

    DiscoveredGroupRpcService(@Nonnull final DiscoveredGroupUploader discoveredGroupUploader) {
        this.discoveredGroupUploader = discoveredGroupUploader;
    }

    /**
     * {@inheritDoc}}
     */
    public void getDiscoveredGroups(GetDiscoveredGroupsRequest request,
                            StreamObserver<GetDiscoveredGroupsResponse> responseObserver) {
        final GetDiscoveredGroupsResponse.Builder responseBuilder =
                GetDiscoveredGroupsResponse.newBuilder();
        discoveredGroupUploader.getDiscoveredGroupInfoByTarget().forEach((targetId, groupInfo) -> {
            if (!request.hasTargetId() || request.getTargetId() == targetId) {
                responseBuilder.putGroupsByTargetId(targetId,
                        TargetDiscoveredGroups.newBuilder()
                                .addAllGroup(groupInfo)
                                .build());
            }
        });
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
