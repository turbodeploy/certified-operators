package com.vmturbo.group.service;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.DiscoveredCollectionsServiceGrpc.DiscoveredCollectionsServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredCollectionsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredCollectionsResponse;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.group.persistent.ClusterStore;
import com.vmturbo.group.persistent.DatabaseException;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;

public class DiscoveredCollectionsRpcService extends DiscoveredCollectionsServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final GroupStore groupStore;

    private final PolicyStore policyStore;

    private final ClusterStore clusterStore;

    private final HealthStatusProvider healthMonitor;

    public DiscoveredCollectionsRpcService(@Nonnull final GroupStore groupStore,
                    @Nonnull final PolicyStore policyStore,
                    @Nonnull final ClusterStore clusterStore,
                    @Nonnull final HealthStatusProvider healthMonitor) {
        this.groupStore = Objects.requireNonNull(groupStore);
        this.policyStore = Objects.requireNonNull(policyStore);
        this.clusterStore = Objects.requireNonNull(clusterStore);
        this.healthMonitor = healthMonitor;
    }

    public void storeDiscoveredCollections(StoreDiscoveredCollectionsRequest request,
                           StreamObserver<StoreDiscoveredCollectionsResponse> responseObserver) {
        if (!request.hasTargetId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Request must have a target ID.").asException());
            return;
        }

        if (!healthMonitor.getHealthStatus().isHealthy()) {
            responseObserver.onError(Status.UNAVAILABLE
                .withDescription("Group component not healthy.").asException());
            return;
        }

        try {
            Map<String, Long> groupsMap =
                            groupStore.updateTargetGroups(request.getTargetId(),
                                request.getDiscoveredGroupList());
            Map<String, Long> clusterMap =
                            clusterStore.updateTargetClusters(request.getTargetId(),
                                request.getDiscoveredClusterList());
            groupsMap.putAll(clusterMap); // mapping of all group/cluster name to oid
            policyStore.updateTargetPolicies(request.getTargetId(),
                            request.getDiscoveredPolicyInfosList(), groupsMap);
            responseObserver.onNext(StoreDiscoveredCollectionsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (DatabaseException e) {
            logger.error("Failed to store discovered collections due to a database query error.", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }
}
