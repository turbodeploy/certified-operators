package com.vmturbo.group.service;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.DiscoveredGroupServiceGrpc.DiscoveredGroupServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsResponse;
import com.vmturbo.components.common.health.HealthStatusProvider;
import com.vmturbo.group.persistent.DatabaseException;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;

public class DiscoveredGroupsRpcService extends DiscoveredGroupServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final GroupStore groupStore;

    private final PolicyStore policyStore;

    private final HealthStatusProvider healthMonitor;

    public DiscoveredGroupsRpcService(@Nonnull final GroupStore groupStore,
                                      @Nonnull final PolicyStore policyStore,
                                      @Nonnull final HealthStatusProvider healthMonitor) {
        this.groupStore = Objects.requireNonNull(groupStore);
        this.policyStore = Objects.requireNonNull(policyStore);
        this.healthMonitor = healthMonitor;
    }

    @Override
    public void storeDiscoveredGroups(StoreDiscoveredGroupsRequest request,
                           StreamObserver<StoreDiscoveredGroupsResponse> responseObserver) {
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
            final Map<String, Long> groupsMap = groupStore.updateTargetGroups(request.getTargetId(),
                    request.getDiscoveredGroupList(),
                    request.getDiscoveredClusterList());

            policyStore.updateTargetPolicies(request.getTargetId(),
                            request.getDiscoveredPolicyInfosList(), groupsMap);
            responseObserver.onNext(StoreDiscoveredGroupsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (DatabaseException e) {
            logger.error("Failed to store discovered collections due to a database query error.", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }
}
