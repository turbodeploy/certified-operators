package com.vmturbo.group.service;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.DiscoveredGroupServiceGrpc.DiscoveredGroupServiceImplBase;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.StoreDiscoveredGroupsResponse;
import com.vmturbo.group.group.GroupStore;
import com.vmturbo.group.policy.PolicyStore;
import com.vmturbo.group.setting.SettingStore;

public class DiscoveredGroupsRpcService extends DiscoveredGroupServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final DSLContext dslContext;

    private final GroupStore groupStore;

    private final PolicyStore policyStore;

    private final SettingStore settingStore;

    public DiscoveredGroupsRpcService(@Nonnull final DSLContext dslContext,
                                      @Nonnull final GroupStore groupStore,
                                      @Nonnull final PolicyStore policyStore,
                                      @Nonnull final SettingStore settingStore) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.groupStore = Objects.requireNonNull(groupStore);
        this.policyStore = Objects.requireNonNull(policyStore);
        this.settingStore = Objects.requireNonNull(settingStore);
    }

    @Override
    public void storeDiscoveredGroups(StoreDiscoveredGroupsRequest request,
                           StreamObserver<StoreDiscoveredGroupsResponse> responseObserver) {
        if (!request.hasTargetId()) {
            responseObserver.onError(Status.INVALID_ARGUMENT
                .withDescription("Request must have a target ID.").asException());
            return;
        }

        try {
            dslContext.transaction(configuration -> {
                final DSLContext transactionContext = DSL.using(configuration);
                final Map<String, Long> groupsMap = groupStore.updateTargetGroups(transactionContext,
                        request.getTargetId(),
                        request.getDiscoveredGroupList(),
                        request.getDiscoveredClusterList());

                policyStore.updateTargetPolicies(transactionContext, request.getTargetId(),
                        request.getDiscoveredPolicyInfosList(), groupsMap);
                settingStore.updateTargetSettingPolicies(transactionContext, request.getTargetId(),
                        request.getDiscoveredSettingPoliciesList(), groupsMap);
            });

            responseObserver.onNext(StoreDiscoveredGroupsResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (DataAccessException e) {
            logger.error("Failed to store discovered collections due to a database query error.", e);
            responseObserver.onError(Status.INTERNAL
                .withDescription(e.getLocalizedMessage()).asException());
        }
    }
}
