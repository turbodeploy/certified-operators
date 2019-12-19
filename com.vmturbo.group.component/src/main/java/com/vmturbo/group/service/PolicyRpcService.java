package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyCreateResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyDeleteResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyEditResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.MergePolicy.MergeType;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceImplBase;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.group.IGroupStore;
import com.vmturbo.group.policy.PolicyStore;

public class PolicyRpcService extends PolicyServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final PolicyStore policyStore;

    private final GroupRpcService groupService;

    private final IGroupStore groupStore;

    private final UserSessionContext userSessionContext;

    public PolicyRpcService(final PolicyStore policyStoreArg,
                            final GroupRpcService groupService,
                            final IGroupStore groupStore,
                            UserSessionContext userSessionContext) {
        policyStore = Objects.requireNonNull(policyStoreArg);
        this.groupService = groupService;
        this.groupStore = groupStore;
        this.userSessionContext = userSessionContext;
    }

    @Override
    public void getAllPolicies(final PolicyDTO.PolicyRequest request,
                               final StreamObserver<PolicyDTO.PolicyResponse> responseObserver) {
        final Predicate<Policy> userScopeFilter;
        final Collection<Policy> allPolicies = policyStore.getAll();
        if (userSessionContext.isUserScoped()) {
            // create a predicate that will check user access to a group id using a cache of results.
            // (the cache is to help speed up the case where the same groups are used in several policies)
            final Set<Long> accessedGroups = new HashSet<>();
            final Set<Long> groups = allPolicies.stream()
                    .map(GroupProtoUtil::getPolicyGroupIds)
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            try {
                for (Long groupId : groups) {
                    if (groupService.userHasAccessToGrouping(groupStore, groupId)) {
                        accessedGroups.add(groupId);
                    }
                }
            } catch (StoreOperationException e) {
                responseObserver.onError(e.getStatus().asException());
                return;
            }
            userScopeFilter =
                    policy -> accessedGroups.containsAll(GroupProtoUtil.getPolicyGroupIds(policy));
        } else {
            userScopeFilter = policy -> true;
        }

        allPolicies.stream()
                .filter(userScopeFilter)
                .map(policy -> PolicyDTO.PolicyResponse.newBuilder().setPolicy(policy).build())
                .forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void getPolicy(final PolicyDTO.PolicyRequest request,
                          final StreamObserver<PolicyDTO.PolicyResponse> responseObserver) {
        if (!request.hasPolicyId()) {
            final String errMsg = "Incoming policy get request does not contain any policy ID";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }

        final long policyID = request.getPolicyId();

        logger.info("Getting policy with ID {}", policyID);

        final PolicyDTO.PolicyResponse.Builder response = PolicyDTO.PolicyResponse.newBuilder();
        final Optional<Policy> policy = policyStore.get(policyID);
        if (policy.isPresent()) {
            try {
                checkPolicyAccess(policy.get());
            } catch (StoreOperationException e) {
                responseObserver.onError(e.getStatus().asException());
                return;
            }
            response.setPolicy(policy.get());
        }
        responseObserver.onNext(response.build());
        responseObserver.onCompleted();
    }

    @Override
    public void createPolicy(final PolicyDTO.PolicyCreateRequest request,
                             final StreamObserver<PolicyDTO.PolicyCreateResponse> responseObserver) {
        if (!request.hasPolicyInfo()) {
            final String errMsg = "Incoming policy create request does not contain an input policy info";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }

        try {
            // check access on the policy contents before proceeding
            checkPolicyAccess(Policy.newBuilder().setPolicyInfo(request.getPolicyInfo()).build());
        } catch (StoreOperationException e) {
            responseObserver.onError(e.getStatus().asException());
            return;
        }

        // make sure we're not merging clusters that are already in a merge policy
        try {
            checkForInvalidClusterMergePolicy(request.getPolicyInfo(), Optional.empty());
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        }

        final PolicyDTO.Policy policy;
        try {
            policy = policyStore.newUserPolicy(request.getPolicyInfo());
            responseObserver.onNext(PolicyCreateResponse.newBuilder()
                    .setPolicy(policy)
                    .build());
            responseObserver.onCompleted();
        } catch (DuplicateNameException e) {
            logger.error("Failed to create a policy due to duplicate name exception!", e);
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription(e.getMessage())
                    .asException());
        }
    }

    @Override
    public void editPolicy(final PolicyDTO.PolicyEditRequest request,
                           final StreamObserver<PolicyDTO.PolicyEditResponse> responseObserver) {
        if (!request.hasNewPolicyInfo()) {
            final String errMsg = "Incoming policy edit request does not contain a policy info";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        } else if (!request.hasPolicyId()) {
            final String errMsg = "Incoming policy edit request does not contain a policy ID";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
            return;
        }
        try {
            if (userSessionContext.isUserScoped()) {
                try {
                    // verify that the updated policy would fit the scoping rules
                    checkPolicyAccess(Policy.newBuilder().setPolicyInfo(request.getNewPolicyInfo()).build());

                    // verify the existing policy is accessible too
                    final Optional<Policy> policy = policyStore.get(request.getPolicyId());
                    if (policy.isPresent()) {
                        checkPolicyAccess(policy.get());
                    }
                } catch (StoreOperationException e) {
                    responseObserver.onError(e.getStatus().asException());
                    return;
                }
            }

            // make sure we're not merging any cluster that's already merged elsewhere
            checkForInvalidClusterMergePolicy(request.getNewPolicyInfo(),
                    Optional.of(request.getPolicyId()));
        } catch (IllegalArgumentException e) {
            responseObserver.onError(
                Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            return;
        }

        final long id = request.getPolicyId();
        try {
            final PolicyDTO.Policy updatedPolicy =
                    policyStore.editPolicy(id, request.getNewPolicyInfo());
            responseObserver.onNext(PolicyEditResponse.newBuilder()
                    .setPolicy(updatedPolicy)
                    .build());
            responseObserver.onCompleted();
        } catch (DuplicateNameException | PolicyNotFoundException e) {
            logger.error("Failed to update policy " + id + "!", e);
            responseObserver.onError(
                    Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void deletePolicy(final PolicyDTO.PolicyDeleteRequest request,
                             final StreamObserver<PolicyDTO.PolicyDeleteResponse> responseObserver) {
        if (!request.hasPolicyId()) {
            final String errMsg = "Delete policy: policy ID is missing";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asException());
        } else {
            final long id = request.getPolicyId();
            // verify the user has access to the policy
            try {
                if (userSessionContext.isUserScoped()) {
                    final Optional<Policy> policy = policyStore.get(request.getPolicyId());
                    if (policy.isPresent()) {
                        checkPolicyAccess(policy.get());
                    }
                }
            } catch (StoreOperationException e) {
                responseObserver.onError(e.getStatus().asException());
                return;
            }
            try {
                final PolicyDTO.Policy policy = policyStore.deleteUserPolicy(id);
                responseObserver.onNext(PolicyDeleteResponse.newBuilder()
                        .setPolicy(policy)
                        .build());
                responseObserver.onCompleted();
            } catch (PolicyNotFoundException | ImmutablePolicyUpdateException e) {
                logger.error("Failed to update policy " + id + "!", e);
                responseObserver.onError(
                        Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asException());
            }
        }
    }

    void checkForInvalidClusterMergePolicy(@Nonnull final PolicyInfo policyInfo,
                                           @Nonnull Optional<Long> ownPolicyId) {
        if (policyInfo.getPolicyDetailCase() != PolicyDetailCase.MERGE
            || policyInfo.getMerge().getMergeType() != MergeType.CLUSTER) {
            // not a cluster merge policy, nothing to check
            return;
        }

        // ids of clusters being merged
        Set<Long> clustersToMerge = new HashSet<>(policyInfo.getMerge().getMergeGroupIdsList());

        // discovered conflicts: (clusterId, policyName)
        Set<Pair<Long, String>> conflicts = new HashSet<>();

        policyStore.getAll()
            .forEach(policy -> {
                if ( // ignore own policy if we're editing it
                    ownPolicyId.map(id -> id != policy.getId()).orElse(true)
                        // and only consider merge policies
                        && policy.hasPolicyInfo() && policy.getPolicyInfo().hasMerge()) {
                    final MergePolicy merge = policy.getPolicyInfo().getMerge();
                    // only cluster merge policies
                    if (merge.hasMergeType() && merge.getMergeType() == MergeType.CLUSTER) {
                        merge.getMergeGroupIdsList().stream()
                            .filter(clustersToMerge::contains)
                            // found a conflict - record it
                            .map(clusterUuid ->
                                Pair.of(clusterUuid, policy.getPolicyInfo().getName()))
                            .forEach(conflicts::add);
                    }
                }
            });
        if (!conflicts.isEmpty()) {
            // we have conflicts - create a string describing each in terms of cluster name
            // and conflicting policy name
            final String details = conflicts.stream()
                .map(p -> String.format("cluster %s is in policy %s",
                    getClusterName(p.getLeft(), "[unknown]"),
                    p.getRight()))
                .collect(Collectors.joining("; "));
            final String msg = String.format(
                "A cluster may not participate in multiple merge policies: %s", details);
            // and abort the operation
            throw new IllegalArgumentException(msg);
        }
    }

    private String getClusterName(long clusterId, String defaultName) {
        final Collection<GroupDTO.Grouping> clusterGroups =
                groupStore.getGroupsById(Collections.singleton(clusterId));
        if (clusterGroups.isEmpty()) {
            return defaultName;
        }
        final GroupDTO.Grouping clusterGroup = clusterGroups.iterator().next();
        return clusterGroup.hasDefinition() && clusterGroup.getDefinition().hasDisplayName() ?
                clusterGroup.getDefinition().getDisplayName() : defaultName;
    }

    private void checkPolicyAccess(Policy policy) throws StoreOperationException {
        if (!userSessionContext.isUserScoped()) {
            return;
        }
        final Set<Long> groupsInPolicy = GroupProtoUtil.getPolicyGroupIds(policy);
        for (Long groupId: groupsInPolicy) {
            if (!groupService.userHasAccessToGrouping(groupStore, groupId)) {
                throw new UserAccessException("User does not have access to policy");
            }
        }
    }
}
