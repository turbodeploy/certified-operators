package com.vmturbo.group.service;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceImplBase;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.ClusterStore;
import com.vmturbo.group.persistent.DatabaseException;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;

public class PolicyService extends PolicyServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final PolicyStore policyStore;
    private final GroupStore groupStore;
    private final ClusterStore clusterStore;
    private final IdentityProvider identityProvider;

    public PolicyService(final PolicyStore policyStoreArg,
                         final GroupStore groupStoreArg,
                         final ClusterStore clusterStoreArg,
                         final IdentityProvider identityProviderArg) {
        policyStore = Objects.requireNonNull(policyStoreArg);
        groupStore = Objects.requireNonNull(groupStoreArg);
        clusterStore = Objects.requireNonNull(clusterStoreArg);
        identityProvider = Objects.requireNonNull(identityProviderArg);
    }

    /**
     * Convert an {@link com.vmturbo.common.protobuf.group.PolicyDTO.InputGroup} to
     * a {@link PolicyDTO.PolicyGroupingID}.
     * ;w
     * @param inputGroup The InputGroup to convert.
     * @return {@link PolicyGroupingID}.
     * @throws DatabaseException if the query for the group failed.
     */
    private PolicyDTO.PolicyGroupingID convertToGroupID(final PolicyDTO.InputGroup inputGroup)
            throws DatabaseException {
        if (inputGroup.hasGroupId()) {
            final long groupId = inputGroup.getGroupId();
            final PolicyDTO.PolicyGroupingID.Builder builder = PolicyGroupingID.newBuilder();
            final Optional<GroupDTO.Group> groupOptional = groupStore.get(groupId);
            if (groupOptional.isPresent()) {
                builder.setGroupId(groupId);
            } else {
                final Optional<GroupDTO.Cluster> clusterOptional = clusterStore.get(groupId);
                if (clusterOptional.isPresent()) {
                    builder.setClusterId(groupId);
                } else {
                    throw new IllegalArgumentException(
                        "Cannot find a group or cluster with id " + groupId);
                }
            }
            return builder.build();
        } else {
            throw new IllegalArgumentException("InputGroup does not contain an ID");
        }
    }

    /**
     * Convert a {@link PolicyDTO.InputPolicy} into a {@link PolicyDTO.Policy}.
     *
     * @param inputPolicy The <code>InputPolicy</code> we want to convert.
     * @return Either a {@link Throwable} or a {@link com.vmturbo.common.protobuf.group.PolicyDTO.Policy}.
     */
    private Either<Throwable, PolicyDTO.Policy> convertToPolicy(final PolicyDTO.InputPolicy inputPolicy) {
        final PolicyDTO.Policy.Builder policyBuilder = PolicyDTO.Policy.newBuilder();

        try {
            policyBuilder.setId(inputPolicy.getId());
            policyBuilder.setName(inputPolicy.getName());
            policyBuilder.setEnabled(inputPolicy.getEnabled());
            policyBuilder.setCommodityType(inputPolicy.getCommodityType());

            PolicyDTO.InputGroup inputConsumerGroup;
            PolicyDTO.InputGroup inputProviderGroup;
            switch (inputPolicy.getPolicyDetailCase()) {
                case AT_MOST_N:
                    inputConsumerGroup = inputPolicy.getAtMostN().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getAtMostN().getProviderGroup();
                    policyBuilder.setAtMostN(PolicyDTO.Policy.AtMostNPolicy.newBuilder()
                            .setConsumerGroupId(convertToGroupID(inputConsumerGroup))
                            .setProviderGroupId(convertToGroupID(inputProviderGroup))
                            .setCapacity(inputPolicy.getAtMostN().getCapacity())
                            .build());
                    break;
                case AT_MOST_NBOUND:
                    inputConsumerGroup = inputPolicy.getAtMostNbound().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getAtMostNbound().getProviderGroup();
                    final float capacity = inputPolicy.getAtMostNbound().getCapacity();
                    policyBuilder.setAtMostNbound(PolicyDTO.Policy.AtMostNBoundPolicy.newBuilder()
                            .setConsumerGroupId(convertToGroupID(inputConsumerGroup))
                            .setProviderGroupId(convertToGroupID(inputProviderGroup))
                            .setCapacity(capacity)
                            .build());
                    break;
                case BIND_TO_COMPLEMENTARY_GROUP:
                    inputConsumerGroup = inputPolicy.getBindToComplementaryGroup().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getBindToComplementaryGroup().getProviderGroup();
                    policyBuilder.setBindToComplementaryGroup(PolicyDTO.Policy.BindToComplementaryGroupPolicy.newBuilder()
                            .setConsumerGroupId(convertToGroupID(inputConsumerGroup))
                            .setProviderGroupId(convertToGroupID(inputProviderGroup))
                            .build());
                    break;
                case BIND_TO_GROUP:
                    inputConsumerGroup = inputPolicy.getBindToGroup().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getBindToGroup().getProviderGroup();
                    policyBuilder.setBindToGroup(PolicyDTO.Policy.BindToGroupPolicy.newBuilder()
                            .setConsumerGroupId(convertToGroupID(inputConsumerGroup))
                            .setProviderGroupId(convertToGroupID(inputProviderGroup))
                            .build());
                    break;
                case BIND_TO_GROUP_AND_LICENSE:
                    inputConsumerGroup = inputPolicy.getBindToGroupAndLicense().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getBindToGroupAndLicense().getProviderGroup();
                    policyBuilder.setBindToGroupAndLicense(PolicyDTO.Policy.BindToGroupAndLicencePolicy.newBuilder()
                            .setConsumerGroupId(convertToGroupID(inputConsumerGroup))
                            .setProviderGroupId(convertToGroupID(inputProviderGroup))
                            .build());
                    break;
                case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                    inputConsumerGroup = inputPolicy.getBindToGroupAndGeoRedundancy().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getBindToGroupAndGeoRedundancy().getProviderGroup();
                    policyBuilder.setBindToGroupAndGeoRedundancy(PolicyDTO.Policy.BindToGroupAndGeoRedundancyPolicy.newBuilder()
                            .setConsumerGroupId(convertToGroupID(inputConsumerGroup))
                            .setProviderGroupId(convertToGroupID(inputProviderGroup))
                            .build());
                    break;
                case MERGE:
                    final PolicyDTO.MergeType mergeType = inputPolicy.getMerge().getMergeType();
                    final List<PolicyDTO.InputGroup> inputMergeGroups = inputPolicy.getMerge().getMergeGroupsList();
                    policyBuilder.setMerge(PolicyDTO.Policy.MergePolicy.newBuilder()
                            .setMergeType(mergeType)
                            .addAllMergeGroupIds(inputMergeGroups.stream().map(ig -> {
                                try {
                                    PolicyGroupingID group = convertToGroupID(ig);
                                    return group;
                                } catch (DatabaseException dbe) {
                                    // Rethrow as a runtime exception to cross lambda boundaries.
                                    throw new RuntimeException(dbe);
                                }
                            }).collect(Collectors.toList()))
                            .build());
                    break;
                case MUST_RUN_TOGETHER:
                    inputConsumerGroup = inputPolicy.getMustRunTogether().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getMustRunTogether().getProviderGroup();
                    policyBuilder.setMustRunTogether(PolicyDTO.Policy.MustRunTogetherPolicy.newBuilder()
                            .setConsumerGroupId(convertToGroupID(inputConsumerGroup))
                            .setProviderGroupId(convertToGroupID(inputProviderGroup))
                            .build());
                    break;
            }
            return Either.right(policyBuilder.build());
        } catch (DatabaseException e) {
            logger.error("Failed to convert input policy to a policy due to a query failure.", e);
            return Either.left(e);
        } catch (RuntimeException e) {
            logger.error("Failed to convert input policy to a policy.", e);
            return Either.left(e);
        }
    }

    @Override
    public void getAllPolicies(final PolicyDTO.PolicyRequest request,
                               final StreamObserver<PolicyDTO.PolicyResponse> responseObserver) {
        try {
            final Collection<PolicyDTO.InputPolicy> allInputPolicies = policyStore.getAll();

            logger.info("Get all policies - will return {} policies", allInputPolicies.size());

            allInputPolicies.forEach(inputPolicy -> {
                final Either<Throwable, PolicyDTO.Policy> convertedPolicy = convertToPolicy(inputPolicy);

                convertedPolicy.forEach(policy -> {
                    final PolicyDTO.PolicyResponse response = PolicyDTO.PolicyResponse.newBuilder()
                            .setPolicy(policy)
                            .build();
                    responseObserver.onNext(response);
                });

                if (convertedPolicy.isLeft() || convertedPolicy.isEmpty()) {
                    logger.error("Exception encountered while converting policy with ID " + inputPolicy.getId(),
                              convertedPolicy.left());
                }
            });
            responseObserver.onCompleted();
        } catch (RuntimeException e) {
            logger.error("Error while getting all policies", e);
            final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
            responseObserver.onError(status.asRuntimeException());
        }
    }

    @Override
    public void getPolicy(final PolicyDTO.PolicyRequest request,
                          final StreamObserver<PolicyDTO.PolicyResponse> responseObserver) {
        if (!request.hasPolicyId()) {
            final String errMsg = "Incoming policy get request does not contain any policy ID";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
        } else {
            try {
                final long policyID = request.getPolicyId();

                logger.info("Getting policy with ID {}", policyID);
                final Optional<PolicyDTO.InputPolicy> inputPolicyOpt = policyStore.get(policyID);

                if (inputPolicyOpt.isPresent()) {
                    final PolicyDTO.InputPolicy inputPolicy = inputPolicyOpt.get();
                    final Either<Throwable, PolicyDTO.Policy> convertedPolicy = convertToPolicy(inputPolicy);

                    if (convertedPolicy.isLeft()) {
                        final Status status = Status.ABORTED.withCause(convertedPolicy.getLeft())
                                                            .withDescription(convertedPolicy.getLeft().getMessage());
                        responseObserver.onError(status.asRuntimeException());
                    }

                    convertedPolicy.forEach(policy -> {
                        final PolicyDTO.PolicyResponse response = PolicyDTO.PolicyResponse.newBuilder()
                                .setPolicy(policy)
                                .build();
                        responseObserver.onNext(response);
                        responseObserver.onCompleted();
                    });
                } else {
                    final String errMsg = "Cannot find a policy with id " + policyID;
                    logger.error(errMsg);
                    responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
                }
            } catch (RuntimeException e) {
                logger.error("Exception encountered while getting a policy", e);
                final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
                responseObserver.onError(status.asRuntimeException());
            }
        }
    }

    @Override
    public void createPolicy(final PolicyDTO.PolicyCreateRequest request,
                             final StreamObserver<PolicyDTO.PolicyCreateResponse> responseObserver) {
        if (!request.hasInputPolicy()) {
            final String errMsg = "Incoming policy create request does not contain an input policy";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
        } else {
            try {
                final long id = identityProvider.next();
                final PolicyDTO.InputPolicy reqInputPolicy = request.getInputPolicy();
                final PolicyDTO.InputPolicy inputPolicyToStore = PolicyDTO.InputPolicy.newBuilder(reqInputPolicy)
                        .setId(id).build();
                final boolean success = policyStore.save(id, inputPolicyToStore);

                if (success) {
                    logger.info("Created a policy with id {} and name {}", id, inputPolicyToStore.getName());
                    responseObserver.onNext(PolicyDTO.PolicyCreateResponse.newBuilder()
                            .setPolicyId(id).build());
                    responseObserver.onCompleted();
                } else {
                    final String errMsg = "Input policy was not stored successfully";
                    logger.error(errMsg);
                    responseObserver.onError(Status.ABORTED.withDescription(errMsg).asRuntimeException());
                }
            } catch (RuntimeException e) {
                logger.error("Exception encountered while creating policy", e);
                final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
                responseObserver.onError(status.asRuntimeException());
            }
        }
    }

    @Override
    public void editPolicy(final PolicyDTO.PolicyEditRequest request,
                           final StreamObserver<PolicyDTO.PolicyEditResponse> responseObserver) {
        if (!request.hasInputPolicy()) {
            final String errMsg = "Incoming policy edit request does not contain an input policy";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
        } else if (!request.hasPolicyId()) {
            final String errMsg = "Incoming policy edit request does not contain a policy ID";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
        } else {
            try {
                final long id = request.getPolicyId();
                final Optional<PolicyDTO.InputPolicy> inputPolicyOpt = policyStore.get(id);
                // If policy has target Id, it should be added to input policy.
                final Optional<Long> targetId = inputPolicyOpt.map(InputPolicy::getTargetId);
                final PolicyDTO.InputPolicy reqInputPolicy = request.getInputPolicy();
                final PolicyDTO.InputPolicy.Builder inputPolicyBuilderToStore = PolicyDTO.InputPolicy.newBuilder(reqInputPolicy)
                    .setId(id);
                targetId.ifPresent(inputPolicyBuilderToStore::setTargetId);
                final PolicyDTO.InputPolicy inputPolicyToStore = inputPolicyBuilderToStore.build();
                final boolean success = policyStore.save(id, inputPolicyToStore);

                if (success) {
                    logger.info("Policy {} was edited successfully", id);
                    responseObserver.onNext(PolicyDTO.PolicyEditResponse.newBuilder().build());
                    responseObserver.onCompleted();
                } else {
                    final String errMsg = "Policy edit was not successful";
                    logger.error(errMsg);
                    responseObserver.onError(Status.ABORTED.withDescription(errMsg).asRuntimeException());
                }
            } catch (RuntimeException e) {
                logger.error("Exception encountered while editing policy", e);
                final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
                responseObserver.onError(status.asRuntimeException());
            }
        }
    }

    @Override
    public void deletePolicy(final PolicyDTO.PolicyDeleteRequest request,
                             final StreamObserver<PolicyDTO.PolicyDeleteResponse> responseObserver) {
        if (!request.hasPolicyId()) {
            final String errMsg = "Delete policy: policy ID is missing";
            logger.error(errMsg);
            responseObserver.onError(Status.INVALID_ARGUMENT.withDescription(errMsg).asRuntimeException());
        } else {
            try {
                final long policyToDelete = request.getPolicyId();
                final boolean success = policyStore.delete(policyToDelete);

                if (success) {
                    logger.info("Policy {} was deleted", policyToDelete);
                    responseObserver.onNext(PolicyDTO.PolicyDeleteResponse.getDefaultInstance());
                    responseObserver.onCompleted();
                } else {
                    final String errMsg = "Policy was not deleted";
                    logger.error(errMsg);
                    responseObserver.onError(Status.ABORTED.withDescription(errMsg).asRuntimeException());
                }
            } catch (RuntimeException e) {
                logger.error("Exception encountered while deleting a policy", e);
                final Status status = Status.ABORTED.withCause(e).withDescription(e.getMessage());
                responseObserver.onError(status.asRuntimeException());
            }
        }
    }
}
