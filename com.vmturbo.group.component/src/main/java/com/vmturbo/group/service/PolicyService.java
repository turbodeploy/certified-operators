package com.vmturbo.group.service;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import javaslang.control.Either;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.InputPolicy;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceImplBase;
import com.vmturbo.group.identity.IdentityProvider;
import com.vmturbo.group.persistent.GroupStore;
import com.vmturbo.group.persistent.PolicyStore;

public class PolicyService extends PolicyServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final PolicyStore policyStore;
    private final GroupStore groupStore;
    private final IdentityProvider identityProvider;

    public PolicyService(final PolicyStore policyStoreArg,
                         final GroupStore groupStoreArg,
                         final IdentityProvider identityProviderArg) {
        policyStore = Objects.requireNonNull(policyStoreArg);
        groupStore = Objects.requireNonNull(groupStoreArg);
        identityProvider = Objects.requireNonNull(identityProviderArg);
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

            final long inputConsumerGroup;
            final long inputProviderGroup;
            switch (inputPolicy.getPolicyDetailCase()) {
                case AT_MOST_N:
                    inputConsumerGroup = inputPolicy.getAtMostN().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getAtMostN().getProviderGroup();
                    policyBuilder.setAtMostN(PolicyDTO.Policy.AtMostNPolicy.newBuilder()
                            .setConsumerGroupId(inputConsumerGroup)
                            .setProviderGroupId(inputProviderGroup)
                            .setCapacity(inputPolicy.getAtMostN().getCapacity())
                            .build());
                    break;
                case AT_MOST_NBOUND:
                    inputConsumerGroup = inputPolicy.getAtMostNbound().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getAtMostNbound().getProviderGroup();
                    final float capacity = inputPolicy.getAtMostNbound().getCapacity();
                    policyBuilder.setAtMostNbound(PolicyDTO.Policy.AtMostNBoundPolicy.newBuilder()
                            .setConsumerGroupId(inputConsumerGroup)
                            .setProviderGroupId(inputProviderGroup)
                            .setCapacity(capacity)
                            .build());
                    break;
                case BIND_TO_COMPLEMENTARY_GROUP:
                    inputConsumerGroup = inputPolicy.getBindToComplementaryGroup().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getBindToComplementaryGroup().getProviderGroup();
                    policyBuilder.setBindToComplementaryGroup(PolicyDTO.Policy.BindToComplementaryGroupPolicy.newBuilder()
                            .setConsumerGroupId(inputConsumerGroup)
                            .setProviderGroupId(inputProviderGroup)
                            .build());
                    break;
                case BIND_TO_GROUP:
                    inputConsumerGroup = inputPolicy.getBindToGroup().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getBindToGroup().getProviderGroup();
                    policyBuilder.setBindToGroup(PolicyDTO.Policy.BindToGroupPolicy.newBuilder()
                            .setConsumerGroupId(inputConsumerGroup)
                            .setProviderGroupId(inputProviderGroup)
                            .build());
                    break;
                case BIND_TO_GROUP_AND_LICENSE:
                    inputConsumerGroup = inputPolicy.getBindToGroupAndLicense().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getBindToGroupAndLicense().getProviderGroup();
                    policyBuilder.setBindToGroupAndLicense(PolicyDTO.Policy.BindToGroupAndLicencePolicy.newBuilder()
                            .setConsumerGroupId(inputConsumerGroup)
                            .setProviderGroupId(inputProviderGroup)
                            .build());
                    break;
                case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                    inputConsumerGroup = inputPolicy.getBindToGroupAndGeoRedundancy().getConsumerGroup();
                    inputProviderGroup = inputPolicy.getBindToGroupAndGeoRedundancy().getProviderGroup();
                    policyBuilder.setBindToGroupAndGeoRedundancy(PolicyDTO.Policy.BindToGroupAndGeoRedundancyPolicy.newBuilder()
                            .setConsumerGroupId(inputConsumerGroup)
                            .setProviderGroupId(inputProviderGroup)
                            .build());
                    break;
                case MERGE:
                    final PolicyDTO.MergeType mergeType = inputPolicy.getMerge().getMergeType();
                    policyBuilder.setMerge(PolicyDTO.Policy.MergePolicy.newBuilder()
                            .setMergeType(mergeType)
                            .addAllMergeGroupIds(inputPolicy.getMerge().getMergeGroupsList())
                            .build());
                    break;
                case MUST_RUN_TOGETHER:
                    inputConsumerGroup = inputPolicy.getMustRunTogether().getGroup();
                    policyBuilder.setMustRunTogether(PolicyDTO.Policy.MustRunTogetherPolicy.newBuilder()
                            .setGroupId(inputConsumerGroup)
                            .setProviderEntityType(inputPolicy.getMustRunTogether().getProviderEntityType())
                            .build());
                    break;
                case MUST_NOT_RUN_TOGETHER:
                    inputConsumerGroup = inputPolicy.getMustNotRunTogether().getGroup();
                    policyBuilder.setMustNotRunTogether(PolicyDTO.Policy.MustNotRunTogetherPolicy.newBuilder()
                            .setGroupId(inputConsumerGroup)
                            .setProviderEntityType(inputPolicy.getMustNotRunTogether().getProviderEntityType())
                            .build());
                    break;
            }
            return Either.right(policyBuilder.build());
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

            logger.debug("Get all policies - will return {} policies", allInputPolicies.size());

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
            return;
        }

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
                    return;
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
                final Optional<Long> targetId = inputPolicyOpt.filter(InputPolicy::hasTargetId)
                        .map(InputPolicy::getTargetId);
                // For DRS policy, it set commodity type as "DrsSegmentationCommodity" in order to
                // let UI tell if it is a imported policy, and it needs to keep its original commodity
                // type when user try to disable/enable imported policies from API.
                final Optional<String> commodityType = inputPolicyOpt.filter(InputPolicy::hasCommodityType)
                        .map(InputPolicy::getCommodityType);
                final PolicyDTO.InputPolicy reqInputPolicy = request.getInputPolicy();
                final PolicyDTO.InputPolicy.Builder inputPolicyBuilderToStore = PolicyDTO.InputPolicy.newBuilder(reqInputPolicy)
                    .setId(id);
                targetId.ifPresent(inputPolicyBuilderToStore::setTargetId);
                commodityType.ifPresent(inputPolicyBuilderToStore::setCommodityType);
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
