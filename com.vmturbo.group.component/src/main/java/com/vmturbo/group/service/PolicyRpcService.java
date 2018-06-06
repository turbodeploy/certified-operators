package com.vmturbo.group.service;

import java.util.Objects;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyCreateResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyDeleteResponse;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyEditResponse;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceImplBase;
import com.vmturbo.group.common.DuplicateNameException;
import com.vmturbo.group.common.ImmutableUpdateException.ImmutablePolicyUpdateException;
import com.vmturbo.group.common.ItemNotFoundException.PolicyNotFoundException;
import com.vmturbo.group.policy.PolicyStore;

public class PolicyRpcService extends PolicyServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final PolicyStore policyStore;

    public PolicyRpcService(final PolicyStore policyStoreArg) {
        policyStore = Objects.requireNonNull(policyStoreArg);
    }

    @Override
    public void getAllPolicies(final PolicyDTO.PolicyRequest request,
                               final StreamObserver<PolicyDTO.PolicyResponse> responseObserver) {
        policyStore.getAll().stream()
                .map(policy -> PolicyDTO.PolicyResponse.newBuilder()
                        .setPolicy(policy)
                        .build())
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
        policyStore.get(policyID).ifPresent(response::setPolicy);
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
}
