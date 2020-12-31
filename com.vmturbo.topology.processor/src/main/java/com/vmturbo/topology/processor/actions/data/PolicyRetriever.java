package com.vmturbo.topology.processor.actions.data;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.SinglePolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;

/**
 * The class for retrieving policy.
 */
public class PolicyRetriever {

    /**
     * The policy service from which to retrieve policy definitions.
     */
    private final PolicyServiceBlockingStub policyService;

    /**
     * Creates an instance of policy retriever.
     *
     * @param policyService the policy service.
     */
    public PolicyRetriever(PolicyServiceBlockingStub policyService) {
        this.policyService = policyService;
    }

    /**
     * Gets the policy by its oid.
     *
     * @param oid the oid for the policy.
     * @return the policy object.
     */
    public Policy retrievePolicy(long oid) {
        PolicyDTO.PolicyResponse policyResponse = policyService
            .getPolicy(SinglePolicyRequest.newBuilder()
            .setPolicyId(oid)
            .build());

        if (policyResponse.hasPolicy()) {
            return policyResponse.getPolicy();
        }
        return null;
    }
}
