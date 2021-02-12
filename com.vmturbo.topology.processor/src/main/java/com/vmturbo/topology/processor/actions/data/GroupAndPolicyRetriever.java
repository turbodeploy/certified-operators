package com.vmturbo.topology.processor.actions.data;

import java.util.Optional;

import com.vmturbo.common.protobuf.group.GroupDTO;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.SinglePolicyRequest;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * The class for retrieving policy.
 */
public class GroupAndPolicyRetriever {

    /**
     * The policy service from which to retrieve policy definitions.
     */
    private final PolicyServiceBlockingStub policyService;

    /**
     * The group service from which to retrieve group definitions.
     */
    private final GroupServiceBlockingStub groupService;

    /**
     * Creates an instance of policy retriever.
     *
     * @param groupService the group service.
     * @param policyService the policy service.
     */
    public GroupAndPolicyRetriever(GroupServiceBlockingStub groupService,
                                   PolicyServiceBlockingStub policyService) {
        this.policyService = policyService;
        this.groupService = groupService;
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

    /**
     * Gets the cluster for the input entity.
     *
     * @param entityId the id of entity we want tog et the group for.
     * @return the cluster of input entity if present.
     */
    public Optional<GroupDTO.Grouping> getHostCluster(long entityId) {
        return groupService.getGroupsForEntities(GroupDTO.GetGroupsForEntitiesRequest.newBuilder()
            .addEntityId(entityId)
            .addGroupType(CommonDTO.GroupDTO.GroupType.COMPUTE_HOST_CLUSTER)
            .setLoadGroupObjects(true)
            .build()).getGroupsList().stream().findAny();
    }
}
