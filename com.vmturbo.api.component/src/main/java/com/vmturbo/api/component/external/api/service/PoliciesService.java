package com.vmturbo.api.component.external.api.service;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.serviceinterfaces.IPoliciesService;
import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;

/**
 * Service implementation of Policies.
 **/
public class PoliciesService implements IPoliciesService {
    private static final Logger LOG = LogManager.getLogger();

    private final PolicyServiceBlockingStub policyService;

    private final PolicyMapper policyMapper;

    private final GroupServiceBlockingStub groupService;

    public PoliciesService(final PolicyServiceBlockingStub policyService,
                           final GroupServiceBlockingStub groupService,
                           final PolicyMapper policyMapper) {
        this.policyService = Objects.requireNonNull(policyService);
        this.groupService = Objects.requireNonNull(groupService);
        this.policyMapper = Objects.requireNonNull(policyMapper);
    }

    @Override
    public PolicyApiDTO getPolicies() throws Exception {
        throw ApiUtils.notImplementedInXL();
    }

    @Override
    public PolicyApiDTO getPolicyByUuid(String uuid) throws Exception {
        try {
            final PolicyDTO.PolicyRequest request = PolicyDTO.PolicyRequest.newBuilder()
                    .setPolicyId(Long.valueOf(uuid))
                    .build();

            final PolicyDTO.Policy policy = policyService.getPolicy(request).getPolicy();
            return toPolicyApiDTO(policy);
        } catch (RuntimeException e) {
            LOG.error("Cannot get policy with id " + uuid, e);
            // rethrow
            throw  e;
        }
    }

    /**
     * Convert a {@link PolicyDTO.Policy} to a {@link PolicyApiDTO}.
     *
     * @param policy a server representation of a policy.
     * @return the UI representation of the policy.
     */
    public PolicyApiDTO toPolicyApiDTO(PolicyDTO.Policy policy) {
        final Set<Long> groupingIDS = GroupProtoUtil.getPolicyGroupIds(policy);
        final Map<Long, Group> involvedGroups = new HashMap<>(groupingIDS.size());
        if (!groupingIDS.isEmpty()) {
            groupService.getGroups(GetGroupsRequest.newBuilder()
                    .addAllId(groupingIDS)
                    .build())
                    .forEachRemaining(group -> involvedGroups.put(group.getId(), group));
        }

        return policyMapper.policyToApiDto(policy, involvedGroups);
    }

    @Override
    public void validateInput(Object o, Errors errors) {
        // TODO Will perform validation later.
        return;
    }

    /**
     * Convert a {@link @link PolicyApiDTO} used by the API component to a
     * {@link com.vmturbo.common.protobuf.group.PolicyDTO.Policy} used by the group component.
     *
     * @param dto The policy API DTO to convert.
     * @return The converted policy.
     */
    public PolicyDTO.Policy toPolicy(PolicyApiDTO dto) {
        return policyMapper.policyApiDtoToProto(dto);
    }
}
