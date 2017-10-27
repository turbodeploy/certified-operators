package com.vmturbo.api.component.external.api.service;

import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.common.protobuf.GroupDTOUtil;
import com.vmturbo.common.protobuf.group.GroupFetcher;
import com.vmturbo.api.dto.policy.PolicyApiDTO;
import com.vmturbo.api.serviceinterfaces.IPoliciesService;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import com.google.common.collect.Sets;

/**
 * Service implementation of Policies
 **/
public class PoliciesService implements IPoliciesService {
    private final static Logger LOG = LogManager.getLogger();

    private final PolicyServiceBlockingStub policyService;

    private final PolicyMapper policyMapper;

    private final GroupFetcher groupFetcher;

    public PoliciesService(final PolicyServiceBlockingStub policyServiceArg,
                           final GroupFetcher groupFetcherArg,
                           final PolicyMapper policyMapperArg) {
        policyService = Objects.requireNonNull(policyServiceArg);
        policyMapper = Objects.requireNonNull(policyMapperArg);
        groupFetcher = Objects.requireNonNull(groupFetcherArg);
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

            final List<PolicyGroupingID> groupingIDS = GroupDTOUtil.retrieveIdsFromPolicy(policy);
            final Map<PolicyGroupingID, PolicyGrouping> groupings = groupFetcher
                    .getGroupings(Sets.newHashSet(groupingIDS));

            return policyMapper.policyToApiDto(policy, groupings);
        } catch (RuntimeException e) {
            LOG.error("Cannot get policy with id " + uuid, e);
            // rethrow
            throw  e;
        }
    }

    @Override
    public void validateInput(Object o, Errors errors) {
        // TODO Will perform validation later.
        return;
    }
}
