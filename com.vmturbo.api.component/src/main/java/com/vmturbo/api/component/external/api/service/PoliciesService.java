package com.vmturbo.api.component.external.api.service;

import com.vmturbo.api.component.external.api.mapper.PolicyMapper;
import com.vmturbo.api.component.external.api.util.ApiUtils;
import com.vmturbo.api.dto.PolicyApiDTO;
import com.vmturbo.api.serviceinterfaces.IPoliciesService;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyServiceGrpc.PolicyServiceBlockingStub;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.validation.Errors;

import java.util.Objects;

/**
 * Service implementation of Policies
 **/
public class PoliciesService implements IPoliciesService {
    private final static Logger LOG = LogManager.getLogger();

    private final PolicyServiceBlockingStub policyService;

    private final PolicyMapper policyMapper;

    public PoliciesService(final PolicyServiceBlockingStub policyServiceArg,
                           final PolicyMapper policyMapperArg) {
        policyService = Objects.requireNonNull(policyServiceArg);
        policyMapper = Objects.requireNonNull(policyMapperArg);
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

            final PolicyDTO.PolicyResponse policyResp = policyService.getPolicy(request);
            final PolicyDTO.Policy policyProto = policyResp.getPolicy();
            final PolicyApiDTO policyApiDTO = policyMapper.policyToApiDto(policyProto);
            return policyApiDTO;
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
