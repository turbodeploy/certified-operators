package com.vmturbo.topology.processor.group.policy.application;

import java.util.Objects;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;

/**
 * A policy that limits the entities in the consumer group to run only on the entities in the provider group.
 * Common use case: VM->Host or VM->Storage affinity.
 */
public class BindToGroupPolicy extends PlacementPolicy {

    private final PolicyDTO.PolicyInfo policyInfo;

    private final PolicyEntities consumerPolicyEntities;
    private final PolicyEntities providerPolicyEntities;

    /**
     * Create a new BindToGroupPolicy.
     * The policy should be of type BindToGroup.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     * @param consumerPolicyEntities consumer entities of current policy.
     * @param providerPolicyEntities provider entities of current policy.
     */
    public BindToGroupPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                             @Nonnull final PolicyEntities consumerPolicyEntities,
                             @Nonnull final PolicyEntities providerPolicyEntities) {
        super(policyDefinition);
        Objects.requireNonNull(policyDefinition);

        this.policyInfo = policyDefinition.getPolicyInfo();
        this.consumerPolicyEntities = Objects.requireNonNull(consumerPolicyEntities);
        this.providerPolicyEntities = Objects.requireNonNull(providerPolicyEntities);
        GroupProtoUtil.checkEntityTypeForPolicy(providerPolicyEntities.getGroup());
    }

    @Nonnull
    public PolicyInfo getPolicyInfo() {
        return policyInfo;
    }

    @Nonnull
    public PolicyEntities getConsumerPolicyEntities() {
        return consumerPolicyEntities;
    }

    @Nonnull
    public PolicyEntities getProviderPolicyEntities() {
        return providerPolicyEntities;
    }
}
