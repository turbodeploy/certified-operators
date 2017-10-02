package com.vmturbo.topology.processor.group.policy;

import javax.annotation.Nonnull;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.vmturbo.common.protobuf.group.PolicyDTO;

/**
 * A factory for constructing policies based on policy definitions.
 */
public class PolicyFactory {

    /**
     * Create a new PolicyFactory.
     */
    public PolicyFactory() {

    }

    /**
     * Construct a new policy for application to a topology.
     *
     * @param policyDefinition The definition of the policy to be created.
     * @return A new policy for application to a topology.
     */
    public PlacementPolicy newPolicy(@Nonnull final PolicyDTO.Policy policyDefinition) {
        switch (policyDefinition.getPolicyDetailCase()) {
            case BIND_TO_GROUP:
                return new BindToGroupPolicy(policyDefinition);
            case BIND_TO_COMPLEMENTARY_GROUP:
                return new BindToComplementaryGroupPolicy(policyDefinition);
            case AT_MOST_N:
                return new AtMostNPolicy(policyDefinition);
            case AT_MOST_N_BOUND:
                return new AtMostNBoundPolicy(policyDefinition);
            case MUST_RUN_TOGETHER:
                return new MustRunTogetherPolicy(policyDefinition);
            case MERGE:
                throw new NotImplementedException();
            case BIND_TO_GROUP_AND_LICENSE:
                throw new NotImplementedException();
            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                throw new NotImplementedException();
            default:
                throw new NotImplementedException();
        }
    }
}
