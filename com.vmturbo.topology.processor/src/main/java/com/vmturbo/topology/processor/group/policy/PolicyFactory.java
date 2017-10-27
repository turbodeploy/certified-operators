package com.vmturbo.topology.processor.group.policy;

import java.util.Map;
import javax.annotation.Nonnull;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGroupingID;

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
    public PlacementPolicy newPolicy(@Nonnull final Policy policyDefinition,
                                     @Nonnull final Map<PolicyGroupingID, PolicyGrouping> groups) {
        switch (policyDefinition.getPolicyDetailCase()) {
            case BIND_TO_GROUP:
                return new BindToGroupPolicy(policyDefinition,
                        groups.get(policyDefinition.getBindToGroup().getConsumerGroupId()),
                        groups.get(policyDefinition.getBindToGroup().getProviderGroupId()));
            case BIND_TO_COMPLEMENTARY_GROUP:
                return new BindToComplementaryGroupPolicy(policyDefinition,
                        groups.get(policyDefinition
                                .getBindToComplementaryGroup().getConsumerGroupId()),
                        groups.get(policyDefinition
                                .getBindToComplementaryGroup().getProviderGroupId()));
            case AT_MOST_N:
                return new AtMostNPolicy(policyDefinition,
                        groups.get(policyDefinition.getAtMostN().getConsumerGroupId()),
                        groups.get(policyDefinition.getAtMostN().getProviderGroupId()));
            case AT_MOST_NBOUND:
                return new AtMostNBoundPolicy(policyDefinition,
                        groups.get(policyDefinition.getAtMostNbound().getConsumerGroupId()),
                        groups.get(policyDefinition.getAtMostNbound().getProviderGroupId()));
            case MUST_RUN_TOGETHER:
                return new MustRunTogetherPolicy(policyDefinition,
                        groups.get(policyDefinition.getMustRunTogether().getConsumerGroupId()),
                        groups.get(policyDefinition.getMustRunTogether().getProviderGroupId()));
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
