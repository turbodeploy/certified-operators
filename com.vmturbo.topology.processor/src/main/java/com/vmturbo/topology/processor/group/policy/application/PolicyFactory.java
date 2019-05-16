package com.vmturbo.topology.processor.group.policy.application;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.NotImplementedException;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * A factory for constructing policies based on policy definitions.
 */
public class PolicyFactory {

    /**
     * Create a new PolicyFactory.
     */
    public PolicyFactory() {

    }

    public PlacementPolicyApplication newPolicyApplication(
            @Nonnull final PolicyDetailCase policyType,
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final TopologyGraph topologyGraph) {
        switch (policyType) {
            case AT_MOST_N:
                return new AtMostNPolicyApplication(groupResolver, topologyGraph);
            case AT_MOST_NBOUND:
                return new AtMostNBoundPolicyApplication(groupResolver, topologyGraph);
            case BIND_TO_COMPLEMENTARY_GROUP:
                return new BindToComplementaryGroupPolicyApplication(groupResolver, topologyGraph);
            case BIND_TO_GROUP:
                return new BindToGroupPolicyApplication(groupResolver, topologyGraph);
            case MERGE:
                return new MergePolicyApplication(groupResolver, topologyGraph);
            case MUST_RUN_TOGETHER:
                return new MustRunTogetherPolicyApplication(groupResolver, topologyGraph);
            case MUST_NOT_RUN_TOGETHER:
                return new MustNotRunTogetherPolicyApplication(groupResolver, topologyGraph);
            case BIND_TO_GROUP_AND_LICENSE:
                throw new NotImplementedException(policyType + " not supported.");
            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                throw new NotImplementedException(policyType + " not supported.");
            default:
                throw new IllegalArgumentException("Invalid policy type: " + policyType);
        }
    }

    /**
     * Construct a new policy for application to a topology.
     *
     * @param policyDefinition The definition of the policy to be created.
     * @param groups a Map which key is group id, value is {@link Group}
     * @param additionalConsumers contains a list of entity which should be part of consumers, but they
     *                         are not contains in consumer group.
     * @return A new policy for application to a topology.
     */
    public PlacementPolicy newPolicy(@Nonnull final Policy policyDefinition,
                                     @Nonnull final Map<Long, Group> groups,
                                     @Nonnull final Set<Long> additionalConsumers,
                                     @Nonnull final Set<Long> additionalProviders) {
        final PolicyDTO.PolicyInfo policyInfo = policyDefinition.getPolicyInfo();
        switch (policyInfo.getPolicyDetailCase()) {
            case BIND_TO_GROUP:
                return new BindToGroupPolicy(policyDefinition,
                    new PolicyEntities(groups.get(policyInfo.getBindToGroup().getConsumerGroupId()),
                            additionalConsumers),
                    new PolicyEntities(groups.get(policyInfo.getBindToGroup().getProviderGroupId()),
                            additionalProviders));
            case BIND_TO_COMPLEMENTARY_GROUP:
                return new BindToComplementaryGroupPolicy(policyDefinition,
                    new PolicyEntities(groups.get(policyInfo.getBindToComplementaryGroup().getConsumerGroupId()),
                            additionalConsumers),
                    new PolicyEntities(groups.get(policyInfo
                            .getBindToComplementaryGroup().getProviderGroupId())));
            case AT_MOST_N:
                return new AtMostNPolicy(policyDefinition,
                    new PolicyEntities(groups.get(policyInfo.getAtMostN().getConsumerGroupId()),
                            additionalConsumers),
                    new PolicyEntities(groups.get(policyInfo.getAtMostN().getProviderGroupId()),
                            additionalProviders));
            case AT_MOST_NBOUND:
                return new AtMostNBoundPolicy(policyDefinition,
                    new PolicyEntities(groups.get(policyInfo.getAtMostNbound().getConsumerGroupId()),
                            additionalConsumers),
                    new PolicyEntities(groups.get(policyInfo.getAtMostNbound().getProviderGroupId()),
                            additionalProviders));
            case MUST_RUN_TOGETHER:
                return new MustRunTogetherPolicy(policyDefinition,
                    new PolicyEntities(groups.get(policyInfo.getMustRunTogether().getGroupId()),
                            additionalConsumers));
            case MUST_NOT_RUN_TOGETHER:
                return new MustNotRunTogetherPolicy(policyDefinition,
                    new PolicyEntities(groups.get(policyInfo.getMustNotRunTogether().getGroupId()),
                            additionalConsumers));
            case MERGE:
                //TODO: support additionalConsumers and additionalProviders
                return buildMergePolicy(policyDefinition, groups);
            case BIND_TO_GROUP_AND_LICENSE:
                throw new NotImplementedException(policyInfo.getPolicyDetailCase() + " not supported.");
            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                throw new NotImplementedException(policyInfo.getPolicyDetailCase() + " not supported.");
            default:
                throw new IllegalArgumentException(policyInfo.getPolicyDetailCase() + " invalid.");
        }
    }

    /**
     *
     * @param policyDefinition merge policy
     * @param groups the group referenced by Policy
     * @return merge policy
     */
    private PlacementPolicy buildMergePolicy(@Nonnull final Policy policyDefinition,
                                             @Nonnull final Map<Long, Group> groups) {
        final List<Long> mergeGroupIdList =
                policyDefinition.getPolicyInfo().getMerge().getMergeGroupIdsList();
        final List<PolicyEntities> policyEntitiesList = mergeGroupIdList.stream()
                .map(id -> new PolicyEntities(groups.get(id)))
                .collect(Collectors.toList());
        return new MergePolicy(policyDefinition, policyEntitiesList);
    }

    /**
     * This class is a wrapper class for provider or consumer entities of Policy. It should not have
     * any intersection between group members with additional entities.
     */
    public static class PolicyEntities {
        // the provider/consumer group referenced by Policy.
        private final Group group;
        // additional entities need to apply Policy, it should have no intersection with group members
        // It should be only populated in very special case, for example, in some plan configuration,
        // we want to have some entities to apply the policy without adding them to original group.
        private final Set<Long> additionalEntities;

        public PolicyEntities(@Nonnull final Group group, @Nonnull final Set<Long> additionalEntities) {
            this.group = group;
            this.additionalEntities = additionalEntities;
        }

        public PolicyEntities(@Nonnull final Group group) {
            this.group = group;
            this.additionalEntities = Collections.emptySet();
        }

        public Group getGroup() {
            return this.group;
        }

        public Set<Long> getAdditionalEntities() {
            return this.additionalEntities;
        }
    }
}
