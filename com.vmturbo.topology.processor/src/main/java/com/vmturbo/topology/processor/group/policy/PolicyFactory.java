package com.vmturbo.topology.processor.group.policy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;

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
     * @param groups a Map which key is group id, value is {@link Group}
     * @param additionalConsumers contains a list of entity which should be part of consumers, but they
     *                         are not contains in consumer group.
     * @return A new policy for application to a topology.
     */
    public PlacementPolicy newPolicy(@Nonnull final Policy policyDefinition,
                                     @Nonnull final Map<Long, Group> groups,
                                     @Nonnull final Set<Long> additionalConsumers,
                                     @Nonnull final Set<Long> additionalProviders) {
        switch (policyDefinition.getPolicyDetailCase()) {
            case BIND_TO_GROUP:
                return new BindToGroupPolicy(policyDefinition,
                        new PolicyEntities(groups.get(policyDefinition.getBindToGroup().getConsumerGroupId()),
                                additionalConsumers),
                        new PolicyEntities(groups.get(policyDefinition.getBindToGroup().getProviderGroupId()),
                                additionalProviders));
            case BIND_TO_COMPLEMENTARY_GROUP:
                return new BindToComplementaryGroupPolicy(policyDefinition,
                        new PolicyEntities(groups.get(policyDefinition.getBindToComplementaryGroup().getConsumerGroupId()),
                                additionalConsumers),
                        new PolicyEntities(groups.get(policyDefinition
                                .getBindToComplementaryGroup().getProviderGroupId())));
            case AT_MOST_N:
                return new AtMostNPolicy(policyDefinition,
                        new PolicyEntities(groups.get(policyDefinition.getAtMostN().getConsumerGroupId()),
                                additionalConsumers),
                        new PolicyEntities(groups.get(policyDefinition.getAtMostN().getProviderGroupId()),
                                additionalProviders));
            case AT_MOST_NBOUND:
                return new AtMostNBoundPolicy(policyDefinition,
                        new PolicyEntities(groups.get(policyDefinition.getAtMostNbound().getConsumerGroupId()),
                                additionalConsumers),
                        new PolicyEntities(groups.get(policyDefinition.getAtMostNbound().getProviderGroupId()),
                                additionalProviders));
            case MUST_RUN_TOGETHER:
                return new MustRunTogetherPolicy(policyDefinition,
                        new PolicyEntities(groups.get(policyDefinition.getMustRunTogether().getConsumerGroupId()),
                                additionalConsumers),
                        new PolicyEntities(groups.get(policyDefinition.getMustRunTogether().getProviderGroupId()),
                                additionalProviders));
            case MERGE:
                //TODO: support additionalConsumers and additionalProviders
                return buildMergePolicy(policyDefinition, groups);
            case BIND_TO_GROUP_AND_LICENSE:
                throw new NotImplementedException();
            case BIND_TO_GROUP_AND_GEO_REDUNDANCY:
                throw new NotImplementedException();
            default:
                throw new NotImplementedException();
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
        List<Long> mergeGroupIdList = policyDefinition.getMerge().getMergeGroupIdsList();
        List<PolicyEntities> policyEntitiesList = mergeGroupIdList.stream()
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
