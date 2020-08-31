package com.vmturbo.topology.processor.group.policy.application;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.NotImplementedException;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.Policy;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo.PolicyDetailCase;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

/**
 * A factory for constructing policies based on policy definitions.
 */
public class PolicyFactory {

    private final TopologyInvertedIndexFactory invertedIndexFactory;

    /**
     * Create a new PolicyFactory.
     *
     * @param invertedIndexFactory The {@link TopologyInvertedIndexFactory} policy applications can
     * use to create an inverted index.
     */
    public PolicyFactory(@Nonnull final TopologyInvertedIndexFactory invertedIndexFactory) {
        this.invertedIndexFactory = invertedIndexFactory;
    }

    public PlacementPolicyApplication newPolicyApplication(
            @Nonnull final PolicyDetailCase policyType,
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        switch (policyType) {
            case AT_MOST_N:
                return new AtMostNPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
            case AT_MOST_NBOUND:
                return new AtMostNBoundPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
            case BIND_TO_COMPLEMENTARY_GROUP:
                return new BindToComplementaryGroupPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
            case BIND_TO_GROUP:
                return new BindToGroupPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
            case MERGE:
                return new MergePolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
            case MUST_RUN_TOGETHER:
                return new MustRunTogetherPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
            case MUST_NOT_RUN_TOGETHER:
                return new MustNotRunTogetherPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
            case BIND_TO_GROUP_AND_LICENSE:
                // TODO: implement BindToGroupAndLicencePolicyApplication class
                return new BindToGroupPolicyApplication(groupResolver, topologyGraph, invertedIndexFactory);
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
     * @param groups a Map which key is group id, value is {@link Grouping}
     * @param additionalConsumers contains a list of entity which should be part of consumers, but they
     *                         are not contains in consumer group.
     * @return A new policy for application to a topology.
     */
    public PlacementPolicy newPolicy(@Nonnull final Policy policyDefinition,
                                     @Nonnull final Map<Long, Grouping> groups,
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
                return new BindToGroupPolicy(policyDefinition,
                        new PolicyEntities(groups.get(policyInfo.getBindToGroupAndLicense().getConsumerGroupId()),
                                additionalConsumers),
                        new PolicyEntities(groups.get(policyInfo.getBindToGroupAndLicense().getProviderGroupId()),
                                additionalProviders));
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
                                             @Nonnull final Map<Long, Grouping> groups) {
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
        private final Grouping group;
        // additional entities need to apply Policy, it should have no intersection with group members
        // It should be only populated in very special case, for example, in some plan configuration,
        // we want to have some entities to apply the policy without adding them to original group.
        private final Set<Long> additionalEntities;

        /**
         * Constructor for {@link PolicyEntities}.
         * @param group the provider/consumer group referenced by Policy.
         * @param additionalEntities additional entities need to apply Policy.
         */
        public PolicyEntities(@Nonnull final Grouping group, @Nonnull final Set<Long> additionalEntities) {
            this.group = group;
            this.additionalEntities = additionalEntities;
        }

        /**
         * Constructor for {@link PolicyEntities}.
         * @param group the provider/consumer group referenced by Policy.
         */
        public PolicyEntities(@Nonnull final Grouping group) {
            this.group = group;
            this.additionalEntities = Collections.emptySet();
        }

        /**
         * Getter for the group.
         * @return the group.
         */
        public Grouping getGroup() {
            return this.group;
        }

        public Set<Long> getAdditionalEntities() {
            return this.additionalEntities;
        }
    }
}
