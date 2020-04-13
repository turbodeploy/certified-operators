package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;

/**
 * Applies a collection of {@link BindToGroupPolicy}s. No bulk optimizations.
 */
public class BindToGroupPolicyApplication extends PlacementPolicyApplication {
    protected BindToGroupPolicyApplication(final GroupResolver groupResolver,
                                           final TopologyGraph<TopologyEntity> topologyGraph) {
        super(groupResolver, topologyGraph);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            @Nonnull final List<PlacementPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.stream()
            .filter(policy -> policy instanceof BindToGroupPolicy)
            .map(policy -> (BindToGroupPolicy)policy)
            .forEach(policy -> {
                try {
                    logger.debug("Applying bindToGroup policy.");
                    final Grouping providerGroup = policy.getProviderPolicyEntities().getGroup();
                    final Grouping consumerGroup = policy.getConsumerPolicyEntities().getGroup();
                    GroupProtoUtil.checkEntityTypeForPolicy(providerGroup);
                    // Resolve the relevant groups
                    final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph),
                        policy.getProviderPolicyEntities().getAdditionalEntities());
                    final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                        policy.getConsumerPolicyEntities().getAdditionalEntities());

                    // Add the commodity to the appropriate entities.
                    addCommoditySold(providers, commoditySold(policy));
                    //checkEntityType logic makes sure that the group only has only one entity type here
                    addCommodityBought(consumers, GroupProtoUtil.getEntityTypes(providerGroup).iterator().next().typeNumber(),
                        commodityBought(policy));
                } catch (GroupResolutionException e) {
                    errors.put(policy, new PolicyApplicationException(e));
                } catch (PolicyApplicationException e2) {
                    errors.put(policy, e2);
                }
            });
        return errors;
    }
}
