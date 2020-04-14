package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;

/**
 * Applies a collection of {@link AtMostNBoundPolicy}s. No bulk optimizations.
 */
public class AtMostNBoundPolicyApplication extends PlacementPolicyApplication {

    protected AtMostNBoundPolicyApplication(final GroupResolver groupResolver,
                                            final TopologyGraph<TopologyEntity> topologyGraph) {
        super(groupResolver, topologyGraph);
    }

    /**
     * Constrain entities in the consumer group to be forced to reside on entities in the providers group
     * by creating a segmentation commodity bought by the consumers and sold ONLY by the providers
     * in the provider's group with the specified capacity.
     *
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            @Nonnull final List<PlacementPolicy> atMostNBoundPolicies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        atMostNBoundPolicies.stream()
            .filter(policy -> policy instanceof AtMostNBoundPolicy)
            .map(policy -> (AtMostNBoundPolicy)policy)
            .forEach(policy -> {
                try {
                    logger.debug("Applying AtMostNBound policy with capacity of {}.",
                        policy.getDetails().getCapacity());
                    final Grouping providerGroup = policy.getProviderPolicyEntities().getGroup();
                    final Grouping consumerGroup = policy.getConsumerPolicyEntities().getGroup();
                    GroupProtoUtil.checkEntityTypeForPolicy(providerGroup);
                    //Above check makes sure that the group only has only one entity type here
                    final ApiEntityType providerEntityType = GroupProtoUtil.getEntityTypes(providerGroup)
                        .iterator().next();
                    final ApiEntityType consumerEntityType = GroupProtoUtil.getEntityTypes(consumerGroup)
                        .iterator().next();
                    final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph).getEntitiesOfType(providerEntityType),
                        policy.getProviderPolicyEntities().getAdditionalEntities());
                    final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph).getEntitiesOfType(consumerEntityType),
                        policy.getConsumerPolicyEntities().getAdditionalEntities());

                    // Add the commodity to the appropriate entities.
                    // Add a small delta to the capacity to ensure floating point roundoff error does not accidentally
                    // reduce the atMostNBound capacity below the intended integer equivalent value.
                    addCommoditySold(providers, consumers,
                        policy.getDetails().getCapacity() + SMALL_DELTA_VALUE, policy);
                    addCommodityBought(consumers, providerEntityType.typeNumber(), commodityBought(policy));
                } catch (GroupResolutionException e) {
                    errors.put(policy, new PolicyApplicationException(e));
                } catch (PolicyApplicationException e2) {
                    errors.put(policy, e2);
                }
            });
        return errors;
    }
}
