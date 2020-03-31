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
 * Applies a collection of {@link AtMostNPolicy}s. No bulk optimizations.
 */
public class AtMostNPolicyApplication extends PlacementPolicyApplication {

    protected AtMostNPolicyApplication(final GroupResolver groupResolver,
                                       final TopologyGraph<TopologyEntity> topologyGraph) {
        super(groupResolver, topologyGraph);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(@Nonnull final List<PlacementPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.stream()
            .filter(policy -> policy instanceof AtMostNPolicy)
            .map(policy -> (AtMostNPolicy)policy)
            .forEach(policy -> {
                try {
                    logger.debug("Applying AtMostN policy with capacity of {}.", policy.getDetails().getCapacity());
                    final Grouping providerGroup = policy.getProviderPolicyEntities().getGroup();
                    final Grouping consumerGroup = policy.getConsumerPolicyEntities().getGroup();
                    GroupProtoUtil.checkEntityTypeForPolicy(providerGroup);
                    GroupProtoUtil.checkEntityTypeForPolicy(consumerGroup);
                    //Above check makes sure that the group only has only one entity type here
                    final ApiEntityType providerEntityType = GroupProtoUtil.getEntityTypes(providerGroup).iterator().next();
                    final ApiEntityType consumerEntityType = GroupProtoUtil.getEntityTypes(consumerGroup).iterator().next();
                    final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph).getEntitiesOfType(providerEntityType),
                        policy.getProviderPolicyEntities().getAdditionalEntities());
                    final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph).getEntitiesOfType(consumerEntityType),
                        policy.getConsumerPolicyEntities().getAdditionalEntities());

                    // Add the commodity to the appropriate entities.
                    // Add a small delta to the capacity to ensure floating point roundoff error does not accidentally
                    // reduce the atMostN capacity below the intended integer equivalent value.
                    addCommoditySold(providers, consumers,
                        policy.getDetails().getCapacity() + SMALL_DELTA_VALUE, policy);
                    addCommoditySoldToComplementaryProviders(providers, providerEntityType.typeNumber(),
                        commoditySold(policy));
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
