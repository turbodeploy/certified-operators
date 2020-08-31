package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyInvertedIndexFactory;

/**
 * Applies a collection of {@link AtMostNPolicy}s. No bulk optimizations.
 */
public class AtMostNPolicyApplication extends PlacementPolicyApplication<AtMostNPolicy> {

    protected AtMostNPolicyApplication(final GroupResolver groupResolver,
            final TopologyGraph<TopologyEntity> topologyGraph,
            final TopologyInvertedIndexFactory invertedIndexFactory) {
        super(groupResolver, topologyGraph, invertedIndexFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(@Nonnull final List<AtMostNPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        // Build up an inverted index for the provider types targetted by these policies.
        // We use this inverted index to find which providers we can put segmentation commodities
        // onto (to avoid putting segmentation commodities on ALL providers outside the target
        // provider group).
        final Set<ApiEntityType> providerTypes = policies.stream()
                .flatMap(policy -> GroupProtoUtil.getEntityTypes(policy.getProviderPolicyEntities().getGroup()).stream())
                .collect(Collectors.toSet());
        final InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> invertedIndex =
                invertedIndexFactory.typeInvertedIndex(topologyGraph, providerTypes);
        policies.forEach(policy -> {
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

                if (consumers.isEmpty()) {
                    // Early exit to avoid any shenanigans with empty sets implying "all" entities.
                    logger.debug("Policy {} has no consumers."
                        + " Not creating provider segmentation commodities.", policy);
                    return;
                }

                // Add the commodity to the appropriate entities.
                // Add a small delta to the capacity to ensure floating point roundoff error does not accidentally
                // reduce the atMostN capacity below the intended integer equivalent value.
                addCommoditySold(providers, consumers,
                    policy.getDetails().getCapacity() + SMALL_DELTA_VALUE, policy);
                // Other providers are still valid destinations, with no limit on the capacity.
                addCommoditySoldToComplementaryProviders(consumers, providers, providerEntityType.typeNumber(),
                    invertedIndex, commoditySold(policy));
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
