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

    private final int invertedIndexMinimalScanThreshold;

    protected AtMostNPolicyApplication(final GroupResolver groupResolver,
            final TopologyGraph<TopologyEntity> topologyGraph,
            final TopologyInvertedIndexFactory invertedIndexFactory,
            final int invertedIndexMinimalScanThreshold) {
        super(groupResolver, topologyGraph, invertedIndexFactory);
        this.invertedIndexMinimalScanThreshold = invertedIndexMinimalScanThreshold;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(@Nonnull final List<AtMostNPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        final Map<Long, Set<Long>> consumersByPolicyId = new HashMap<>();
        // We create all the commodities bought for all the policies in the end (after all the
        // sold commodities for all the policies are created). This is because
        // these bought commodities will show up on the entity but are not present in the inverted
        // index. And if the same entity appears in a subsequent policy then we will look for sellers
        // selling the bought commodity in the index and find nothing.
        createAllCommSold(policies, errors, consumersByPolicyId);
        createAllCommBought(policies, errors, consumersByPolicyId);
        return errors;
    }

    /**
     * Create all the commodities sold for all the policies.
     * @param policies all the AtMostN policies
     * @param errors place to store any errors
     * @param consumersByPolicyId map to store the consumers for each policy
     */
    private void createAllCommSold(@Nonnull final List<AtMostNPolicy> policies,
                                   @Nonnull final Map<PlacementPolicy, PolicyApplicationException> errors,
                                   @Nonnull final Map<Long, Set<Long>> consumersByPolicyId) {
        // Build up an inverted index for the provider types targetted by these policies.
        // We use this inverted index to find which providers we can put segmentation commodities
        // onto (to avoid putting segmentation commodities on ALL providers outside the target
        // provider group).
        final Set<ApiEntityType> providerTypes = policies.stream()
            .flatMap(policy -> GroupProtoUtil.getEntityTypes(policy.getProviderPolicyEntities().getGroup()).stream())
            .collect(Collectors.toSet());
        final InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> invertedIndex =
            invertedIndexFactory.typeInvertedIndex(topologyGraph, providerTypes, invertedIndexMinimalScanThreshold);
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
                consumersByPolicyId.put(policy.getPolicyDefinition().getId(), consumers);

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
            } catch (GroupResolutionException e) {
                errors.put(policy, new PolicyApplicationException(e));
            } catch (IllegalArgumentException e) {
                errors.put(policy, new PolicyApplicationException(e));
            }
        });
    }

    /**
     * Create all the commodities bought for all the policies.
     * @param policies all the AtMostN policies
     * @param errors plave to store any errors
     * @param consumersByPolicyId read the consumers for the policy from this map. We do this to
     *                            avoid making another RPC call.
     */
    private void createAllCommBought(@Nonnull final List<AtMostNPolicy> policies,
                                     @Nonnull final Map<PlacementPolicy, PolicyApplicationException> errors,
                                     @Nonnull final Map<Long, Set<Long>> consumersByPolicyId) {
        policies.forEach(policy -> {
            try {
                Set<Long> consumers = consumersByPolicyId.get(policy.getPolicyDefinition().getId());
                if (consumers != null && !consumers.isEmpty()) {
                    final Grouping providerGroup = policy.getProviderPolicyEntities().getGroup();
                    GroupProtoUtil.checkEntityTypeForPolicy(providerGroup);
                    final ApiEntityType providerEntityType = GroupProtoUtil.getEntityTypes(providerGroup).iterator().next();
                    addCommodityBought(consumers, providerEntityType.typeNumber(),
                        commodityBought(policy));
                }
            } catch (PolicyApplicationException e) {
                errors.put(policy, e);
            }
        });
    }
}
