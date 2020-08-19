package com.vmturbo.topology.processor.group.policy.application;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
 * Applies a collection of {@link BindToComplementaryGroupPolicy}s. No bulk optimizations.
 */
public class BindToComplementaryGroupPolicyApplication extends PlacementPolicyApplication<BindToComplementaryGroupPolicy> {
    protected BindToComplementaryGroupPolicyApplication(
            final GroupResolver groupResolver,
            final TopologyGraph<TopologyEntity> topologyGraph,
            TopologyInvertedIndexFactory invertedIndexFactory) {
        super(groupResolver, topologyGraph, invertedIndexFactory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            @Nonnull final List<BindToComplementaryGroupPolicy> policies) {
        // Build up an inverted index for the provider types targetted by these policies.
        // We use this inverted index to find which providers we can put segmentation commodities
        // onto (to avoid putting segmentation commodities on ALL providers outside the target
        // provider group).
        final Set<ApiEntityType> providerTypes = policies.stream()
            .flatMap(policy -> GroupProtoUtil.getEntityTypes(policy.getProviderPolicyEntities().getGroup()).stream())
            .collect(Collectors.toSet());
        final InvertedIndex<TopologyEntity, CommoditiesBoughtFromProvider> invertedIndex =
                invertedIndexFactory.typeInvertedIndex(topologyGraph, providerTypes);

        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.forEach(policy -> {
            try {
                logger.debug("Applying bindToComplementaryGroup policy.");
                final Grouping providerGroup = policy.getProviderPolicyEntities().getGroup();
                final Grouping consumerGroup = policy.getConsumerPolicyEntities().getGroup();
                GroupProtoUtil.checkEntityTypeForPolicy(providerGroup);
                GroupProtoUtil.checkEntityTypeForPolicy(consumerGroup);
                final ApiEntityType providerEntityType = GroupProtoUtil.getEntityTypes(providerGroup).iterator().next();
                final ApiEntityType consumerEntityType = GroupProtoUtil.getEntityTypes(consumerGroup).iterator().next();
                final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph)
                        .getEntitiesOfType(providerEntityType), policy.getProviderPolicyEntities().getAdditionalEntities());
                Set<Long> consumers = groupResolver.resolve(consumerGroup, topologyGraph).getEntitiesOfType(consumerEntityType);
                consumers = consumers.stream().filter(id -> {
                    Optional<TopologyEntity> entity = topologyGraph.getEntity(id);
                    if (entity.isPresent() && entity.get().hasReservationOrigin()) {
                        return false;
                    }
                    return true;
                }).collect(Collectors.toSet());
                consumers.addAll(policy.getConsumerPolicyEntities().getAdditionalEntities());
                if (consumers.isEmpty()) {
                    // If there are no consumers to bind, there's nothing more to do.
                    return;
                }

                //checkEntityType logic makes sure that the group only has only one entity type here
                // if providers have been replaced, add them to the list of providers so as to skip them
                Set<Long> replacedProviders = new HashSet<>();
                providers.forEach(providerId -> topologyGraph.getEntity(providerId)
                    .map(TopologyEntity::getTopologyEntityDtoBuilder)
                    .ifPresent(provider -> {
                        if (provider.hasEdit() && provider.getEdit().hasReplaced()) {
                            replacedProviders.add(provider.getEditBuilder().getReplaced().getReplacementId());
                        }
                    }));

                final Set<Long> allBlockedProviders = Sets.union(providers, replacedProviders);

                // Add the commodity to the appropriate entities
                addCommoditySoldToComplementaryProviders(consumers, allBlockedProviders,
                        providerEntityType.typeNumber(), invertedIndex, commoditySold(policy));
                addCommodityBought(consumers, providerEntityType.typeNumber(),
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
