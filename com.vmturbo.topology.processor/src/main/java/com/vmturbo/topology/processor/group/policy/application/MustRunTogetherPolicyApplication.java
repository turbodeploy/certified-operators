package com.vmturbo.topology.processor.group.policy.application;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;

/**
 * Applies a collection of {@link MustRunTogetherPolicyApplication}s. No bulk optimizations.
 */
public class MustRunTogetherPolicyApplication extends PlacementPolicyApplication {

    protected MustRunTogetherPolicyApplication(final GroupResolver groupResolver,
                                               final TopologyGraph<TopologyEntity> topologyGraph) {
        super(groupResolver, topologyGraph);
    }

    /**
     * Constrain entities in the consumer group to be forced to reside on only one entity (we choose
     * the entity which has max number of consumers on it to avoid disruptive ping-ponging of
     * consumers in customer's environment, if there are two providers sell same number of consumers,
     * use it's id to break tie) in the providers group by creating a segmentation commodity bought
     * by the consumers and sold ONLY by that one entity.
     *
     * {@inheritDoc}
     */
    @Override
    protected Map<PlacementPolicy, PolicyApplicationException> applyInternal(
            @Nonnull final List<PlacementPolicy> policies) {
        final Map<PlacementPolicy, PolicyApplicationException> errors = new HashMap<>();
        policies.stream()
            .filter(policy -> policy instanceof MustRunTogetherPolicy)
            .map(policy -> (MustRunTogetherPolicy)policy)
            .forEach(policy -> {
                try {
                    logger.debug("Applying MustRunTogether policy.");

                    // get group of entities that need to run together (consumers)
                    final Grouping consumerGroup = policy.getPolicyEntities().getGroup();
                    Set<Long> additionalEntities = policy.getPolicyEntities().getAdditionalEntities();
                    final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                        additionalEntities);

                    // Add the commodity sold to the provider
                    addCommoditySoldToSelectedProvider(policy, consumers);

                    // Add the commodity bought to the entities that need to run separate
                    addCommodityBought(consumers, policy.getDetails().getProviderEntityType(),
                        commodityBought(policy));
                } catch (GroupResolutionException e) {
                    errors.put(policy, new PolicyApplicationException(e));
                } catch (PolicyApplicationException e2) {
                    errors.put(policy, e2);
                }
            });
        return errors;
    }

    /**
     * Add commoditySold to provider which contains max number of consumers in group, if there are two
     * providers sell same number of consumers, use it's id to break tie.
     *
     * @param consumers The consumers that belong to the segment.
     */
    private void addCommoditySoldToSelectedProvider(@Nonnull final MustRunTogetherPolicy policy,
                                                    @Nonnull final Set<Long> consumers) {

        // find out which provider they need to consume from
        // we are picking the provider where the biggest number of consumers are already running on.
        final Map<Long, Long> providerMatchCountMap = new HashMap<>();
        for (long consumerOid : consumers) {
            topologyGraph.getProviders(consumerOid)
                // filter out only providers of the type that we are interested in
                .filter(provider -> provider.getEntityType() == policy.getDetails().getProviderEntityType())
                // add the provider to the map, incrementing the count if already found
                .forEach(provider -> providerMatchCountMap.merge(provider.getOid(), 1L, Long::sum));
        }

        Comparator<Long> providerCompare = Comparator.comparingLong(providerMatchCountMap::get);
        providerMatchCountMap.keySet().stream()
            .max(providerCompare.thenComparing(Long::compare))
            .flatMap(topologyGraph::getEntity)
            .map(TopologyEntity::getTopologyEntityDtoBuilder)
            .ifPresent(provider -> addCommoditySold(Collections.singleton(provider.getOid()), commoditySold(policy)));
    }
}
