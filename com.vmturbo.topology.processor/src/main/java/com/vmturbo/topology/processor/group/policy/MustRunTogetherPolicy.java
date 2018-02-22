package com.vmturbo.topology.processor.group.policy;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.processor.group.policy.PolicyFactory.PolicyEntities;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Requires that all entities in the consumer group must run together on a single provider of the
 * specified type.
 * Common use case: VM->VM affinity, keep all the VMs in the group on the same host.
 */
public class MustRunTogetherPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.MustRunTogetherPolicy mustRunTogetherPolicy;

    private final PolicyEntities policyEntities;

    /**
     * Create a new {@link MustRunTogetherPolicy}, the policy should be of type
     * {@link PolicyDTO.Policy.MustRunTogetherPolicy}.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     * @param policyEntities consumer entities of current policy.
     */
    public MustRunTogetherPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                                 @Nonnull final PolicyEntities policyEntities) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasMustRunTogether());
        this.mustRunTogetherPolicy = Objects.requireNonNull(policyDefinition.getMustRunTogether());
        this.policyEntities = Objects.requireNonNull(policyEntities);
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
    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
            throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying MustRunTogether policy.");

        // get group of entities that need to run together (consumers)
        final Group consumerGroup = policyEntities.getGroup();
        Set<Long> additionalEntities = policyEntities.getAdditionalEntities();
        final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                additionalEntities);

        // Add the commodity sold to the provider
        addCommoditySoldToSelectedProvider(consumers, topologyGraph);

        // Add the commodity bought to the entities that need to run separate
        addCommodityBought(consumers, topologyGraph, mustRunTogetherPolicy.getProviderEntityType(),
                commodityBought());

    }

    /**
     * Add commoditySold to provider which contains max number of consumers in group, if there are two
     * providers sell same number of consumers, use it's id to break tie.
     *
     * @param consumers The consumers that belong to the segment.
     * @param topologyGraph The graph containing the topology.
     */
    private void addCommoditySoldToSelectedProvider(@Nonnull final Set<Long> consumers,
                                  @Nonnull final TopologyGraph topologyGraph) {

        // find out which provider they need to consume from
        // we are picking the provider where the biggest number of consumers are already running on.
        final Map<Long, Long> providerMatchCountMap = new HashMap<>();
        for (long consumerOid : consumers) {
            topologyGraph.getProviders(consumerOid)
                    // filter out only providers of the type that we are interested in
                    .filter(provider -> provider.getEntityType() == mustRunTogetherPolicy.getProviderEntityType())
                    // add the provider to the map, incrementing the count if already found
                    .forEach(provider -> providerMatchCountMap.merge(provider.getOid(), 1L, Long::sum));
        }

        Comparator<Long> providerCompare = Comparator.comparingLong(providerMatchCountMap::get);
        providerMatchCountMap.keySet().stream()
                .max(providerCompare.thenComparing(Long::compare))
                .flatMap(topologyGraph::getEntity)
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .ifPresent(provider -> provider.addCommoditySoldList(commoditySold()));
    }
}
