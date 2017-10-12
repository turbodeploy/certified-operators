package com.vmturbo.topology.processor.group.policy;

import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;
import com.vmturbo.topology.processor.topology.TopologyGraph.Vertex;

/**
 * Requires that all entities in the consumer group must run together on a single provider in
 * the provider group.
 * Common use case: VM->VM affinity.
 */
public class MustRunTogetherPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.MustRunTogetherPolicy mustRunTogetherPolicy;

    private final PolicyGrouping consumerGrouping;
    private final PolicyGrouping providerGrouping;

    /**
     * Create a new MustRunTogetherPolicy, the policy should be of type MustRunTogether.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     */
    public MustRunTogetherPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                                 @Nonnull final PolicyGrouping consumerGrouping,
                                 @Nonnull final PolicyGrouping providerGrouping) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasMustRunTogether());
        this.mustRunTogetherPolicy = policyDefinition.getMustRunTogether();
        this.consumerGrouping = consumerGrouping;
        this.providerGrouping = providerGrouping;
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
        logger.debug("Applying mustRunTogether policy.");

        final Set<Long> providers = groupResolver.resolve(providerGrouping, topologyGraph);
        final Set<Long> consumers = groupResolver.resolve(consumerGrouping, topologyGraph);

        final int providerType = entityType(providerGrouping);

        addCommoditySold(providers, consumers, topologyGraph);
        addCommodityBought(consumers, topologyGraph, providerType, commodityBought());
    }

    /**
     * Add commoditySold to provider which contains max number of consumers in group, if there are two
     * providers sell same number of consumers, use it's id to break tie.
     *
     * @param providers The providers that belong to the segment.
     * @param consumers The consumers that belong to the segment.
     * @param topologyGraph The graph containing the topology.
     */
    private void addCommoditySold(@Nonnull final Set<Long> providers,
                                  @Nonnull final Set<Long> consumers,
                                  @Nonnull final TopologyGraph topologyGraph) {
        final Map<Long, Long> providerMatchCountMap = providers.stream()
            .collect(Collectors.toMap(Function.identity(),
                provider -> topologyGraph.getConsumers(provider)
                    .map(Vertex::getOid)
                    .filter(consumers::contains)
                    .count())
            );

        Comparator<Long> providerCompare = Comparator.comparingLong(providerMatchCountMap::get);
        providers.stream()
            .max(providerCompare.thenComparing(Long::compare))
            .flatMap(topologyGraph::getVertex)
            .map(Vertex::getTopologyEntityDtoBuilder)
            .ifPresent(provider -> provider.addCommoditySoldList(commoditySold()));
    }
}
