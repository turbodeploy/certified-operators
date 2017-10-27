package com.vmturbo.topology.processor.group.policy;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyGrouping;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Limits the entities in the consumer group to run only on the entities in the provider group
 * and limit the number of entities that can run on each of the providers.
 *
 * Common use case: VM->VM anti-affinity
 */
public class AtMostNBoundPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.AtMostNBoundPolicy atMostNBound;
    private final PolicyGrouping providerGrouping;
    private final PolicyGrouping consumerGrouping;

    /**
     * Create a new AtMostNBoundPolicy.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     *                         The policy should be of type AtMostN.
     */
    public AtMostNBoundPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                              @Nonnull final PolicyGrouping consumerGrouping,
                              @Nonnull final PolicyGrouping providerGrouping) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasAtMostNbound(), "Must be AtMostNBoundPolicy");

        this.providerGrouping = providerGrouping;
        this.consumerGrouping = consumerGrouping;
        this.atMostNBound = policyDefinition.getAtMostNbound();
        Preconditions.checkArgument(hasEntityType(providerGrouping),
            "ProviderGroup entity type required");
        Preconditions.checkArgument(this.atMostNBound.hasCapacity(),
            "Capacity required");
    }

    /**
     * Constrain entities in the consumer group to be forced to reside on entities in the providers group
     * by creating a segmentation commodity bought by the consumers and sold ONLY by the providers
     * in the provider's group with the specified capacity.
     *
     * {@inheritDoc}
     */
    @Override
    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
        throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying AtMostNBound policy with capacity of {}.", atMostNBound.getCapacity());

        // Resolve the relevant groups
        final int providerEntityType = entityType(providerGrouping);
        final Set<Long> providers = groupResolver.resolve(providerGrouping, topologyGraph);
        final Set<Long> consumers = groupResolver.resolve(consumerGrouping, topologyGraph);

        // Add the commodity to the appropriate entities.
        // Add a small epsilon to the capacity to ensure floating point roundoff error does not accidentally
        // reduce the atMostNBound capacity below the intended integer equivalent value.
        addCommoditySold(providers, consumers, topologyGraph, atMostNBound.getCapacity() + 0.1f);
        addCommodityBought(consumers, topologyGraph, providerEntityType, commodityBought());
    }
}
