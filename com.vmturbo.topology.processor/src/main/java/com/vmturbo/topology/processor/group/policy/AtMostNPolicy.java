package com.vmturbo.topology.processor.group.policy;

import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.TopologyGraph;

/**
 * Defines a limit on the number of entities from the consumer group that can run on each
 * provider in the provider group without limiting the consumers to run only on the entities
 * in the provider group.
 * Common use case: VM->VM anti-affinity
 */
public class AtMostNPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.AtMostNPolicy atMostN;
    private final Group providerGroup;
    private final Group consumerGroup;

    /**
     * Create a new AtMostNPolicy.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     *                         The policy should be of type AtMostN.
     */
    public AtMostNPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                         @Nonnull final Group consumerGroup,
                         @Nonnull final Group providerGroup) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasAtMostN(), "Must be AtMostNPolicy");

        this.consumerGroup = consumerGroup;
        this.providerGroup = providerGroup;
        this.atMostN = policyDefinition.getAtMostN();
        GroupProtoUtil.checkEntityType(providerGroup);
        Preconditions.checkArgument(this.atMostN.hasCapacity(),
            "Capacity required");
    }

    /**
     * Constrain entities in the consumer group to be forced to reside on entities in the providers group
     * by creating a segmentation commodity bought by the consumers and sold by the providers.
     *
     * Providers in the complement of the providers group sell with infinite capacity, while members
     * of the provider's group sell only at the capacity.
     *
     * {@inheritDoc}
     */
    @Override
    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
        throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying AtMostN policy with capacity of {}.", atMostN.getCapacity());

        // Resolve the relevant groups
        final int providerEntityType = GroupProtoUtil.getEntityType(providerGroup);
        final Set<Long> providers = groupResolver.resolve(providerGroup, topologyGraph);
        final Set<Long> consumers = groupResolver.resolve(consumerGroup, topologyGraph);

        // Add the commodity to the appropriate entities.
        // Add a small epsilon to the capacity to ensure floating point roundoff error does not accidentally
        // reduce the atMostN capacity below the intended integer equivalent value.
        addCommoditySold(providers, consumers, topologyGraph, atMostN.getCapacity() + 0.1f);
        addCommoditySoldToComplementaryProviders(providers, providerEntityType, topologyGraph,
            commoditySold());
        addCommodityBought(consumers, topologyGraph, providerEntityType, commodityBought());
    }
}
