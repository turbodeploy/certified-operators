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
 * Limits the entities in the consumer group to run only on the entities in the provider group
 * and limit the number of entities that can run on each of the providers.
 *
 * Common use case: VM->VM anti-affinity
 */
public class AtMostNBoundPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.AtMostNBoundPolicy atMostNBound;

    private final Group providerGroup;

    private final Group consumerGroup;

    /**
     * Create a new AtMostNBoundPolicy.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     *                         The policy should be of type AtMostN.
     */
    public AtMostNBoundPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                              @Nonnull final Group consumerGroup,
                              @Nonnull final Group providerGroup) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasAtMostNbound(), "Must be AtMostNBoundPolicy");

        this.providerGroup = providerGroup;
        this.consumerGroup = consumerGroup;
        this.atMostNBound = policyDefinition.getAtMostNbound();
        GroupProtoUtil.checkEntityType(providerGroup);
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
        final int providerEntityType = GroupProtoUtil.getEntityType(providerGroup);
        final Set<Long> providers = groupResolver.resolve(providerGroup, topologyGraph);
        final Set<Long> consumers = groupResolver.resolve(consumerGroup, topologyGraph);

        // Add the commodity to the appropriate entities.
        // Add a small epsilon to the capacity to ensure floating point roundoff error does not accidentally
        // reduce the atMostNBound capacity below the intended integer equivalent value.
        addCommoditySold(providers, consumers, topologyGraph, atMostNBound.getCapacity() + 0.1f);
        addCommodityBought(consumers, topologyGraph, providerEntityType, commodityBought());
    }
}
