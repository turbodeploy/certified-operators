package com.vmturbo.topology.processor.group.policy;

import java.util.Objects;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.Group;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.PolicyFactory.PolicyEntities;
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

    private final PolicyEntities providerPolicyEntities;

    private final PolicyEntities consumerPolicyEntities;

    /**
     * Create a new AtMostNBoundPolicy.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     *                         The policy should be of type AtMostN.
     * @param consumerPolicyEntities consumer entities of current policy.
     * @param providerPolicyEntities provider entities of current policy.
     */
    public AtMostNBoundPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                              @Nonnull final PolicyEntities consumerPolicyEntities,
                              @Nonnull final PolicyEntities providerPolicyEntities) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasAtMostNbound(), "Must be AtMostNBoundPolicy");

        this.providerPolicyEntities = Objects.requireNonNull(providerPolicyEntities);
        this.consumerPolicyEntities = Objects.requireNonNull(consumerPolicyEntities);
        this.atMostNBound = Objects.requireNonNull(policyDefinition.getAtMostNbound());
        GroupProtoUtil.checkEntityType(providerPolicyEntities.getGroup());
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
        final Group providerGroup = providerPolicyEntities.getGroup();
        final Group consumerGroup = consumerPolicyEntities.getGroup();
        // Resolve the relevant groups
        final int providerEntityType = GroupProtoUtil.getEntityType(providerGroup);
        final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph),
                providerPolicyEntities.getAdditionalEntities());
        final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                consumerPolicyEntities.getAdditionalEntities());

        // Add the commodity to the appropriate entities.
        // Add a small delta to the capacity to ensure floating point roundoff error does not accidentally
        // reduce the atMostNBound capacity below the intended integer equivalent value.
        addCommoditySold(providers, consumers, topologyGraph,
                atMostNBound.getCapacity() + SMALL_DELTA_VALUE);
        addCommodityBought(consumers, topologyGraph, providerEntityType, commodityBought());
    }
}
