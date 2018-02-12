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
 * Defines a limit on the number of entities from the consumer group that can run on each
 * provider in the provider group without limiting the consumers to run only on the entities
 * in the provider group.
 * Common use case: VM->VM anti-affinity
 */
public class AtMostNPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.AtMostNPolicy atMostN;
    private final PolicyEntities providerPolicyEntities;
    private final PolicyEntities consumerPolicyEntities;

    /**
     * Create a new AtMostNPolicy.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     *                         The policy should be of type AtMostN.
     * @param consumerPolicyEntities consumer entities of current policy.
     * @param providerPolicyEntities provider entities of current policy.
     */
    public AtMostNPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                         @Nonnull final PolicyEntities consumerPolicyEntities,
                         @Nonnull final PolicyEntities providerPolicyEntities) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasAtMostN(), "Must be AtMostNPolicy");

        this.consumerPolicyEntities = Objects.requireNonNull(consumerPolicyEntities);
        this.providerPolicyEntities = Objects.requireNonNull(providerPolicyEntities);
        this.atMostN = Objects.requireNonNull(policyDefinition.getAtMostN());
        GroupProtoUtil.checkEntityType(providerPolicyEntities.getGroup());
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
        final Group providerGroup = providerPolicyEntities.getGroup();
        final Group consumerGroup = consumerPolicyEntities.getGroup();
        // Resolve the relevant groups
        final int providerEntityType = GroupProtoUtil.getEntityType(providerGroup);
        final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph),
                providerPolicyEntities.getAdditionalEntities());
        final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                consumerPolicyEntities.getAdditionalEntities());

        // Add the commodity to the appropriate entities.
        // Add a small epsilon to the capacity to ensure floating point roundoff error does not accidentally
        // reduce the atMostN capacity below the intended integer equivalent value.
        addCommoditySold(providers, consumers, topologyGraph, atMostN.getCapacity() + 0.1f);
        addCommoditySoldToComplementaryProviders(providers, providerEntityType, topologyGraph,
            commoditySold());
        addCommodityBought(consumers, topologyGraph, providerEntityType, commodityBought());
    }
}
