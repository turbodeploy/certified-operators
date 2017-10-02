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
 * A policy that limits the entities in the consumer group to run only on the entities in the provider group.
 * Common use case: VM->Host or VM->Storage affinity.
 */
public class BindToGroupPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.BindToGroupPolicy bindToGroup;

    /**
     * Create a new BindToGroupPolicy.
     * The policy should be of type BindToGroup.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     */
    public BindToGroupPolicy(@Nonnull final PolicyDTO.Policy policyDefinition) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasBindToGroup(), "Must be BindToGroupPolicy");

        this.bindToGroup = policyDefinition.getBindToGroup();
        Preconditions.checkArgument(hasEntityType(this.bindToGroup.getProviderGroup()),
                        "ProviderGroup entity type required");
    }

    /**
     * Constrain entities in the consumer group to be forced to reside on entities in the providers group
     * by creating a segmentation commodity bought by the consumers and sold ONLY by the providers.
     *
     * {@inheritDoc}
     */
    @Override
    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
        throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying bindToGroup policy.");

        PolicyGrouping providerGroup = bindToGroup.getProviderGroup();
        // Resolve the relevant groups
        final Set<Long> providers = groupResolver.resolve(providerGroup, topologyGraph);
        final Set<Long> consumers = groupResolver.resolve(bindToGroup.getConsumerGroup(), topologyGraph);

        // Add the commodity to the appropriate entities.
        addCommoditySold(providers, topologyGraph, commoditySold());
        addCommodityBought(consumers, topologyGraph, entityType(providerGroup), commodityBought());
    }
}
