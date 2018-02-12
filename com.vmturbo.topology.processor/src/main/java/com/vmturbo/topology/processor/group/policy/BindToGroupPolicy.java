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
 * A policy that limits the entities in the consumer group to run only on the entities in the provider group.
 * Common use case: VM->Host or VM->Storage affinity.
 */
public class BindToGroupPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.BindToGroupPolicy bindToGroup;
    private final PolicyEntities consumerPolicyEntities;
    private final PolicyEntities providerPolicyEntities;

    /**
     * Create a new BindToGroupPolicy.
     * The policy should be of type BindToGroup.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     * @param consumerPolicyEntities consumer entities of current policy.
     * @param providerPolicyEntities provider entities of current policy.
     */
    public BindToGroupPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                             @Nonnull final PolicyEntities consumerPolicyEntities,
                             @Nonnull final PolicyEntities providerPolicyEntities) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasBindToGroup(), "Must be BindToGroupPolicy");

        this.bindToGroup = Objects.requireNonNull(policyDefinition.getBindToGroup());
        this.consumerPolicyEntities = Objects.requireNonNull(consumerPolicyEntities);
        this.providerPolicyEntities = Objects.requireNonNull(providerPolicyEntities);
        GroupProtoUtil.checkEntityType(providerPolicyEntities.getGroup());
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
        final Group providerGroup = providerPolicyEntities.getGroup();
        final Group consumerGroup = consumerPolicyEntities.getGroup();
        // Resolve the relevant groups
        final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup, topologyGraph),
                providerPolicyEntities.getAdditionalEntities());
        final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup, topologyGraph),
                consumerPolicyEntities.getAdditionalEntities());

        // Add the commodity to the appropriate entities.
        addCommoditySold(providers, topologyGraph, commoditySold());
        addCommodityBought(consumers, topologyGraph,
                GroupProtoUtil.getEntityType(providerGroup), commodityBought());
    }
}
