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
 * Entities in the consumer group can not run on any entity in the provider group.
 * Common use case: VM->Host or VM->Storage anti-affinity.
 */
public class BindToComplementaryGroupPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.Policy.BindToComplementaryGroupPolicy bindToComplementaryGroup;
    private final Group providerGroup;
    private final Group consumerGroup;

    /**
     * Create a new bind to complementary group policy.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     */
    public BindToComplementaryGroupPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                                          @Nonnull final Group consumerGroup,
                                          @Nonnull final Group providerGroup) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.hasBindToComplementaryGroup());
        this.bindToComplementaryGroup = policyDefinition.getBindToComplementaryGroup();
        this.consumerGroup = consumerGroup;
        this.providerGroup = providerGroup;
    }

    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
            throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying bindToComplementaryGroup policy.");

        // Resolve the relevant groups
        final Set<Long> providers = groupResolver.resolve(providerGroup,
                topologyGraph);
        final Set<Long> consumers = groupResolver.resolve(consumerGroup,
                topologyGraph);

        final int providerType = GroupProtoUtil.getEntityType(providerGroup);
        // Add the commodity to the appropriate entities
        addCommoditySoldToComplementaryProviders(providers, providerType, topologyGraph, commoditySold());
        addCommodityBought(consumers, topologyGraph, providerType, commodityBought());
    }
}