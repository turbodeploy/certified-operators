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
 * Entities in the consumer group can not run on any entity in the provider group.
 * Common use case: VM->Host or VM->Storage anti-affinity.
 */
public class BindToComplementaryGroupPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.PolicyInfo.BindToComplementaryGroupPolicy bindToComplementaryGroup;
    private final PolicyEntities providerPolicyEntities;
    private final PolicyEntities consumerPolicyEntities;

    /**
     * Create a new bind to complementary group policy.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     * @param consumerPolicyEntities consumer entities of current policy.
     * @param providerPolicyEntities provider entities of current policy.
     */
    public BindToComplementaryGroupPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                                          @Nonnull final PolicyEntities consumerPolicyEntities,
                                          @Nonnull final PolicyEntities providerPolicyEntities) {
        super(policyDefinition);
        Objects.requireNonNull(policyDefinition);
        Preconditions.checkArgument(policyDefinition.getPolicyInfo().hasBindToComplementaryGroup());
        this.bindToComplementaryGroup = policyDefinition.getPolicyInfo().getBindToComplementaryGroup();
        this.consumerPolicyEntities = Objects.requireNonNull(consumerPolicyEntities);
        this.providerPolicyEntities = Objects.requireNonNull(providerPolicyEntities);
    }

    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph topologyGraph)
            throws GroupResolutionException, PolicyApplicationException {
        logger.debug("Applying bindToComplementaryGroup policy.");
        final Group providerGroup = providerPolicyEntities.getGroup();
        final Group consumerGroup = consumerPolicyEntities.getGroup();
        // Resolve the relevant groups
        final Set<Long> providers = Sets.union(groupResolver.resolve(providerGroup,
                topologyGraph), providerPolicyEntities.getAdditionalEntities());
        final Set<Long> consumers = Sets.union(groupResolver.resolve(consumerGroup,
                topologyGraph), consumerPolicyEntities.getAdditionalEntities());

        final int providerType = GroupProtoUtil.getEntityType(providerGroup);
        // Add the commodity to the appropriate entities
        addCommoditySoldToComplementaryProviders(providers, providerType, topologyGraph, commoditySold());
        addCommodityBought(consumers, topologyGraph, providerType, commodityBought());
    }
}