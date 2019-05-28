package com.vmturbo.topology.processor.group.policy.application;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;

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

    public void applyInternal(@Nonnull final GroupResolver groupResolver, @Nonnull final TopologyGraph<TopologyEntity> topologyGraph)
            throws GroupResolutionException, PolicyApplicationException {
    }

    @Nonnull
    public PolicyInfo.BindToComplementaryGroupPolicy getDetails() {
        return bindToComplementaryGroup;
    }

    @Nonnull
    public PolicyEntities getProviderPolicyEntities() {
        return providerPolicyEntities;
    }

    @Nonnull
    public PolicyEntities getConsumerPolicyEntities() {
        return consumerPolicyEntities;
    }
}