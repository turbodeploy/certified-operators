package com.vmturbo.topology.processor.group.policy.application;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.GroupProtoUtil;
import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;

/**
 * Limits the entities in the consumer group to run only on the entities in the provider group
 * and limit the number of entities that can run on each of the providers.
 *
 * Common use case: VM->VM anti-affinity
 */
public class AtMostNBoundPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.PolicyInfo.AtMostNBoundPolicy atMostNBound;

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

        Objects.requireNonNull(policyDefinition);
        Preconditions.checkArgument(policyDefinition.getPolicyInfo().hasAtMostNbound(), "Must be AtMostNBoundPolicy");

        this.providerPolicyEntities = Objects.requireNonNull(providerPolicyEntities);
        this.consumerPolicyEntities = Objects.requireNonNull(consumerPolicyEntities);
        this.atMostNBound = policyDefinition.getPolicyInfo().getAtMostNbound();
        GroupProtoUtil.checkEntityTypeForPolicy(providerPolicyEntities.getGroup());
        Preconditions.checkArgument(this.atMostNBound.hasCapacity(),
            "Capacity required");
    }

    public PolicyEntities getProviderPolicyEntities() {
        return providerPolicyEntities;
    }

    public PolicyEntities getConsumerPolicyEntities() {
        return consumerPolicyEntities;
    }

    public PolicyInfo.AtMostNBoundPolicy getDetails() {
        return atMostNBound;
    }

}
