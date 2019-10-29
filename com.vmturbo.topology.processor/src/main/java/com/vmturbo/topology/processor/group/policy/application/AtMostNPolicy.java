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
 * Defines a limit on the number of entities from the consumer group that can run on each
 * provider in the provider group without limiting the consumers to run only on the entities
 * in the provider group.
 * Common use case: VM->VM anti-affinity
 */
public class AtMostNPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.PolicyInfo.AtMostNPolicy atMostN;
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
        Objects.requireNonNull(policyDefinition);
        Preconditions.checkArgument(policyDefinition.getPolicyInfo().hasAtMostN(), "Must be AtMostNPolicy");

        this.consumerPolicyEntities = Objects.requireNonNull(consumerPolicyEntities);
        this.providerPolicyEntities = Objects.requireNonNull(providerPolicyEntities);
        this.atMostN = policyDefinition.getPolicyInfo().getAtMostN();
        GroupProtoUtil.checkEntityTypeForPolicy(providerPolicyEntities.getGroup());
        Preconditions.checkArgument(this.atMostN.hasCapacity(),
            "Capacity required");
    }

    public PolicyEntities getProviderPolicyEntities() {
        return providerPolicyEntities;
    }

    public PolicyEntities getConsumerPolicyEntities() {
        return consumerPolicyEntities;
    }

    public PolicyInfo.AtMostNPolicy getDetails() {
        return atMostN;
    }
}
