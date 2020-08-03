package com.vmturbo.topology.processor.group.policy.application;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.group.PolicyDTO;
import com.vmturbo.common.protobuf.group.PolicyDTO.PolicyInfo;
import com.vmturbo.topology.processor.group.policy.application.PolicyFactory.PolicyEntities;

/**
 * Requires that all entities in the consumer group must run together on a single provider of the
 * specified type.
 * Common use case: VM->VM affinity, keep all the VMs in the group on the same host.
 */
public class MustRunTogetherPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.PolicyInfo.MustRunTogetherPolicy mustRunTogetherPolicy;

    private final PolicyEntities policyEntities;

    /**
     * Create a new {@link MustRunTogetherPolicy}, the policy should be of type
     * {@link PolicyDTO.PolicyInfo.MustRunTogetherPolicy}.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     * @param policyEntities consumer entities of current policy.
     */
    public MustRunTogetherPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                                 @Nonnull final PolicyEntities policyEntities) {
        super(policyDefinition);
        Objects.requireNonNull(policyDefinition);
        Preconditions.checkArgument(policyDefinition.getPolicyInfo().hasMustRunTogether());
        this.mustRunTogetherPolicy = policyDefinition.getPolicyInfo().getMustRunTogether();
        this.policyEntities = Objects.requireNonNull(policyEntities);
    }

    @Nonnull
    public PolicyInfo.MustRunTogetherPolicy getDetails() {
        return mustRunTogetherPolicy;
    }

    @Nonnull
    public PolicyEntities getPolicyEntities() {
        return policyEntities;
    }
}
