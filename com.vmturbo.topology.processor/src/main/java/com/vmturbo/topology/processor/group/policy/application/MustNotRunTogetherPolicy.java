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
 * Requires that all entities in the consumer group must run in separate providers of the
 * specified type.
 * Common use case: VM->VM anti-affinity, keep VMs in separate hosts (we can also use it to keep VMs
 * in separate storages).
 *
 * Please note that this is different from the AtMostN and AtMostNBound policies. Those last two
 * require you to also limit on which providers the entities should run; and also limit how many
 * of them to run, per provider.
 *
 * Note also that the consumer group might span over multiple clusters (and be used in combination
 * with a cluster merge policy)
 */
public class MustNotRunTogetherPolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy mustNotRunTogetherPolicy;

    private final PolicyEntities policyEntities;

    /**
     * Create a new {@link MustNotRunTogetherPolicy}, the policy should be of type
     * {@link PolicyDTO.PolicyInfo.MustNotRunTogetherPolicy}.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     * @param policyEntities consumer entities of current policy.
     */
    public MustNotRunTogetherPolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                                 @Nonnull final PolicyEntities policyEntities) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.getPolicyInfo().hasMustNotRunTogether());
        this.mustNotRunTogetherPolicy = Objects.requireNonNull(policyDefinition.getPolicyInfo().getMustNotRunTogether());
        this.policyEntities = Objects.requireNonNull(policyEntities);
    }

    @Nonnull
    public PolicyInfo.MustNotRunTogetherPolicy getDetails() {
        return mustNotRunTogetherPolicy;
    }

    @Nonnull
    public PolicyEntities getPolicyEntities() {
        return policyEntities;
    }

}
