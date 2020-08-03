package com.vmturbo.topology.processor.group.policy.application;

import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import com.vmturbo.common.protobuf.group.PolicyDTO;

/**
 * A policy applies a constraint on a topology to restrict the possible options available to the market
 * during market analysis.
 *
 * Policies work by adding, removing, or modifying commodities on a given topology during the policy application
 * process.
 *
 * Where possible, policies that create or modify commodities that require a key should use
 * the policy ID as the key.
 *
 * Subclassed by various specific policy types (ie Merge, BindToGroup, etc.).
 */
public abstract class PlacementPolicy {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The policy definition describing the details of the policy to be applied.
     */
    protected final PolicyDTO.Policy policyDefinition;

    /**
     * Construct a new policy.
     *
     * Note that the Policy ID must be present for a policy to be successfully applied
     * because the ID will be used as the key.
     *
     * @param policyDefinition The policy definition describing the details of the policy to be applied.
     */
    protected PlacementPolicy(@Nonnull final PolicyDTO.Policy policyDefinition) {
        Preconditions.checkArgument(policyDefinition.hasId());
        this.policyDefinition = Objects.requireNonNull(policyDefinition);
    }

    /**
     * Get the policy definition describing the details of the policy to be applied.
     *
     * @return The policy definition describing the details of the policy to be applied.
     */
    public PolicyDTO.Policy getPolicyDefinition() {
        return policyDefinition;
    }

    /**
     * Returns whether the policy is currently enabled. Policies that are not enabled should not be applied.
     *
     * @return Whether the policy is currently enabled.
     */
    public boolean isEnabled() {
        return policyDefinition.getPolicyInfo().getEnabled();
    }
}