package com.vmturbo.topology.processor.group.policy.application;

import java.util.List;
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
 * A policy that removes cluster boundaries. Merge policies merge multiple clusters
 * into a single logical group for the purpose of workload placement.
 * <p>
 */
public class MergePolicy extends PlacementPolicy {

    private static final Logger logger = LogManager.getLogger();

    private final PolicyInfo.MergePolicy mergePolicy;

    // list of PolicyEntities which is wrapper for Group and additional entities
    private final List<PolicyEntities> mergePolicyEntitiesList;

    /**
     * Create a new MergePolicy.
     * The policy should be MergePolicy
     *
     * @param policyDefinition        The policy definition describing the details of the policy to be applied.
     * @param mergePolicyEntitiesList list of entities in merge policy.
     */
    public MergePolicy(@Nonnull final PolicyDTO.Policy policyDefinition,
                       @Nonnull final List<PolicyEntities> mergePolicyEntitiesList) {
        super(policyDefinition);
        Preconditions.checkArgument(policyDefinition.getPolicyInfo().hasMerge(), "Must be MergePolicy");
        this.mergePolicyEntitiesList = Objects.requireNonNull(mergePolicyEntitiesList);
        this.mergePolicy = policyDefinition.getPolicyInfo().getMerge();
        mergePolicyEntitiesList
                .forEach(mergePolicyEntities -> GroupProtoUtil.checkEntityTypeForPolicy(mergePolicyEntities.getGroup()));
    }

    @Nonnull
    public List<PolicyEntities> getMergePolicyEntitiesList() {
        return mergePolicyEntitiesList;
    }

    @Nonnull
    public PolicyInfo.MergePolicy getDetails() {
        return mergePolicy;
    }
}