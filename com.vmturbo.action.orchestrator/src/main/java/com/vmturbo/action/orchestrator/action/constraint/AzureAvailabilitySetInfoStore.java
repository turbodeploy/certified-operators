package com.vmturbo.action.orchestrator.action.constraint;

import java.util.HashSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.AzureAvailabilitySetInfo;

/**
 * This class is used to store recommend only policies created due to availablity set scaling
 * failures. This is a singleton class.
 */
public class AzureAvailabilitySetInfoStore implements ActionConstraintStore {
    private static AzureAvailabilitySetInfoStore azureAvailabilitySetInfoStore = new AzureAvailabilitySetInfoStore();
    private Set<Long> recommendOnlyPolicyIds = new HashSet<>();

    /**
     * Private to prevent instantiation.
     */
    private AzureAvailabilitySetInfoStore() {}

    /**
     * Get the only instance of the class.
     *
     * @return the only instance of the class
     */
    @VisibleForTesting
    public static AzureAvailabilitySetInfoStore getAzureAvailabilitySetInfo() {
        return azureAvailabilitySetInfoStore;
    }

    /**
     * {@inheritDoc}
     * synchronized is used here to ensure thread safety.
     */
    @Override
    public synchronized void updateActionConstraintInfo(ActionConstraintInfo actionConstraintInfo) {
        if (!actionConstraintInfo.hasAzureAvailabilitySetInfo()) {
            return;
        }

        AzureAvailabilitySetInfo info = actionConstraintInfo.getAzureAvailabilitySetInfo();
        recommendOnlyPolicyIds.clear();
        recommendOnlyPolicyIds.addAll(info.getPolicyIdsList());
    }

    public Set<Long> getRecommendOnlyPolicyIds() {
        return this.recommendOnlyPolicyIds;
    }
}
