package com.vmturbo.action.orchestrator.action.constraint;

import java.util.HashSet;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;

import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo;
import com.vmturbo.common.protobuf.action.ActionConstraintDTO.ActionConstraintInfo.AzureScaleSetInfo;

/**
 * This class is used to store all core quota info. This is a singleton class.
 */
public class AzureScaleSetInfoStore implements ActionConstraintStore {
    private static AzureScaleSetInfoStore azureScaleSetInfoStore = new AzureScaleSetInfoStore();
    private Set<String> azureScaleSetNames = new HashSet<>();

    /**
     * Private to prevent instantiation.
     */
    private AzureScaleSetInfoStore() {}

    /**
     * Get the only instance of the class.
     *
     * @return the only instance of the class
     */
    @VisibleForTesting
    public static AzureScaleSetInfoStore getAzureScaleSetInfo() {
        return azureScaleSetInfoStore;
    }

    /**
     * {@inheritDoc}
     * synchronized is used here to ensure thread safety.
     */
    @Override
    public synchronized void updateActionConstraintInfo(ActionConstraintInfo actionConstraintInfo) {
        if (!actionConstraintInfo.hasAzureScaleSetInfo()) {
            return;
        }

        AzureScaleSetInfo info = actionConstraintInfo.getAzureScaleSetInfo();
        azureScaleSetNames.clear();
        azureScaleSetNames.addAll(info.getNamesList());
    }

    public Set<String> getAzureScaleSetNames() {
        return this.azureScaleSetNames;
    }
}
