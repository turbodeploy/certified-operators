package com.vmturbo.topology.processor.rest;

import java.util.Collections;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.vmturbo.platform.sdk.common.MediationMessage.ProbeInfo.CreationMode;

/**
 * Enum for all the target operations possible.
 */
public enum TargetOperation {
    ADD,
    DISCOVER,
    VALIDATE,
    REMOVE,
    UPDATE;


    // A set of invalid creation mode for each target operation.
    private Set<CreationMode> invalidForCreationMode;

    /**
     * Return the set of invalid creation modes for this operation.
     *
     * @return the set of invalid creation modes associated with the operation
     */
    @VisibleForTesting
    Set<CreationMode> getInvalidCreationModes() {
        return invalidForCreationMode;
    }

    // Setting invalid creation mode for each target operation.
    static {
        ADD.invalidForCreationMode = Sets.newHashSet(CreationMode.OTHER);
        DISCOVER.invalidForCreationMode = Collections.emptySet();
        VALIDATE.invalidForCreationMode = Collections.emptySet();
        REMOVE.invalidForCreationMode = Sets.newHashSet(CreationMode.OTHER);
        UPDATE.invalidForCreationMode = Sets.newHashSet(CreationMode.OTHER);
    }

    /**
     * Checks according to the invalidForCreationMode map if the operation is valid.
     *
     * @param creationMode of the target
     * @return true/false if the operation is valid/invalid
     */
    public boolean isValidTargetOperation(CreationMode creationMode) {
        return !invalidForCreationMode.contains(creationMode);
    }
}
