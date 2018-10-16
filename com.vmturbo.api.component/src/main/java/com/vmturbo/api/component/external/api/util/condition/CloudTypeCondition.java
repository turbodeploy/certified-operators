package com.vmturbo.api.component.external.api.util.condition;

import com.vmturbo.api.enums.CloudType;

/**
 * Interfact to model Cloud type condtions. E.g. AWS only, AWS || Azure.
 */
public interface CloudTypeCondition {
    /**
     * Is the Cloud type allowed by condition.
     * @param cloudType the ClouldType to check
     * @return yes, if the Cloud type is allowed
     */
    boolean isAllowed(CloudType cloudType);

    /**
     * Support Or condition
     * @param other another condition
     * @return new condition support Or condition
     */
    CloudTypeCondition or(CloudTypeCondition other);
}
