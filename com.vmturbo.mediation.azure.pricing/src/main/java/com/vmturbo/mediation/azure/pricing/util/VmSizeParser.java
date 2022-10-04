package com.vmturbo.mediation.azure.pricing.util;

import java.util.Set;

import com.vmturbo.mediation.cost.parser.azure.VMSizes.VMSize;

/**
 * Interface for VMSize parser method.
 */
public interface VmSizeParser {
    /**
     * This should be implemented by the subclass to return the VMSize list.
     *
     * @return list of VMSize
     */
    Set<VMSize> getAllSizes();
}
