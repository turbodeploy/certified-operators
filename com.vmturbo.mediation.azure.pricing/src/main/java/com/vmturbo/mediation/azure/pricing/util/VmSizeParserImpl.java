package com.vmturbo.mediation.azure.pricing.util;

import java.util.Set;

import com.vmturbo.mediation.cost.parser.azure.VMSizes;
import com.vmturbo.mediation.cost.parser.azure.VMSizes.VMSize;

/**
 * Provides implementation for VmSizeParser.
 */
public class VmSizeParserImpl implements VmSizeParser {
    @Override
    public Set<VMSize> getAllSizes() {
        return VMSizes.getAllSizes();
    }
}
