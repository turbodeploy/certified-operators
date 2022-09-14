package com.vmturbo.mediation.azure.pricing.resolver;

import java.util.Optional;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;

/**
 * Interface for determining the meaning of a particular Azure meter, eg what type of thing
 * it represents, regions to which it applies, possibly a SKU, etc.
 */
public interface MeterResolver {
    /**
     * Given a AzureMeter, determine for what kind of thing it provides pricing.
     *
     * @param record The meter to resolve
     * @return an objest describing the meaning of the meter.
     */
    Optional<AzureMeterDescriptor> resolveMeter(AzureMeter record);
}
