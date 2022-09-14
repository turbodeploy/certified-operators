package com.vmturbo.mediation.azure.pricing.resolver;

import java.util.Optional;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;

/**
 * An implementation of AzureMeterResolver using the static azure-meter-descriptors.json
 * file from the legacy Azure Cost Probe.
 */
public class MeterDescriptorsFileResolver implements MeterResolver {
    @Override
    public Optional<AzureMeterDescriptor> resolveMeter(@Nonnull AzureMeter meter) {
        return AzureMeterDescriptors.describeMeter(meter.getMeterId());
    }
}
