package com.vmturbo.mediation.azure.pricing.resolver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.azure.pricing.AzureMeter;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor;
import com.vmturbo.mediation.cost.parser.azure.AzureMeterDescriptors.AzureMeterDescriptor.MeterType;
import com.vmturbo.mediation.cost.parser.azure.VMSizes;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * An implementation of AzureMeterResolver using the static azure-meter-descriptors.json
 * file from the legacy Azure Cost Probe.
 */
public class MeterDescriptorsFileResolver implements MeterResolver {
    private static final Logger LOGGER = LogManager.getLogger();
    private HashSet<Pair<String, String>> explicitlyPricedInstanceTypes = new HashSet<>();

    /**
     * Construct the meter descriptors file based resolver.
     */
    public MeterDescriptorsFileResolver() {
        for (AzureMeterDescriptor descriptor : AzureMeterDescriptors.getDescriptors()) {
            if (descriptor.getType() == MeterType.VMProfile) {
                for (String sku: descriptor.getSkus()) {
                    explicitlyPricedInstanceTypes.add(new Pair(sku, descriptor.getSubType()));
                }
            }
        }
    }

    @Override
    public Optional<AzureMeterDescriptor> resolveMeter(@Nonnull AzureMeter meter) {
        return AzureMeterDescriptors.describeMeter(meter.getMeterId())
            .map(this::addSamePriceSkus);
    }

    @Nonnull
    private AzureMeterDescriptor addSamePriceSkus(@Nonnull AzureMeterDescriptor descriptor) {
        if (descriptor.getType() == MeterType.VMProfile) {
            // The descriptors file only includes the "main" sku for VM sizes.
            // use the VMSizes utility to add in the other skus that share pricing.
            // If there is any descriptor that explicitly prices a size, don't add
            // the pricedAs: version. This is a hack to work around the fact that
            // some sizes have explicit pricing for one OS but share pricing with another size
            // for another OS, and the azure-vm-sizes.yml pricedAs: doesn't capture this fact.

            Set<String> skus = descriptor.getSkus().stream()
                .map(VMSizes::fromName)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .flatMap(vmSize -> vmSize.getSizesSharingPricing().stream())
                .filter(sizeName -> !explicitlyPricedInstanceTypes
                    .contains(new Pair(sizeName, descriptor.getSubType())))
                .collect(Collectors.toSet());

            // Add in the original, explicitly listed skus
            skus.addAll(descriptor.getSkus());

            return new SimpleMeterDescriptor(MeterType.VMProfile, descriptor.getSubType(),
                new ArrayList<>(skus), descriptor.getRegions(), descriptor.getRedundancy(),
                null);
        } else {
            return descriptor;
        }
    }
}
