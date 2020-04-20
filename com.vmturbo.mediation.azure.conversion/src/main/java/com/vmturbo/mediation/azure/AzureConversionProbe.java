package com.vmturbo.mediation.azure;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * The wrapper probe around original Azure probe, which stands between mediation and Azure probe. It
 * takes the topology discovered by Azure probe and converts it into the topology in new cloud model.
 */
public class AzureConversionProbe extends AzureProbe {

    private final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull AzureAccount azureAccount,
            @Nullable DiscoveryContextDTO discoveryContext) throws InterruptedException {
        logger.debug("Started converting discovery response for Azure target {}",
                azureAccount::getName);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiscoveryResponse newDiscoveryResponse = new CloudDiscoveryConverter(
                getRawDiscoveryResponse(azureAccount, discoveryContext),
                new AzureConversionContext()).convert();

        logger.debug("Done converting discovery response for Azure target {} within {} ms",
                azureAccount::getName, () -> stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return newDiscoveryResponse;
    }

    /**
     * Get the raw discovery response from original aws probe.
     */
    DiscoveryResponse getRawDiscoveryResponse(@Nonnull AzureAccount azureAccount,
            @Nullable DiscoveryContextDTO discoveryContext) throws InterruptedException {
        return super.discoverTarget(azureAccount, discoveryContext);
    }
}
