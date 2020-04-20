package com.vmturbo.mediation.gcp;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.gcp.client.GcpAccount;
import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * The wrapper probe around original GCP probe, which stands between mediation and GCP probe. It
 * takes the topology discovered by GCP probe and converts it into the topology in new cloud model.
 */
public class GcpConversionProbe extends GcpProbe {

    private final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull GcpAccount gcpAccount,
            @Nullable DiscoveryContextDTO discoveryContext)
            throws InterruptedException, NullPointerException {
        logger.debug("Started converting discovery response for GCP target {}",
                gcpAccount::getAddress);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiscoveryResponse newDiscoveryResponse = new CloudDiscoveryConverter(
                getRawDiscoveryResponse(gcpAccount, discoveryContext),
                new GcpConversionContext()).convert();

        logger.debug("Done converting discovery response for GCP target {} within {} ms",
                gcpAccount::getAddress, () -> stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return newDiscoveryResponse;
    }

    /**
     * Get the raw discovery response from original gcp probe.
     */
    DiscoveryResponse getRawDiscoveryResponse(@Nonnull GcpAccount gcpAccount,
            @Nullable DiscoveryContextDTO discoveryContext)
            throws InterruptedException, NullPointerException {
        return super.discoverTarget(gcpAccount, discoveryContext);
    }
}
