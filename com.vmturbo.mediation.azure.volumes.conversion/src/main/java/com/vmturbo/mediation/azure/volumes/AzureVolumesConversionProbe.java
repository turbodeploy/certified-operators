package com.vmturbo.mediation.azure.volumes;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.azure.AzureAccount;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * The wrapper probe around original Azure Volumes probe, which stands between mediation and the
 * probe. It takes the topology discovered by Azure Volumes probe and converts it into the topology
 * in the new cloud model.  Specifically, the information in the Storage DTO is extracted and up
 * to 3 entities are created - a Volume for each file listed in the storage_data, a Region extracted
 * from the "lunuuid" property which is really the id of the Storage from the Azure probe, and a
 * Storage Tier from the "storageType" property.
 */
public class AzureVolumesConversionProbe extends AzureVolumesProbe {

    private final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull AzureAccount azureAccount)
        throws InterruptedException {
        logger.debug("Started converting discovery response for Azure volumes target {}",
                azureAccount::getName);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiscoveryResponse newDiscoveryResponse = new AzureVolumesCloudDiscoveryConverter(
                getRawDiscoveryResponse(azureAccount),
                new AzureVolumesConversionContext()).convert();

        logger.debug("Done converting discovery response for Azure target {} within {} ms",
                azureAccount::getName, () -> stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return newDiscoveryResponse;
    }

    /**
     * Get the raw discovery response from original aws probe.
     */
    DiscoveryResponse getRawDiscoveryResponse(@Nonnull AzureAccount azureAccount)
        throws InterruptedException {
        return super.discoverTarget(azureAccount);
    }
}
