package com.vmturbo.mediation.aws;

import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.aws.client.AwsAccount;
import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * The wrapper probe around original AWS probe, which stands between mediation and AWS probe. It
 * takes the topology discovered by AWS probe and converts it into the topology in new cloud model.
 */
public class AwsConversionProbe extends AwsProbe {

    private final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull AwsAccount awsAccount,
            @Nullable DiscoveryContextDTO discoveryContext)
            throws InterruptedException, NullPointerException {
        logger.debug("Started converting discovery response for AWS target {}",
                awsAccount::getAddress);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiscoveryResponse newDiscoveryResponse = new CloudDiscoveryConverter(
                getRawDiscoveryResponse(awsAccount, discoveryContext),
                new AwsConversionContext()).convert();

        logger.debug("Done converting discovery response for AWS target {} within {} ms",
                awsAccount::getAddress, () -> stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return newDiscoveryResponse;
    }

    /**
     * Get the raw discovery response from original aws probe.
     */
    DiscoveryResponse getRawDiscoveryResponse(@Nonnull AwsAccount awsAccount,
            @Nullable DiscoveryContextDTO discoveryContext)
            throws InterruptedException, NullPointerException {
        return super.discoverTarget(awsAccount, discoveryContext);
    }
}
