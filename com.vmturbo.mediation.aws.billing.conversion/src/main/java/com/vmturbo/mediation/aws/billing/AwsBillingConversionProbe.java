package com.vmturbo.mediation.aws.billing;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;

/**
 * Wrapper probe on top of {@link AwsBillingProbe}.
 * It is introduced for stitching some data discovered from billing with those discovered by the
 * main AWS probe.
 * In particular, it makes VMs "discovered" by the probe by copying them from BillingData to the
 * top level EntityDTO object in DiscoveryResponse.
 */
public class AwsBillingConversionProbe extends AwsBillingProbe {

    /**
     * Discovers the target, creating an EntityDTO representation of every object discovered by the
     * probe.
     *
     * @param account Account data
     * @param discoveryContext Discovery context with the properties sent by the probe on the
     *     previous discovery.
     * @return discovery response, consisting of both entities and errors, if any
     * @throws InterruptedException if discovery process has been interrupted. Probe
     *      implementation is responsible to leave the target in a consistent state if the thread
     *      is interrupted
     */
    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull final AwsAccount account,
                @Nullable final DiscoveryContextDTO discoveryContext) throws InterruptedException {
        return new AwsBillingDiscoveryConverter(
            getRawDiscoveryResponse(account, discoveryContext)).convert();
    }

    /**
     * Get the raw discovery response from original aws billing probe.
     *
     * @param awsAccount    Account data
     * @param discoveryContext  Discovery context with the properties sent by the probe on the
     *      previous discovery.
     * @return Discovery response returned by classic Aws Billing probe
     * @throws InterruptedException
     */
     DiscoveryResponse getRawDiscoveryResponse(@Nonnull AwsAccount awsAccount,
              @Nullable DiscoveryContextDTO discoveryContext)
        throws InterruptedException {
        return super.discoverTarget(awsAccount, discoveryContext);
     }
}
