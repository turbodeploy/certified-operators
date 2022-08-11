package com.vmturbo.mediation.azure.pricing;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;

/**
 *  Azure Pricing Probe. Discovers the following items:
 * <ul>
 *   <li>Billing data</li>
 *   <li>Reserved Instance data</li>
 * </ul>
 */
public class AzurePricingProbe implements IDiscoveryProbe<AzurePricingAccount> {
    private final Logger logger = LogManager.getLogger();

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull final AzurePricingAccount accountValues) throws InterruptedException {
        logger.info("Starting discovery");
        // TODO Azure Pricing discovery
        return DiscoveryResponse.newBuilder().build();
    }

    @Nonnull
    @Override
    public Class<AzurePricingAccount> getAccountDefinitionClass() {
        return AzurePricingAccount.class;
    }

    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull final AzurePricingAccount accountValues) throws InterruptedException {
        return ValidationResponse.newBuilder().build();
    }
}
