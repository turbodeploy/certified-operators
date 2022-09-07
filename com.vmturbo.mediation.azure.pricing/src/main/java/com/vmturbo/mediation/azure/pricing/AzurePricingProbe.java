package com.vmturbo.mediation.azure.pricing;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.azure.pricing.controller.MCAPricingDiscoveryController;
import com.vmturbo.mediation.azure.pricing.controller.PricingDiscoveryController;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.Discovery.ValidationResponse;
import com.vmturbo.platform.sdk.common.util.SDKUtil;
import com.vmturbo.platform.sdk.probe.IDiscoveryProbe;
import com.vmturbo.platform.sdk.probe.IProbeContext;
import com.vmturbo.platform.sdk.probe.ProbeConfiguration;
import com.vmturbo.platform.sdk.probe.properties.IPropertyProvider;

/**
 *  Azure Pricing Probe. Discovers the following items:
 * <ul>
 *   <li>Billing data</li>
 *   <li>Reserved Instance data</li>
 * </ul>
 */
public class AzurePricingProbe implements IDiscoveryProbe<AzurePricingAccount> {
    private final Logger logger = LogManager.getLogger();
    private IPropertyProvider propertyProvider;

    private static MCAPricingDiscoveryController mcaController;
    private static List<PricingDiscoveryController<AzurePricingAccount>> controllers;

    @Override
    public void initialize(@Nonnull IProbeContext probeContext,
            @Nullable ProbeConfiguration configurationAttributes) {
        propertyProvider = probeContext.getPropertyProvider();
    }

    /**
     * Get the discovery controllers, creating them if necessary.
     *
     * @return a list of discovery controllers for different kinds of pricing discovery
     */
    @Nonnull
    public synchronized List<PricingDiscoveryController<AzurePricingAccount>>
    getControllers() {
        if (controllers == null) {
            mcaController = new MCAPricingDiscoveryController(propertyProvider);
            controllers = ImmutableList.of(mcaController);
        }

        return controllers;
    }

    /**
     * Discover a target with the given account values.
     *
     * @param accountValues specifies the account to discover
     * @return a discovery response DTO
     */
    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull final AzurePricingAccount accountValues) {
        logger.info("Starting discovery");

        for (PricingDiscoveryController<AzurePricingAccount> controller : getControllers()) {
            if (controller.appliesToAccount(accountValues)) {
                return controller.discoverTarget(accountValues, propertyProvider);
            }
        }

        return SDKUtil.createDiscoveryError("Unable to find discovery controller for account "
                + accountValues.toString());
    }

    @Nonnull
    @Override
    public Class<AzurePricingAccount> getAccountDefinitionClass() {
        return AzurePricingAccount.class;
    }

    @Nonnull
    @Override
    public ValidationResponse validateTarget(@Nonnull final AzurePricingAccount accountValues) throws InterruptedException {
        for (PricingDiscoveryController<AzurePricingAccount> controller : getControllers()) {
            if (controller.appliesToAccount(accountValues)) {
                return controller.validateTarget(accountValues);
            }
        }

        return SDKUtil.createValidationError("Unable to find discovery controller for account "
            + accountValues.toString());
    }
}
