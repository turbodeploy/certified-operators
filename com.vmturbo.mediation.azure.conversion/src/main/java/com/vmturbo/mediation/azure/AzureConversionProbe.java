package com.vmturbo.mediation.azure;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;

/**
 * The wrapper probe around original Azure probe, which stands between mediation and Azure probe. It
 * takes the topology discovered by Azure probe and converts it into the topology in new cloud model.
 */
public class AzureConversionProbe extends AzureProbe {

    private final Logger logger = LogManager.getLogger();

    /**
     * List of new cloud entity types to create supply chain nodes for, which don't
     * exist in original Azure probe supply chain definition.
     */
    @VisibleForTesting
    protected static final Set<EntityType> NEW_ENTITY_TYPES = ImmutableSet.of(
            EntityType.CLOUD_SERVICE,
            EntityType.COMPUTE_TIER,
            EntityType.STORAGE_TIER,
            EntityType.DATABASE_TIER,
            EntityType.REGION,
            EntityType.VIRTUAL_VOLUME
    );

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

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        final Set<TemplateDTO> sc = Sets.newHashSet(super.getSupplyChainDefinition());

        // create supply chain nodes for new entity types
        for (EntityType entityType : NEW_ENTITY_TYPES) {
            sc.add(new SupplyChainNodeBuilder()
                    .entity(entityType)
                    .buildEntity());
        }

        return sc;
    }
}
