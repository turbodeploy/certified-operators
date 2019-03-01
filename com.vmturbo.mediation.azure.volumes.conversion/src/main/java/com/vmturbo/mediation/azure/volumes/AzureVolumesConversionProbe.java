package com.vmturbo.mediation.azure.volumes;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.mediation.azure.AzureAccount;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;

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


    /**
     * List of new non-shared cloud entity types to create supply chain nodes for, which don't
     * exist in original Azure probe supply chain definition.
     */
    private static Set<EntityType> NEW_NON_SHARED_ENTITY_TYPES = ImmutableSet.of(
        EntityType.VIRTUAL_VOLUME,
        EntityType.REGION,
        EntityType.STORAGE_TIER
    );

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull AzureAccount azureAccount)
        throws InterruptedException {
        logger.debug("Started converting discovery response for Azure volumes target {}",
                azureAccount::getAddress);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiscoveryResponse newDiscoveryResponse = new AzureVolumesCloudDiscoveryConverter(
                getRawDiscoveryResponse(azureAccount),
                new AzureVolumesConversionContext()).convert();

        logger.debug("Done converting discovery response for Azure target {} within {} ms",
                azureAccount::getAddress, () -> stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return newDiscoveryResponse;
    }

    /**
     * Get the raw discovery response from original aws probe.
     */
    DiscoveryResponse getRawDiscoveryResponse(@Nonnull AzureAccount azureAccount)
        throws InterruptedException {
        return super.discoverTarget(azureAccount);
    }

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        final Set<TemplateDTO> sc = Sets.newHashSet();

        // create supply chain nodes for new non-shared entities
        for (EntityType entityType : NEW_NON_SHARED_ENTITY_TYPES) {
            sc.add(new SupplyChainNodeBuilder()
                .entity(entityType)
                .mergedBy(createMergedEntityMetadata())
                .buildEntity());
        }

        return sc;
    }

    /**
     * Create MergedEntityMetadata for supply chain node used for stitching. Cloud entities are
     * shared across different targets, and only one set should be kept in topology. This metadata
     * matches based on id of the EntityDTO.
     *
     * @return MergedEntityMetadata for use by stitching
     */
    private static MergedEntityMetadata createMergedEntityMetadata() {
        return new MergedEntityMetadataBuilder()
                .internalMatchingField(SupplyChainConstants.ID, Lists.newArrayList())
                .internalMatchingType(ReturnType.STRING)
                .externalMatchingField(SupplyChainConstants.ID, Lists.newArrayList())
                .externalMatchingType(ReturnType.STRING)
                .build();
    }
}
