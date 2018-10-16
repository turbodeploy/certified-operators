package com.vmturbo.mediation.azure;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;

/**
 * The wrapper probe around original Azure probe, which stands between mediation and Azure probe. It
 * takes the topology discovered by Azure probe and converts it into the topology in new cloud model.
 */
public class AzureConversionProbe extends AzureProbe {

    private final Logger logger = LogManager.getLogger();

    /**
     * List of new cloud entity types to create supply chain node for, which don't exist in
     * original Azure probe discovery response.
     */
    private static Set<EntityType> NEW_SHARED_ENTITY_TYPES = ImmutableSet.of(
            EntityType.CLOUD_SERVICE,
            EntityType.COMPUTE_TIER,
            EntityType.STORAGE_TIER,
            EntityType.DATABASE_TIER,
            EntityType.REGION
    );

    /**
     * List of new non-shared cloud entity types to create supply chain nodes for, which don't
     * exist in original Azure probe supply chain definition.
     */
    private static Set<EntityType> NEW_NON_SHARED_ENTITY_TYPES = ImmutableSet.of(
            EntityType.VIRTUAL_VOLUME
    );

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull AzureAccount azureAccount,
            @Nullable DiscoveryContextDTO discoveryContext) {
        logger.debug("Started converting discovery response for Azure target {}",
                azureAccount::getAddress);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiscoveryResponse newDiscoveryResponse = new CloudDiscoveryConverter(
                getRawDiscoveryResponse(azureAccount, discoveryContext),
                new AzureConversionContext()).convert();

        logger.debug("Done converting discovery response for Azure target {} within {} ms",
                azureAccount::getAddress, () -> stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return newDiscoveryResponse;
    }

    /**
     * Get the raw discovery response from original aws probe.
     */
    DiscoveryResponse getRawDiscoveryResponse(@Nonnull AzureAccount azureAccount,
            @Nullable DiscoveryContextDTO discoveryContext) {
        return super.discoverTarget(azureAccount, discoveryContext);
    }

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        final Set<TemplateDTO> sc = Sets.newHashSet();

        // add stitching metadata for existing nodes (BUSINESS_ACCOUNT)
        for (TemplateDTO templateDTO : super.getSupplyChainDefinition()) {
            if (templateDTO.getTemplateClass() == EntityType.BUSINESS_ACCOUNT) {
                sc.add(templateDTO.toBuilder()
                        .setMergedEntityMetaData(createMergedEntityMetadata())
                        .build());
            } else {
                sc.add(templateDTO);
            }
        }

        // create supply chain nodes for new shared entities and add stitching metadata
        for (EntityType entityType : NEW_SHARED_ENTITY_TYPES) {
            sc.add(new SupplyChainNodeBuilder()
                    .entity(entityType)
                    .mergedBy(createMergedEntityMetadata())
                    .buildEntity());
        }

        // create supply chain nodes for new non-shared entities
        for (EntityType entityType : NEW_NON_SHARED_ENTITY_TYPES) {
            sc.add(new SupplyChainNodeBuilder()
                    .entity(entityType)
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
                .internalMatchingField("id", Lists.newArrayList())
                .internalMatchingType(ReturnType.STRING)
                .externalMatchingField("id", Lists.newArrayList())
                .externalMatchingType(ReturnType.STRING)
                .build();
    }
}
