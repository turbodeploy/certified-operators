package com.vmturbo.mediation.vcenter;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.vmturbo.mediation.vmware.browsing.sdk.VimStorageBrowsingProbe;
import com.vmturbo.mediation.vmware.sdk.VimAccount;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata.ReturnType;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.sdk.common.supplychain.MergedEntityMetadataBuilder;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainConstants;
import com.vmturbo.platform.sdk.common.supplychain.SupplyChainNodeBuilder;

/**
 * The wrapper probe around original VimStorageBrowsing probe, which stands between mediation and
 * VimStorageBrowsing probe. It takes the topology discovered by VimStorageBrowsing probe and
 * adds VirtualVolumes between VMs and the Storages that host their files.  We also associate one
 * VirtualVolume with each Storage which will eventually hold the Wasted Files (after the
 * appropriate PostStitchingOperation has run).
 */
public class VimStorageBrowsingConversionProbe extends VimStorageBrowsingProbe {

    private final Logger logger = LogManager.getLogger();

    /**
     * List of entity types which already exist in the supply chain.
     */
    private static Set<EntityType> EXISTING_ENTITY_TYPES = ImmutableSet.of(
            EntityType.VIRTUAL_MACHINE,
            EntityType.STORAGE
    );

    /**
     * Map of existing entity type to the MergedEntityMetadataBuilder for that entity type.
     */
    private static Map<EntityType, MergedEntityMetadataBuilder> MERGED_ENTITY_METADATA_BUILDER_MAP =
            new ImmutableMap.Builder<EntityType, MergedEntityMetadataBuilder>()
                    .put(EntityType.VIRTUAL_MACHINE,
                            createVirtualMachineMergedEntityMetadataBuilder())
                    .put(EntityType.STORAGE, createStorageMergedEntityMetadataBuilder())
                    .build();

    /**
     * List of new entity types to create supply chain nodes for, which don't
     * exist in original Storage Browsing probe supply chain definition.
     */
    private static Set<EntityType> NEW_ENTITY_TYPES = ImmutableSet.of(
            EntityType.VIRTUAL_VOLUME
    );

    @Nonnull
    @Override
    public DiscoveryResponse discoverTarget(@Nonnull VimAccount vimAccount,
            @Nullable DiscoveryContextDTO discoveryContext)
            throws InterruptedException, NullPointerException {
        logger.debug("Started converting discovery response for VCenter Storage Browsing target {}",
                vimAccount::getAddress);

        final Stopwatch stopwatch = Stopwatch.createStarted();
        final DiscoveryResponse newDiscoveryResponse = new VimStorageBrowsingDiscoveryConverter(
                getRawDiscoveryResponse(vimAccount, discoveryContext)).convert();

        logger.debug("Done converting discovery response for VCenter Storage Browsing"
                + " target {} within {} ms",
                vimAccount::getAddress, () -> stopwatch.elapsed(TimeUnit.MILLISECONDS));

        return newDiscoveryResponse;
    }

    /**
     * Get the raw discovery response from original storage browsing probe.
     */
    DiscoveryResponse getRawDiscoveryResponse(@Nonnull VimAccount vimAccount,
            @Nullable DiscoveryContextDTO discoveryContext)
            throws InterruptedException, NullPointerException {
        return super.discoverTarget(vimAccount, discoveryContext);
    }

    @Nonnull
    @Override
    public Set<TemplateDTO> getSupplyChainDefinition() {
        final Set<TemplateDTO> sc = Sets.newHashSet();

        // add stitching metadata for existing nodes (VIRTUAL_MACHINE and STORAGE)
        for (TemplateDTO templateDTO : super.getSupplyChainDefinition()) {
            if (EXISTING_ENTITY_TYPES.contains(templateDTO.getTemplateClass())) {
                sc.add(templateDTO.toBuilder()
                        .setMergedEntityMetaData(MERGED_ENTITY_METADATA_BUILDER_MAP
                                .get(templateDTO.getTemplateClass())
                                .build())
                        .build());
            } else {
                sc.add(templateDTO);
            }
        }

        // create supply chain nodes for new entity types
        for (EntityType entityType : NEW_ENTITY_TYPES) {
            sc.add(new SupplyChainNodeBuilder()
                    .entity(entityType)
                    .buildEntity());
        }

        return sc;
    }

    /**
     * Create MergedEntityMetadataBuilder for Storage supply chain node used for stitching.
     * Storages from the StorageBrowsing probe have an entity property, STORAGE_ID, that
     * matches the ID of a storage from the VC probe.  We have no commodities to merge.  Only the
     * connectedFrom relationship to the VirtualVolume is needed from the StorageBrowsing probe.
     *
     * @return MergedEntityMetadataBuilder for use by stitching
     */
    private static MergedEntityMetadataBuilder createStorageMergedEntityMetadataBuilder() {
        return new MergedEntityMetadataBuilder()
                .internalMatchingProperty(SupplyChainConstants.STORAGE_ID)
                .internalMatchingType(ReturnType.STRING)
                .externalMatchingField("id", Collections.emptyList())
                .externalMatchingType(ReturnType.STRING);
    }

    /**
     * Create MergedEntityMetadataBuilder for VirtualMachine supply chain node used for stitching.
     * The StorageBrowsing VirtualMachine has an INTERNAL_NAME_TGT_ID property that matches the
     * same property on the VC VirtualMachine.  Like storage, we have no commodities to stitch, we
     * only care about the connectedTo relationship with the VirtualVolume.
     *
     * @return MergedEntityMetadataBuilder for use by stitching
     */
    private static MergedEntityMetadataBuilder createVirtualMachineMergedEntityMetadataBuilder() {
        return new MergedEntityMetadataBuilder()
                .internalMatchingProperty(SupplyChainConstants.INTERNAL_NAME_TGT_ID)
                .internalMatchingType(ReturnType.STRING)
                .externalMatchingProperty(SupplyChainConstants.INTERNAL_NAME_TGT_ID)
                .externalMatchingType(ReturnType.STRING);
    }
}
