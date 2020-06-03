package com.vmturbo.mediation.vcenter;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.conversion.onprem.AddVirtualVolumeDiscoveryConverter;
import com.vmturbo.mediation.vmware.browsing.sdk.VimStorageBrowsingProbe;
import com.vmturbo.mediation.vmware.sdk.VimAccount;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryContextDTO;
import com.vmturbo.platform.common.dto.Discovery.DiscoveryResponse;
import com.vmturbo.platform.common.dto.SupplyChain.MergedEntityMetadata;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO;
import com.vmturbo.platform.common.dto.SupplyChain.TemplateDTO.TemplateType;
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
     * Map of existing entity type to the MergedEntityMetadata for that entity type.
     */
    private static Map<EntityType, MergedEntityMetadata> MERGED_ENTITY_METADATA_MAP =
            new ImmutableMap.Builder<EntityType, MergedEntityMetadata>()
                .put(EntityType.VIRTUAL_MACHINE, createVirtualMachineMergedEntityMetadata())
                .put(EntityType.STORAGE, createStorageMergedEntityMetadata())
                .put(EntityType.VIRTUAL_VOLUME, createVirtualVolumeMergedEntityMetadata())
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
        final DiscoveryResponse newDiscoveryResponse = new AddVirtualVolumeDiscoveryConverter(
            getRawDiscoveryResponse(vimAccount, discoveryContext), true).convert();

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
                        .setMergedEntityMetaData(MERGED_ENTITY_METADATA_MAP.get(templateDTO.getTemplateClass()))
                        .build());
            } else {
                sc.add(templateDTO);
            }
        }

        // create supply chain nodes for new entity types
        for (EntityType entityType : NEW_ENTITY_TYPES) {
            // Since the volume is stitched with the vc volume, the template priority must be
            // reduced from 0 (default value) to -1, so that vc template will take higher priority
            // and it will avoid warnings in TP
            SupplyChainNodeBuilder nodeBuilder = new SupplyChainNodeBuilder().entity(entityType,
                TemplateType.BASE, -1);
            if (MERGED_ENTITY_METADATA_MAP.containsKey(entityType)) {
                nodeBuilder.mergedBy(MERGED_ENTITY_METADATA_MAP.get(entityType));
            }
            sc.add(nodeBuilder.buildEntity());
        }

        return sc;
    }

    /**
     * Create MergedEntityMetadata for Storage supply chain node used for stitching.
     * Storages from the StorageBrowsing probe have an entity property, STORAGE_ID, that
     * matches the ID of a storage from the VC probe.  We have no commodities to merge.  Only the
     * connectedFrom relationship to the VirtualVolume is needed from the StorageBrowsing probe.
     *
     * @return MergedEntityMetadata for use by stitching
     */
    private static MergedEntityMetadata createStorageMergedEntityMetadata() {
        return new MergedEntityMetadataBuilder()
                .internalMatchingProperty(SupplyChainConstants.STORAGE_ID)
                .externalMatchingField(SupplyChainConstants.ID, Collections.emptyList())
                .build();
    }

    /**
     * Create MergedEntityMetadata for VirtualMachine supply chain node used for stitching.
     * The StorageBrowsing VirtualMachine has an INTERNAL_NAME_TGT_ID property that matches the
     * same property on the VC VirtualMachine.  Like storage, we have no commodities to stitch, we
     * only care about the connectedTo relationship with the VirtualVolume.
     *
     * @return MergedEntityMetadata for use by stitching
     */
    private static MergedEntityMetadata createVirtualMachineMergedEntityMetadata() {
        return new MergedEntityMetadataBuilder()
                .internalMatchingProperty(SupplyChainConstants.INTERNAL_NAME_TGT_ID)
                .externalMatchingProperty(SupplyChainConstants.INTERNAL_NAME_TGT_ID)
                .build();
    }

    /**
     * Create MergedEntityMetadata for VirtualVolume supply chain node used for stitching. The
     * matching is based on the id of the VirtualVolume. And the field "virtual_volume_data" of
     * this DTO will be patched to matching virtual volume of main vc probe.
     *
     * @return MergedEntityMetadata for use by stitching for VirtualVolume
     */
    private static MergedEntityMetadata createVirtualVolumeMergedEntityMetadata() {
        return new MergedEntityMetadataBuilder()
                .internalMatchingField(SupplyChainConstants.ID, Collections.emptyList())
                .externalMatchingField(SupplyChainConstants.ID, Collections.emptyList())
                .mergedField("virtual_volume_data", Collections.emptyList())
                .build();
    }
}
