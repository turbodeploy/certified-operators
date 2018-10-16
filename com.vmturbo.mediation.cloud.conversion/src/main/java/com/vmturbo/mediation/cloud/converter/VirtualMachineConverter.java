package com.vmturbo.mediation.cloud.converter;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableSet;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.mediation.hybrid.cloud.utils.OSType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.CommodityBought.Builder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * CloudDiscoveryConverter for Cloud VMs. The commodities bought will be shifted from Availability
 * Zones and Storages to Compute Tiers and Storage Tiers respectively. The VMs will also be
 * connected to the appropriate AZ or Region.
 */
public class VirtualMachineConverter implements IEntityConverter {

    // set of access commodity types that should be removed from bought commodities list
    // todo: we can't remove VMPM_ACCESS for now, since AWS probe may use it for storing
    // rootDeviceType (see rb23528), we should remove it once probe use some property to store it
    private static Set<CommodityType> ACCESS_COMMODITY_TYPES_TO_REMOVE = ImmutableSet.of(
            CommodityType.DSPM_ACCESS,
            CommodityType.DATACENTER,
            CommodityType.DATASTORE
    );

    // set of commodities that the active field should be cleared (set back to true by default)
    public static Set<CommodityType> COMMODITIES_TO_CLEAR_ACTIVE = ImmutableSet.of(
            CommodityType.MEM_PROVISIONED,
            CommodityType.CPU_PROVISIONED
    );

    private SDKProbeType probeType;

    public VirtualMachineConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        // if the VM doesn't have profileId, then it's a fake VM created for hosting
        // DatabaseServer in AWS. flag this entity for removal
        if (!entity.hasProfileId()) {
            return false;
        }

        List<CommodityBought> newCommodityBoughtList = entity.getCommoditiesBoughtList().stream()
                .map(commodityBought -> {
                    Builder cbBuilder = commodityBought.toBuilder();

                    // filter out access commodities
                    cbBuilder.clearBought();
                    cbBuilder.addAllBought(commodityBought.getBoughtList().stream()
                            .filter(commodityDTO -> !ACCESS_COMMODITY_TYPES_TO_REMOVE.contains(
                                    commodityDTO.getCommodityType()))
                            .map(commodityDTO ->
                                    // clear active field (active is true by default) for some
                                    // commodities since they are set to false in probe, we can't
                                    // change probe since it will affect classic
                                    COMMODITIES_TO_CLEAR_ACTIVE.contains(commodityDTO.getCommodityType()) ?
                                            commodityDTO.toBuilder().clearActive().build() : commodityDTO)
                            .collect(Collectors.toList()));

                    // change provider
                    String providerId = commodityBought.getProviderId();
                    EntityDTO provider = converter.getRawEntityDTO(providerId);
                    EntityType providerEntityType = provider.getEntityType();

                    if (providerEntityType == EntityType.PHYSICAL_MACHINE) {
                        if (probeType == SDKProbeType.AWS) {
                            // connect to AZ
                            entity.addLayeredOver(providerId);
                        } else if (probeType == SDKProbeType.AZURE) {
                            // connect to Region
                            entity.addLayeredOver(converter.getRegionIdFromAzId(providerId));
                        }

                        // create License_Access commodity
                        OSType osType = OSType.lookupByPattern(Optional.ofNullable(
                                entity.getVirtualMachineData().getGuestName()));
                        cbBuilder.addBought(CommodityDTO.newBuilder()
                                .setCommodityType(CommodityType.LICENSE_ACCESS)
                                .setKey(osType.getName())
                                .build());
                        // change commodity provider from AZ to CT
                        cbBuilder.setProviderId(entity.getProfileId());
                        cbBuilder.setProviderType(EntityType.COMPUTE_TIER);
                    } else if (providerEntityType == EntityType.STORAGE) {
                        String storageTier = provider.getStorageData().getStorageTier();
                        String storageTierId = converter.getStorageTierId(storageTier);

                        // change commodity provider from Storage to StorageTier
                        cbBuilder.setProviderId(storageTierId);
                        cbBuilder.setProviderType(EntityType.STORAGE_TIER);

                        //  add connected relationship between vm, volume, storage tier and zone
                        if (commodityBought.hasSubDivision()) {
                            // connect vm to volume
                            String svId = commodityBought.getSubDivision().getSubDivisionId();
                            if (!entity.getLayeredOverList().contains(svId)) {
                                entity.addLayeredOver(svId);
                            }

                            // connect volume to storage tier
                            EntityDTO.Builder volume = converter.getNewEntityBuilder(svId);
                            if (!volume.getLayeredOverList().contains(storageTierId)) {
                                volume.addLayeredOver(storageTierId);
                            }

                            // connect volume to az for aws
                            Optional<String> azId = converter.getRawEntityDTO(entity.getId())
                                    .getCommoditiesBoughtList().stream()
                                    .map(CommodityBought::getProviderId)
                                    .filter(id -> converter.getRawEntityDTO(id).getEntityType()
                                            == EntityType.PHYSICAL_MACHINE)
                                    .findAny();
                            if (probeType == SDKProbeType.AWS) {
                                azId.ifPresent(az -> {
                                    if (!volume.getLayeredOverList().contains(az)) {
                                        volume.addLayeredOver(az);
                                    }
                                });
                            } else if (probeType == SDKProbeType.AZURE) {
                                // connnect volume to region for azure
                                azId.ifPresent(az -> {
                                    String regionId = converter.getRegionIdFromAzId(az);
                                    if (!volume.getLayeredOverList().contains(regionId)) {
                                        volume.addLayeredOver(regionId);
                                    }
                                });
                            }

                            // volume owned by business account
                            converter.ownedByBusinessAccount(svId);
                        }
                    }

                    return cbBuilder.build();
                }).collect(Collectors.toList());

        // set new commodities bought
        entity.clearCommoditiesBought();
        entity.addAllCommoditiesBought(newCommodityBoughtList);

        // VM owned by business account
        converter.ownedByBusinessAccount(entity.getId());

        return true;
    }
}
