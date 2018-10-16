package com.vmturbo.mediation.cloud.converter;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.vmturbo.mediation.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.cloud.IEntityConverter;
import com.vmturbo.platform.common.builders.CommodityBuilderIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Cloud Storages each represent a combination of storage tier + availability zone. We don't
 * want to model all of those permutations directly as entities in XL, so we will merge these
 * Storages into a primary set of Storage Tier entities, decoupling Storage Tier from Availability
 * Zone. The Storage entities will be removed. We will move buyers from this storage to storage
 * tiers when converting the VMs and other consumers.
 */
public class StorageConverter implements IEntityConverter {

    private SDKProbeType probeType;

    public StorageConverter(@Nonnull SDKProbeType probeType) {
        this.probeType = probeType;
    }

    @Override
    public boolean convert(@Nonnull EntityDTO.Builder entity, @Nonnull CloudDiscoveryConverter converter) {
        // if no storage data, just remove
        if (!entity.hasStorageData()) {
            return false;
        }

        String storageTier = entity.getStorageData().getStorageTier();
        String storageTierId = converter.getStorageTierId(storageTier);

        final EntityDTO.Builder storageTierEntity = converter.getNewEntityBuilder(storageTierId);

        // find az
        entity.getCommoditiesSoldList().stream()
                .filter(commodity -> commodity.getCommodityType() == CommodityType.DSPM_ACCESS)
                .map(commodityDTO -> CloudDiscoveryConverter.keyToUuid(commodityDTO.getKey()))
                .findAny()
                .ifPresent(azId -> {
                    // connect storage tier to region
                    final String regionId = converter.getRegionIdFromAzId(azId);
                    if (!storageTierEntity.getLayeredOverList().contains(regionId)) {
                        storageTierEntity.addLayeredOver(regionId);
                    }

                    // set up connected relationship from volume to storage tier and az
                    // these are wasted files which are currently only for aws probe
                    String regionName = CloudDiscoveryConverter.getRegionNameFromAzId(azId);
                    entity.getStorageData().getFileList().forEach(file ->
                        converter.getVolumeId(regionName, file.getPath()).ifPresent(volumeId -> {
                            // get volume
                            EntityDTO.Builder volume = converter.getNewEntityBuilder(volumeId);

                            if (probeType == SDKProbeType.AWS) {
                                // connect to AZ for aws
                                if (!volume.getLayeredOverList().contains(azId)) {
                                    volume.addLayeredOver(azId);
                                }
                            } else if (probeType == SDKProbeType.AZURE) {
                                // connect to region for azure
                                if (!volume.getLayeredOverList().contains(regionId)) {
                                    volume.addLayeredOver(regionId);
                                }
                            }

                            // connect to storage tier
                            if (!volume.getLayeredOverList().contains(storageTierId)) {
                                volume.addLayeredOver(storageTierId);
                            }

                            // volume owned by business account
                            converter.ownedByBusinessAccount(volumeId);
                        })
                    );
                });

        // merge commodities sold from storage into storage tier
        List<CommodityDTO> soldCommodities = mergeCommodities(entity.getCommoditiesSoldList(),
                storageTierEntity.getCommoditiesSoldList());
        storageTierEntity.clearCommoditiesSold();
        storageTierEntity.addAllCommoditiesSold(soldCommodities);

        // StorageTier owned by CloudService
        converter.ownedByCloudService(EntityType.STORAGE_TIER, storageTierId);

        return false;
    }

    /**
     * Merge two commodities list and return a new list of commodities, which has unique
     * combination of commodity type and key. The capacity and used value in the fromCommodities
     * list wins.
     *
     * @param fromCommodities commodities list to merge from
     * @param toCommodities commodities list to merge to
     * @return new list of commodities after merge
     */
    private List<CommodityDTO> mergeCommodities(@Nonnull List<CommodityDTO> fromCommodities,
                                                @Nonnull List<CommodityDTO> toCommodities) {
        final Map<CommodityBuilderIdentifier, CommodityDTO.Builder> toCommoditiesMap =
                toCommodities.stream()
                        .map(CommodityDTO::toBuilder)
                        .collect(Collectors.toMap(commodity -> new CommodityBuilderIdentifier(
                                commodity.getCommodityType(), commodity.getKey()), Function.identity()));

        fromCommodities.forEach(fromCommodity -> {
            CommodityBuilderIdentifier fromCommodityIdentifier = new CommodityBuilderIdentifier(
                    fromCommodity.getCommodityType(), fromCommodity.getKey());
            CommodityDTO.Builder toCommodity = toCommoditiesMap.get(fromCommodityIdentifier);
            // new commodity
            if (toCommodity == null) {
                // do not add DSPM_ACCESS commodity since it is represented with connection
                if (fromCommodity.getCommodityType() != CommodityType.DSPM_ACCESS) {
                    toCommoditiesMap.put(fromCommodityIdentifier, fromCommodity.toBuilder());
                }
            } else {
                // overlapping commodity
                if (fromCommodity.hasCapacity()) {
                    toCommodity.setCapacity(fromCommodity.getCapacity());
                }
                if (fromCommodity.hasUsed()) {
                    toCommodity.setUsed(fromCommodity.getUsed());
                }
            }
        });

        return toCommoditiesMap.values().stream()
                .map(CommodityDTO.Builder::build)
                .collect(Collectors.toList());
    }
}
