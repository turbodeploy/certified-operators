package com.vmturbo.mediation.conversion.cloud.converter;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.mediation.conversion.cloud.CloudDiscoveryConverter;
import com.vmturbo.mediation.conversion.cloud.IEntityConverter;
import com.vmturbo.mediation.conversion.util.ConverterUtils;
import com.vmturbo.mediation.conversion.util.ConverterUtils.CommodityCapacityWrapper;
import com.vmturbo.platform.common.builders.CommodityBuilderIdentifier;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.RatioDependency;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ConsumerPolicy;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityProperty;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData;
import com.vmturbo.platform.sdk.common.util.SDKProbeType;

/**
 * Cloud Storages each represent a combination of storage tier + availability zone. We don't
 * want to model all of those permutations directly as entities in XL, so we will merge these
 * Storages into a primary set of Storage Tier entities, decoupling Storage Tier from Availability
 * Zone. The Storage entities will be removed. We will move buyers from this storage to storage
 * tiers when converting the VMs and other consumers.
 */
public class StorageConverter implements IEntityConverter {

    private final Logger logger = LogManager.getLogger();

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

        String storageTier = converter.getStorageTier(entity);
        String storageTierId = converter.getStorageTierId(storageTier);

        final EntityDTO.Builder storageTierEntity = converter.getNewEntityBuilder(storageTierId);

        // find az
        converter.getAvailabilityZone(entity)
            .ifPresent(azId -> {
                // connect storage tier to region
                final String regionId = converter.getRegionIdFromAzId(azId);
                if (!storageTierEntity.getLayeredOverList().contains(regionId)) {
                    storageTierEntity.addLayeredOver(regionId);
                }

                // set up connected relationship from volume to storage tier and az
                // these are wasted files
                String regionName = CloudDiscoveryConverter.getRegionNameFromAzId(azId);
                entity.getStorageData().getFileList().forEach(file ->
                    converter.getVolumeId(regionName, file.getPath()).ifPresent(volumeId -> {
                        // get volume
                        EntityDTO.Builder volume = converter.getNewEntityBuilder(volumeId);

                        if (probeType == SDKProbeType.AWS || probeType == SDKProbeType.GCP) {
                            // connect to AZ for aws or gcp
                            if (!volume.getLayeredOverList().contains(azId)) {
                                volume.addLayeredOver(azId);
                            }
                        } else if (probeType == SDKProbeType.AZURE
                            || probeType == SDKProbeType.AZURE_STORAGE_BROWSE) {
                            // connect to region for azure
                            if (!volume.getLayeredOverList().contains(regionId)) {
                                volume.addLayeredOver(regionId);
                            }
                        }

                        // connect to storage tier
                        if (!volume.getLayeredOverList().contains(storageTierId)) {
                            volume.addLayeredOver(storageTierId);
                        }

                        final VirtualVolumeData volumeDataAfterPropertyUpdates =
                            updateVirtualVolumeData(volume.getVirtualVolumeData(),
                                file.getVolumePropertiesList());
                        volume.setVirtualVolumeData(volumeDataAfterPropertyUpdates);

                        // volume owned by business account
                        converter.ownedByBusinessAccount(volumeId);

                        // Set Volume Consumer Policy based on its descriptor's value
                        if (file.getDoNotDelete()) {
                            logger.debug("File {} with doNotDelete==true -> Setting ConsumerPolicy.deletable to false", file.getPath());
                            volume.setConsumerPolicy(ConsumerPolicy.newBuilder().setDeletable(false).build());
                        }
                    })
                );
            });

        // merge commodities sold from storage into storage tier
        List<CommodityDTO> soldCommodities = mergeCommodities(entity.getCommoditiesSoldList(),
                storageTierEntity.getCommoditiesSoldList());
        storageTierEntity.clearCommoditiesSold();
        List<CommodityDTO> storageAmountDTO = soldCommodities.stream()
                        .filter(c -> c.getCommodityType() == CommodityType.STORAGE_AMOUNT)
                        .collect(Collectors.toList());
        List<CommodityDTO> storageAccessDTO = soldCommodities.stream()
                        .filter(c -> c.getCommodityType() == CommodityType.STORAGE_ACCESS)
                        .collect(Collectors.toList());
        CommodityCapacityWrapper commCapacityWrapper = ConverterUtils.cloudStorageCapacityMap.get(storageTier);
        // if the entity has commodity capacity constraint
        if (storageAmountDTO.size() == 1 && storageAccessDTO.size() == 1 && commCapacityWrapper != null) {
            CommodityDTO storageAmount = storageAmountDTO.get(0);
            CommodityDTO storageAccess = storageAccessDTO.get(0);
            // add storage amount min and max capacity for consumer
            CommodityDTO.Builder newStAmt = storageAmount.toBuilder()
                            .setMinAmountForConsumer(commCapacityWrapper.storageAmountMinCapacity)
                            .setMaxAmountForConsumer(commCapacityWrapper.storageAmountMaxCapacity);
            CommodityDTO.Builder newStAcc = storageAccess.toBuilder();
            // set storage amount and storage access ratio constraint which will be used to populate
            // ResourceDpendencyLimitation in costDTO
            if (storageTier.equals(ConverterUtils.GP2)) {
                newStAcc.setRatioDependency(RatioDependency.newBuilder()
                                            .setBaseCommodity(CommodityType.STORAGE_AMOUNT)
                                            .setRatio(ConverterUtils.GP2_IOPS_TO_STORAGE_AMOUNT_RATIO)
                                            .build())
                .setMinAmountForConsumer(commCapacityWrapper.storageAccessMinCapacity)
                .setMaxAmountForConsumer(commCapacityWrapper.storageAccessMaxCapacity);
            } else if (storageTier.equals(ConverterUtils.IO1)) {
                newStAcc.setRatioDependency(RatioDependency.newBuilder()
                                            .setBaseCommodity(CommodityType.STORAGE_AMOUNT)
                                            .setRatio(ConverterUtils.IO1_IOPS_TO_STORAGE_AMOUNT_RATIO)
                                            .build())
                .setMinAmountForConsumer(commCapacityWrapper.storageAccessMinCapacity)
                .setMaxAmountForConsumer(commCapacityWrapper.storageAccessMaxCapacity);
            } else {
                newStAcc.setMinAmountForConsumer(commCapacityWrapper.storageAccessMinCapacity)
                .setMaxAmountForConsumer(commCapacityWrapper.storageAccessMaxCapacity);
            }
            soldCommodities.remove(storageAmount);
            soldCommodities.remove(storageAccess);
            soldCommodities.add(newStAmt.build());
            soldCommodities.add(newStAcc.build());
        }
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

    protected VirtualVolumeData updateVirtualVolumeData(@Nonnull final VirtualVolumeData preexistingVolumeData,
                                                        @Nonnull final List<EntityProperty> volumeFieldUpdateProperties) {
        return preexistingVolumeData;
    }
}
