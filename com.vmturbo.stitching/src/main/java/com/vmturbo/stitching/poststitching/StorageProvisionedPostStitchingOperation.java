package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.utils.HCIUtils;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

public abstract class StorageProvisionedPostStitchingOperation extends OverprovisionCapacityPostStitchingOperation {

    public StorageProvisionedPostStitchingOperation() {
        super(EntitySettingSpecs.StorageOverprovisionedPercentage, CommodityType.STORAGE_AMOUNT,
            CommodityType.STORAGE_PROVISIONED);
    }

    @Override
    boolean shouldOverwriteCapacity() {
        return false;
    }

    /**
     * Post-stitching operation for the purpose of setting sold StorageProvisioned commodity
     * capacity and bought StorageProvisioned used for Storage entities.
     *
     * If the entity in question has a Storage Amount commodity, a Storage Provisioned commodity with
     * unset capacity, and a setting for storage overprovisioned percentage, then the Storage
     * Provisioned commodity's capacity is set to the Storage Amount commodity capacity multiplied by
     * the overprovisioned percentage.
     *
     * If the entity buys a StorageProvisioned commodity, and sells a StorageAmount commodity, and
     * the StorageAmount has capacity set, it should set the bought StorageProvisioned used to be
     * the capacity of sold StorageAmount.
     */
    public static class StorageEntityStorageProvisionedPostStitchingOperation extends
                                                    StorageProvisionedPostStitchingOperation {

        private static final Logger logger = LogManager.getLogger();

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.STORAGE);
        }

        @Nonnull
        @Override
        public TopologicalChangelog<TopologyEntity> performOperation(
                @Nonnull Stream<TopologyEntity> entities,
                @Nonnull EntitySettingsCollection settingsCollection,
                @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
            // collect to list so the entities can be looped twice
            List<TopologyEntity> list = entities.collect(Collectors.toList());
            // set sold StorageProvisioned capacity
            super.performOperation(list.stream(), settingsCollection, resultBuilder);
            // set bought StorageProvisioned used value to be the same of its sold StorageAmount capacity
            list.forEach(entity -> {
                TopologyEntityDTO.Builder entityBuilder = entity.getTopologyEntityDtoBuilder();
                if (HCIUtils.isVSAN(entityBuilder))    {
                    //For now we consider here only vSAN, but this may be a scheme correct for all HCI.
                    setUsedBasedOnBoughtAmount(entityBuilder, entity,
                                    settingsCollection, resultBuilder);
                } else {
                    setUsedBasedOnSoldAmount(entityBuilder, entity, resultBuilder);
                }
            });
            return resultBuilder.build();
        }

        /**
         * Sets used value based on bought Storage Amount.
         * @param entityBuilder builder of the entity
         * @param entity        entity
         * @param settingsCollection    collection of settings for the entity
         * @param resultBuilder a builder for the result
         */
        private void setUsedBasedOnBoughtAmount(@Nonnull TopologyEntityDTO.Builder entityBuilder,
                        @Nonnull TopologyEntity entity,
                        @Nonnull EntitySettingsCollection settingsCollection,
                        @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
            Optional<Setting> overprovisionPercentage = settingsCollection.getEntitySetting(
                            entity, EntitySettingSpecs.StorageOverprovisionedPercentage);
            if (!overprovisionPercentage.isPresent() ||
                            !overprovisionPercentage.get().hasNumericSettingValue())    {
                logger.error("Could not update StorageProvisioned bought used values"
                                + " for entity {}; no {} setting found.", entity.getOid(),
                                EntitySettingSpecs.StorageOverprovisionedPercentage);
                return;
            }
            float overprovisionCoefficient = overprovisionPercentage.get()
                            .getNumericSettingValue().getValue() / 100;

            for (CommoditiesBoughtFromProvider.Builder commoditiesBoughtFromProvider :
                    entityBuilder.getCommoditiesBoughtFromProvidersBuilderList())   {
                List<Double> usedStorageAmounts = commoditiesBoughtFromProvider
                    .getCommodityBoughtBuilderList().stream().filter(commodityBoughtDTO ->
                        commodityBoughtDTO.getCommodityType().getType() ==
                            CommodityType.STORAGE_AMOUNT_VALUE && commodityBoughtDTO.hasUsed())
                    .map(CommodityBoughtDTO.Builder::getUsed).collect(Collectors.toList());
                if (usedStorageAmounts.size() != 1)  {
                    logger.error("Wrong number of StorageAmount commodities bought "
                        + "from provider with ID={}: {} commodities",
                        commoditiesBoughtFromProvider.getProviderId(),
                        usedStorageAmounts.size());
                    continue;
                }
                Double amountBoughtUsed = usedStorageAmounts.iterator().next();

                for (CommodityBoughtDTO.Builder boughtBuilder : commoditiesBoughtFromProvider
                                .getCommodityBoughtBuilderList())  {
                    if (boughtBuilder.getCommodityType().getType() ==
                                    CommodityType.STORAGE_PROVISIONED_VALUE)  {
                        resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate -> {
                            boughtBuilder.setUsed(amountBoughtUsed * overprovisionCoefficient);
                            logger.debug("Setting bough StorageProvisioned used "
                                            + "value for entity {} based on its "
                                            + "bought StorageAmount used value.",
                                            entityForUpdate.getOid());
                        });
                        break;
                    }
                }
            }
        }

        /**
         * Sets used value for bought Storage Provisioned based on sold Storage Amount.
         * @param entityBuilder builder of the entity
         * @param entity        entity
         * @param resultBuilder the name speaks for itself
         */
        private void setUsedBasedOnSoldAmount(@Nonnull TopologyEntityDTO.Builder entityBuilder,
                        @Nonnull TopologyEntity entity,
                        @Nonnull EntityChangesBuilder<TopologyEntity> resultBuilder) {
            entityBuilder.getCommoditySoldListBuilderList().stream()
            .filter(commoditySoldDTO -> commoditySoldDTO.getCommodityType().getType() ==
                    CommodityType.STORAGE_AMOUNT_VALUE && commoditySoldDTO.hasCapacity())
            .map(CommoditySoldDTO.Builder::getCapacity)
            .findAny()
            .ifPresent(storageAmountCapacity ->
                    entityBuilder.getCommoditiesBoughtFromProvidersBuilderList().stream()
                        .flatMap(commoditiesBoughtFromProvider ->
                                commoditiesBoughtFromProvider.getCommodityBoughtBuilderList().stream())
                        .filter(commodityBoughtDTO -> commodityBoughtDTO.getCommodityType().getType() ==
                                CommodityType.STORAGE_PROVISIONED_VALUE)
                        .forEach(commodityBoughtDTO -> resultBuilder.queueUpdateEntityAlone(entity,
                                entityForUpdate -> {
                            commodityBoughtDTO.setUsed(storageAmountCapacity);
                            logger.debug("Setting bought StorageProvisioned used " +
                                            "value for entity {} to its sold " +
                                            "StorageAmount capacity {}",
                                    entityForUpdate.getOid(), storageAmountCapacity);
                        }))
            );
        }
    }

    /**
     * Post-stitching operation for the purpose of setting Storage Provisioned commodity capacities for
     * Logical Pool entities.
     *
     * If the entity in question has a Storage Amount commodity, a Storage Provisioned commodity with
     * unset capacity, and a setting for storage overprovisioned percentage, then the Storage
     * Provisioned commodity's capacity is set to the Storage Amount commodity capacity multiplied by
     * the overprovisioned percentage.
     */
    public static class LogicalPoolStorageProvisionedPostStitchingOperation extends
                                            StorageProvisionedPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.LOGICAL_POOL);
        }

    }

    /**
     * Post-stitching operation for the purpose of setting Storage Provisioned commodity capacities for
     * Disk Array entities.
     *
     * If the entity in question has a Storage Amount commodity, a Storage Provisioned commodity with
     * unset capacity, and a setting for storage overprovisioned percentage, then the Storage
     * Provisioned commodity's capacity is set to the Storage Amount commodity capacity multiplied by
     * the overprovisioned percentage.
     */
    public static class DiskArrayStorageProvisionedPostStitchingOperation extends
                                                    StorageProvisionedPostStitchingOperation {

        @Nonnull
        @Override
        public StitchingScope<TopologyEntity> getScope(@Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
            return stitchingScopeFactory.entityTypeScope(EntityType.DISK_ARRAY);
        }
    }
}
