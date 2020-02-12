package com.vmturbo.stitching.poststitching;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.util.ProbeCategory;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.PostStitchingOperation;
import com.vmturbo.stitching.StitchingScope;
import com.vmturbo.stitching.StitchingScope.StitchingScopeFactory;
import com.vmturbo.stitching.TopologicalChangelog;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Set commodity capacity (auto scaled range).
 */
public class SetAutoSetCommodityCapacityPostStitchingOperation implements PostStitchingOperation {

    private final EntityType entityType;
    private final ProbeCategory probeCategory;
    private final String capacitySettingName;
    private final String autoSetSettingName;
    private final CommodityType commodityType;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Creates an instance of this class.
     * @param entityType entity type
     * @param probeCategory probe category
     * @param commodityType commodity type
     * @param capacitySettingName capacity setting name
     * @param autoSetSettingName auto scale setting name
     */
    public SetAutoSetCommodityCapacityPostStitchingOperation(
            @Nonnull final EntityType entityType,
            @Nonnull final ProbeCategory probeCategory,
            @Nonnull final CommodityType commodityType,
            @Nonnull final String capacitySettingName,
            @Nonnull final String autoSetSettingName) {
        this.entityType = Objects.requireNonNull(entityType);
        this.probeCategory = Objects.requireNonNull(probeCategory);
        this.commodityType = Objects.requireNonNull(commodityType);
        this.capacitySettingName = Objects.requireNonNull(capacitySettingName);
        this.autoSetSettingName = Objects.requireNonNull(autoSetSettingName);
    }

    @Nonnull
    @Override
    public StitchingScope<TopologyEntity> getScope(
            @Nonnull final StitchingScopeFactory<TopologyEntity> stitchingScopeFactory) {
        return stitchingScopeFactory.probeCategoryEntityTypeScope(probeCategory, entityType);
    }

    @Nonnull
    @Override
    public TopologicalChangelog<TopologyEntity> performOperation(
            @Nonnull final Stream<TopologyEntity> entities,
            @Nonnull final EntitySettingsCollection settingsCollection,
            @Nonnull final EntityChangesBuilder<TopologyEntity> resultBuilder) {

        // iterate over entities and if the named setting exists for that entity, find all
        // sold commodities of the correct type and set their capacities according to the
        // value in the setting.
        entities.forEach(entity -> {
            final Optional<Setting> autoSetSetting = settingsCollection.getEntitySetting(entity.getOid(),
                    autoSetSettingName);
            final boolean autoSetFlag = autoSetSetting.isPresent() &&
                    autoSetSetting.get().getBooleanSettingValue().getValue();
            final Optional<Setting> capacitySetting = settingsCollection
                    .getEntitySetting(entity.getOid(), capacitySettingName);
            if (!capacitySetting.isPresent()) {
                logger.error("Capacity Setting {} does not exist for entity {}."
                                + " Not setting capacity for it.", capacitySettingName,
                        entity.getDisplayName());
            } else {
                final float capacitySettingValue = capacitySetting.get().getNumericSettingValue()
                        .getValue();
                resultBuilder.queueUpdateEntityAlone(entity,
                        entityToUpdate -> entityToUpdate.getTopologyEntityDtoBuilder()
                                .getCommoditySoldListBuilderList().stream()
                                .filter(this::commodityTypeMatches)
                                .forEach(commSold ->
                                        commSold.setCapacity(getCapacityValueToSet(commSold,
                                                capacitySettingValue, autoSetFlag))));
            }
        });
        return resultBuilder.build();
    }

    @Nonnull
    @Override
    public String getOperationName() {
        return String.join("_", getClass().getSimpleName(),
                probeCategory.getCategory(), entityType.toString(), commodityType.name(),
                capacitySettingName, autoSetSettingName);
    }

    private boolean commodityTypeMatches(TopologyDTO.CommoditySoldDTO.Builder commodity) {
        return commodity.getCommodityType().getType() == commodityType.getNumber();
    }

    /**
     * Based on the value of the autoSetFlag, the commodity used and capacity values, and the
     * setting value return the correct value for the capacity as follows:  if autoSet is false
     * return capacitySettingValue.  Otherwise, return the maximum of capacitySettingValue,
     * the commodity's used value, and the commodity's capacity value.
     *
     * @param commodity The transaction commodity.
     * @param capacitySettingValue The value of the capacity setting.
     * @param autoSetFlag A boolean indicating whether to calculate a value or just return the
     *                    value in the setting.
     * @return capacity value to set the commodity capacity to.
     */
    private double getCapacityValueToSet(TopologyDTO.CommoditySoldDTO.Builder commodity,
                                        float capacitySettingValue,
                                        boolean autoSetFlag) {
        // if autoSet is false, just use the setting value
        if (!autoSetFlag) {
            return capacitySettingValue;
        }

        // return the max of commodity's used and capacity and the setting's capacity
        double originalCapacity = Float.NEGATIVE_INFINITY;
        if (commodity.hasCapacity()) {
            originalCapacity = commodity.getCapacity();
        }
        double originalUsed = Float.NEGATIVE_INFINITY;
        if (commodity.hasUsed()) {
            originalUsed = commodity.getUsed();
        }
        return Math.max(capacitySettingValue, Math.max(originalCapacity, originalUsed));
    }
}
