package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProviderOrBuilder;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Base class for setting applicators.
 */
public abstract class BaseSettingApplicator implements SettingApplicator {

    private final Logger logger = LogManager.getLogger(getClass());

    /**
     * Get entity sold commodities by type.
     *
     * @param entity entity
     * @param commodityType commodity type
     * @return sold commodities
     */
    @Nonnull
    protected static Collection<CommoditySoldDTO.Builder> getCommoditySoldBuilders(
            @Nonnull TopologyEntityDTO.Builder entity, CommodityType commodityType) {
        return entity.getCommoditySoldListBuilderList().stream().filter(
                commodity -> commodity.getCommodityType().getType() == commodityType.getNumber())
                .collect(Collectors.toList());
    }

    /**
     * Get boolean setting value.
     *
     * @param settings settings
     * @param spec setting spec
     * @return boolean value
     */
    protected boolean getBooleanSetting(@Nonnull Map<EntitySettingSpecs, Setting> settings,
            @Nonnull EntitySettingSpecs spec) {
        Setting setting = settings.get(spec);

        if (setting == null || !setting.hasBooleanSettingValue()) {
            logger.error("The boolean setting " + spec.getDisplayName()
                    + " is missing. Using defaults. ");
            return spec.getBooleanDefault();
        }

        return setting.getBooleanSettingValue().getValue();
    }

    /**
     * Get numeric setting value.
     *
     * @param settings settings
     * @param spec setting spec
     * @return numeric value
     */
    protected double getNumericSetting(@Nonnull Map<EntitySettingSpecs, Setting> settings,
            @Nonnull EntitySettingSpecs spec) {
        Setting setting = settings.get(spec);

        if (setting == null || !setting.hasNumericSettingValue()) {
            logger.error("The numeric setting " + spec.getDisplayName()
                    + " is missing.  Using defaults. ");
            return spec.getNumericDefault();
        }

        return setting.getNumericSettingValue().getValue();
    }

    /**
     * Apply the movable flag to the commodities of the entity which satisfies the provided
     * override condition.
     *
     * @param entity to apply the setting
     * @param movable is movable or not
     * @param predicate condition function which the commodity should apply the movable or not.
     */
    protected void applyMovableToCommodities(@Nonnull TopologyEntityDTO.Builder entity,
                                             boolean movable,
                                             @Nonnull Predicate<Builder> predicate) {
        entity.getCommoditiesBoughtFromProvidersBuilderList()
                .stream()
                .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderId)
                .filter(CommoditiesBoughtFromProviderOrBuilder::hasProviderEntityType)
                .filter(predicate)
                .forEach(c -> {
                    // Apply setting value only if move is not disabled by the entity
                    if (!c.hasMovable() || c.getMovable()) {
                        c.setMovable(movable);
                    } else {
                        // Do not override with the setting if move has been disabled at entity level
                        logger.trace("{}:{} Not overriding move setting, move is disabled at entity level {}",
                                entity::getEntityType, entity::getDisplayName,
                                c::getMovable);
                    }
                });
    }
}
