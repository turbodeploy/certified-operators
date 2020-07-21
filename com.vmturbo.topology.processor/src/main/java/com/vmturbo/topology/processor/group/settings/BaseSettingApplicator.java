package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
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
}
