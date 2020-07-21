package com.vmturbo.topology.processor.group.settings;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Settings applicator for the entities originated from the plan, e.g. created
 * from templates.
 */
public class PlanOriginEntityApplicator extends BaseSettingApplicator {

    private static final Logger logger = LogManager.getLogger();

    @Override
    public void apply(@Nonnull TopologyEntityDTO.Builder entity,
            @Nonnull Map<EntitySettingSpecs, Setting> settings) {
        if (!isPlanOriginEntity(entity)) {
            return;
        }

        setLatencyCapacity(entity, settings);
    }

    /**
     * Sets capacities for sold Storage Latency commodities, if they are not
     * set.
     *
     * @param entity entity
     * @param settings settings
     */
    private void setLatencyCapacity(@Nonnull TopologyEntityDTO.Builder entity,
            @Nonnull Map<EntitySettingSpecs, Setting> settings) {
        double latencyCapacity = getNumericSetting(settings, EntitySettingSpecs.LatencyCapacity);
        Collection<CommoditySoldDTO.Builder> latencyComms = getCommoditySoldBuilders(entity,
                CommodityType.STORAGE_LATENCY);

        for (CommoditySoldDTO.Builder comm : latencyComms) {
            if (!comm.hasCapacity()) {
                comm.setCapacity(latencyCapacity);
                logger.debug("Setting Storage Latency capacity for entity '{}' to {}",
                        entity.clearDisplayName(), latencyCapacity);
            }
        }
    }

    private boolean isPlanOriginEntity(@Nonnull TopologyEntityDTO.Builder entity) {
        return entity.hasOrigin() && entity.getOrigin().hasPlanScenarioOrigin();
    }
}
