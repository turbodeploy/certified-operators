package com.vmturbo.stitching.poststitching;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.components.common.setting.EntitySettingSpecs;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.stitching.EntitySettingsCollection;
import com.vmturbo.stitching.TopologicalChangelog.EntityChangesBuilder;
import com.vmturbo.stitching.TopologyEntity;

/**
 * Base class for capacity post stitching operations.
 * For propagating up, down, taking from settings, etc.
 */
public abstract class BaseEntityCapacityPostStitchingOperation {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Retrieve commodity capacity from providers to be propagated to that entity.
     * The entity should have exactly one provider with a commodity of given type,
     * that has capacity greater than zero. If it has more than one, the first to be
     * found is used.
     *
     * @param providers providers to search for capacity
     * @param commodityType commodity type
     * @param allowedProviderTypes provider types to look in
     * @return any eligible provider's capacity or Optional.empty() if none is found
     */
    protected static Optional<Double> getProviderCapacity(List<TopologyEntity> providers,
                    int commodityType, Set<Integer> allowedProviderTypes) {
        return providers.stream()
            .filter(provider -> allowedProviderTypes.contains(provider.getEntityType()))
            .flatMap(provider ->
                provider.getTopologyEntityDtoBuilder().getCommoditySoldListList().stream())
            .filter(commodity -> commodity.getCommodityType().getType() == commodityType)
            .filter(commodity -> commodity.hasCapacity() && commodity.getCapacity() > 0)
            .map(CommoditySoldDTO::getCapacity)
            .findFirst();
    }

    /**
     * Get the numeric setting value for a given entity and spec.
     *
     * @param entity topology entity
     * @param settingsCollection settings collection
     * @param spec setting specification
     * @return policy value, default if policy is not configured for entity
     */
    protected static double getNumericPolicyValue(TopologyEntity entity,
                    EntitySettingsCollection settingsCollection, EntitySettingSpecs spec) {
        Optional<Setting> setting = settingsCollection.getEntitySetting(entity, spec);
        if (setting.isPresent()) {
            return setting.get().getNumericSettingValue().getValue();
        } else {
            double settingVal = spec.getSettingSpec().getNumericSettingValueType().getDefault();
            logger.debug("Could not find policy setting {} capacity for Entity {} ({}). Using default capacity {}",
                            spec.getDisplayName(), entity.getOid(), entity.getDisplayName(),
                            settingVal);
            return settingVal;
        }
    }

    /**
     * Enqueue the capacity update.
     *
     * @param resultBuilder changes builder
     * @param entity topology entity
     * @param commodityType commodity type
     * @param override whether to override capacity provided by mediation
     * @param newCapacity capacity value
     */
    protected static void setCapacity(EntityChangesBuilder<TopologyEntity> resultBuilder,
                    TopologyEntity entity, int commodityType, boolean override, double newCapacity) {
        if (getApplicableCommodities(entity, commodityType, override).findAny().isPresent()) {
            resultBuilder.queueUpdateEntityAlone(entity, entityForUpdate -> {
                if (override) {
                    logger.debug("Overriding commodity {} capacities for Entity {} with {}",
                                    commodityType, entityForUpdate.getOid(), newCapacity);
                } else {
                    // this situation is standard, log in lower level
                    logger.trace("Setting unset commodity {} capacities for Entity {} with {}",
                            commodityType, entityForUpdate.getOid(), newCapacity);
                }
                getApplicableCommodities(entity, commodityType, override).forEach(
                        commodity -> commodity.setCapacity(newCapacity));
            });
        }
    }

    @Nonnull
    protected static Optional<Double> getCapacity(@Nonnull TopologyEntity entity,
            CommodityType commodityType) {
        return getApplicableCommodities(entity, commodityType.getNumber(), true)
                .findAny()
                .filter(comm -> comm.hasCapacity() && comm.getCapacity() != 0)
                .map(CommoditySoldDTO.Builder::getCapacity);
    }

    private static Stream<CommoditySoldDTO.Builder> getApplicableCommodities(TopologyEntity entity,
            int commodityType, boolean override) {
        return entity
                .getTopologyEntityDtoBuilder()
                .getCommoditySoldListBuilderList()
                .stream()
                .filter(commodity -> commodity.getCommodityType().getType() == commodityType)
                .filter(comm -> override ? true : !comm.hasCapacity() || comm.getCapacity() == 0);
    }
}
