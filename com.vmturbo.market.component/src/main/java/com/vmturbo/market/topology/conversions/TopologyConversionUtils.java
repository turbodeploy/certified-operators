package com.vmturbo.market.topology.conversions;

import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.cost.Cost.EntityReservedInstanceCoverage;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.EntityState;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.Units;
import com.vmturbo.market.settings.EntitySettings;
import com.vmturbo.market.topology.TopologyConversionConstants;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderSettingsTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTOREST.EntityDTO.StorageType;

public class TopologyConversionUtils {
    private static final float MIN_DESIRED_UTILIZATION_VALUE = 0.0f;
    private static final float MAX_DESIRED_UTILIZATION_VALUE = 1.0f;

    private static final Logger logger = LogManager.getLogger();

    /**
     * Return state of trader in market analysis based
     * on state of entity in topology.
     *
     * @param entity {@link TopologyEntityDTO} being converted
     * @return {@link TraderStateTO} for economy
     */
    @Nonnull
    public static EconomyDTOs.TraderStateTO traderState(@Nonnull final TopologyEntityDTO entity) {
        EntityState entityState = entity.getEntityState();
        return entityState == TopologyDTO.EntityState.POWERED_ON ? EconomyDTOs.TraderStateTO.ACTIVE
                        : entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE
                                        ? EconomyDTOs.TraderStateTO.IDLE
                                        : EconomyDTOs.TraderStateTO.INACTIVE;
    }

    /**
     * Creates {@link TraderSettingsTO.Builder} for any {@link TopologyEntityDTO} entity.
     *
     * @param entity {@link TopologyEntityDTO} entity being coverted to trader
     * @param topology topology map
     * @return {@link TraderSettingsTO.Builder}
     */
    static TraderSettingsTO.Builder createCommonTraderSettingsTOBuilder(TopologyEntityDTO entity,
                    @Nonnull Map<Long, TopologyEntityDTO> topology) {
        final boolean shopTogether = entity.getAnalysisSettings().getShopTogether();
        final EconomyDTOs.TraderSettingsTO.Builder settingsBuilder = EconomyDTOs.TraderSettingsTO
                        .newBuilder().setMinDesiredUtilization(getMinDesiredUtilization(entity))
                        .setMaxDesiredUtilization(getMaxDesiredUtilization(entity))
                        .setGuaranteedBuyer(isGuaranteedBuyer(entity, topology))
                        .setIsShopTogether(shopTogether);
        return settingsBuilder;
    }

    /**
     * Check if the entity is consuming providers that are from cloud.
     */
    public static boolean isEntityConsumingCloud(TopologyEntityDTO entity) {
        return entity.getCommoditiesBoughtFromProvidersList().stream()
                        .anyMatch(g -> TopologyDTOUtil.isTierEntityType(g.getProviderEntityType()));
    }

    static double convertMarketUnitToTopologyUnit(final int commodityType,
                                                  final double valueToConvert,
                                                  @Nullable final TopologyEntityDTO entityDTO) {
        if (entityDTO != null && isEntityConsumingCloud(entityDTO)
            && entityDTO.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE
            && commodityType == CommodityType.STORAGE_AMOUNT_VALUE) {
            return valueToConvert * Units.KBYTE;
        }
        return valueToConvert;
    }

    /**
     * An entity is a guaranteed buyer if it is a VDC that consumes (directly) from
     * storage or PM, or if it is a DPod.
     *
     * @param topologyDTO the entity to examine
     * @return whether the entity is a guaranteed buyer
     */
    private static boolean isGuaranteedBuyer(TopologyDTO.TopologyEntityDTO topologyDTO,
                    @Nonnull Map<Long, TopologyEntityDTO> topology) {
        int entityType = topologyDTO.getEntityType();
        return (entityType == EntityType.VIRTUAL_DATACENTER_VALUE)
                        && topologyDTO.getCommoditiesBoughtFromProvidersList().stream()
                                        .filter(CommoditiesBoughtFromProvider::hasProviderId)
                                        .map(CommoditiesBoughtFromProvider::getProviderId)
                                        .collect(Collectors.toSet()).stream().map(topology::get)
                                        .map(TopologyDTO.TopologyEntityDTO::getEntityType)
                                        .anyMatch(type -> MarketAnalysisUtils.GUARANTEED_SELLER_TYPES
                                                        .contains(type))
                        || entityType == EntityType.DPOD_VALUE
                        || entityType == EntityType.SERVICE_VALUE;
    }

    @VisibleForTesting
    static float getMinDesiredUtilization(@Nonnull final TopologyEntityDTO topologyDTO) {

        final TopologyEntityDTO.AnalysisSettings analysisSettings =
                        topologyDTO.getAnalysisSettings();

        if (analysisSettings.hasDesiredUtilizationTarget()
                        && analysisSettings.hasDesiredUtilizationRange()) {

            return limitFloatRange((analysisSettings.getDesiredUtilizationTarget()
                            - (analysisSettings.getDesiredUtilizationRange() / 2.0f)) / 100.0f,
                            MIN_DESIRED_UTILIZATION_VALUE, MAX_DESIRED_UTILIZATION_VALUE);
        } else {
            return EntitySettings.NumericKey.DESIRED_UTILIZATION_MIN.value(topologyDTO);
        }
    }

    @VisibleForTesting
    static float getMaxDesiredUtilization(@Nonnull final TopologyEntityDTO topologyDTO) {

        final TopologyEntityDTO.AnalysisSettings analysisSettings =
                        topologyDTO.getAnalysisSettings();

        if (analysisSettings.hasDesiredUtilizationTarget()
                        && analysisSettings.hasDesiredUtilizationRange()) {

            return limitFloatRange((analysisSettings.getDesiredUtilizationTarget()
                            + (analysisSettings.getDesiredUtilizationRange() / 2.0f)) / 100.0f,
                            MIN_DESIRED_UTILIZATION_VALUE, MAX_DESIRED_UTILIZATION_VALUE);
        } else {
            return EntitySettings.NumericKey.DESIRED_UTILIZATION_MAX.value(topologyDTO);
        }
    }

    public static float limitFloatRange(float value, float min, float max) {
        Preconditions.checkArgument(min <= max, "Min: %s must be <= max: %s", min, max);
        return Math.min(max, Math.max(value, min));
    }

    public static boolean areFloatsEqual(float a, float b) {
        return Math.abs(a - b) < TopologyConversionConstants.FLOAT_COMPARISON_DELTA;
    }

    /**
     * Should the entity type be converted to trader?
     * We do not perform analysis on static infrastructure like compute tiers / storage tiers /
     * regions etc. So these are converted to traders. Volumes are also not converted to traders
     * because volumes are currently represented using the storage shopping lists of VMs.
     *
     * @param entityType the entity type
     * @return true if the entity type should be converted to trader, false otherwise
     */
    public static boolean shouldConvertToTrader(int entityType) {
        return !TopologyConversionConstants.ENTITY_TYPES_TO_SKIP_TRADER_CREATION
                        .contains(entityType);
    }

    /**
     * Checks if the entity passed is a vSan Storage.
     *
     * @param buyer the topology entity
     * @return true if its a vsan Storage. False if it isn't.
     */
    public static boolean isVsanStorage(final TopologyEntityDTO buyer) {
        if (buyer.getEntityType() == EntityType.STORAGE_VALUE
                && buyer.getTypeSpecificInfo() != null
                && buyer.getTypeSpecificInfo().getStorage() != null
                && buyer.getTypeSpecificInfo().getStorage().getStorageType() != null
                && StorageType.VSAN.getValue()
                    == buyer.getTypeSpecificInfo().getStorage().getStorageType().getNumber()) {
            return true;
        }
        return false;
    }

    /**
     * Gets the total number of coupons covered from a given entity ri coverage.
     *
     * @param riCoverage the entity ri coverage using which total number of coupons covered are calculated.
     * @return the total number of coupons covered
     */
    public static float getTotalNumberOfCouponsCovered(@Nonnull EntityReservedInstanceCoverage riCoverage) {
        return (float)riCoverage.getCouponsCoveredByRiMap().values().stream()
            .mapToDouble(Double::new).sum();
    }
}
