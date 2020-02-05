package com.vmturbo.stitching.utilities;

import java.util.function.Consumer;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.ActionOnProviderEligibility;

/**
 * Helper class to copy action eligibility settings from EntityDTO to TopologyDTO.
 */
public class CopyActionEligibility {
    private static final Logger logger = LogManager.getLogger();

    /**
     * Prevent construction of this class because it only contains static utility methods.
     */
    private CopyActionEligibility() {

    }

    /**
     * Copy action eligibility from the CommodityBought of a SDK EntityDTO to the
     * {@link CommoditiesBought} that corresponds to this CommodityBought.
     *
     * @param commodityBought   CommodityBought message from the SDK EntityDTO
     * @param bought            CommoditiesBought corresponding to this CommodityBought
     */
    public static void transferActionEligibilitySettingsFromEntityDTO(
            @Nonnull CommonDTO.EntityDTO.CommodityBought commodityBought,
            @Nonnull CommoditiesBought bought) {

        transferActionEligibilitySettingsFromEntityDTO(commodityBought,
                bought::setMovable,
                bought::setStartable,
                bought::setScalable);
    }

    /**
     * Copy action eligibility from the CommodityBought of a SDK EntityDTO to the
     * {@link CommoditiesBoughtFromProvider.Builder} that corresponds to this CommodityBought.
     *
     * @param commodityBought   CommodityBought message from the SDK EntityDTO
     * @param cbBuilder CommoditiesBoughtFromProvider.Builder that corresponds to this CommodityBought
     */
    public static void transferActionEligibilitySettingsFromEntityDTO(
            @Nonnull CommonDTO.EntityDTO.CommodityBought commodityBought,
            @Nonnull CommoditiesBoughtFromProvider.Builder cbBuilder) {

        transferActionEligibilitySettingsFromEntityDTO(commodityBought,
                cbBuilder::setMovable,
                cbBuilder::setStartable,
                cbBuilder::setScalable);
    }

    /**
     * Helper method that will transfer the settings for the move, start, scale actions
     * from the CommodityBought section.
     *
     * @param commodityBought   CommodityBought message from the SDK EntityDTO
     * @param movableConsumer   Consumer that will accept the movable setting
     * @param startableConsumer Consumer that will accept the startable setting
     * @param scalableConsumer  Consumer that will accept the scalable setting
     */
    private static void transferActionEligibilitySettingsFromEntityDTO(
            @Nonnull CommonDTO.EntityDTO.CommodityBought commodityBought,
            Consumer<Boolean> movableConsumer,
            Consumer<Boolean> startableConsumer,
            Consumer<Boolean> scalableConsumer) {

        ActionOnProviderEligibility actionEligibility = commodityBought.getActionEligibility();
        if (commodityBought.hasActionEligibility()) {
            if (actionEligibility.hasMovable()) {
                movableConsumer.accept(actionEligibility.getMovable());
            }
            if (actionEligibility.hasStartable()) {
                startableConsumer.accept(actionEligibility.getStartable());
            }
            if (actionEligibility.hasScalable()) {
                scalableConsumer.accept(actionEligibility.getScalable());
            }
        }
    }

    /**
     * Copy action eligibility from the {@link CommoditiesBought} to the
     * {@link CommoditiesBoughtFromProvider.Builder} that corresponds to this CommoditiesBought.
     *
     * @param commoditiesBought CommoditiesBought representing the commodities bought from a provider
     * @param cbBuilder CommoditiesBoughtFromProvider.Builder that corresponds to this CommoditiesBought
     */
    public static void transferActionEligibilitySettingsFromCommoditiesBought(
            @Nonnull CommoditiesBought commoditiesBought,
            @Nonnull CommoditiesBoughtFromProvider.Builder cbBuilder) {

        commoditiesBought.getMovable().ifPresent(cbBuilder::setMovable);
        commoditiesBought.getStartable().ifPresent(cbBuilder::setStartable);
        commoditiesBought.getScalable().ifPresent(cbBuilder::setScalable);
    }
}
