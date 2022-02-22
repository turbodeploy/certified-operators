package com.vmturbo.market.topology.conversions;

import java.util.Map;
import java.util.Objects;

import javax.annotation.Nullable;

import com.vmturbo.api.enums.DatabasePricingModel;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * This factory can be used to create a {@link ChangeExplainer}.
 */
public class ChangeExplainerFactory {
    private ChangeExplainerFactory() {}

    /**
     * Create change explainer. Calling this will return an instance of ChangeExplainer.
     *
     * @param projectedEntity projected target entity
     * @param commoditiesResizeTracker commodities resize tracker
     * @param cloudTc cloud topology converter
     * @param projectedRICoverageCalculator projected RI coverage calculator
     * @param shoppingListOidToInfos map of shopping list oids to shopping list infos
     * @param commodityIndex the commodity index
     * @param originalTopology the original topology which came into market
     * @return An instance of ChangeExplainer
     */
    public static ChangeExplainer createChangeExplainer(@Nullable TopologyDTO.TopologyEntityDTO projectedEntity,
                                                        CommoditiesResizeTracker commoditiesResizeTracker,
                                                        CloudTopologyConverter cloudTc,
                                                        ProjectedRICoverageCalculator projectedRICoverageCalculator,
                                                        Map<Long, ShoppingListInfo> shoppingListOidToInfos,
                                                        CommodityIndex commodityIndex,
                                                        Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology) {
        int actionTargetEntityType = Objects.nonNull(projectedEntity) ? projectedEntity.getEntityType() : -1;
        switch (actionTargetEntityType) {
            case CommonDTO.EntityDTO.EntityType.DATABASE_SERVER_VALUE:
                return new MergedActionChangeExplainer(commoditiesResizeTracker, cloudTc, shoppingListOidToInfos, commodityIndex);
            case CommonDTO.EntityDTO.EntityType.DATABASE_VALUE:
                String pricingModel = projectedEntity.getEntityPropertyMapOrDefault(StringConstants.DB_PRICING_MODEL, null);
                if (pricingModel != null && DatabasePricingModel.vCore.toString().equals(pricingModel)) {
                    return new MergedActionChangeExplainer(commoditiesResizeTracker, cloudTc, shoppingListOidToInfos, commodityIndex);
                }
            default:
                return new DefaultChangeExplainer(commoditiesResizeTracker, cloudTc, projectedRICoverageCalculator,
                        shoppingListOidToInfos, commodityIndex, originalTopology);
        }
    }
}