package com.vmturbo.market.topology.conversions;

import java.util.Map;

import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * This factory can be used to create a {@link ChangeExplainer}.
 */
public class ChangeExplainerFactory {

    private ChangeExplainerFactory() {}

    /**
     * Create change explainer. Calling this will return an instance of ChangeExplainer.
     *
     * @param targetEntityType target entity type
     * @param commoditiesResizeTracker commodities resize tracker
     * @param cloudTc cloud topology converter
     * @param projectedRICoverageCalculator projected RI coverage calculator
     * @param shoppingListOidToInfos map of shopping list oids to shopping list infos
     * @param commodityIndex the commodity index
     * @param originalTopology the original topology which came into market
     * @return An instance of ChangeExplainer
     */
    public static ChangeExplainer createChangeExplainer(int targetEntityType,
                                                        CommoditiesResizeTracker commoditiesResizeTracker,
                                                        CloudTopologyConverter cloudTc,
                                                        ProjectedRICoverageCalculator projectedRICoverageCalculator,
                                                        Map<Long, ShoppingListInfo> shoppingListOidToInfos,
                                                        CommodityIndex commodityIndex,
                                                        Map<Long, TopologyDTO.TopologyEntityDTO> originalTopology) {
        if (targetEntityType == CommonDTO.EntityDTO.EntityType.DATABASE_SERVER_VALUE) {
            return new MergedActionChangeExplainer(commoditiesResizeTracker, cloudTc, shoppingListOidToInfos, commodityIndex);
        } else {
            return new DefaultChangeExplainer(commoditiesResizeTracker, cloudTc, projectedRICoverageCalculator,
                    shoppingListOidToInfos, commodityIndex, originalTopology);
        }
    }
}