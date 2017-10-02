package com.vmturbo.market.topology.conversions;

import javax.annotation.Nonnull;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.platform.analysis.economy.ShoppingList;

/**
 * Information about a {@link ShoppingList} and how it relates to the topology used to create it.
 */
public class ShoppingListInfo {

    /**
     * The ID of the shopping list.
     */
    public final long id;

    /**
     * The buyer that's purchasing the shopping list.
     */
    public final long buyerId;

    /**
     * The current seller of the shopping list.
     */
    public final long sellerId;

    /**
     * The commodity list used to create the shopping list.
     */
    public final TopologyEntityDTO.CommodityBoughtList commodities;

    public ShoppingListInfo(final long id,
                            final long buyerId,
                            final long sellerId,
                            @Nonnull final TopologyEntityDTO.CommodityBoughtList commodities) {
        this.id = id;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
        this.commodities = commodities;
    }

}
