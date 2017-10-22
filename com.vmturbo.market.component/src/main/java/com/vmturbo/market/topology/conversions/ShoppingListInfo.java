package com.vmturbo.market.topology.conversions;

import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
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
     * The current seller of the shopping list and it is optional.
     */
    public final Long sellerId;

    /**
     * The commodity list used to create the shopping list.
     */
    public final List<CommodityBoughtDTO> commodities;

    public ShoppingListInfo(final long id,
                            final long buyerId,
                            @Nullable final Long sellerId,
                            @Nonnull final List<CommodityBoughtDTO> commodities) {
        this.id = id;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
        this.commodities = commodities;
    }

}
