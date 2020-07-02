package com.vmturbo.market.topology.conversions;

import java.util.List;
import java.util.Optional;

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
     *  The resource id. For eg. the volume id of a VM.
     */
    public final Long resourceId;

    /**
     * The entity type of seller of the shopping list and it could be null when the shopping list
     * is from newly provisioned trader.
     */
    public final Integer sellerEntityType;

    /**
     * The commodity list used to create the shopping list.
     */
    public final List<CommodityBoughtDTO> commodities;

    public ShoppingListInfo(final long id,
                            final long buyerId,
                            @Nullable final Long sellerId,
                            @Nullable final Long resourceId,
                            @Nullable final Integer sellerEntityType,
                            @Nonnull final List<CommodityBoughtDTO> commodities) {
        this.id = id;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
        this.resourceId = resourceId;
        this.sellerEntityType = sellerEntityType;
        this.commodities = commodities;
    }

    public long getBuyerId() {
        return buyerId;
    }

    public Long getSellerId() {
        return sellerId;
    }

    public Optional<Integer> getSellerEntityType() {
        return Optional.ofNullable(this.sellerEntityType);
    }

    /**
     *  The resource id. For eg. the volume id of a VM.
     */
    public Optional<Long> getResourceId() {
        return Optional.ofNullable(this.resourceId);
    }
}
