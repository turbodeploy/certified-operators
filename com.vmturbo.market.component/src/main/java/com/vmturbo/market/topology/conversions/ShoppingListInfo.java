package com.vmturbo.market.topology.conversions;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections4.CollectionUtils;

import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.platform.analysis.economy.ShoppingList;

/**
 * Information about a {@link ShoppingList} and how it relates to the topology used to create it.
 */
public class ShoppingListInfo {

    /**
     * The ID of the shopping list.
     */
    private final long id;

    /**
     * The buyer that's purchasing the shopping list.
     */
    private final long buyerId;

    /**
     * The current seller of the shopping list and it is optional.
     */
    private final Long sellerId;

    /**
     *  The resource ids. For eg. the volume ids of a VM on same storage.
     */
    private final Set<Long> resourceIds;

    /**
     *  The collapsed buyer id. For eg. cloud volume id.
     *  TODO this thing and the above resourceId thing are semantically identical
     *  and used in parallel! we only need one of the two
     */
    private final Long collapsedBuyerId;

    /**
     * The entity type of seller of the shopping list and it could be null when the shopping list
     * is from newly provisioned trader.
     */
    private final Integer sellerEntityType;

    /**
     * The commodity list used to create the shopping list.
     */
    private final List<CommodityBoughtDTO> commodities;

    public ShoppingListInfo(final long id,
                            final long buyerId,
                            @Nullable final Long sellerId,
                            @Nullable final Long resourceId,
                            @Nullable final Long collapsedBuyerId,
                            @Nullable final Integer sellerEntityType,
                            @Nonnull final List<CommodityBoughtDTO> commodities) {
        this(id, buyerId, sellerId, Collections.singleton(resourceId), collapsedBuyerId,
                        sellerEntityType, commodities);
    }

    /**
     * Construct the SL related information to retain across analysis invocation.
     *
     * @param id SL identity
     * @param buyerId buyer identity
     * @param sellerId seller identity
     * @param resourceIds associated resources (e.g. volume ids), can be empty
     * @param collapsedBuyerId collapsed buyer identity, can be unset
     * @param sellerEntityType entity type of a seller
     * @param commodities commodities in the SL
     */
    public ShoppingListInfo(final long id, final long buyerId, @Nullable final Long sellerId,
                    @Nonnull Set<Long> resourceIds, @Nullable final Long collapsedBuyerId,
                    @Nullable final Integer sellerEntityType,
                    @Nonnull final List<CommodityBoughtDTO> commodities) {
        this.id = id;
        this.buyerId = buyerId;
        this.sellerId = sellerId;
        this.resourceIds = resourceIds;
        this.collapsedBuyerId = collapsedBuyerId;
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
     * The resource id. For eg. any volume id of a VM.
     *
     * @return any resource id, null if none registered
     */
    @Nullable
    public Long getResourceId() {
        return CollectionUtils.isEmpty(resourceIds) ? null : resourceIds.iterator().next();
    }

    /**
     *  The resource ids. For eg. the volume ids of a VM.
     *
     * @return all associated resource ids
     */
    @Nullable
    public Set<Long> getResourceIds() {
        return resourceIds;
    }

    /**
     * Get collapsedBuyerId for a shoppingList. For example, cloud volume id.
     *
     * @return collapsed buyer id.
     */
    public Optional<Long> getCollapsedBuyerId() {
        return Optional.ofNullable(collapsedBuyerId);
    }

    /**
     * Get the optional identity of a resource identifying the shopping list.
     * If collapsed buyer is set, then collapsed buyer, otherwise resource (on-prem volume),
     * null if neither is set.
     *
     * @return identifying resource id
     */
    public Long getActingId() {
        if (collapsedBuyerId != null) {
            return collapsedBuyerId;
        }
        return getResourceId();
    }

    public List<CommodityBoughtDTO> getCommodities() {
        return commodities;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof ShoppingListInfo)) {
            return false;
        }
        final ShoppingListInfo that = (ShoppingListInfo)obj;
        return Objects.equals(id, that.id);
    }

    @Override
    public String toString() {
        return "ShoppingListInfo{" + "id=" + id + ", buyerId=" + buyerId + ", sellerId=" + sellerId
                        + ", resourceIds=" + resourceIds + ", collapsedBuyerId=" + collapsedBuyerId
                        + ", sellerEntityType=" + sellerEntityType + '}';
    }
}
