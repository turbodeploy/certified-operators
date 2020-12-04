package com.vmturbo.market.topology.conversions;

import java.util.Optional;

import com.vmturbo.market.topology.MarketTier;
import com.vmturbo.platform.analysis.protobuf.CommodityDTOs.CommoditySoldTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.ShoppingListTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderStateTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.TraderTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;

/**
 * Represents the data from the original input {@link TraderTO} that we need to keep around in
 * order to interpret analysis results. Used instead of the full {@link TraderTO} to save memory.
 *
 * <p/>At BofA, the map of these traders takes up about 70MB.
 */
public class MinimalOriginalTrader {
    private final long oid;
    private final TraderStateTO state;
    private final long[] suppliers;
    private final int[] soldCommTypes;
    private final float vcpuCapacity;

    /**
     * Create a new instance of the trader.
     *
     * @param originalTrader The original {@link TraderTO}.
     */
    public MinimalOriginalTrader(TraderTO originalTrader) {
        this.oid = originalTrader.getOid();
        this.state = originalTrader.getState();
        this.suppliers = originalTrader.getShoppingListsList().stream()
                .mapToLong(ShoppingListTO::getSupplier)
                .distinct()
                .toArray();
        this.soldCommTypes = originalTrader.getCommoditiesSoldList().stream()
                .mapToInt(c -> c.getSpecification().getType())
                .distinct()
                .toArray();
        this.vcpuCapacity = (float)originalTrader.getCommoditiesSoldList().stream()
                .filter(c -> c.getSpecification().getBaseType() == CommodityDTO.CommodityType.VCPU_VALUE)
                .mapToDouble(CommoditySoldTO::getCapacity)
                .findFirst()
                .orElse(-1);
    }

    public long getOid() {
        return oid;
    }

    public TraderStateTO getState() {
        return state;
    }

    /**
     * Using a specific {@link CloudTopologyConverter}, get the relevant RI Discount tier, if any,
     * that this trader buys from.
     *
     * @param cloudTc The {@link CloudTopologyConverter}.
     * @return Optional containing the discounted RI {@link MarketTier}.
     */
    public Optional<MarketTier> getRiDiscountTier(CloudTopologyConverter cloudTc) {
        for (long supplierId : suppliers) {
            Optional<MarketTier> ret = cloudTc.getRiDiscountedMarketTier(supplierId);
            if (ret.isPresent()) {
                return ret;
            }
        }
        return Optional.empty();
    }

    public int[] getSoldCommTypes() {
        return soldCommTypes;
    }

    /**
     * Return the VCPU sold capacity for this trader, or -1 if this trader does not sell
     * VCPU.
     *
     * @return The VCPU sold capacity, or -1.
     */
    public float getVcpuCapacity() {
        return vcpuCapacity;
    }
}
