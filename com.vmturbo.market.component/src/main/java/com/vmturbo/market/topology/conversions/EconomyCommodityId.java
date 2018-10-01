package com.vmturbo.market.topology.conversions;

import javax.annotation.Nonnull;

import com.vmturbo.platform.analysis.protobuf.CommodityDTOs;

public class EconomyCommodityId {
    /**
     * The {@link CommodityDTOs.CommoditySpecificationTO}s we get back from the market aren't exactly
     * equal to the ones we pass in - for example, the debug info may be absent. We only want to
     * compare the type and base_type.
     */

    final int type;
    final int baseType;

    EconomyCommodityId(@Nonnull final CommodityDTOs.CommoditySpecificationTO economyCommodity) {
        this.type = economyCommodity.getType();
        this.baseType = economyCommodity.getBaseType();
    }

    @Override
    public int hashCode() {
        return type & baseType;
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof EconomyCommodityId) {
            final EconomyCommodityId otherId = (EconomyCommodityId)other;
            return otherId.type == type && otherId.baseType == baseType;
        } else {
            return false;
        }
    }
}