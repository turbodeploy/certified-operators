package com.vmturbo.cost.calculation.topology;

import java.util.Objects;
import java.util.Optional;

import com.vmturbo.common.protobuf.cost.Cost.Discount;
import com.vmturbo.common.protobuf.cost.Pricing.PriceTable;
import com.vmturbo.components.api.SetOnce;

/**
 * A Pricing Data Identifier class representing a unique combination of a price table and a discount.
 */
public class PricingDataIdentifier {
    private final Optional<Discount> discount;

    private final Long priceTableOid;

    private final SetOnce<Integer> hashCode = new SetOnce<>();

    /**
     * Constructor for the Pricing Data Identifier.
     *
     * @param discount The discount.
     * @param priceTableOid price table identifier
     */
    public PricingDataIdentifier(Optional<Discount> discount, Long priceTableOid) {
        this.discount = discount;
        this.priceTableOid = priceTableOid;
    }

    @Override
    public String toString() {
        return "PricingDataIdentifier [hashCode=" + hashCode
                + ", discount.hashCode=" + discount.hashCode()
                + ", priceTableOid=" + priceTableOid
                + "]";
    }

    public Optional<Discount> getDiscount() {
        return this.discount;
    }

    public Long getPriceTableOid() {
        return this.priceTableOid;
    }

    @Override
    public int hashCode() {
        return hashCode.ensureSet(() -> Objects.hash(discount.orElse(null), priceTableOid));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof PricingDataIdentifier)) {
            return false;
        }

        PricingDataIdentifier otherPricingIdentifier = (PricingDataIdentifier)other;

        return this.getDiscount().equals(otherPricingIdentifier.getDiscount())
                && this.getPriceTableOid().equals(otherPricingIdentifier.getPriceTableOid());
    }
}
