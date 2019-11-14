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

    private final PriceTable priceTable;

    private final SetOnce<Integer> hashCode = new SetOnce<>();

    /**
     * Constructor for the Pricing Data Identifier.
     *
     * @param discount The discount.
     * @param priceTable The price table.
     */
    public PricingDataIdentifier(Optional<Discount> discount, PriceTable priceTable) {
        this.discount = discount;
        this.priceTable = priceTable;
    }

    public Optional<Discount> getDiscount() {
        return this.discount;
    }

    public PriceTable getPriceTable() {
        return this.priceTable;
    }

    @Override
    public int hashCode() {
        return hashCode.ensureSet(() -> Objects.hash(discount.orElse(null), priceTable));
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
                && this.getPriceTable().equals(otherPricingIdentifier.getPriceTable());
    }
}
