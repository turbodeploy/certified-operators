package com.vmturbo.platform.analysis.utilities;

import java.util.Optional;
import java.util.Set;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * A helper class to store the explanation for infinite quote.
 */
public class InfiniteQuoteExplanation {

    /**
     * Whether the infinite quote is due to cost missing or not.
     */
    public boolean costUnavailable;
    /**
     * A set of commodityBundle associated with commodity that failed the placement.
     */
    public Set<CommodityBundle> commBundle;
    /**
     * The seller associated with a given infinity quote, can be optional.
     */
    public Optional<Trader> seller;
    /**
     * The supplier type of the shopping list, can be optional.
     */
    public Optional<Integer> providerType;

    /**
     * Constructor.
     *
     * @param costUnavailable Whether the infinite quote is due to cost missing or not.
     * @param commBundle A set of commodityBundle associated with commodity that failed the placement.
     * @param seller The seller associated with a given infinity quote.
     * @param providerType The supplier type of the shopping list.
     */
    public InfiniteQuoteExplanation(final boolean costUnavailable, final Set<CommodityBundle> commBundle,
                                    final Optional<Trader> seller, final Optional<Integer> providerType) {
        this.costUnavailable = costUnavailable;
        this.commBundle = commBundle;
        this.seller = seller;
        this.providerType = providerType;
    }

    /**
     * A helper class to represent the commodity resources that requested by the buyer, yet can not
     * be satisfied by any seller.
     */
    public static class CommodityBundle {

        /**
         * CommoditySpecification.
         */
        public CommoditySpecification commSpec;
        /**
         * The requested amount of a given commodity.
         */
        public double requestedAmount;
        /**
         * The max quantity available of a given commodity, can be optional if the commodity
         * resource failed due to some constraint(typically in cloud), or if there is no seller.
         */
        public Optional<Double> maxAvailable;

        /**
         * Constructor.
         *
         * @param commSpec CommoditySpecification.
         * @param requestedAmount The requested amount of a given commodity.
         * @param maxAvailable The max quantity available of a given commodity.
         */
        public CommodityBundle(final CommoditySpecification commSpec, final double requestedAmount,
                               final Optional<Double> maxAvailable) {
            this.commSpec = commSpec;
            this.requestedAmount = requestedAmount;
            this.maxAvailable = maxAvailable;
        }
    }
}
