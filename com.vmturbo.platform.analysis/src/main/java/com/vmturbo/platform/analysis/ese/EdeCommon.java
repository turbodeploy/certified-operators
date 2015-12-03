package com.vmturbo.platform.analysis.ese;

import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * EdeCommon contains a number of methods that are common across decisions algorithms in the engine.
 *
 */
public final class EdeCommon {

    /**
     * Calculate the quote of a seller for a basket bought by a buyer.
     *
     * @param buyerParticipation - participation buying specific quantities of the basket commodities
     * @param basket - the basket bought by the buyer participation
     * @param currentSupplier - the current supplier for the buyerParticipation. Can be null.
     * @param seller - the seller that will give the quote
     */
    @Pure
    public static double quote(@NonNull BuyerParticipation buyerParticipation, @NonNull Basket basket,
                    Trader currentSupplier, @NonNull Trader seller) {
        //TODO (Apostolos): we have not dealt with equivalent commodities
        double quote = 0.0;
        boolean isCurrentSupplier = seller == currentSupplier;

        // get the quantity and peak quantity to buy for each commodity of the basket
        final double[] quantities = buyerParticipation.getQuantities();
        final double[] peakQuantities = buyerParticipation.getPeakQuantities();

        // go over all commodities in basket
        for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basket.size(); boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);

            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.isSatisfiedBy(seller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }
            CommoditySold commSold = seller.getCommoditiesSold().get(soldIndex);

            // add quantities bought by buyer, to quantities already used at seller
            final double effectiveCapacity = commSold.getEffectiveCapacity();
            final double newQuantity = isCurrentSupplier ? commSold.getQuantity()
                                     : quantities[boughtIndex] + commSold.getQuantity();
            final double newPeakQuantity = isCurrentSupplier ? commSold.getPeakQuantity()
                                         : peakQuantities[boughtIndex] + commSold.getPeakQuantity();
            final double utilUpperBound = commSold.getSettings().getUtilizationUpperBound();
            final double excessQuantity = peakQuantities[boughtIndex] - quantities[boughtIndex];

            if (newQuantity > effectiveCapacity || newPeakQuantity > effectiveCapacity) {
                return Double.POSITIVE_INFINITY;
            }

            // calculate the price per unit for quantity and peak quantity
            final PriceFunction pf = commSold.getSettings().getPriceFunction();
            final double priceUsed = pf.unitPrice(newQuantity/effectiveCapacity);
            final double pricePeak = pf.unitPrice(Math.max(0, newPeakQuantity-newQuantity)
                                   / (effectiveCapacity - utilUpperBound*newQuantity));

            // calculate quote
            // TODO: decide what to do if peakQuantity is less than quantity
            quote += (quantities[boughtIndex]*priceUsed + (excessQuantity > 0 ? excessQuantity*pricePeak : 0))
                   / commSold.getCapacity();
        }
        return quote;
    }

    static class QuoteMinimizer {
        private final @NonNull Economy economy_;
        private final @NonNull List<StateItem> state_;
        private final long time_;
        private final @NonNull BuyerParticipation participation_;
        private final @NonNull Basket basket_;
        private final Trader supplier_;

        private Trader bestSeller_;
        private double bestQuote_ = Double.POSITIVE_INFINITY;
        private double currentQuote_ = Double.POSITIVE_INFINITY;

        public QuoteMinimizer(@NonNull Economy economy, @NonNull List<StateItem> state, long time,
                @NonNull BuyerParticipation participation, @NonNull Basket basket, Trader supplier) {
            economy_ = economy;
            state_ = state;
            time_ = time;
            participation_ = participation;
            basket_ = basket;
            supplier_ = supplier;

            bestSeller_ = supplier;
        }

        @Pure
        public double bestQuote(@ReadOnly QuoteMinimizer this) {
            return bestQuote_;
        }

        @Pure
        public Trader bestSeller(@ReadOnly QuoteMinimizer this) {
            return bestSeller_;
        }

        @Pure
        public double currentQuote(@ReadOnly QuoteMinimizer this) {
            return currentQuote_;
        }

        public void accept(@NonNull Trader seller) {
            // if we cannot move to this seller and it is not the current supplier, skip it
            if (seller != supplier_
                && time_ < state_.get(economy_.getIndex(seller)).getMoveToOnlyAfterThisTime()) {
                return;
            }

            final double quote = EdeCommon.quote(participation_, basket_, supplier_, seller);

            if (seller == supplier_) {
                currentQuote_ = quote;
            }

            // keep the minimum between quotes
            if (quote < bestQuote_) {
                bestQuote_ = quote;
                bestSeller_ = seller;
            }
        }

        public void combine(@NonNull @ReadOnly QuoteMinimizer other) {
            if (other.bestQuote_ < bestQuote_) {
                bestQuote_ = other.bestQuote_;
                bestSeller_ = other.bestSeller_;
            }

            if (other.currentQuote_ != Double.POSITIVE_INFINITY) {
                currentQuote_ = other.currentQuote_;
            }
        }
    } // end QuoteMinimizer class

} // end EdeCommon class
