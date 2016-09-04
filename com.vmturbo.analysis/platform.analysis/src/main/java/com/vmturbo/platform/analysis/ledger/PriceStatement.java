package com.vmturbo.platform.analysis.ledger;

import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * Store the price index of a {@link Trader}, i.e., the max price among all commodities sold by the
 * {@link Trader}, at two distinct states of the {@link Economy} the start state and the end state.
 *
 * @author shravan
 */
public class PriceStatement {

    private final @NonNull List<@NonNull TraderPriceStatement> traderPriceStatements_ = new ArrayList<>();

    public PriceStatement() {
        // Initializes an empty list of traderPriceStatements
    }

    public class TraderPriceStatement {

        // Fields
        private double startPriceIndex_;
        private double endPriceIndex_;

        // Constructors

        /**
         * Constructs a new TraderPriceStatement instance
         *
         */
        public TraderPriceStatement() {
            startPriceIndex_ = 0;
            endPriceIndex_ = 0;
        }

        // Methods

        /**
        *
        * <p>
        *  Get the start or end price index for the {@link Trader}
        * </p>
        *
        * @param isStart value of true indicates that the we get the startPriceIndex and
        *        false indicates that we set the endPriceIndex
        * @return startPriceIndex or endPriceIndex of the entity
        * @see #setPriceIndex()
        */
        public double getPriceIndex(boolean isStart) {
            return isStart ? startPriceIndex_ : endPriceIndex_;
        }

        /**
        * Sets the value of the <b>startPriceIndex_</b> or <b>endPriceIndex_</b> field.
        *
        * <p>
        *  Has no observable side-effects except setting the above field.
        * </p>
        *
        * @param priceIndex the new value for the field. Must be non-negative.
        * @param isStart value of true indicates that the we set the startPriceIndex and
        *        false indicates that we set the endPriceIndex
        * @return {@code this}
        *
        * @see #getPriceIndex()
        */
        public TraderPriceStatement setPriceIndex(double priceIndex, boolean isStart) {
            if (isStart) {
                startPriceIndex_ = priceIndex;
            } else {
                endPriceIndex_ = priceIndex;
            }
            return this;
        }
    } // end class TraderPriceStatement

    /**
     * Calculates the priceIndex of all the {@link Trader}s in an economy
     *
     * @param economy the {@link Economy} for whose entities we compute the priceIndex
     * @param isStart value of true indicates that we compute the startPriceIndex and
     *        false indicates that we compute the endPriceIndex for all Traders
     * @return {@code this}
     *
     */
    public PriceStatement computePriceIndex(Economy economy, boolean isStart) {
        for (Trader trader : economy.getTraders()) {
            TraderPriceStatement traderPriceStmt = null;
            if (trader.getEconomyIndex() < traderPriceStatements_.size()) {
                traderPriceStmt = traderPriceStatements_.get(trader.getEconomyIndex());
            } else {
                traderPriceStmt = new TraderPriceStatement();
            }
            for (CommoditySold commSold : trader.getCommoditiesSold()) {
                double commSoldUtil = commSold.getQuantity()/commSold.getEffectiveCapacity();
                if (commSoldUtil != 0) {
                    PriceFunction pf = commSold.getSettings().getPriceFunction();
                    Double commPrice = pf.unitPrice(commSoldUtil);
                    if (commPrice > traderPriceStmt.getPriceIndex(isStart)) {
                        traderPriceStmt.setPriceIndex(commPrice, isStart);
                    }
                }
            }
            if (trader.getEconomyIndex() >= traderPriceStatements_.size()) {
                traderPriceStatements_.add(traderPriceStmt);
            }
        }
        return this;
    }

} // end class PriceStatement
