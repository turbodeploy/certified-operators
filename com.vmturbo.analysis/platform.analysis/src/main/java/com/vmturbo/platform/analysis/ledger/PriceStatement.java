package com.vmturbo.platform.analysis.ledger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * Store the price index of a {@link Trader}, i.e., the max price among all commodities sold by the
 * {@link Trader}, at two distinct states of the {@link UnmodifiableEconomy} the start state and the end state.
 *
 * @author shravan
 */
public class PriceStatement {

    private final @NonNull List<@NonNull TraderPriceStatement> traderPriceStatements_ = new ArrayList<>();

    // Cached unmodifiable view of the traderPriceStatements_ list.
    private final @NonNull List<@NonNull TraderPriceStatement> unmodifiabletTraderPriceStatements_
                                = Collections.unmodifiableList(traderPriceStatements_);

    public PriceStatement() {
        // Initializes an empty list of traderPriceStatements
    }

    public class TraderPriceStatement {

        // Fields
        private double priceIndex_;

        // Constructors

        /**
         * Constructs a new TraderPriceStatement instance
         *
         */
        public TraderPriceStatement() {
            priceIndex_ = 0;
        }

        // Methods

        /**
        *
        * <p>
        *  Get the start or end price index for the {@link Trader}
        * </p>
        *
        * @return priceIndex of the entity
        * @see #setPriceIndex(double)
        */
        public double getPriceIndex() {
            return priceIndex_;
        }

        /**
        * Sets the value of the <b>priceIndex_</b> field.
        *
        * <p>
        *  Has no observable side-effects except setting the above field.
        * </p>
        *
        * @param priceIndex the new value for the field. Must be non-negative.
        * @return {@code this}
        *
        * @see #getPriceIndex()
        */
        public TraderPriceStatement setPriceIndex(double priceIndex) {
            priceIndex_ = priceIndex;
            return this;
        }
    } // end class TraderPriceStatement

    /**
     * Returns a unmodifiable list of {@link TraderPriceStatement}s
     *
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly TraderPriceStatement> getTraderPriceStatements
                                                      (@ReadOnly PriceStatement this) {
        return unmodifiabletTraderPriceStatements_;
    }

    /**
     * Calculates the priceIndex of all the {@link Trader}s in an economy
     *
     * @param economy the {@link UnmodifiableEconomy} for whose entities we compute the priceIndex
     * @return {@code this}
     *
     */
    public PriceStatement computePriceIndex(@NonNull final UnmodifiableEconomy economy) {
        for (Trader trader : economy.getTraders()) {
            TraderPriceStatement traderPriceStmt = new TraderPriceStatement();
            traderPriceStatements_.add(traderPriceStmt);
            for (CommoditySold commSold : trader.getCommoditiesSold()) {
                double commSoldUtil = commSold.getQuantity()/commSold.getEffectiveCapacity();
                PriceFunction pf = commSold.getSettings().getPriceFunction();
                Double commPrice = pf.unitPrice(commSoldUtil, null, trader, commSold, economy);
                if (commPrice > traderPriceStmt.getPriceIndex()) {
                    traderPriceStmt.setPriceIndex(commPrice);
                }
            }
        }
        return this;
    }

} // end class PriceStatement
