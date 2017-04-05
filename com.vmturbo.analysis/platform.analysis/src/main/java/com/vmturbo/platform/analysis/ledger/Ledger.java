package com.vmturbo.platform.analysis.ledger;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.EdeCommon;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * A bookkeeper of the expenses and revenues of all the traders and the commodities that are
 * present in the economy
 *
 * <p>
 *  The 2 lists present maintain the IncomeStatements of all the traders and IncomeStatements of
 *  the commoditiesSold by these traders
 * </p>
 */
public class Ledger {
    // Fields
    // The list of all IncomeStatements participating in the Ledger.
    private final @NonNull List<@NonNull IncomeStatement> traderIncomeStatements_ = new ArrayList<>();

    // The list of IncomeStatements of commoditiesSold, indexed by the economyIndex of a trader
    private final @NonNull ArrayList<@NonNull ArrayList<IncomeStatement>> commodityIncomeStatements_
                                                                                = new ArrayList<>();

    // Cached data

    // Cached unmodifiable view of the traderIncomeStatements list.
    private final @NonNull List<@NonNull IncomeStatement> unmodifiableTraderIncomeStatements_
                                                                = Collections.unmodifiableList
                                                                        (traderIncomeStatements_);
    // Cached unmodifiable view of the commodityIncomeStatements of all traders
    private final @NonNull List<@NonNull ArrayList<IncomeStatement>>
                                                           unmodifiableCommodityIncomeStatements_
                                                                = Collections.unmodifiableList
                                                                    (commodityIncomeStatements_);

    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly IncomeStatement> getTraderIncomeStatements
                                                                            (@ReadOnly Ledger this) {
        return unmodifiableTraderIncomeStatements_;
    }

    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly IncomeStatement>
                                                            getCommodityIncomeStatements
                                                                (@ReadOnly Ledger this,
                                                                 @NonNull @ReadOnly Trader trader) {
        return Collections.unmodifiableList(unmodifiableCommodityIncomeStatements_
                                                .get(trader.getEconomyIndex()));
    }

    private static final Logger logger = LogManager.getLogger(Ledger.class);

    // Constructor

    /**
     * Constructs a new Ledger instance that is going to hold all the incomeStatements
     * of every trader in the economy and the commoditiesSold by all these traders
     *
     */
    public Ledger(@NonNull Economy  economy) {

        economy.getTraders().forEach(trader-> addTraderIncomeStatement(trader));

    }

    // Methods

    /**
     * Adds a new {@link IncomeStatement} for a trader to the traderIncomeStatement list that the
     * ledger maintains we also add a IncomeStatement for every commodity that this trader sells
     *
     * @param trader this is the new entity that is being added to the economy
     *
     * @return The incomeStatement that was created for this newly added trader
     */
    @Deterministic
    public @NonNull IncomeStatement addTraderIncomeStatement(@NonNull Trader trader) {

        IncomeStatement traderIncomeStatement = new IncomeStatement();
        traderIncomeStatements_.add(traderIncomeStatement);

        checkArgument(traderIncomeStatements_.indexOf(traderIncomeStatement)
                            == trader.getEconomyIndex());
        int eIndex = trader.getEconomyIndex();

        if (commodityIncomeStatements_.size() <= eIndex) {
            commodityIncomeStatements_.add(eIndex, new ArrayList<IncomeStatement>());
        }

        // adding an incomeStatement per commoditySold
        trader.getCommoditiesSold().forEach(commSold->commodityIncomeStatements_.get(eIndex)
                                                            .add(new IncomeStatement()));

        return traderIncomeStatement;
    }

    private @NonNull Ledger resetTraderIncomeStatement(Trader trader) {
        int eIndex = trader.getEconomyIndex();
        IncomeStatement traderIncomeStmt = traderIncomeStatements_.get(eIndex);
        traderIncomeStmt.resetIncomeStatement();

        for (int i = 0; i < trader.getCommoditiesSold().size(); i++) {
            commodityIncomeStatements_.get(eIndex).get(i).resetIncomeStatement();
        }

        return this;
    }

    /**
     * removes an {@link IncomeStatement} for a trader from the traderIncomeStatement list that the
     * ledger maintains we also remove the IncomeStatements for every commodity that this trader
     * sells from the commodityIncomeStatement list
     *
     * @param trader this is the entity that is being removed from the economy
     *
     * @return The incomeStatement that was created for this newly added trader
     */
    public @NonNull Ledger removeTraderIncomeStatement(@NonNull Trader trader) {
        traderIncomeStatements_.remove(trader.getEconomyIndex());
        commodityIncomeStatements_.remove(trader.getEconomyIndex());
        return this;
    }

    /**
     * computes the {@link IncomeStatement} for every seller present in a particular market and
     * updates the traderIncomeStatement list accordingly
     *
     * @param economy {@link Economy} where the <b>seller</b> trades
     * @param market the {@link Market} whose sellers expenses and revenues are to be computed
     * @return The Ledger containing the updated list of traderIncomeStatements
     */
    public Ledger calculateExpAndRevForSellersInMarket(Economy economy, Market market) {
        List<Trader> sellers = market.getActiveSellers();
        (sellers.size() < economy.getSettings().getMinSellersForParallelism()
            ? sellers.stream() : sellers.parallelStream())
            .forEach(seller -> calculateExpRevForTrader(economy, seller));
        return this;
    }

    /**
     * Computes the expenses for all buyers in the {@link Market}
     *
     * @param economy {@link Economy} where the <b>seller</b> trades
     * @param market the {@link Market} whose buyer's expenses and revenues are to be computed
     * @return The Ledger containing the updated list of traderIncomeStatements
     */
    public Ledger calculateExpensesForBuyersInMarket(Economy economy, Market market) {
        List<ShoppingList> buyers = market.getBuyers();
        (buyers.size() < economy.getSettings().getMinSellersForParallelism()
            ? buyers.stream() : buyers.parallelStream())
            .forEach(shoppingList -> {
                resetTraderIncomeStatement(shoppingList.getBuyer());
                calculateExpensesForBuyer(economy, shoppingList);
            });
        return this;
    }

    /**
     * computes the {@link IncomeStatement} for a seller
     *
     * @param economy {@link Economy} where the <b>seller</b> trades
     * @param seller the {@link Trader} whose expenses and revenues are to be computed
     */
    public void calculateExpRevForTrader(Economy economy, Trader seller) {

        IncomeStatement sellerIncomeStmt = getTraderIncomeStatements().get(seller.getEconomyIndex());
        sellerIncomeStmt.resetIncomeStatement();
        double sellerMaxDesUtil = seller.getSettings().getMaxDesiredUtil();
        double sellerMinDesUtil = seller.getSettings().getMinDesiredUtil();
        // compute the revenues based on the utilization of the commoditiesSold
        CommoditySold[] topSellingComm = {null, null};
        double[] topRev = new double[2];
        // Find the top 2 commodities that generate the most revenue
        for (CommoditySold commSold : seller.getCommoditiesSold()) {
            double commSoldUtil = commSold.getQuantity()/commSold.getEffectiveCapacity();
            if (commSoldUtil != 0) {
                PriceFunction pf = commSold.getSettings().getPriceFunction();
                double revFromComm = pf.unitPrice(commSoldUtil) * commSoldUtil;
                if (topRev[0] < revFromComm) {
                    topRev[1] = topRev[0];
                    topRev[0] = revFromComm;
                    topSellingComm[1] = topSellingComm[0];
                    topSellingComm[0] = commSold;
                } else if (topRev[1] < revFromComm) {
                    topRev[1] = revFromComm;
                    topSellingComm[1] = commSold;
                }
            }
        }
        // compute total revenue with the top 2 commodities
        double[] tempCurrMinMaxRev = new double[3];
        IntStream.range(0, topRev.length).forEach(i -> {
            if (Double.isFinite(tempCurrMinMaxRev[0])) {
                CommoditySold cs = topSellingComm[i];
                if (cs != null) {
                    PriceFunction pf = cs.getSettings().getPriceFunction();
                    tempCurrMinMaxRev[0] = tempCurrMinMaxRev[0] + topRev[i];
                    tempCurrMinMaxRev[1] = tempCurrMinMaxRev[1] + pf.unitPrice(sellerMinDesUtil);
                    tempCurrMinMaxRev[2] = tempCurrMinMaxRev[2] + pf.unitPrice(sellerMaxDesUtil);
                }
            }
        });

        sellerIncomeStmt.setRevenues(tempCurrMinMaxRev[0])
                        .setMinDesiredRevenues(tempCurrMinMaxRev[1] * sellerMinDesUtil)
                        .setMaxDesiredRevenues(tempCurrMinMaxRev[2] * sellerMaxDesUtil);

        calculateExpensesForSeller(economy, seller);
    }

    /**
     * Computes the expenses and revenues for the traders in this {@link Economy}
     *
     * @param economy {@link Economy} where the <b>seller</b> trades
     */
    public void calculateExpRevForTradersInEconomy (Economy economy) {
        for (Trader trader : economy.getTraders()) {
            calculateExpRevForTrader(economy, trader);
        }
    }

    /**
     * Computes the expenses for the given seller
     *
     * @param economy {@link Economy} where the <b>seller</b> trades
     * @param seller the {@link Trader} whose expenses are to be computed
     */
    private void calculateExpensesForSeller(Economy economy, Trader seller) {
        IncomeStatement sellerIncomeStmt = getTraderIncomeStatements().get(seller.getEconomyIndex());
        double[] quote = {0,0,0};
        // calculate expenses using the quote from every supplier that this trader buys from
        for (ShoppingList sl : economy.getMarketsAsBuyer(seller).keySet()) {
            // skip markets in which the trader in question is unplaced and
            // not consider expense for this trader in those markets
            if (sl.getSupplier() != null) {
                try {
                    double[] tempQuote = EdeCommon.quote(economy, sl, sl.getSupplier()
                                            , Double.POSITIVE_INFINITY, true);
                    for (int i=0; i<3; i++) {
                        quote[i] = quote[i] + tempQuote[i];
                    }
                } catch (IndexOutOfBoundsException e) {
                    // TODO (shravan): move try catch inside edeCommon
                    // setting expense to INFINITY
                    for (int i=0; i<3; i++) {
                        quote[i] = Double.POSITIVE_INFINITY;
                    }
                    logger.warn("seller " + seller.getDebugInfoNeverUseInCode() + " placed on the "
                            + "wrong provider " + sl.getSupplier().getDebugInfoNeverUseInCode() +
                            " hence returning INFINITE expense");
                }
            }
            if (Double.isInfinite(quote[0])) {
                break;
            }
        }
        // update trader's minDesExpRev only if the expense(quote[0] here) is > 0
        if (quote[0] != 0) {
            sellerIncomeStmt.setExpenses(quote[0])
                            .setMinDesiredExpenses(quote[1])
                            .setMaxDesiredExpenses(quote[2]);
        }
    }

    /**
     * Computes the expenses for the given buyer
     *
     * @param economy {@link Economy} where the <b>buyer</b> trades
     * @param shoppingList the {@link ShoppingList} whose expenses are to be computed
     */
    private void calculateExpensesForBuyer(Economy economy, ShoppingList shoppingList) {
        IncomeStatement buyerIncomeStmt = getTraderIncomeStatements().get(shoppingList.getBuyer().getEconomyIndex());
        double[] quote = {0,0,0};
        // skip markets in which the trader in question is unplaced and
        // not consider expense for this trader in those markets
        if (shoppingList.getSupplier() != null) {
            try {
                double[] tempQuote = EdeCommon.quote(economy, shoppingList, shoppingList.getSupplier()
                                        , Double.POSITIVE_INFINITY, true);
                for (int i=0; i<3; i++) {
                    quote[i] = quote[i] + tempQuote[i];
                }
            } catch (IndexOutOfBoundsException e) {
                // TODO (shravan): move try catch inside edeCommon
                // setting expense to INFINITY
                for (int i=0; i<3; i++) {
                    quote[i] = Double.POSITIVE_INFINITY;
                }
                logger.warn("seller " + shoppingList.getBuyer().getDebugInfoNeverUseInCode() + " placed on the "
                        + "wrong provider " + shoppingList.getSupplier().getDebugInfoNeverUseInCode() +
                        " hence returning INFINITE expense");
            }
        }

        // update trader's minDesExpRev only if the expense(quote[0] here) is > 0
        if (quote[0] != 0) {
            buyerIncomeStmt.setExpenses(buyerIncomeStmt.getExpenses() + quote[0])
                            .setMinDesiredExpenses(buyerIncomeStmt.getMinDesiredExpenses() + quote[1])
                            .setMaxDesiredExpenses(buyerIncomeStmt.getMaxDesiredExpenses() + quote[2]);
        }
    }

    /**
     * computes the {@link IncomeStatement} for every resizable commodity sold by all the traders
     * present in the economy and updates the commodityIncomeStatement list
     *
     * @param economy the {@link Economy} which contains all the commodities whose income
     * statements are to be calculated
     *
     * @return The ledger containing the updated list of commodityIncomeStatements
     */
    public @NonNull Ledger calculateAllCommodityExpensesAndRevenues(@NonNull Economy economy) {
        List<Trader> traders = economy.getTraders();
        (traders.size() < economy.getSettings().getMinSellersForParallelism()
            ? traders.stream() : traders.parallelStream()).forEach(buyer -> {
            resetTraderIncomeStatement(buyer);
            List<CommoditySold> commSoldList = buyer.getCommoditiesSold();
            for (int commSoldIndex = 0; commSoldIndex < commSoldList.size(); commSoldIndex++) {
                CommoditySold cs = commSoldList.get(commSoldIndex);
                // compute rev/exp for resizable commodities
                if (!cs.getSettings().isResizable()) {
                    continue;
                }

                IncomeStatement commSoldIS = commodityIncomeStatements_.get(
                                buyer.getEconomyIndex()).get(commSoldIndex);
                // rev of consumer is utilOfCommSold*priceFn(utilOfCommSold)
                // expense of this consumer is utilOfCommBought*priceFn(utilOfCommBought)
                double maxDesUtil = buyer.getSettings().getMaxDesiredUtil();
                double minDesUtil = buyer.getSettings().getMinDesiredUtil();
                double commSoldUtil = cs.getUtilization() / cs.getSettings()
                                        .getUtilizationUpperBound();
                PriceFunction pf = cs.getSettings().getPriceFunction();

                if (Double.isNaN(commSoldUtil)) {
                    continue;
                }

                commSoldIS.setRevenues(pf.unitPrice(commSoldUtil)*commSoldUtil);
                commSoldIS.setMaxDesiredRevenues(pf.unitPrice(maxDesUtil)*maxDesUtil);
                commSoldIS.setMinDesiredRevenues(pf.unitPrice(minDesUtil)*minDesUtil);

                List<Integer> typeOfCommsBought = economy.getRawMaterials(buyer.getBasketSold()
                                                             .get(commSoldIndex).getBaseType());

                if (typeOfCommsBought == null) {
                    continue;
                }

                for (ShoppingList shoppingList : economy.getMarketsAsBuyer(buyer).keySet()) {
                    Basket basketBought = shoppingList.getBasket();

                    Trader supplier = shoppingList.getSupplier();
                    if (supplier == null) {
                        continue;
                    }
                    // TODO: make indexOf return 2 values minIndex and the maxIndex.
                    // All comm's btw these indices will be of this type
                    // (needed when we have 2 comms of same type bought)
                    for (Integer typeOfCommBought : typeOfCommsBought) {
                        int boughtIndex = basketBought.indexOfBaseType(typeOfCommBought.intValue());

                        // if the required commodity is not in the shoppingList, skip the list
                        if (boughtIndex == -1) {
                            continue;
                        }

                        // find the right provider comm and use it to compute the expenses
                        CommoditySold commSoldBySeller = supplier.getCommoditySold(basketBought
                                                                           .get(boughtIndex));
                        // using capacity to remain consistent with quote
                        double commBoughtUtil = shoppingList.getQuantity(boughtIndex)
                                                        /commSoldBySeller.getEffectiveCapacity();
                        if (commBoughtUtil != 0) {
                            PriceFunction priceFunction = commSoldBySeller.getSettings()
                                                                          .getPriceFunction();

                            commSoldIS.setExpenses(commSoldIS.getExpenses()
                                          + priceFunction.unitPrice(commSoldBySeller.getQuantity()
                                                 /commSoldBySeller.getEffectiveCapacity())
                                                     *commBoughtUtil);
                            commSoldIS.setMaxDesiredExpenses(commSoldIS.getMaxDesiredExpenses()
                                          + priceFunction.unitPrice(maxDesUtil) * commBoughtUtil);
                            commSoldIS.setMinDesiredExpenses(commSoldIS.getMinDesiredExpenses()
                                          + priceFunction.unitPrice(minDesUtil) * commBoughtUtil);
                        }
                    }
                }
            }
        });
        return this;
    }

    /**
     * Calculate the total expenses for buyers in the given {@link Market}
     *
     * @param economy The {@link Economy}
     * @param market The {@link Market}
     * @return total expenses for the buyers in the given {@link Market}
     */
    public double calculateTotalExpensesForBuyers(Economy economy, Market market) {
        calculateExpensesForBuyersInMarket(economy, market);
        double totalExpenses = 0;
        Set<Trader> marketBuyers = new HashSet<>();
        market.getBuyers().stream().forEach(sl -> marketBuyers.add(sl.getBuyer()));
        for (Trader buyer: marketBuyers) {
             totalExpenses += getTraderIncomeStatements().get(buyer.getEconomyIndex())
                                 .getExpenses();
             if (Double.isInfinite(totalExpenses)) {
                 break;
             }
        }
        return totalExpenses;
    }

} // end class Ledger
