package com.vmturbo.platform.analysis.ledger;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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

    // Constructor

    /**
     * Constructs a new Ledger instance that is going to hold all the incomeStatements
     * of every trader in the economy and the commoditiesSold by all these traders
     *
     */
    public Ledger(@NonNull Economy  economy) {

        economy.getTraders().forEach(trader->{addTraderIncomeStatement(trader);});

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
        commodityIncomeStatements_.remove(trader);

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
        market.getActiveSellers().forEach(seller -> calculateExpRevForSeller(economy, seller));
        return this;
    }

    /**
     * computes the {@link IncomeStatement} for a seller
     *
     * @param economy {@link Economy} where the <b>seller</b> trades
     * @param seller the {@link Trader} whose expenses and revenues are to be computed
     */
    public void calculateExpRevForSeller(Economy economy, Trader seller) {

        IncomeStatement sellerIncomeStmt = getTraderIncomeStatements().get(seller.getEconomyIndex());
        sellerIncomeStmt.resetIncomeStatement();
        double sellerMaxDesUtil = seller.getSettings().getMaxDesiredUtil();
        double sellerMinDesUtil = seller.getSettings().getMinDesiredUtil();
        // compute the revenues based on the utilization of the commoditiesSold
        for (CommoditySold commSold : seller.getCommoditiesSold()) {
            double commSoldUtil = commSold.getQuantity()/commSold.getEffectiveCapacity();
            if (commSoldUtil != 0) {
                PriceFunction pf = commSold.getSettings().getPriceFunction();
                sellerIncomeStmt.setRevenues(sellerIncomeStmt.getRevenues()
                                    + pf.unitPrice(commSoldUtil)*commSoldUtil)
                    .setMinDesiredRevenues(sellerIncomeStmt.getMinDesiredRevenues()
                                    + pf.unitPrice(sellerMinDesUtil)*sellerMinDesUtil)
                    .setMaxDesiredRevenues(sellerIncomeStmt.getMaxDesiredRevenues()
                                    + pf.unitPrice(sellerMaxDesUtil)*sellerMaxDesUtil);
                if (Double.isInfinite(sellerIncomeStmt.getRevenues())) {
                    break;
                }
            }
        }

        double[] quote = {0,0,0};
        // calculate expenses using the quote from every supplier that this trader buys from
        for (ShoppingList sl : economy.getMarketsAsBuyer(seller).keySet()) {
            // skip markets in which the trader in question is unplaced and
            // not consider expense for this trader in those markets
            if (sl.getSupplier() != null) {
                double[] tempQuote = EdeCommon.quote(economy, sl, sl.getSupplier()
                                        , Double.POSITIVE_INFINITY, true);
                for (int i=0; i<3; i++) {
                    quote[i] = quote[i] + tempQuote[i];
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
     * computes the {@link IncomeStatement} for every resizable commodity sold by all the traders
     * present in the economy and updates the commodityIncomeStatement list
     *
     * @param economy the {@link Economy} which contains all the commodities whose income
     * statements are to be calculated
     *
     * @return The ledger containing the updated list of commodityIncomeStatements
     */
    public @NonNull Ledger calculateAllCommodityExpensesAndRevenues(@NonNull Economy economy) {

        for (Trader buyer : economy.getTraders()) {

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
                double commSoldUtil = cs.getUtilization();
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
                                                        /commSoldBySeller.getCapacity();
                        if (commBoughtUtil != 0) {
                            PriceFunction priceFunction = commSoldBySeller.getSettings()
                                                                          .getPriceFunction();

                            commSoldIS.setExpenses(commSoldIS.getExpenses()
                                          + priceFunction.unitPrice(commSoldBySeller.getQuantity()
                                                 /commSoldBySeller.getEffectiveCapacity())
                                                     *commBoughtUtil);
                            commSoldIS.setMaxDesiredExpenses(commSoldIS.getMaxDesiredExpenses()
                                          + priceFunction.unitPrice(maxDesUtil)*commBoughtUtil);
                            commSoldIS.setMinDesiredExpenses(commSoldIS.getMinDesiredExpenses()
                                          + priceFunction.unitPrice(minDesUtil)*commBoughtUtil);
                        }
                    }
                }
            }
        }

        return this;
    }

} // end class Ledger
