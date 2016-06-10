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
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * A bookkeeper of the expenses and revenues of all the traders and the commodities that are present in the economy
 *
 * <p>
 *  The 2 lists present maintain the IncomeStatements of all the traders and IncomeStatements of the commoditiesSold by these traders
 * </p>
 */
public class Ledger {
    // Fields
    // The list of all IncomeStatements participating in the Ledger.
    private final @NonNull List<@NonNull IncomeStatement> traderIncomeStatements_ = new ArrayList<>();

    // The list of IncomeStatements of commoditiesSold, indexed by the economyIndex of a trader
    private final @NonNull ArrayList<@NonNull ArrayList<IncomeStatement>> commodityIncomeStatements_ = new ArrayList<>();

    // Cached data

    // Cached unmodifiable view of the traderIncomeStatements list.
    private final @NonNull List<@NonNull IncomeStatement> unmodifiableTraderIncomeStatements_ = Collections.unmodifiableList
                                                                                                            (traderIncomeStatements_);
    // Cached unmodifiable view of the commodityIncomeStatements of all traders
    private final @NonNull List<@NonNull ArrayList<IncomeStatement>>
                                                       unmodifiableCommodityIncomeStatements_ = Collections.unmodifiableList
                                                                                                            (commodityIncomeStatements_);

    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly IncomeStatement> getTraderIncomeStatements(@ReadOnly Ledger this) {
        return unmodifiableTraderIncomeStatements_;
    }

    @Pure
    public @NonNull @ReadOnly List<@NonNull @ReadOnly IncomeStatement> getCommodityIncomeStatements(@ReadOnly Ledger this, @NonNull @ReadOnly Trader trader) {
        return Collections.unmodifiableList(unmodifiableCommodityIncomeStatements_.get(trader.getEconomyIndex()));
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
     * Adds a new {@link IncomeStatement} for a trader to the traderIncomeStatement list that the ledger maintains
     * we also add a IncomeStatement for every commodity that this trader sells
     *
     * @param trader this is the new entity that is being added to the economy
     *
     * @return The incomeStatement that was created for this newly added trader
     */
    @Deterministic
    private @NonNull IncomeStatement addTraderIncomeStatement(@NonNull Trader trader) {

        IncomeStatement traderIncomeStatement = new IncomeStatement();
        traderIncomeStatements_.add(traderIncomeStatement);

        checkArgument(traderIncomeStatements_.indexOf(traderIncomeStatement) == trader.getEconomyIndex());
        int eIndex = trader.getEconomyIndex();

        if (commodityIncomeStatements_.size() <= eIndex) {
            commodityIncomeStatements_.add(eIndex, new ArrayList<IncomeStatement>());
        }

        // adding an incomeStatement per commoditySold
        trader.getCommoditiesSold().forEach(commSold->{commodityIncomeStatements_.get(eIndex).add(new IncomeStatement());});

        return traderIncomeStatement;
    }


    /**
     * Adds a new {@link IncomeStatement} for a trader to the traderIncomeStatement list that the ledger maintains
     * we also add a IncomeStatement for every commodity that this trader sells to the commodityIncomeStatement list
     *
     * @param trader this is the new entity that is being added to the economy
     * @param commodityIndex is the index of the commodity in the basket that the trader sells
     *
     * @return The incomeStatement that was created for the commodity
     */
    @Deterministic
    private @NonNull IncomeStatement addCommodityIncomeStatement(Trader trader, int commodityIndex) {

        int eIndex = trader.getEconomyIndex();

        if (commodityIncomeStatements_.size() <= eIndex) {
            commodityIncomeStatements_.add(eIndex, new ArrayList<IncomeStatement>());
        }

        IncomeStatement commodityIncomeStatement = new IncomeStatement();
        commodityIncomeStatements_.get(eIndex).add(commodityIncomeStatement);

        checkArgument(commodityIncomeStatements_.get(eIndex).indexOf(commodityIncomeStatement) == commodityIndex);
        return commodityIncomeStatement;
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
     * removes an {@link IncomeStatement} for a trader from the traderIncomeStatement list that the ledger maintains
     * we also remove the IncomeStatements for every commodity that this trader sells from the commodityIncomeStatement list
     *
     * @param trader this is the entity that is being removed from the economy
     *
     * @return The incomeStatement that was created for this newly added trader
     */
    @NonNull Ledger removeTraderIncomeStatement(@NonNull Trader trader) {
        traderIncomeStatements_.remove(trader.getEconomyIndex());
        commodityIncomeStatements_.remove(trader);

        return this;
    }

    /**
     * computes the {@link IncomeStatement} for every trader present in the economy and updates the traderIncomeStatement list
     *
     * @param economy {@link Economy} which contains all the traders whose income statements are to be calculated
     *
     * @return The Ledger containing the updated list of traderIncomeStatements
     */
    public @NonNull Ledger calculateAllTraderExpensesAndRevenues (@NonNull Economy economy) {
        List<Trader> traders = economy.getTraders();

        calculateAllCommodityExpensesAndRevenues(economy);

        for (Trader trader : traders) {
            int eIndex = trader.getEconomyIndex();
            IncomeStatement traderIncomeStmt = traderIncomeStatements_.get(eIndex);
            double traderExpense = traderIncomeStmt.getExpenses();
            double traderRevenue = traderIncomeStmt.getRevenues();
            for (IncomeStatement commIS : commodityIncomeStatements_.get(eIndex)) {
                traderExpense += commIS.getExpenses();
                traderRevenue += commIS.getRevenues();
            }
            traderIncomeStmt.setExpenses(traderExpense);
            traderIncomeStmt.setRevenues(traderRevenue);
        }

        return this;
    }

    /**
     * computes the {@link IncomeStatement} for every resizable commodity spld by all the traders present in the economy and updates the
     * commodityIncomeStatement list
     *
     * @param economy the {@link Economy} which contains all the commodities whose income statements are to be calculated
     *
     * @return The ledger containing the updated list of commodityIncomeStatements
     */
    @NonNull
    public Ledger calculateAllCommodityExpensesAndRevenues(@NonNull Economy economy) {

        for (Trader buyer : economy.getTraders()) {

            resetTraderIncomeStatement(buyer);
            List<CommoditySold> commSoldList = buyer.getCommoditiesSold();
            for (CommoditySold cs : commSoldList) {
                int indexOfCommSold = commSoldList.indexOf(cs);
                IncomeStatement consumerCommIS = commodityIncomeStatements_.get(buyer.getEconomyIndex()).get(indexOfCommSold);
                // rev of consumer is utilOfCommSold*priceFn(utilOfCommSold)
                // expense of this consumer is utilOfCommBought*priceFn(utilOfCommBought)
                double maxDesiredUtil = buyer.getSettings().getMaxDesiredUtil();
                double minDesiredUtil = buyer.getSettings().getMinDesiredUtil();

                consumerCommIS.setRevenues(cs.getSettings().getPriceFunction().unitPrice(cs.getUtilization())*cs.getUtilization());
                consumerCommIS.setMaxDesiredRevenues(cs.getSettings().getPriceFunction().unitPrice(maxDesiredUtil)*cs.getUtilization());
                consumerCommIS.setMinDesiredRevenues(cs.getSettings().getPriceFunction().unitPrice(minDesiredUtil)*cs.getUtilization());
                // type of mem from type of vMem for example
                List<Integer> typeOfCommsBought = economy.getRawMaterials(buyer.getBasketSold().get(indexOfCommSold).getType());

                if (typeOfCommsBought == null) {
                    continue;
                }

                // reset trader and associated commodity InsomeStatement before we compute the exp and rev of all commodities

                for (ShoppingList shoppingList : economy.getMarketsAsBuyer(buyer).keySet()) {
                    Basket basketBought = shoppingList.getBasket();

                    // TODO: make indexOf return 2 values minIndex and the maxIndex. All comm's btw these indices will be of this type
                    // (needed when we have 2 comms of same type sold)
                    for (Integer typeOfCommBought : typeOfCommsBought) {
                        int boughtIndex = basketBought.indexOf(typeOfCommBought.intValue());

                        // if the required commodity is not in the shopping list skip shoppingList
                        if (boughtIndex == -1) {
                            continue;
                        }

                        Trader supplier = shoppingList.getSupplier();
                        CommoditySpecification basketCommSpec = basketBought.get(boughtIndex);

                        // find the right provider comm and use it to compute the expenses
                        int soldIndex = 0;
                        while (basketCommSpec.getType() != supplier.getBasketSold().get(soldIndex).getType()) {
                            soldIndex++;
                        }
                        CommoditySold commSoldBySeller = supplier.getCommoditiesSold().get(soldIndex);
                        double commBoughtUtil = shoppingList.getQuantity(boughtIndex)/commSoldBySeller.getEffectiveCapacity();
                        double price = commSoldBySeller.getSettings().getPriceFunction().unitPrice(commBoughtUtil);

                        consumerCommIS.setExpenses(consumerCommIS.getExpenses() + price*commBoughtUtil);
                        consumerCommIS.setMaxDesiredExpenses(consumerCommIS.getMaxDesiredExpenses() + price*maxDesiredUtil);
                        consumerCommIS.setMinDesiredExpenses(consumerCommIS.getMinDesiredExpenses() + price*minDesiredUtil);
                    }
                }
            }
        }

        return this;
    }

} // end class Ledger
