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
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.EdeCommon;

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

        /// adding an incomeStatement per commoditySold
        trader.getCommoditiesSold().forEach(commSold->{commodityIncomeStatements_.get(eIndex).add(new IncomeStatement());});

        return traderIncomeStatement;
    }


    /**
     * Adds a new {@link IncomeStatement} for a trader to the traderIncomeStatement list that the ledger maintains
     * we also add a IncomeStatement for every commodity that this trader sells to the commodityIncomeStatement list
     *
     * @param trader this is the new entity that is being added to the economy
     * @param commodityIndex is the index of the commodity in the shoppingList that the trader sells
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
    @NonNull Ledger calculateAllTraderExpensesAndRevenues (@NonNull Economy economy) {
        List<Trader> traders = economy.getTraders();

        for (Trader trader : traders) {
            economy.getMarketsAsBuyer(trader).forEach((shoppingList, market) -> {
                Trader currentSupplier = shoppingList.getSupplier();
                double quote = EdeCommon.quote(economy, shoppingList, null, currentSupplier);
                IncomeStatement consumerIncomsStmt = traderIncomeStatements_.get(trader.getEconomyIndex());
                IncomeStatement providerIncomsStmt = traderIncomeStatements_.get(currentSupplier.getEconomyIndex());

                // updating the expenses and revenues of the provider and consumer respectively
                consumerIncomsStmt.setExpenses(consumerIncomsStmt.getExpenses() + quote);
                providerIncomsStmt.setRevenues(providerIncomsStmt.getRevenues() + quote);

            });

        }

        return this;
    }

    /**
     * computes the {@link IncomeStatement} for every commodity bought by all the traders present in the economy and updates the
     * commodityIncomeStatement list
     *
     * @param economy the {@link Economy} which contains all the commodities whose income statements are to be calculated
     *
     * @return The ledger containing the updated list of commodityIncomeStatements
     */
    @NonNull Ledger calculateAllCommodityExpensesAndRevenues(@NonNull Economy economy) {
        List<Trader> traders = economy.getTraders();

        for (Trader buyer : traders) {
            economy.getMarketsAsBuyer(buyer).forEach((shoppingList, market) -> {
                Trader currentSeller = shoppingList.getSupplier();
                Basket basketBought = market.getBasket();

                for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basketBought.size(); boughtIndex++, soldIndex++) {
                    CommoditySpecification basketCommSpec = basketBought.get(boughtIndex);
                    // Find corresponding commodity sold. Commodities sold are ordered the same way as the
                    // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
                    while (basketCommSpec.getType() != currentSeller.getBasketSold().get(soldIndex).getType()) {
                        soldIndex++;
                    }

                    int commSoldIndex = buyer.getBasketSold().indexOf(economy.getRawMaterialOf(basketCommSpec.getType()));
                    if (!(currentSeller.getCommoditiesSold().get(soldIndex).getSettings().isResizable()
                            || buyer.getCommoditiesSold().get(commSoldIndex).getSettings().isResizable())) {

                        double commodityCost = EdeCommon.computeCommodityCost(economy, shoppingList, currentSeller, soldIndex, boughtIndex);

                        IncomeStatement consumerCommIS = commodityIncomeStatements_.get(buyer.getEconomyIndex()).get(commSoldIndex);
                        IncomeStatement providerCommIS = commodityIncomeStatements_.get(currentSeller.getEconomyIndex()).get(soldIndex);
                        consumerCommIS.setExpenses(consumerCommIS.getExpenses() + commodityCost);
                        providerCommIS.setRevenues(providerCommIS.getRevenues() + commodityCost);
                    } else {
                        continue;
                    }

                }
            });
        }

        return this;
    }

} // end class Ledger
