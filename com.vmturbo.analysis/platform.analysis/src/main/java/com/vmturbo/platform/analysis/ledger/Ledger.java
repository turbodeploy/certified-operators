package com.vmturbo.platform.analysis.ledger;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.commons.Pair;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ByProducts;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.RawMaterialMetadata;
import com.vmturbo.platform.analysis.economy.RawMaterials;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.EdeCommon;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.updatingfunction.ProjectionFunction;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunction;

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
    public static int exceptionCounter = 0;

    // Constructor

    /**
     * Constructs a new Ledger instance that is going to hold all the incomeStatements
     * of every trader in the economy and the commoditiesSold by all these traders
     *
     */
    public Ledger(@NonNull Economy  economy) {

        economy.getTraders().forEach(trader -> {
            try {
                addTraderIncomeStatement(trader);
            } catch (Exception e) {
                if (exceptionCounter < EconomyConstants.EXCEPTION_PRINT_LIMIT) {
                    logger.error(EconomyConstants.EXCEPTION_MESSAGE, trader.getDebugInfoNeverUseInCode(),
                            e.getMessage(), e);
                    exceptionCounter++;
                }
                economy.getExceptionTraders().add(trader.getOid());
            }
        });

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
        checkArgument(trader.getEconomyIndex() == traderIncomeStatements_.size());
        traderIncomeStatements_.add(traderIncomeStatement);

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
        Set<Trader> sellers = market.getActiveSellers();
        (sellers.size() < economy.getSettings().getMinSellersForParallelism()
            ? sellers.stream() : sellers.parallelStream())
            .forEach(seller -> calculateExpRevForTraderAndGetTopRevenue(economy, seller));
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
     * computes the {@link IncomeStatement} for a seller and returns the revenue of the
     * richest commodity
     *
     * @param economy {@link Economy} where the <b>seller</b> trades
     * @param seller the {@link Trader} whose expenses and revenues are to be computed
     *
     * @return price of the {@link CommoditySold} that generates most revenue
     */
    public MostExpensiveCommodityDetails calculateExpRevForTraderAndGetTopRevenue(Economy economy, Trader seller) {

        IncomeStatement sellerIncomeStmt = getTraderIncomeStatements().get(seller.getEconomyIndex());
        sellerIncomeStmt.resetIncomeStatement();
        double sellerMaxDesUtil = seller.getSettings().getMaxDesiredUtil();
        double sellerMinDesUtil = seller.getSettings().getMinDesiredUtil();
        // compute the revenues based on the utilization of the commoditiesSold
        CommoditySold[] topSellingComm = {null, null};
        double[] topRev = new double[2];
        // Find the top 2 commodities that generate the most revenue
        if (seller.isDebugEnabled()) {
            logger.debug("________________________________________________________________________________");
            logger.debug(seller.getDebugInfoNeverUseInCode());
        }
        for (int index = 0; index < seller.getCommoditiesSold().size(); index++) {
            CommoditySold commSold = seller.getCommoditiesSold().get(index);
            if (commSold.getNumConsumers() < 1) {
                // Skip commodity with no consumers. It should not contribute to the revenue
                // of the entity.
                continue;
            }
            CommoditySpecification commoditySpecification = seller.getBasketSold().get(index);
            Set<Integer> commoditiesToIgnore = economy.getCommoditiesToIgnoreForProvisionAndSuspensionEntry(seller.getType());
            if (commoditiesToIgnore.contains(commoditySpecification.getBaseType())) {
                continue;
            }
            double commSoldUtil = commSold.getQuantity()/commSold.getEffectiveCapacity();
            if (commSoldUtil != 0) {
                PriceFunction pf = commSold.getSettings().getPriceFunction();
                double revFromComm = pf.unitPrice(commSoldUtil, null, seller, commSold, economy) * commSoldUtil;
                if (seller.isDebugEnabled()) {
                    logger.debug(seller.getBasketSold().get(index).getDebugInfoNeverUseInCode() + " - util:" +
                                commSoldUtil + ", price:" + revFromComm + ", MinDP:" +
                                pf.unitPrice(sellerMinDesUtil, null, seller, commSold, economy) + ", MaxDP:" +
                                pf.unitPrice(sellerMaxDesUtil, null, seller, commSold, economy));
                }
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
        double[] tempCurrMinMaxRev = new double[4];
        IntStream.range(0, topRev.length).forEach(i -> {
            if (Double.isFinite(tempCurrMinMaxRev[0])) {
                CommoditySold cs = topSellingComm[i];
                if (cs != null) {
                    PriceFunction pf = cs.getSettings().getPriceFunction();
                    tempCurrMinMaxRev[0] = tempCurrMinMaxRev[0] + topRev[i];
                    tempCurrMinMaxRev[1] = tempCurrMinMaxRev[1] + pf.unitPrice(sellerMinDesUtil
                                                                         , null , seller, cs, economy);
                    tempCurrMinMaxRev[2] = tempCurrMinMaxRev[2] + pf.unitPrice(sellerMaxDesUtil
                                                                          , null , seller, cs, economy);
                    tempCurrMinMaxRev[3] = tempCurrMinMaxRev[3] + pf.unitPrice((sellerMaxDesUtil
                        + sellerMinDesUtil) / 2, null , seller, cs, economy);
                }
            }
        });

        sellerIncomeStmt.setRevenues(tempCurrMinMaxRev[0])
                        .setMinDesiredRevenues(tempCurrMinMaxRev[1] * sellerMinDesUtil)
                        .setMaxDesiredRevenues(tempCurrMinMaxRev[2] * sellerMaxDesUtil)
                        .setDesiredRevenues(tempCurrMinMaxRev[3] *
                                (sellerMinDesUtil + sellerMaxDesUtil) / 2);

        calculateExpensesForSeller(economy, seller);
        final CommoditySold topComm = topSellingComm[0];
        if (topComm == null) {
            return new MostExpensiveCommodityDetails(null, topRev[0], 0.0, null);
        }
        return new MostExpensiveCommodityDetails(
                seller.getBasketSold().get(seller.getCommoditiesSold().indexOf(topComm)),
                topRev[0],
                topComm.getQuantity(),
                topComm.getSettings().getUpdatingFunction());
    }

    /**
     * MostExpensiveCommodityDetails contains most expensive commodity specification and
     * related revenues and quantities.
     */
    public static class MostExpensiveCommodityDetails {

        CommoditySpecification commSpec;
        double revenues;
        double quantity;
        UpdatingFunction updatingFunction;

        MostExpensiveCommodityDetails(CommoditySpecification commoditySpec,
                                      double topRevenue,
                                      double topQuantity,
                                      @Nullable UpdatingFunction updatingFunc) {
            commSpec = commoditySpec;
            revenues = topRevenue;
            quantity = topQuantity;
            updatingFunction = updatingFunc;
        }

        public CommoditySpecification getCommoditySpecification() {
            return commSpec;
        }

        public double getRevenues() {
            return revenues;
        }

        public double getQuantity() {
            return quantity;
        }

        @Nullable public UpdatingFunction getUpdatingFunction() {
            return updatingFunction;
        }
    }
    /**
     * Computes the expenses and revenues for the traders in this {@link Economy}
     *
     * @param economy {@link Economy} where the <b>seller</b> trades
     */
    public void calculateExpRevForTradersInEconomy (Economy economy) {
        for (Trader trader : economy.getTraders()) {
            calculateExpRevForTraderAndGetTopRevenue(economy, trader);
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
        double[] quote = {0, 0, 0, 0};
        // calculate expenses using the quote from every supplier that this trader buys from
        for (ShoppingList sl : economy.getMarketsAsBuyer(seller).keySet()) {
            // skip markets in which the trader in question is unplaced and
            // not consider expense for this trader in those markets.
            // And calculate expenses only for non-cloud trader where corresponding supplier has
            // null costFunction.
            if (sl.getSupplier() != null && sl.getSupplier().getSettings().getCostFunction() == null) {
                try {
                    double[] tempQuote = EdeCommon.quote(economy, sl, sl.getSupplier()
                                            , Double.POSITIVE_INFINITY, true).getQuoteValues();
                    for (int i=0; i<4; i++) {
                        quote[i] = quote[i] + tempQuote[i];
                    }
                } catch (IndexOutOfBoundsException e) {
                    // TODO (shravan): move try catch inside edeCommon
                    // setting expense to INFINITY
                    for (int i=0; i<4; i++) {
                        quote[i] = Double.POSITIVE_INFINITY;
                    }
                    logWrongProvider(sl);
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
                            .setMaxDesiredExpenses(quote[2])
                            .setDesiredExpenses(quote[3]);
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
        double[] quote = {0,0,0,0};
        // skip markets in which the trader in question is unplaced and
        // not consider expense for this trader in those markets
        if (shoppingList.getSupplier() != null) {
            try {
                double[] tempQuote = EdeCommon.quote(economy, shoppingList, shoppingList.getSupplier()
                                        , Double.POSITIVE_INFINITY, true).getQuoteValues();
                for (int i=0; i<3; i++) {
                    quote[i] = quote[i] + tempQuote[i];
                }
            } catch (IndexOutOfBoundsException e) {
                // TODO (shravan): move try catch inside edeCommon
                // setting expense to INFINITY
                for (int i=0; i<4; i++) {
                    quote[i] = Double.POSITIVE_INFINITY;
                }
                logWrongProvider(shoppingList);
            }
        }

        // update trader's minDesExpRev only if the expense(quote[0] here) is > 0
        if (quote[0] != 0) {
            buyerIncomeStmt.setExpenses(buyerIncomeStmt.getExpenses() + quote[0])
                            .setMinDesiredExpenses(buyerIncomeStmt.getMinDesiredExpenses() + quote[1])
                            .setMaxDesiredExpenses(buyerIncomeStmt.getMaxDesiredExpenses() + quote[2])
                            .setDesiredExpenses(buyerIncomeStmt.getDesiredExpenses() + quote[3]);
        }
    }

    /**
      * Log a wrong provider message stating which buyer is incorrectly buying from which seller due to
      * which missing commodities.
      *
      * @param shoppingList The {@link ShoppingList} being bought from the wrong provider.
      */
    private void logWrongProvider(@NonNull final ShoppingList shoppingList) {
        final Trader seller = shoppingList.getSupplier();
        final Set<CommoditySpecification> bought = Sets.newHashSet(shoppingList.getBasket());
        final Set<CommoditySpecification> sold = Sets.newHashSet(seller.getBasketSold());
        final Set<CommoditySpecification> missing = Sets.difference(bought, sold);
        final String missingMessage = missing.stream()
            .map(commoditySpecification -> commoditySpecification.getDebugInfoNeverUseInCode())
            .collect(Collectors.joining(",", "Missing=[", "]"));
        logger.debug("Buyer " + shoppingList.getBuyer().getDebugInfoNeverUseInCode() + " placed on the "
            + "wrong provider " + seller.getDebugInfoNeverUseInCode() + " " + missingMessage
            + " hence returning INFINITE expense");
    }

    /**
     * computes the {@link IncomeStatement} for every resizable commodity sold by this particular trader
     * and updates the commodityIncomeStatement list
     *
     * @param economy the {@link Economy} which contains all the commodities whose income
     *                statements are to be calculated
     * @param buyer   the {@link Trader} whose income statement needs to be updated.
     * @return The ledger containing the updated list of commodityIncomeStatements
     */
    public @NonNull Ledger calculateCommodityExpensesAndRevenuesForTrader(@NonNull Economy economy, Trader buyer) {
        boolean isDebugTrader = buyer.isDebugEnabled();
        String buyerDebugInfo = buyer.getDebugInfoNeverUseInCode();
        resetTraderIncomeStatement(buyer);
        List<CommoditySold> commSoldList = buyer.getCommoditiesSold();
        double maxDesUtil = buyer.getSettings().getMaxDesiredUtil();
        double minDesUtil = buyer.getSettings().getMinDesiredUtil();
        for (int commSoldIndex = 0; commSoldIndex < commSoldList.size(); commSoldIndex++) {
            // cs is the CommoditySold by the buyer
            CommoditySold cs = commSoldList.get(commSoldIndex);
            String commSoldInformationForLogs = buyer.getBasketSold()
                        .get(commSoldIndex).getDebugInfoNeverUseInCode();
            // compute rev/exp for resizable commodities
            if (!cs.getSettings().isResizable()) {
                continue;
            }

            IncomeStatement commSoldIS = commodityIncomeStatements_.get(
                    buyer.getEconomyIndex()).get(commSoldIndex);
            // rev of consumer is utilOfCommSold*priceFn(utilOfCommSold)
            // expense of this consumer is utilOfCommBought*priceFn(utilOfCommBought)
            boolean validRevenue = calculateRevenueAndUpdateIncomeStatement(commSoldIndex, minDesUtil,
                    maxDesUtil, buyer, economy, commSoldIS, true);
            if (!validRevenue) {
                continue;
            }

            int commSoldBaseType = buyer.getBasketSold().get(commSoldIndex).getBaseType();
            Optional<ByProducts> optionalByProducts = economy.getByProducts(commSoldBaseType);
            if (optionalByProducts.isPresent()) {
                for (Map.Entry<Integer, ProjectionFunction> baseTypeEntry : optionalByProducts.get().getByProductMap().entrySet()) {
                    calculateRevenueAndUpdateIncomeStatement(buyer.getBasketSold().indexOfBaseType(baseTypeEntry.getKey()),
                            minDesUtil, maxDesUtil, buyer, economy, commSoldIS, false);
                }
            }
            Optional<RawMaterials> rawMaterials = economy.getRawMaterials(buyer.getBasketSold()
                    .get(commSoldIndex).getBaseType());

            if (!rawMaterials.isPresent()) {
                continue;
            }
            RawMaterialMetadata[] rawMaterialsMetadata = rawMaterials.get().getMaterials();
            boolean relevantShoppingListProcessed = false;
            for (ShoppingList shoppingList : economy.getMarketsAsBuyer(buyer).keySet()) {

                if (relevantShoppingListProcessed) {
                    break;
                }

                Basket basketBought = shoppingList.getBasket();
                Trader supplier = shoppingList.getSupplier();
                if (supplier == null) {
                    continue;
                }
                // TODO: make indexOf return 2 values minIndex and the maxIndex.
                // All comm's btw these indices will be of this type
                // (needed when we have 2 commodities of same type bought)
                boolean commBoughtExists = Arrays.stream(rawMaterialsMetadata)
                        .anyMatch(material -> basketBought.indexOfBaseType(material.getMaterial()) != -1);
                for (RawMaterialMetadata rawMaterial : rawMaterialsMetadata) {
                    // do not consider soft constraints for expense.
                    if (!rawMaterial.isHardConstraint()) {
                        continue;
                    }
                    int typeOfCommBought = rawMaterial.getMaterial();
                    int boughtIndex = basketBought.indexOfBaseType(typeOfCommBought);

                    // if the required commodity is not in the shoppingList, set small constant
                    // expenses and skip the list, unless it is a related commodity in the rawMaterialsMap
                    // That does have a commodity in the shoppingList in which case we want to increase expenses.
                    // If the required commodity is found in the shopping list, we calculate and
                    // add the expenses to the income statement.
                    if (boughtIndex == -1) {
                        // We expect a charged by sold commodity to have a related commodity that
                        // is found in the shopping list to be bought by the supplier.
                        // For example, DBMem commodity has DBCacheHitRate and VMem in the
                        // raw materials map. VMem is found in the shopping list while
                        // DBCacheHitRate is not because it is sold just like DBMem, but DBMem
                        // is charged by it.
                        if (commBoughtExists && buyer.getBasketSold()
                                .get(commSoldIndex).getBaseType() != typeOfCommBought) {
                            Basket basketSold = buyer.getBasketSold();
                            final int index = basketSold.indexOfBaseType(typeOfCommBought);
                            if (index >= 0) {
                                // There are entities such as containers that can be hosted by
                                // different entities.  In this case, the raw materials
                                // commodities list will contain commodities that are neither
                                // bought nor sold by the entity.  In the container's case, it
                                // will buy VMem, and the raw materials list will contain Mem
                                // and VMem, depending on whether it is hosted by a VM or PM.
                                // If the container is hosted by a VM, the check above will fail
                                // for the Mem raw material.  In this case, we just skip
                                // updating the expenses for that commodity.
                                CommoditySold relatedCommSoldByBuyer = commSoldList.get(index);
                                // Use desired utilization if we are using historical quantity to
                                // resize. Otherwise, use the quantity of the related commodity
                                double relatedCommSoldUtil = cs.isHistoricalQuantitySet()
                                                ? (minDesUtil + maxDesUtil) / 2
                                                : relatedCommSoldByBuyer.getQuantity()
                                                                / relatedCommSoldByBuyer
                                                                                .getEffectiveCapacity();

                                double[] incomeStatementExpenses = calculateIncomeStatementExpenses(relatedCommSoldByBuyer,
                                        shoppingList, supplier, economy, relatedCommSoldUtil, maxDesUtil, minDesUtil);

                                // Use average of min and max desired expense as current expense
                                // when using historical quantity for generating resize action.
                                // Otherwise, the expense are calculated based on current quantity
                                // of the shopping list.
                                double historicalExpenses = cs.isHistoricalQuantitySet()
                                                                ? incomeStatementExpenses[3]
                                                                : incomeStatementExpenses[0];

                                commSoldIS.setExpenses(relevantShoppingListProcessed
                                    ? (commSoldIS.getExpenses() + historicalExpenses)
                                        : historicalExpenses);
                                commSoldIS.setMaxDesiredExpenses(relevantShoppingListProcessed
                                    ? (commSoldIS.getMaxDesiredExpenses() + incomeStatementExpenses[1])
                                        : incomeStatementExpenses[1]);
                                commSoldIS.setMinDesiredExpenses(relevantShoppingListProcessed
                                    ? (commSoldIS.getMinDesiredExpenses() + incomeStatementExpenses[2])
                                        : incomeStatementExpenses[2]);
                                commSoldIS.setDesiredExpenses(relevantShoppingListProcessed
                                    ? (commSoldIS.getDesiredExpenses() + incomeStatementExpenses[3])
                                        : incomeStatementExpenses[3]);
                                relevantShoppingListProcessed = true;
                            }
                        } else if (!commBoughtExists) {
                            // Only set expenses to 1, if all the commodities in typeOfCommsBought
                            // are not found in the shopping list in order to not overwrite existing
                            // expenses. For example PortChanel for switches with Mem commodity which
                            // won't be found.
                            // The else case is to continue because we don't want the default
                            // to set expenses to 1.0 because for VMs for example:
                            // VCPU relates to VCPU for containers and CPU for vm buying from
                            // Host. CPU is found in the shopping list and we calculate the
                            // expenses, but VCPU isn't found and we don't want to calculate/increase
                            // expenses due to it so we just continue.
                            commSoldIS.setExpenses(1.0);
                            commSoldIS.setMaxDesiredExpenses(1.0);
                            commSoldIS.setMinDesiredExpenses(1.0);
                            commSoldIS.setDesiredExpenses(1.0);
                            if (logger.isTraceEnabled() || isDebugTrader) {
                                logger.info("No raw material found for the trader "
                                        + buyerDebugInfo + " and shopping list " + shoppingList.getDebugInfoNeverUseInCode()
                                        + " for commodity sold "
                                        + commSoldInformationForLogs + ", thus the expenses, max desired"
                                        + " expenses and min desired expenses are all set to"
                                        + " 1.0.");
                            }
                        }
                        continue;
                    }

                    // find the right provider comm and use it to compute the expenses
                    Pair<CommoditySold, Trader> rawMaterialPair = RawMaterials.findResoldRawMaterialOnSeller(economy,
                            supplier, basketBought.get(boughtIndex));
                    CommoditySold commSoldBySeller = rawMaterialPair.first;
                    String commBoughtInformationForLogs = basketBought.get(boughtIndex).getDebugInfoNeverUseInCode();
                    if (commSoldBySeller == null) {
                        logger.warn("Trying to set expenses for commodity sold " + commSoldInformationForLogs
                            + " by Trader " + buyerDebugInfo
                            + " but Trader's SL " + shoppingList.getDebugInfoNeverUseInCode()
                            + " with supplier " + supplier.getDebugInfoNeverUseInCode()
                            + " attempts to buy commodity " + commBoughtInformationForLogs
                            + " which supplier is not selling.");
                        commSoldIS.setExpenses(relevantShoppingListProcessed ? commSoldIS.getExpenses() : 1.0);
                        commSoldIS.setMaxDesiredExpenses(relevantShoppingListProcessed ? commSoldIS.getMaxDesiredExpenses() : 1.0);
                        commSoldIS.setMinDesiredExpenses(relevantShoppingListProcessed ? commSoldIS.getMinDesiredExpenses() : 1.0);
                        commSoldIS.setDesiredExpenses(relevantShoppingListProcessed ? commSoldIS.getDesiredExpenses() : 1.0);
                    } else {
                        // Use desired utilization if we are using historical quantity for populating
                        // income statement otherwise calculate the utilization based on shopping list
                        // quantity
                        // the boughtUtil is still a constant when the resizing commodity is using percentile
                        double commBoughtUtil = cs.isHistoricalQuantitySet()
                            ? ((minDesUtil + maxDesUtil) / 2)
                                * (cs.getEffectiveCapacity() / commSoldBySeller.getEffectiveCapacity())
                            : shoppingList.getQuantity(boughtIndex)
                                            / commSoldBySeller.getEffectiveCapacity();
                        if (commBoughtUtil != 0) {
                            double[] incomeStatementExpenses = calculateIncomeStatementExpenses(commSoldBySeller,
                                    shoppingList, supplier, economy, commBoughtUtil, maxDesUtil, minDesUtil);
                            // Use average of min and max desired expenses if we are using historical
                            // quantity for resizing otherwise use current expenses
                            double historicalExpenses = cs.isHistoricalQuantitySet()
                                            ? (commSoldBySeller.isHistoricalQuantitySet() ? incomeStatementExpenses[0]
                                                : incomeStatementExpenses[3])
                                            : incomeStatementExpenses[0];
                            commSoldIS.setExpenses(relevantShoppingListProcessed
                                ? (commSoldIS.getExpenses() + historicalExpenses)
                                    : historicalExpenses);
                            commSoldIS.setMaxDesiredExpenses(relevantShoppingListProcessed
                                ? (commSoldIS.getMaxDesiredExpenses() + incomeStatementExpenses[1])
                                    : incomeStatementExpenses[1]);
                            commSoldIS.setMinDesiredExpenses(relevantShoppingListProcessed
                                ? (commSoldIS.getMinDesiredExpenses() + incomeStatementExpenses[2])
                                    : incomeStatementExpenses[2]);
                            commSoldIS.setDesiredExpenses(relevantShoppingListProcessed
                                ? (commSoldIS.getDesiredExpenses() + incomeStatementExpenses[3])
                                    : incomeStatementExpenses[3]);
                            // Set relevantShoppingList as it contains a valid bought index in current shopping list.
                            relevantShoppingListProcessed = true;

                            if (logger.isTraceEnabled() || isDebugTrader) {
                                logger.info("Using relevant shopping list " + shoppingList.getDebugInfoNeverUseInCode()
                                        + " to set expenses for the trader " + buyerDebugInfo + ", the"
                                        + " commodity sold " + commSoldInformationForLogs + " and"
                                        + " the raw material bought " + commBoughtInformationForLogs
                                        + " the expenses are " + incomeStatementExpenses[0] + ", the max desired"
                                        + " expenses are " + incomeStatementExpenses[1] + " and the min"
                                        + " desired expenses are " + incomeStatementExpenses[2] + ".");
                            }
                        }
                    }
                }
            }
        }

        return this;
    }

    /**
     * Compute the revenue of the sold commodity and update the {@link IncomeStatement}.
     *
     * @param commSoldIndex is the index of the sold commodity whose revenue is to be computed.
     * @param minDesUtil is the minimum desired utilization.
     * @param maxDesUtil is the maximum desired utilization.
     * @param buyer   the {@link Trader} whose income statement needs to be updated.
     * @param economy the {@link Economy} which contains all the commodities whose income
     *                statements are to be calculated
     * @param commSoldIS is the {@link IncomeStatement} of the sold commodity.
     * @param updateDesiredRevenues is true when the desired revenue is to be updated.
     * @return The ledger containing the updated list of commodityIncomeStatements
     */
    private boolean calculateRevenueAndUpdateIncomeStatement(int commSoldIndex, double minDesUtil,
                                                          double maxDesUtil, Trader buyer, Economy economy,
                                                          IncomeStatement commSoldIS, boolean updateDesiredRevenues) {
        if (commSoldIndex == -1) {
            // invalid commodity to calculate revenue for.
            return false;
        }
        CommoditySold cs = buyer.getCommoditiesSold().get(commSoldIndex);
        double commSoldUtil = cs.getHistoricalOrElseCurrentUtilization()
                / cs.getSettings().getUtilizationUpperBound();

        if (Double.isNaN(commSoldUtil)) {
            // invalid utilization
            return false;
        }

        PriceFunction pf = cs.getSettings().getPriceFunction();
        double revenues = pf.unitPrice(commSoldUtil, null, buyer, cs,
                economy) * commSoldUtil;
        commSoldIS.addRevenue(revenues);

        if (updateDesiredRevenues) {
            double maxDesiredRevenues = pf.unitPrice(maxDesUtil, null,
                    buyer, cs, economy) * maxDesUtil;
            commSoldIS.addMaxDesiredRevenue(maxDesiredRevenues);
            double minDesiredRevenues = pf.unitPrice(minDesUtil, null,
                    buyer, cs, economy) * minDesUtil;
            commSoldIS.addMinDesiredRevenue(minDesiredRevenues);
            double desiredRevenues = pf.unitPrice((minDesUtil + maxDesUtil)
                    / 2, null, buyer, cs, economy)
                    * (maxDesUtil + minDesUtil) / 2;
            commSoldIS.addDesiredRevenue(desiredRevenues);
            if (logger.isTraceEnabled() || buyer.isDebugEnabled()) {
                String commSoldInformationForLogs = buyer.getBasketSold()
                        .get(commSoldIndex).getDebugInfoNeverUseInCode();
                logger.info("For the trader " + buyer.getDebugInfoNeverUseInCode() + " and the commodity"
                        + " sold " + commSoldInformationForLogs + " the revenues are "
                        + revenues + ", the max desired revenues are " + maxDesiredRevenues
                        + ", the min desired revenues are " + minDesiredRevenues + " and"
                        + " the desired revenues are " + desiredRevenues + ".");
            }
        }

        return true;
    }

    /**
     * Calculate income statement expenses to be set on the income statement
     *
     * @param commSold the Commodity sold
     * @param shoppingList the shopping list
     * @param supplier the supplier of the buyer
     * @param economy the economy
     * @param commUtil the commodity utilization
     * @param maxDesUtil the max desired util for the buyer
     * @param minDesUtil the min desired util for the buyer
     * @return array of expenses, maxDesiredExpenses and minDesiredExpenses
     */
    private double[] calculateIncomeStatementExpenses(CommoditySold commSold, ShoppingList shoppingList,
        Trader supplier, Economy economy, double commUtil, double maxDesUtil, double minDesUtil) {
            PriceFunction commPriceFunction = commSold.getSettings().getPriceFunction();
            // the expense calculation uses the percentile usage of the provider when present. When a VM has had a
            // consistent historically high usage, we want a high expense and thereby reduce chances of scaleUps
            double expenses = commPriceFunction.unitPrice(commSold.getHistoricalOrElseCurrentQuantity()
                / commSold.getEffectiveCapacity(), shoppingList, supplier, commSold, economy) * commUtil;
            double maxDesiredExpenses = commPriceFunction.unitPrice(maxDesUtil, shoppingList, supplier,
                commSold, economy) * commUtil;
            double minDesiredExpenses = commPriceFunction.unitPrice(minDesUtil, shoppingList, supplier,
                commSold, economy) * commUtil;
            double desiredExpenses = commPriceFunction.unitPrice((minDesUtil + maxDesUtil) / 2, shoppingList, supplier,
                    commSold, economy) * commUtil;
            double[] allExpenses = {expenses, maxDesiredExpenses, minDesiredExpenses, desiredExpenses};
            return allExpenses;
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
