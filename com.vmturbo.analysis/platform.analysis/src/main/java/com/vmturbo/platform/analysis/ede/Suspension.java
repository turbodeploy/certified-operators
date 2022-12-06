package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import com.vmturbo.platform.analysis.pricefunction.PriceFunctionFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.GuaranteedBuyerHelper;
import com.vmturbo.platform.analysis.actions.Utility;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySoldSettings;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.CausedByRelation;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.RelatedActionTO.CausedByRelation.CausedBySuspension;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;

/**
 * This class implements the suspension of {@link Trader}s in an {@link Economy}.
 */
public class Suspension {

    // a set to keep all traders that is the sole seller in any market.
    private @NonNull Set<@NonNull Trader> soleProviders = new HashSet<>();
    // Keeps track of the guaranteed buyers who have had a supplier suspend
    private @NonNull Set<@NonNull Trader> guaranteedBuyersWithSuspensions = new HashSet<>();

    static final Logger logger = LogManager.getLogger();

    public static int exceptionCounter = 0;

    private Ledger ledger_;

    private Economy economy_;

    // Queue ordered first based on amount of reconfigurable commodities of a trader
    // and then the roi of the trader in order to prioritize suspensions of traders
    // with more reconfigurable commodities and lower roi.
    private PriorityQueue<Trader> suspensionCandidateHeap_ = new PriorityQueue<>((t1, t2) -> {
        if (t1.getReconfigurableCommodityCount() > t2.getReconfigurableCommodityCount()) {
            return -1;
        } else if (t1.getReconfigurableCommodityCount() < t2.getReconfigurableCommodityCount()) {
            return 1;
        } else {
            IncomeStatement is1 = ledger_.getTraderIncomeStatements().get(t1.getEconomyIndex());
            IncomeStatement is2 = ledger_.getTraderIncomeStatements().get(t2.getEconomyIndex());
            double c1 = is1.getROI() / is1.getMinDesiredROI();
            double c2 = is2.getROI() / is2.getMinDesiredROI();
            return c1 > c2 ? 1 : c1 == c2 ? 0 : -1;
        }
    });

    private final @NonNull SuspensionsThrottlingConfig suspensionsThrottlingConfig;

    /**
     * Construct a Suspension with specified throttling level.
     *
     * @param suspensionsThrottlingConfig level of Suspension throttling.
     *         CLUSTER: Make co sellers of suspended seller suspendable false.
     *         DEFAULT: Unlimited suspensions.
     */
    public Suspension(@NonNull SuspensionsThrottlingConfig suspensionsThrottlingConfig) {
        this.suspensionsThrottlingConfig = suspensionsThrottlingConfig;
    }

    /**
     * Return a list of recommendations to optimize the suspension of all eligible traders in the
     * economy.
     *
     * <p>As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the economy whose non-profitable traders we want to suspend while remaining
     *                  in the desired state
     * @param ledger - the class that contains exp/rev about all the traders and commodities in
     *                  the economy
     * @return list of deactivate and move actions
     */
    public @NonNull List<@NonNull Action> suspensionDecisions(@NonNull Economy economy,
                                                              @NonNull Ledger ledger) {
        List<@NonNull Action> allActions = new ArrayList<>();
        int round = 0;
        // suspend entities that are not sellers in any market
        for (Trader seller : economy.getTraders()) {
            try {
                if (seller.getSettings().isSuspendable() && seller.getState().isActive()
                        && !seller.getSettings().isDaemon()
                        && !sellerHasNonDaemonCustomers(seller)
                        // Check whether the seller is not a seller to any non-daemons in any market.
                        // If the seller is only selling to daemons, it can suspend.
                        && !economy.getMarketsAsSeller(seller).stream()
                            .anyMatch(m -> m.getBuyers().stream()
                                .anyMatch(sl -> !sl.getBuyer().getSettings().isDaemon()))) {
                    if (logger.isTraceEnabled() || seller.isDebugEnabled()) {
                        logger.info("Suspending " + seller.getDebugInfoNeverUseInCode()
                                + " as it is not a seller in any market.");
                    }
                    Deactivate deactivate = suspendTrader(economy, seller.getBasketSold(), seller, allActions);
                    if (deactivate != null) {
                        // Suspend any remaining customers.  These should all be daemons.
                        allActions.addAll(suspendOrphanedCustomers(economy, deactivate));
                    }
                    // Avoid further suspensions if setting is CLUSTER
                    if (suspensionsThrottlingConfig == SuspensionsThrottlingConfig.CLUSTER) {
                        makeCoSellersNonSuspendable(economy, seller);
                    }
                }
            } catch (Exception e) {
                if (exceptionCounter < EconomyConstants.EXCEPTION_PRINT_LIMIT) {
                    logger.error(EconomyConstants.EXCEPTION_MESSAGE, seller.getDebugInfoNeverUseInCode(),
                            e.getMessage(), e);
                    exceptionCounter++;
                }
                economy.getExceptionTraders().add(seller.getOid());
            }
        }
        // run suspension for 3 rounds. We can have scenarios where, there are VMs that can move
        // when buyers in a different market make room. In order to enable this, we retry suspensions
        // after a round of economy-wide placements. We do this a third time for better packing as
        // placements is the only expense here
        try {
            while (round < 3) {
                ledger.calculateExpRevForTradersInEconomy(economy);
                // adjust utilThreshold to maxDesiredUtil*utilTh of the seller. Thereby preventing moves
                // that force utilization to exceed maxDesiredUtil*utilTh
                adjustUtilThreshold(economy, true);
                ledger_ = ledger;
                economy_ = economy;

                for (Market market : economy.getMarkets()) {
                    try {
                        if (economy.getForceStop()) {
                            return allActions;
                        }
                        // Skip markets that don't have suspendable active sellers. We consider only
                        // activeSellersAvailableForPlacement as entities should not move out of the ones not available
                        // for placement.
                        // We would skip markets where all nodes have canAcceptNewCustomers false and host only daemons.
                        // We wont suspend the nodes in such a market. TODO: handle this above case.
                        if (market.getActiveSellersAvailableForPlacement().isEmpty() ||
                                market.getActiveSellersAvailableForPlacement().stream().noneMatch(
                                                t -> t.getSettings().isSuspendable())) {
                            continue;
                        }
                        // when we suspend a trader we remove it from activeSellers. To avoid concurrent modification,
                        // we create a new list with candidates.
                        List<Trader> suspensionCandidates = Lists.newArrayList(market.getActiveSellers());
                        for (Trader seller : suspensionCandidates) {
                            // suspension candidates can be only activeSellers that canAcceptNewCustomers
                            // that are suspendable
                            if (!seller.getSettings().isSuspendable() || seller.getSettings().isDaemon()) {
                                continue;
                            }
                            boolean isDebugTrader = seller.isDebugEnabled();
                            String sellerDebugInfo = seller.getDebugInfoNeverUseInCode();
                            // Check for suspendable here because we might set co-sellers to be non suspendable.
                            // Do not suspend daemons here.
                            if (!seller.getSettings().isDaemon() && !sellerHasNonDaemonCustomers(seller)) {
                                if (logger.isTraceEnabled() || isDebugTrader) {
                                    logger.info("Suspending " + sellerDebugInfo
                                        + " as there are no customers.");
                                }
                                Deactivate deactivate = suspendTrader(economy, market.getBasket(), seller, allActions);
                                if (deactivate != null) {
                                    // Suspend any remaining customers.  These should all be daemons.
                                    allActions.addAll(suspendOrphanedCustomers(economy, deactivate));
                                }
                                // Avoid further suspensions if setting is CLUSTER
                                if (suspensionsThrottlingConfig == SuspensionsThrottlingConfig.CLUSTER) {
                                    makeCoSellersNonSuspendable(economy, seller);
                                }
                                continue;
                            }
                            IncomeStatement incomeStmt = ledger.getTraderIncomeStatements()
                                            .get(seller.getEconomyIndex());
                            if (!suspensionCandidateHeap_.contains(seller)
                                && !soleProviders.contains(seller)
                                // Handle case where all commodities sold in the trader are selling
                                // a quantity of zero. In this case, there is an optimization in the income
                                // statement where the desired ROI is not calculated when the ROI is zero.
                                && (incomeStmt.getROI() == 0.0
                                    || incomeStmt.getROI() < incomeStmt.getMinDesiredROI())) {
                                suspensionCandidateHeap_.offer(seller);
                                if (logger.isTraceEnabled() || isDebugTrader) {
                                    logger.info("Inserting " + sellerDebugInfo
                                                + " in suspension candidates heap.");
                                }
                            }
                        }
                    } catch (Exception e) {
                        logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                                market.getActiveSellers().isEmpty() ? "market " + market.toString()
                                    : market + " " + market.toString() + " with first active seller "
                                    + market.getActiveSellers().iterator().next().getDebugInfoNeverUseInCode(),
                                    e.getMessage(), e);
                    }
                }

                Trader trader;
                double oldNumActions = allActions.size();
                while ((trader = suspensionCandidateHeap_.poll()) != null) {
                    if (!soleProviders.contains(trader) && trader.getState().isActive()) {
                        allActions.addAll(deactivateTraderIfPossible(trader, economy, ledger, false));
                    }
                }
                // reset threshold
                adjustUtilThreshold(economy, false);

                if (allActions.size() > oldNumActions) {
                    // run economy wide placements if there are new actions in this round of suspension
                    allActions.addAll(Placement.runPlacementsTillConverge(economy, ledger,
                        EconomyConstants.SUPPLY_PHASE).getActions());
                }
                round++;
            }
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE, "Suspension", e.getMessage(), e);
            adjustUtilThreshold(economy, false);
        }
        return allActions;
    }

    /**
     * If a trader can be suspended, adds to allActions list .Tries to move all customers
     * can move out of current trader and if its customer list is empty the trader is suspended.
     *
     * @param trader The {@link Trader} we try to suspend.
     * @param economy the {@link Economy} which is being evaluated for suspension.
     * @param ledger The {@link Ledger} related to current {@link Economy}
     * @param moveAllPossibleCustomers is true when we want all the possible customers of suspensionCandidate to
     *                                 find placement. Move just the customers of candidate when false.
     * @return action list related to suspension of trader.
     */
    List<Action> deactivateTraderIfPossible(Trader trader, Economy economy, Ledger ledger,
                                            boolean moveAllPossibleCustomers) {
        List<@NonNull Action> suspendActions = new ArrayList<>();
        try {
            boolean isDebugTrader = trader.isDebugEnabled();
            String traderDebugInfo = trader.getDebugInfoNeverUseInCode();
            if (logger.isTraceEnabled() || isDebugTrader) {
                logger.info("Trying to suspend trader " + traderDebugInfo + ".");
            }
            List<Market> markets = economy.getMarketsAsSeller(trader);

            if (!trader.getSettings().isSuspendable()) {
                if (logger.isTraceEnabled() || isDebugTrader) {
                    logger.info("{" + traderDebugInfo + "} is not suspendable.");
                }
                return suspendActions;
            }
            if (GuaranteedBuyerHelper.isTraderReplicasBeyondRange(trader, economy, false)) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Do not suspend trader {} because its current replicas is"
                                    + " not above the minReplicas {}.",
                            traderDebugInfo, trader.getSettings().getMinReplicas());
                }
                return suspendActions;
            }
            boolean isProviderOfResizeThroughSupplier = Utility.isProviderOfResizeThroughSupplierTrader(trader);
            Set<ShoppingList> resizeThroughSupplierCustomers = new LinkedHashSet<>();
            Set<Trader> resizeThroughSuppliers = new LinkedHashSet<>();
            if (isProviderOfResizeThroughSupplier) {
                resizeThroughSuppliers = Utility.getResizeThroughSupplierTradersFromProvider(trader);
                resizeThroughSupplierCustomers = resizeThroughSuppliers.stream()
                                                            .flatMap(t -> t.getCustomers().stream())
                                                                        .collect(Collectors.toCollection(LinkedHashSet::new));
            }

            Set<ShoppingList> customersOfSuspCandidate = new LinkedHashSet<>();
            if (moveAllPossibleCustomers) {
                economy.getMarketsAsSeller(trader).stream()
                        .map(Market::getBuyers)
                        .flatMap(List::stream)
                        .filter(sl -> !Suspension.isDaemon(sl))
                        .filter(sl -> !Utility.isUnmovableRTSShoppingList(sl))
                        .forEach(customersOfSuspCandidate::add);
            } else {
                customersOfSuspCandidate.addAll(getNonDaemonCustomers(trader).stream()
                    .filter(sl -> !Utility.isUnmovableRTSShoppingList(sl))
                        .collect(Collectors.toList()));
            }
            customersOfSuspCandidate.addAll(resizeThroughSupplierCustomers);

            // Need to get this before doing the suspend, or the list will be empty.
            List<ShoppingList> guaranteedBuyerSls = GuaranteedBuyerHelper
                    .findSlsBetweenSellerAndGuaranteedBuyer(trader);
            final Map<Trader, Set<ShoppingList>> slsSponsoredByGuaranteedBuyer =
                    GuaranteedBuyerHelper.getAllSlsSponsoredByGuaranteedBuyer(economy,
                            guaranteedBuyerSls);
            Deactivate deactivate = suspendTrader(economy,
                markets.isEmpty() ? trader.getBasketSold() : markets.get(0).getBasket(),
                trader, suspendActions);
            if (deactivate == null) {
                return suspendActions;
            }

            if (logger.isTraceEnabled() || isDebugTrader) {
                logger.info("Suspending trader " + traderDebugInfo
                            + " and trying to move its customers to other traders.");
            }

            if (!markets.isEmpty()) {
                // perform placement on just the customers on the suspensionCandidate
                // The act of suspension of chains of providerMustClone traders may clear the supplier
                // of some the customers, so remove them first.
                suspendActions.addAll(Placement.runPlacementsTillConverge(
                    economy, customersOfSuspCandidate.stream()
                        .filter(sl -> sl.getSupplier() != null)
                        .collect(Collectors.toList()),
                    ledger, true, EconomyConstants.SUSPENSION_PHASE).getActions());
            }

            // Rollback actions if the trader still has customers.  If all of the customers are
            // guaranteed buyers, it's still okay to proceed with the suspend.
            if (makeNonDaemonCustomerStream(trader)
                    .anyMatch(cust -> !cust.getBuyer().getSettings().isGuaranteedBuyer())) {
                if (logger.isTraceEnabled() || isDebugTrader) {
                    logger.info("{" + traderDebugInfo + "} will not be suspended"
                            + " because it still has customers.");
                }
                return rollBackSuspends(suspendActions);
            }

            // If the new suspensions would cause a guaranteed buyer to get a very large quote,
            // then reverse the suspensions.
            for (Set<ShoppingList> shoppingLists : slsSponsoredByGuaranteedBuyer.values()) {
                for (ShoppingList sl : shoppingLists) {
                    // skip vsan storage's shopping list when considering reverse suspension
                    if (Utility.isUnmovableRTSShoppingList(sl)) {
                        continue;
                    }
                    // consider all valid active sellers to see if the guaranteedBuyer can fit in seller.
                    final @NonNull Set<@NonNull Trader> sellers =
                            economy.getMarket(sl).getActiveSellersAvailableForPlacementForConsumer(sl);
                    final QuoteMinimizer minimizer =
                            sellers.stream()
                                    .collect(() -> new QuoteMinimizer(economy, sl),
                                            QuoteMinimizer::accept, QuoteMinimizer::combine);
                    if (Double.compare(minimizer.getTotalBestQuote(), PriceFunctionFactory.MAX_UNIT_PRICE) > 0) {
                        if (logger.isTraceEnabled() || isDebugTrader) {
                            logger.info("{} will not be suspended because otherwise it will cause"
                                            + " its guaranteed buyer {} to have a quote larger than {}.",
                                    traderDebugInfo, sl.getBuyer().getDebugInfoNeverUseInCode(),
                                    PriceFunctionFactory.MAX_UNIT_PRICE);
                        }
                        return rollBackSuspends(suspendActions);
                    }
                }
            }

            if (isProviderOfResizeThroughSupplier) {
                for (ShoppingList sl : resizeThroughSupplierCustomers) {
                    // the quote should not be affected by the unquoted commodity as  the
                    // provisioned commodity are unquoted and we dont want the host to be suspended
                    // if the cluster is full.. so dont ignore the Unquoted Commodities..
                    List<Integer> unquotedCommodities = new ArrayList<>();
                    unquotedCommodities.addAll(sl.getModifiableUnquotedCommoditiesBaseTypeList());
                    sl.getModifiableUnquotedCommoditiesBaseTypeList().clear();
                    // skip vsan storage's shopping list when considering reverse suspension
                    if (Utility.isUnmovableRTSShoppingList(sl)) {
                        continue;
                    }
                    final @NonNull List<@NonNull Trader> sellers =
                                    economy.getMarket(sl).getActiveSellersAvailableForPlacement();
                    final QuoteMinimizer minimizer = sellers.stream()
                                            .collect(() -> new QuoteMinimizer(economy, sl),
                                                    QuoteMinimizer::accept, QuoteMinimizer::combine);
                    sl.getModifiableUnquotedCommoditiesBaseTypeList().addAll(unquotedCommodities);
                    if (Double.isInfinite(minimizer.getTotalBestQuote())) {
                        return rollBackSuspends(suspendActions);
                    }
                }
            }

            // Suspend any remaining customers.  These should all be daemons.
            suspendActions.addAll(suspendOrphanedCustomers(economy, deactivate));
            // Check if it is the last provider of resize through supplier trader.
            Utility.checkRTSToSuspend(economy, isProviderOfResizeThroughSupplier,
                resizeThroughSuppliers, guaranteedBuyerSls,
                deactivate, suspendActions);
            updateSoleProviders(economy, trader);
            logger.info("{" + traderDebugInfo + "} was suspended.");
            if (suspensionsThrottlingConfig == SuspensionsThrottlingConfig.CLUSTER) {
                makeCoSellersNonSuspendable(economy, trader);
            }
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE, trader.getDebugInfoNeverUseInCode(),
                    e.getMessage(), e);
            economy.getExceptionTraders().add(trader.getOid());
            rollBackSuspends(suspendActions);
        }
        return suspendActions;
    }

    /**
     * Suspend all daemon customers on this Trader as well as the customers of those customers.  If one of
     * the customers is a guaranteed buyer and the guaranteed buyer no longer has any shopping
     * lists, suspend the guaranteed buyer as well.
     * @param economy economy.
     * @param drivingDeactivate Deactivate action of supplier whose customers need to be deactivated.
     * @return list of new Deactivates that were generated.  These actions are also added to the
     *          driving Deactivate action's subsequent actions list.
     */
    List<Action> suspendOrphanedCustomers(final Economy economy,
                                                  final Deactivate drivingDeactivate) {
        List<Action> actions = new ArrayList<>();
        suspendOrphanedCustomersHelper(economy, drivingDeactivate.getActionTarget()
                , drivingDeactivate.getActionTarget(), actions);
        drivingDeactivate.getSubsequentActions().addAll(actions);
        // Add relatedActionTOs to corresponding daemon customer suspensions to indicate the daemon
        // suspensions have CausedByRelation to the driving deactivate action.
        actions.forEach(action -> action.addRelatedAction(RelatedActionTO.newBuilder()
                .setRelatedActionId(drivingDeactivate.getId())
                .setTargetTrader(drivingDeactivate.getActionTarget().getOid())
                .setCausedByRelation(CausedByRelation.newBuilder()
                        .setSuspension(CausedBySuspension.newBuilder()))
                .build()));
        return actions;
    }

    private void suspendOrphanedCustomersHelper(final Economy economy,
                                                final Trader trader,
                                                final Trader reasonTrader,
                                                final List<Action> actions) {
        for (ShoppingList sl : trader.getCustomers()) {
            Trader customer = sl.getBuyer();
            // Double check if the customer is daemon. We should only suspend daemon in this method.
            if (!customer.getSettings().isDaemon()) {
                continue;
            }
            if (customer.getSettings().isGuaranteedBuyer()) {
                // If this guaranteed buyer is a buyer in a single market, then by definition the
                // current trader is the sole supplier.  Since the current trader is suspending and
                // the guaranteed buyer isn't buying from anything else, we should also remove it.
                // Note that this condition occurs very rarely when a daemon is exposed as a
                // service and its underlying node suspends.
                if (economy.getSuppliers(customer).size() > 1) {
                    continue;
                }
            }
            if (customer.getState().isActive()) {
                List<Market> markets = economy.getMarketsAsSeller(customer);
                Basket basket = markets.isEmpty() ? customer.getBasketSold()
                                                  : markets.get(0).getBasket();
                Deactivate deactivateAction = new Deactivate(economy, customer, basket);
                deactivateAction.setExecutable(false);
                deactivateAction.setReasonTrader(reasonTrader);
                actions.add(deactivateAction.take());
            }
            // Call this function again to deactivate this customer's customers as well.
            suspendOrphanedCustomersHelper(economy, customer, reasonTrader, actions);
        }
    }

    private List<Action> rollBackSuspends(List<Action> suspendActions) {
        Lists.reverse(suspendActions).forEach(axn -> axn.rollback());
        return new ArrayList<>();
    }

    /**
     * Adjust the utilThreshold of {@link CommoditySold} by {@link Trader}s to maxDesiredUtil.
     *
     * @param economy - the {@link Economy} which is being evaluated for suspension
     * @param update - set threshold to maxDesiredUtil*utilThreshold if true or reset to original value if false
     */
    @VisibleForTesting
    public static void adjustUtilThreshold(Economy economy, boolean update) {
        if (update) {
            for (Trader seller : economy.getTraders()) {
                double util = seller.getSettings().getMaxDesiredUtil();
                for (CommoditySold cs : seller.getCommoditiesSold()) {
                    double utilThreshold = cs.getSettings().getUtilizationUpperBound();
                    PriceFunction pf = cs.getSettings().getPriceFunction();
                    double priceAtMaxUtil = pf.unitPrice(util * utilThreshold, null, seller, cs, economy);
                    // skip if step and constant priceFns
                    if (!((priceAtMaxUtil == pf.unitPrice(0.0, null, seller, cs, economy)) ||
                          (priceAtMaxUtil == pf.unitPrice(1.0, null, seller, cs, economy)))) {
                        cs.getSettings().setUtilizationUpperBound(util * utilThreshold);
                    }
                }
            }
        } else {
            for (Trader seller : economy.getTraders()) {
                for (CommoditySold cs : seller.getCommoditiesSold()) {
                    CommoditySoldSettings csSett = cs.getSettings();
                    csSett.setUtilizationUpperBound(csSett.getOrigUtilizationUpperBound());
                }
            }
        }
    }

    /**
     * Suspend the <code>bestTraderToEngage</code> and add the action to the
     * <code>actions</code> list.
     *
     * @param economy - the {@link Economy} in which the suspend action is performed
     * @param triggeringBasket - The {@link Basket} of the {@link Market} in which the suspend
     *                         action takes place
     * @param traderToSuspend - the trader that satisfies the engagement criteria best
     * @param actions - a list that the suspend action would be added to
     * @return the Deactivate action generated, or null if the deactivation didn't succeed.
     */
    public @Nullable Deactivate suspendTrader(Economy economy, @NonNull Basket triggeringBasket,
                                          Trader traderToSuspend, List<@NonNull Action> actions) {
        final List<@NonNull Trader> guaranteedBuyers = GuaranteedBuyerHelper
                .findGuaranteedBuyers(traderToSuspend);
        // Do not allow a suspension if the trader's guaranteed buyer already had a supplier
        // suspend. Unless it is a provider of resize through supplier so we are not limited in
        // suspending only 1 host per cluster for vSan.
        // TODO This needs to eventually be handled by the suspension throttling mechanism by
        // creating groups for each guaranteed buyer.
        if (guaranteedBuyers.stream().anyMatch(guaranteedBuyersWithSuspensions::contains)
                        && !Utility.isProviderOfResizeThroughSupplierTrader(traderToSuspend)) {
            return null;
        }

        Deactivate deactivateAction = new Deactivate(economy, traderToSuspend, triggeringBasket);
        // If this trader is supplying guaranteed buyers, add them to the list of guaranteed
        // buyers who have had a suspension this pass.
        guaranteedBuyersWithSuspensions.addAll(guaranteedBuyers);
        actions.add(deactivateAction.take());
        actions.addAll(deactivateAction.getSubsequentActions());
        return deactivateAction;
    }

    /**
     * Construct the set which keeps traders that are the sole seller in any market.
     *
     * @param economy The {@link Economy} in which suspension would take place.
     */
    public void findSoleProviders(Economy economy) {
        for (Trader trader : economy.getTraders()) {
            List<Market> marketsAsSeller = economy.getMarketsAsSeller(trader);
            // being the sole provider means the seller is the only active seller in a market
            // and it has some customers which are not the shopping lists from guaranteed buyers
            if (marketsAsSeller.stream()
                    .anyMatch((m) -> m.getActiveSellersAvailableForPlacement()
                            .size() == 1 && m.getBuyers().stream().anyMatch(
                                        sl -> !sl.getBuyer().getSettings().isGuaranteedBuyer() &&
                                                !sl.getBuyer().getSettings().isDaemon()))) {
                    soleProviders.add(trader);
            }
            if (trader.getSettings().isGuaranteedBuyer()) {
                // updateSoleProviders on the first supplier. If that supplier was a sole provider, it will get
                // added to the list. If not, then the other ones won't be sole providers either.
                economy.getMarketsAsBuyer(trader)
                    .keySet().stream()
                    .findFirst()
                    .ifPresent(sl -> {
                        if (sl.getSupplier() != null) {
                            updateSoleProviders(economy, sl.getSupplier());
                        }
                    });
            }
        }
    }

    /**
     * Identifies sole providers of guaranteed buyers of given trader. We do not want to
     * suspend the last supplier of a guaranteed buyer.
     * @param economy current economy
     * @param trader Trader to check
     */
    public void updateSoleProviders(Economy economy, Trader trader) {
        final List<@NonNull Trader> guaranteedBuyers = GuaranteedBuyerHelper
                        .findGuaranteedBuyers(trader);
        guaranteedBuyers.stream().forEach(t -> {
            final List<Trader> activeTraders = economy.getMarketsAsBuyer(t)
                .keySet().stream()
                .map(ShoppingList::getSupplier)
                .filter(supp -> supp != null && supp.getState().isActive())
                .limit(2)  // We only need to know whether there is 1 or more than 1
                .collect(Collectors.toList());
            if (activeTraders.size() == 1) {
                soleProviders.add(activeTraders.get(0));
            }
        });
    }

    @VisibleForTesting
    public Set<Trader> getSoleProviders() {
        return soleProviders;
    }

    /**
     * Make co-sellers of suspension candidate inactive.
     * get Markets suspension candidate sells in, although INACTIVE
     * disable suspension of all other traders in markets where deactivated trader
     * is a seller including inactive sellers as they may have been picked in the
     * previous round
     * @param economy The economy trader participates in.
     * @param trader The trader, which is the suspension candidate picked.
     */
    protected static void makeCoSellersNonSuspendable(Economy economy, Trader trader) {
        final Trader picked = trader;
        for (Market mktAsSeller : economy.getMarketsAsSeller(trader)) {
            mktAsSeller.getActiveSellers().stream().filter(seller -> seller != picked)
                    .forEach(t -> t.getSettings().setSuspendable(false));
            mktAsSeller.getInactiveSellers().stream().filter(seller -> seller != picked)
                    .forEach(t -> t.getSettings().setSuspendable(false));
        }
    }

    public SuspensionsThrottlingConfig getSuspensionsthrottlingconfig() {
        return suspensionsThrottlingConfig;
    }

    private static Stream<ShoppingList> makeNonDaemonCustomerStream(Trader seller) {
        return seller.getCustomers().stream().filter(sl -> !isDaemon(sl));
    }

    private static boolean isDaemon(ShoppingList buyer) {
        return buyer.getBuyer().getSettings().isDaemon();
    }

    /**
     * Get list of customers of seller that are not daemons.
     * @param seller to check
     * @return list of sellers that are not daemons that are customers of the seller
     */
    private static List<ShoppingList> getNonDaemonCustomers(Trader seller) {
        return makeNonDaemonCustomerStream(seller).collect(Collectors.toList());
    }

    /**
     * Return whether the seller has any customers that are not daemons.
     * @param seller to check
     * @return true if the seller has at least one customer that is not a daemon
     */
    public static boolean sellerHasNonDaemonCustomers(Trader seller) {
        return makeNonDaemonCustomerStream(seller).findFirst().isPresent();
    }
}
