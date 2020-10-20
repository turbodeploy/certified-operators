package com.vmturbo.platform.analysis.ede;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.ProvisionBase;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.ledger.Ledger.MostExpensiveCommodityDetails;
import com.vmturbo.platform.analysis.updatingfunction.MM1Distribution;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;
import com.vmturbo.platform.analysis.utilities.ProvisionUtils;

/*
 * This class is contains the implementation for generating the provision actions on an economy
 * For generating the provisions we first check the engagement criteria on the market,
 * if this passes, we clone the best seller after which we run placements. This is followed
 * by the acceptance criteria. If the acceptance criteria evaluates to be true, we retain the
 * provisioned trader. We do this for every market in the economy.
 *
 * @author shravan
 *
 */
public class Provision {

    private static final Logger logger = LogManager.getLogger();

    /*
     * The bundle contains information about the mostProfitableTrader, most profitable commodity and
     * the revenue of this commodity.
     */
    private static class MostProfitableBundle {

        Trader mostProfitableTrader_;
        MostExpensiveCommodityDetails mostExpensiveCommodityDetails_;
        float minDecreasePct_;

        MostProfitableBundle(Trader mostProfitableTrader,
                             MostExpensiveCommodityDetails mostExpensiveCommodityDetails) {
            super();
            mostProfitableTrader_ = mostProfitableTrader;
            mostExpensiveCommodityDetails_ = mostExpensiveCommodityDetails;
            setMinDecreasePct();
        }

        Trader getMostProfitableTrader() {
            return mostProfitableTrader_;
        }

        MostExpensiveCommodityDetails getMostExpensiveCommodityDetails() {
            return mostExpensiveCommodityDetails_;
        }

        /**
         * Set the minimum desired quantity drop percentage.
         * This is only needed for commodity with MM1 distribution function.
         */
        void setMinDecreasePct() {
            if (mostExpensiveCommodityDetails_ == null) {
                return;
            }
            minDecreasePct_ = Optional.ofNullable(mostExpensiveCommodityDetails_.getUpdatingFunction())
                    .filter(UpdatingFunctionFactory::isMM1DistributionFunction)
                    .map(MM1Distribution.class::cast)
                    .map(MM1Distribution::getMinDecreasePct)
                    .orElse(0.0F);
        }

        /**
         * Get the minimum desired quantity drop percentage.
         * @return the minimum desired quantity drop percentage
         */
        float getMinDecreasePct() {
            return minDecreasePct_;
        }
    }

    /**
     * Return a list of recommendations to optimize the cloning of all eligible traders in the
     * economy.
     *
     * <p>
     *  As a result of invoking this method, both the economy and the state that are passed as
     *  parameters to it, may be changed.
     * </p>
     *
     * @param economy - the economy whose traders we want to clone if profitable while remaining
     *                  in the desired state
     * @param ledger - the class that contains exp/rev about all the traders and commodities in
     *                  the economy
     * @return list of provision and move actions
     */
    public static @NonNull List<@NonNull Action> provisionDecisions(@NonNull Economy economy,
                                                                    @NonNull Ledger ledger) {
        List<@NonNull Action> allActions = new ArrayList<>();
        try {
            if (economy.getSettings().isEstimatesEnabled()) {
                EstimateSupply es = new EstimateSupply(economy, ledger, true);

                allActions.addAll(es.getActions());
                allActions.addAll(Placement.runPlacementsTillConverge(economy, ledger,
                        EconomyConstants.PROVISION_PHASE).getActions());
            }
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                "Proivion - Estimate Supply and Placement ", e.getMessage(), e);
        }
        // copy the markets from economy and use the copy to iterate, because in
        // the provision logic, we may add new basket which result in new market
        List<Market> orignalMkts = new ArrayList<>();
        orignalMkts.addAll(economy.getMarkets());
        for (Market market : orignalMkts) {
            try {
                // if the traders in the market are not eligible for provision, skip this market
                if (!canMarketProvisionSellers(market)) {
                    continue;
                }
                for(;;) {
                    if (economy.getForceStop()) {
                        return allActions;
                    }
                    List<@NonNull Action> actions = new ArrayList<>();

                    ledger.calculateExpAndRevForSellersInMarket(economy, market);
                    // break if there is no seller that is eligible for cloning in the market
                    MostProfitableBundle pb = findBestTraderToEngage(market, ledger, economy);
                    Trader mostProfitableTrader = pb.getMostProfitableTrader();
                    MostExpensiveCommodityDetails mostExpensiveCommodityDetails =
                            pb.getMostExpensiveCommodityDetails();
                    if (mostProfitableTrader == null || mostExpensiveCommodityDetails == null) {
                        break;
                    }
                    boolean isDebugMostProfitableTrader = mostProfitableTrader.isDebugEnabled();
                    String mostProfitableTraderDebugInfo =
                            mostProfitableTrader.getDebugInfoNeverUseInCode();
                    Action provisionAction = null;
                    double origRoI = ledger.getTraderIncomeStatements().get(
                            mostProfitableTrader.getEconomyIndex()).getROI();
                    double oldRevenue = ledger.getTraderIncomeStatements().get(
                            mostProfitableTrader.getEconomyIndex()).getRevenues();

                    Trader provisionedTrader = null;
                    boolean isDebugProvisionedTrader = false;
                    boolean successfulEvaluation = false;
                    if (!market.getInactiveSellers().isEmpty()) {
                        // TODO: pick a trader that is closest to the mostProfitableTrader to activate
                        // reactivate a suspended seller
                        List<Trader> copiedInactiveSellers = new ArrayList<>(market.getInactiveSellers());
                        for (Trader seller : copiedInactiveSellers) {
                            if (isEligibleForActivation(seller, mostProfitableTrader, economy, market)) {
                                provisionAction = new Activate(economy, seller, market.getBasket(),
                                        mostProfitableTrader,
                                        mostExpensiveCommodityDetails.getCommoditySpecification());
                                actions.add(provisionAction.take());
                                provisionedTrader = ((Activate)provisionAction).getTarget();
                                try {
                                    isDebugProvisionedTrader = provisionedTrader.isDebugEnabled();
                                    String provisionedTraderDebugInfo =
                                            provisionedTrader.getDebugInfoNeverUseInCode();

                                    if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                                        logger.info("Activate " + provisionedTraderDebugInfo
                                                + " to reduce ROI of " + mostProfitableTraderDebugInfo
                                                + ". Its original ROI is " + origRoI
                                                + " and its max desired ROI is "
                                                + ledger.getTraderIncomeStatements()
                                                .get(mostProfitableTrader.getEconomyIndex())
                                                .getMaxDesiredROI() + ".");
                                    }

                                    actions.addAll(placementAfterProvisionAction(economy, market, mostProfitableTrader));

                                    if (!evaluateAcceptanceCriteria(economy, ledger, origRoI, pb, provisionedTrader)) {
                                        if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                                            logger.info("Roll back activation of " + provisionedTraderDebugInfo
                                                    + ", because it does not reduce ROI of "
                                                    + mostProfitableTraderDebugInfo + ".");
                                        }
                                        // remove IncomeStatement from ledger and rollback actions
                                        rollBackActionAndUpdateLedger(ledger, provisionedTrader, actions, provisionAction);
                                        actions.clear();
                                        continue;
                                    }
                                    successfulEvaluation = true;
                                    break;
                                } catch (Exception e) {
                                    logger.error(EconomyConstants.EXCEPTION_MESSAGE, seller.getDebugInfoNeverUseInCode(),
                                            e.getMessage(), e);
                                    economy.getExceptionTraders().add(seller.getOid());
                                    rollBackActionAndUpdateLedger(ledger, provisionedTrader, actions, provisionAction);
                                }
                            }
                        }
                    }
                    if (!successfulEvaluation) {
                        // provision a new trader
                        provisionAction = new ProvisionBySupply(economy,
                                mostProfitableTrader, mostExpensiveCommodityDetails.getCommoditySpecification());
                        actions.add(provisionAction.take());
                        provisionedTrader = ((ProvisionBySupply)provisionAction).getProvisionedSeller();
                        try {
                            isDebugProvisionedTrader = provisionedTrader.isDebugEnabled();
                            String provisionedTraderDebugInfo =
                                    provisionedTrader.getDebugInfoNeverUseInCode();

                            if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                                logger.info("Provision " + provisionedTraderDebugInfo
                                        + " to reduce ROI of " + mostProfitableTraderDebugInfo
                                        + ". Its original ROI is " + origRoI
                                        + " and its max desired ROI is "
                                        + ledger.getTraderIncomeStatements()
                                        .get(mostProfitableTrader.getEconomyIndex())
                                        .getMaxDesiredROI() + ".");
                            }

                            List<Action> subActions = ((ProvisionBySupply)provisionAction)
                                    .getSubsequentActions();
                            actions.addAll(subActions);
                            ledger.addTraderIncomeStatement(provisionedTrader);
                            subActions.forEach(action -> {
                                if (action instanceof ProvisionBase) {
                                    ledger.addTraderIncomeStatement(((ProvisionBase)action)
                                            .getProvisionedSeller());
                                }
                            });
                            actions.addAll(placementAfterProvisionAction(economy, market, mostProfitableTrader));
                            if (!evaluateAcceptanceCriteria(economy, ledger, origRoI, pb, provisionedTrader)) {
                                if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                                    logger.info("Roll back provision of " + provisionedTraderDebugInfo
                                            + ", because it does not reduce ROI of "
                                            + mostProfitableTraderDebugInfo + ".");
                                }
                                // Because if we roll back original action, subsequent actions will roll back too.
                                actions.removeAll(subActions);
                                // remove IncomeStatement from ledger and rollback actions
                                rollBackActionAndUpdateLedger(ledger, provisionedTrader, actions, provisionAction);
                                break;
                            }
                        } catch (Exception e) {
                            logger.error(EconomyConstants.EXCEPTION_MESSAGE, mostProfitableTrader.getDebugInfoNeverUseInCode(),
                                    e.getMessage(), e);
                            economy.getExceptionTraders().add(mostProfitableTrader.getOid());
                            rollBackActionAndUpdateLedger(ledger, provisionedTrader, actions, provisionAction);
                            break;
                        }
                    }
                    logger.info(mostProfitableTrader.getDebugInfoNeverUseInCode() + " triggered " +
                            ((provisionAction instanceof Activate) ? "ACTIVATION of " : "PROVISION of ")
                            + provisionedTrader.getDebugInfoNeverUseInCode()
                            + " due to commodity : "
                            + mostExpensiveCommodityDetails.getCommoditySpecification().getDebugInfoNeverUseInCode());
                    ((ActionImpl)provisionAction).setImportance(oldRevenue - ledger
                            .getTraderIncomeStatements().get(mostProfitableTrader
                                    .getEconomyIndex()).getRevenues());

                    if (logger.isTraceEnabled() || isDebugMostProfitableTrader || isDebugProvisionedTrader) {
                        logger.info("New ROI of " + mostProfitableTraderDebugInfo + " is "
                                + ledger.getTraderIncomeStatements()
                                .get(mostProfitableTrader.getEconomyIndex()).getROI()
                                + ".");
                    }

                    allActions.addAll(actions);
                }
            } catch (Exception e) {
                logger.error(EconomyConstants.EXCEPTION_MESSAGE,
                    market.getActiveSellers().isEmpty() ? "market " + market.toString()
                        : market + " " + market.toString() + " with first active seller "
                        + market.getActiveSellers().get(0).getDebugInfoNeverUseInCode(),
                        e.getMessage(), e);
            }
        }
        return allActions;
    }

    /**
     * Return a list of move actions to optimize the placement of the traders in a
     * market, after a provision or activation of a seller.
     *
     * @param economy - the economy where the market exist
     * @param market - the market whose traders we move
     * @param mostProfitableTrader - the most profitable trader of the market
     *
     * @return list of move actions
     */
    public static @NonNull List<@NonNull Action> placementAfterProvisionAction(@NonNull Economy economy
            , @NonNull Market market
            , @NonNull Trader mostProfitableTrader) {
        List<@NonNull Action> actions = new ArrayList<>();
        actions.addAll(Placement.prefPlacementDecisions(economy,
                new ArrayList<>(mostProfitableTrader.getCustomers())).getActions());
        // Allow all buyers in markets where mostProfitableTrader is a seller place again so they
        // can re-balance with the added resources in case these buyers are not part of the
        // current market.
        for (Market m : economy.getMarketsAsSeller(mostProfitableTrader)) {
            actions.addAll(Placement.prefPlacementDecisions(economy, m.getBuyers()).getActions());
        }
        return actions;
    }

    /**
     * returns true if the traders in the market are in the right conditions for them to be
     * considered for cloning
     *
     * @param market - the market whose seller ROIs are checked to verify profitability that implies
     * eligibility to clone
     *
     * @return true if the sellers in the market are eligible for cloning and false otherwise
     */
    private static boolean canMarketProvisionSellers(Market market) {

        // do not consider cloning in this market if there are no active sellers
        // available for placement
        if (market.getActiveSellersAvailableForPlacement().isEmpty()) {
            return false;
        }

        List<ShoppingList> buyers = market.getBuyers();
        // there is no point in cloning in a market with a single buyer, and the single buyer
        // is not a guaranteed buyer
        if (buyers.size() == 1 && !buyers.get(0).getBuyer().getSettings().isGuaranteedBuyer()) {
            return false;
        }

        // if none of the buyers in this market are movable and the immovable buyer is not a
        // guaranteedbuyer, provisioning a seller is not beneficial
        if (buyers.stream().allMatch(shoppingList -> !shoppingList.isMovable()
                && !shoppingList.getBuyer().getSettings().isGuaranteedBuyer())) {
            return false;
        }

        return true;
    }

    /**
     * returns best trader to clone after checking the engagement criteria for all traders of a
     * particular market
     *
     * @param market - the market whose seller ROIs are checked to verify profitability that
     * implies eligibility to clone
     * @param ledger - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param economy - that the market is a part of
     * @return the {@link MostProfitableBundle} containing the mostProfitableTrader if there is one that
     * can clone and NULL otherwise
     */
    private static MostProfitableBundle findBestTraderToEngage(Market market, Ledger ledger, Economy economy) {

        Trader mostProfitableTrader = null;
        double roiOfRichestTrader = 0;
        MostExpensiveCommodityDetails mostProfitableCommodity = null;
        // consider only sellers available for placements. Considering a seller with cloneable false
        // is going to fail acceptanceCriteria since none its customers will move
        for (Trader seller : market.getActiveSellersAvailableForPlacement()) {
            boolean isDebugTrader = seller.isDebugEnabled();
            String traderDebugInfo = seller.getDebugInfoNeverUseInCode();
            MostExpensiveCommodityDetails mostExpensiveCommodity = ledger.calculateExpRevForTraderAndGetTopRevenue(economy, seller);
            if (seller.getSettings().isCloneable()) {
                IncomeStatement traderIS = ledger.getTraderIncomeStatements().get(seller
                        .getEconomyIndex());
                // return the most profitable trader
                double roiOfTrader = traderIS.getROI();
                if (logger.isTraceEnabled() || isDebugTrader) {
                    logger.info("{" + traderDebugInfo + "} trader ROI: " + roiOfTrader
                            + ", max desired ROI: " + traderIS.getMaxDesiredROI() + ".");
                }
                // The seller should have at least 1 customer which is participating in the market
                // so that this customer can be moved out when processing this market.
                Set<ShoppingList> sellerCustomersInCurrentMarket = seller.getCustomers(market);
                // TODO: evaluate if checking for movable customers earlier is beneficial
                // clone candidate should either have at least one customer is movable or
                // all customers that are from guaranteed buyer
                if (sellerCustomersInCurrentMarket.size() > 0
                        && (roiOfTrader > traderIS.getMaxDesiredROI())
                        && (roiOfTrader > roiOfRichestTrader)
                        && (seller.getCustomers().stream().anyMatch(ShoppingList::isMovable)
                        || (seller.getCustomers().stream().allMatch(sl ->
                        sl.getBuyer().getSettings().isGuaranteedBuyer())))) {
                    mostProfitableTrader = seller;
                    roiOfRichestTrader = roiOfTrader;
                    mostProfitableCommodity = mostExpensiveCommodity;
                } else {
                    if (logger.isTraceEnabled() || isDebugTrader) {
                        if (roiOfTrader <= traderIS.getMaxDesiredROI()) {
                            logger.info("{" + traderDebugInfo + "} is not the best trader to"
                                    + " engage because its ROI (" + roiOfTrader + ") is not"
                                    + " bigger than its max desired ROI ("
                                    + traderIS.getMaxDesiredROI() + ").");
                        }
                        if (roiOfTrader <= roiOfRichestTrader) {
                            logger.info("{" + traderDebugInfo + "} is not the best trader to"
                                    + " engage because its ROI (" + roiOfTrader + ") is not"
                                    + " bigger than the ROI od the richest trader so far ("
                                    + roiOfRichestTrader + ").");
                        }
                        if (seller.getCustomers().stream().noneMatch(ShoppingList::isMovable)
                                && seller.getCustomers().stream().anyMatch(sl ->
                                        !sl.getBuyer().getSettings().isGuaranteedBuyer())) {
                            logger.info("{" + traderDebugInfo + "} is not the best trader to"
                                    + " engage because it has no movable customer and none of them"
                                    + " is a guaranteed buyer.");
                        }
                    }
                }
            } else {
                if (logger.isTraceEnabled() || isDebugTrader) {
                    logger.info("{" + traderDebugInfo + "} is not clonable.");

                }
            }
        }
        return new MostProfitableBundle(mostProfitableTrader, mostProfitableCommodity);
    }

    /**
     * Calculate the current ROI of the  <b>mostProfitableTrader</b>. Return true if this has
     * decreased compared to <b>origROI</b>. If the most expensive commodity uses a quantity drop
     * based stop criteria, use that criteria instead of the ROI drop based stop criteria.
     *
     * @param economy              - the {@link Economy} where <b>mostProfitableTrader</b> participates in
     * @param ledger               - the ledger that holds the incomeStatement of the trader whose ROI is checked
     * @param origRoI              - the RoI of the mostProfitableTrader before placements
     * @param mostProfitableBundle - {@link MostProfitableBundle} that contains the trader with the
     *                             highest RoI and was selected to be cloned
     * @param provisionedTrader    - {@link Trader} that has been provisioned in this economy
     * @return true - if (a) the current ROI of the <b>mostProfitableTrader</b> is less than
     * the <b>origROI</b> and (b) and the newly provisioned trader has customers. If the most
     * expensive commodity uses a quantity drop based stop criteria, return true if the quantity
     * of the most expensive commodity after provision is still the same, and the quantity has
     * dropped by at least minDecreasePct.
     */
    private static boolean evaluateAcceptanceCriteria(Economy economy,
                                                      Ledger ledger,
                                                      double origRoI,
                                                      MostProfitableBundle mostProfitableBundle,
                                                      Trader provisionedTrader) {
        // Make sure at least one buyer has moved into the new trader
        if (provisionedTrader.getCustomers().isEmpty()) {
            return false;
        }

        // Get the most profitable commodity's revenue and quantity of the most
        // profitable trader before the provision or activation
        Trader topTrader = mostProfitableBundle.getMostProfitableTrader();
        MostExpensiveCommodityDetails origTopCommDetails =
                mostProfitableBundle.getMostExpensiveCommodityDetails();
        double origTopCommRev = origTopCommDetails.getRevenues();
        double origTopCommQuantity = origTopCommDetails.getQuantity();
        // Get the most profitable commodity's revenue and quantity of the most
        // profitable trader after a new trader is provisioned or activated
        MostExpensiveCommodityDetails newTopCommDetails =
                ledger.calculateExpRevForTraderAndGetTopRevenue(economy, topTrader);
        double newTopCommRev = newTopCommDetails.getRevenues();
        double newTopCommQuantity = newTopCommDetails.getQuantity();
        double newRoI = ledger.getTraderIncomeStatements()
                .get(topTrader.getEconomyIndex())
                .getROI();
        if (logger.isTraceEnabled()) {
            logger.info("Evaluating provision: ROI of {}: original {}, new {}. "
                            + "Revenue: original {}, new {}.",
                    topTrader.getDebugInfoNeverUseInCode(), origRoI, newRoI,
                    origTopCommRev, newTopCommRev);
        }
        // Check quantity drop based stop criteria.
        // For certain commodities like Response Time, we use MM1 distribution after a provision.
        // We want to make sure that, in addition to RoI and Revenue drop, there is also a minimal
        // drop of quantity for the most top commodity to justify the provision.
        if (mostProfitableBundle.getMinDecreasePct() > 0.0
                && origTopCommDetails.getCommoditySpecification()
                == newTopCommDetails.getCommoditySpecification()) {
            final double desiredTopCommQuantity = origTopCommQuantity
                    - origTopCommQuantity * mostProfitableBundle.getMinDecreasePct();
            if (logger.isTraceEnabled()) {
                logger.info("M/M/1: Quantity of {}: original {}, new {}, desired {}",
                        origTopCommDetails.getCommoditySpecification().getDebugInfoNeverUseInCode(),
                        origTopCommQuantity, newTopCommQuantity, desiredTopCommQuantity);
            }
            if (newTopCommQuantity >= desiredTopCommQuantity) {
                return false;
            }
        }
        // Check RoI drop based stop criteria
        return newRoI < origRoI && newTopCommRev < origTopCommRev;
    }

    /**
     * Remove {@link IncomeStatement} of a trader and rollback action after acceptanceCriteria fails
     *
     * @param ledger - the ledger that holds the incomeStatement of the trader that is being removed
     * @param provisionedTrader - {@link Trader} that was cloned
     * @param actions - bunch of actions that were generated after passing the acceptanceCriteria
     *                  that need to be rolledBack
     *
     */
    private static void rollBackActionAndUpdateLedger(Ledger ledger,
                                                      Trader provisionedTrader, List<@NonNull Action> actions, Action provisionAction) {
        // remove IncomeStatement from ledger and rollback actions
        if (provisionAction instanceof ProvisionBySupply) {
            Lists.reverse(((ProvisionBySupply)provisionAction).getSubsequentActions()).forEach(action -> {
                if (action instanceof ProvisionBase) {
                    ledger.removeTraderIncomeStatement(((ProvisionBase)action).getProvisionedSeller());
                }
            });
            ledger.removeTraderIncomeStatement(provisionedTrader);
        }
        Lists.reverse(actions).forEach(axn -> axn.rollback());
    }

    /**
     * Checks if the trader is eligible for activation. It checks if any of the traders on the
     * mostProfitableTrader satisfies 2 conditions:
     * 1. does it have cliques in common with inactive trader and
     * 2. can it fit in the inactive trader
     * If yes, then the inactiveTrader is eligible for activation
     *
     * @param inactiveTrader the inactive trader which is a candidate for activation
     * @param mostProfitableTrader the mostProfitableTrader
     * @param economy the economy
     * @param m the market under process
     * @return true if the trader is eligible for activation, false otherwise
     */
    @VisibleForTesting
    private static boolean isEligibleForActivation(
            Trader inactiveTrader, Trader mostProfitableTrader, Economy economy, Market m) {
        Set<ShoppingList> slsOnMostProfitableTrader = mostProfitableTrader.getCustomers(m);
        for (ShoppingList sl : slsOnMostProfitableTrader) {
            Trader customerOnMostProfitableTrader = sl.getBuyer();
            if ((!customerOnMostProfitableTrader.getSettings().isShopTogether() ||
                    doBuyerAndSellerShareCliques(customerOnMostProfitableTrader, inactiveTrader, economy))
                    && ProvisionUtils.canBuyerFitInSeller(sl, inactiveTrader, economy)) {
                return true;
            }
        }
        if (logger.isTraceEnabled() || inactiveTrader.isDebugEnabled()) {
            logger.debug(inactiveTrader.getDebugInfoNeverUseInCode() + " is not eligible for " +
                    "activation because none of the customers of " +
                    mostProfitableTrader.getDebugInfoNeverUseInCode() + " can be placed on it.");
        }
        return false;
    }

    /**
     * Checks if the buyer's common cliques and seller's cliques have anything in common.
     *
     * @param buyer the Trader which is a buyer
     * @param seller the Trader which is a seller
     * @param economy the economy
     * @return true if buyer's common cliques and seller's cliques have anything in common.
     */
    private static boolean doBuyerAndSellerShareCliques (
            Trader buyer, Trader seller , Economy economy) {
        // economy.getCommonCliques(buyer) returns com.google.common.collect.Sets$SetView,
        // which doesn't support retainAll.
        Set<Long> commonCliquesOfCustomer = new HashSet<>(economy.getCommonCliques(buyer));
        Set<Long> inactiveTraderCliques = seller.getCliques();
        commonCliquesOfCustomer.retainAll(inactiveTraderCliques);
        return !commonCliquesOfCustomer.isEmpty();
    }
}
