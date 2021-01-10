package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

/**
 * This class estimates the excess or lack of supply and optimizes
 * provisions and suspensions.
 */
public class EstimateSupply {
    private Economy economy_;
    private Ledger ledger_;
    // Map of superSeller per market
    private Map<Market, SuperSeller> marketSuperSellers_;
    static final Logger logger = LogManager.getLogger(EstimateSupply.class);
    // clone and suspend sellers list
    private List<Action> actions_;
    List<Trader> suspendReverseList = new ArrayList<>();
    List<Action> deactActions_ = new ArrayList<>();

    private Map<Market, Set<Trader>> suspensionCandidatesMap_ = new HashMap<>();

    public EstimateSupply(Economy economy, Ledger ledger, boolean isProvision) {
        logger.info("Plan started Estimate Supply");
        economy_ = economy;
        ledger_ = ledger;
        marketSuperSellers_ = new HashMap<>();
        actions_ = new ArrayList<>();

        for (Market market : economy.getMarkets().stream()
                        .sorted((m1, m2) -> m1.getBuyers().size() - m2.getBuyers().size())
                        .collect(Collectors.toList())) {
            if (!isEligibleMarket(market)) {
                continue;
            }
            // Construct a super Seller for the market
            constructSuperSeller(market);
            // Order all sellers in the market by commodity capacity, to assist cloning/suspension
            orderInactiveCandidateHeapByComm(market);
            // Take provision and suspension decisions per market
            adjustSupply(market, isProvision);
        }

        if (!isProvision) {
            // rollback deactivate actions
            deactActions_.forEach(Action::rollback);
        }
        // Revert the suspendable field to true for all traders added by adjustSupply
        suspendReverseList.stream().forEach(seller -> seller.getSettings().setSuspendable(true));
        logger.info("Plan Completed Estimate Supply");

    }

    /**
     * Determine if we should estimate the supply in a market.
     * The market is <B>not</B> eligible if any of the following conditions is satisfied:
     * (1) there are no sellers that are availableForPlacement in the market.
     * (2) the market consists only of guaranteed buyers.
     * (3) there are no cloneable or suspendable sellers in the market.
     *
     * @param market The market for which we are determining eligibility
     * @return whether it is an eligible market
     */
    private boolean isEligibleMarket(Market market) {
        return !(market.getActiveSellersAvailableForPlacement().isEmpty()
                        || market.getBuyers().stream()
                            .map(ShoppingList::getBuyer)
                            .map(Trader::getSettings)
                            .allMatch(TraderSettings::isGuaranteedBuyer)
                        || market.getActiveSellersAvailableForPlacement().stream()
                            .map(Trader::getSettings)
                            .anyMatch(EstimateSupply::clonableOrSuspendable));
    }

    private static boolean clonableOrSuspendable(TraderSettings sellerSettings) {
        return sellerSettings.isCloneable() || sellerSettings.isSuspendable();
    }

    /**
     * Construct a super seller for the market.
     *
     * @param market the market for which we construct the super seller
     */
    private void constructSuperSeller(Market market) {
        // TODO: do we need the map, since we are always evaluating a single market?
        // Create super Seller and add to the marketSupperSellers map
        marketSuperSellers_.put(market, new SuperSeller(market, ledger_));
        // Set the usage and capacity of each commodity of the superSeller
        // We don't care about setting priceFn for superSellers in markets with no activeSellers
        // since we don't adjustSupply in these markets
        setSuperSellerUsageAndCapacity(market);
    }

    /**
     * Set the usage and capacity for each commodity sold by the super seller.
     *
     * @param market the market for whose superSeller we set the usage and capacity
     */
    private void setSuperSellerUsageAndCapacity(Market market) {
        SuperSeller superSeller = marketSuperSellers_.get(market);
        // go over all sellers in the market, and for each commodity they sell, add the usage
        // and capacity to the corresponding commodity sold by the superSeller
        boolean firstSeller = true;
        for (Trader seller : market.getActiveSellers()) {
            Basket sellerBasketSold = seller.getBasketSold();
            boolean isCloneable = seller.getSettings().isCloneable();
            boolean isSuspendable = seller.getSettings().isSuspendable();
            List<CommodityResource> superCommSoldList = superSeller.getCommoditySoldList();
            for (int sellerIndex = 0, superIndex = 0; sellerIndex < sellerBasketSold.size()
                            && superIndex < superCommSoldList.size(); sellerIndex++, superIndex++) {
                while (!superCommSoldList.get(superIndex).getCommoditySpecification()
                                .equals(sellerBasketSold.get(sellerIndex))) {
                    sellerIndex++;
                }
                CommodityResource commResourceSold = superCommSoldList.get(superIndex);
                if (firstSeller) {
                    commResourceSold.setPriceFunction(seller.getCommoditiesSold().get(sellerIndex)
                        .getSettings().getPriceFunction());
                }
                CommoditySold commSoldSeller = seller.getCommoditiesSold().get(sellerIndex);
                // Add seller capacity to superSeller
                double sellerEffCapacity = commSoldSeller.getEffectiveCapacity();
                commResourceSold.increaseCapacityBy(sellerEffCapacity);
                // Add seller quantity and peak quantity to superSeller
                commResourceSold.increaseQuantityBy(commSoldSeller.getQuantity());
                commResourceSold.increasePeakQuantityBy(commSoldSeller.getPeakQuantity());
                if (isCloneable) {
                    if (sellerEffCapacity > commResourceSold.getMaxCapacity()) {
                        commResourceSold.setMaxCapacity(sellerEffCapacity);
                        commResourceSold.setCandidateClone(seller);
                    }
                }

                if (isSuspendable) {
                    commResourceSold.getCandidateSuspendHeap().offer(seller);
                }
            }
            firstSeller = false;
        }
        // go over all demand that is currently not placed in any seller in the market, and add
        // the requested quantities to the corresponding commodities sold by the superSeller
        for (ShoppingList buyer : market.getBuyers()) {
            if (buyer.getSupplier() == null) {
                for (int index = 0; index < market.getBasket().size(); index++) {
                    CommodityResource commResource = superSeller.getCommoditySoldList().get(index);
                    commResource.increaseQuantityBy(buyer.getQuantity(index));
                    commResource.increasePeakQuantityBy(buyer.getPeakQuantity(index));
                }
            }
        }
    }

    /**
     * For every commodity sold by the superSeller, order the market sellers that can be suspended,
     * smallest first, and identify the market seller with the largest capacity.
     *
     * @param market the market whose sellers are considered
     */
    private void orderInactiveCandidateHeapByComm(Market market) {
        // Get superSeller for current Market
        SuperSeller superSeller = marketSuperSellers_.get(market);
        for (Trader seller : market.getInactiveSellers()) {
            for (CommodityResource commResource : superSeller.getCommoditySoldList()) {
                commResource.getInactiveCandidateHeap().offer(seller);
            }
        }
    }

    /**
     * Clone/suspend sellers as required, until superSeller ROI is between min and max desired ROI.
     *
     * @param market The market for which we need to adjust supply
     * @param isProvision whether provision is enabled
     */
    private void adjustSupply(Market market, boolean isProvision) {
        // Get superSeller for current Market
        SuperSeller superSeller = marketSuperSellers_.get(market);
        // Calculate super Sellers revenues
        superSeller.calcSuperSellerRevenues(market);
        // Calculate ROI
        ledger_.calculateExpAndRevForSellersInMarket(economy_, market);
        double curROI = superSeller.getCurrROI();
        if (curROI == -1) {
            return;
        }
        double maxDesiredROI = superSeller.getDesiredROI(true);
        double minDesiredROI = superSeller.getDesiredROI(false);
        if (isProvision && (
                        curROI > maxDesiredROI
                        && superSeller.isCloneable()
                        && market.getBuyers().size() > market.getActiveSellers().size())) {
            logValues(market, curROI, maxDesiredROI, minDesiredROI);
            adjustClone(market);
        } else if (!isProvision && (
                        curROI < minDesiredROI
                        && superSeller.isSuspendable()
                        && market.getActiveSellers().size() > 1)) {
            logValues(market, curROI, maxDesiredROI, minDesiredROI);
            adjustSuspend(market);
            adjustSuspend(market);
        }
    }

    private void logValues(Market market, double curROI, double maxDesiredROI, double minDesiredROI) {
        logger.debug("Market with {} active sellers such as {}",
            () -> market.getActiveSellers().size(),
            () -> market.getActiveSellers().iterator().next());
        logger.debug("    current/min/max ROI : {} / {} / {}",
            curROI, minDesiredROI, maxDesiredROI);
    }

    /**
     * Make Cloning decisions. Reactivate existing host, or clone biggest host for commodity of
     * superSeller that has the highest revenues. Continue cloning while ROI > maxDesiredROI.
     *
     * @param market The market that we need to adjust clone
     */
    private void adjustClone(Market market) {
        //get super seller first
        SuperSeller superSeller = marketSuperSellers_.get(market);
        boolean canClone = true;
        // Clone the next seller if and only if canClone condition is true
        while (canClone) {
            CommodityResource commResourceHighestRev = superSeller.getCommoditySoldList().stream()
                            .sorted().findFirst().orElse(null);
            Action action;
            Trader newSeller;
            // Reactivate current inactive host first (if there is one)
            if (!commResourceHighestRev.getInactiveCandidateHeap().isEmpty()) {
                newSeller = commResourceHighestRev.getInactiveCandidateHeap().poll();
                action = (new Activate(economy_, newSeller, market.getBasket(), newSeller,
                    commResourceHighestRev.getCommoditySpecification())).take();
                logger.debug("Activating trader {}", newSeller);
            } else { // clone existing
                newSeller = commResourceHighestRev.getCandidateClone();
                action = (new ProvisionBySupply(economy_, newSeller,
                    commResourceHighestRev.getCommoditySpecification())).take();
                logger.debug("Adding trader {} based on {}",
                    () -> ((ProvisionBySupply)action).getProvisionedSeller(),
                    () -> newSeller);
                ledger_.addTraderIncomeStatement(((ProvisionBySupply)action)
                    .getProvisionedSeller());
            }
            action.getActionTarget().getSettings().setSuspendable(false);
            actions_.add(action);
            suspendReverseList.add(action.getActionTarget());
            logger.debug("Added seller: {}", action.getActionTarget());
            // Update capacity and revenues of superSeller
            superSeller.updateCapacityAndRevenues(newSeller, true);
            // Calculate expenses and revenues of the new seller
            ledger_.calculateExpRevForTraderAndGetTopRevenue(economy_, action.getActionTarget());
            // Decide to clone more or stop cloning
            canClone = superSeller.getCurrROI() > superSeller.getDesiredROI(true);
            logger.debug("        currROI: " + superSeller.getCurrROI());
        }
    }

    /**
     * Make suspension decision here, suspend host from smallest
     * host to biggest host of a commodity with lowest revenue,
     * add all suspend result to list.
     *
     * @param market The market that we need to adjust suspend
     */
    private void adjustSuspend(Market market) {
        // Use suspended set to avoid duplicate suspend
        Set<Trader> suspendedSet = new HashSet<>();
        SuperSeller superSeller = marketSuperSellers_.get(market);
        boolean canSuspend = true;
        while (canSuspend) {
            CommodityResource commResourceLowestRev = superSeller.getCommoditySoldList().stream()
                            .sorted(Collections.reverseOrder()).findFirst().orElse(null);
            Trader suspendCandidate;
            PriorityQueue<Trader> candidateHeap = commResourceLowestRev.getCandidateSuspendHeap();
            // Poll all duplicate sellers
            while (suspendedSet.contains(candidateHeap.peek())) {
                candidateHeap.poll();
            }
            // Get the suspension candidate
            suspendCandidate = commResourceLowestRev.getCandidateSuspendHeap().poll();
            suspendedSet.add(suspendCandidate);
            // Suspend
            Deactivate deactivateAction =
                new Deactivate(economy_, suspendCandidate, market.getBasket());
            deactivateAction.take();

            // Update capacity and revenues of superSeller
            superSeller.updateCapacityAndRevenues(suspendCandidate, false);
            ledger_.calculateExpRevForTraderAndGetTopRevenue(economy_, suspendCandidate);
            // stop suspend if ROI value is too high after suspend
            double desiredROI = (superSeller.getDesiredROI(true) / 2
                            + superSeller.getDesiredROI(false) / 2);
            double oldROI = superSeller.getCurrROI();
            if (Math.abs(superSeller.getCurrROI() - desiredROI) > Math.abs(oldROI - desiredROI)) {
                deactivateAction.rollback();
                suspendedSet.remove(suspendCandidate);
                return;
            }
            deactActions_.add(deactivateAction);
            logger.debug("Suspend Seller: {}", suspendCandidate);
            // Calculate expenses and revenues of the suspended seller
            // Decide to clone more or stop cloning
            canSuspend = superSeller.getCurrROI() < superSeller.getDesiredROI(false)
                            && !candidateHeap.isEmpty() && market.getActiveSellers().size() > 1;
                            logger.debug("        currROI: " + superSeller.getCurrROI());
        }
        suspensionCandidatesMap_.put(market, suspendedSet);
    }

    public Set<Trader> getSuspensionCandidates(Market market) {
        return suspensionCandidatesMap_.get(market);
    }

    public List<Action> getActions() {
        return actions_;
    }

    /**
     * A seller that has the cumulative capacity of all sellers in a market.
     *
     */

    class SuperSeller {
        private List<CommodityResource> commSoldList_;
        private Market market_;
        private Ledger ledger_;
        private boolean cloneable_;
        private boolean suspendable_;

        SuperSeller(Market market, Ledger ledger) {
            commSoldList_ = new ArrayList<>();
            ledger_ = ledger;
            market_ = market;
            // create the list of commodities the superSeller is selling
            for (int index = 0; index < market.getBasket().size(); index++) {
                CommoditySpecification commSpec = market.getBasket().get(index);
                commSoldList_.add(new CommodityResource(commSpec));
            }
            setCloneable(market.getActiveSellers().stream().anyMatch(s -> s.getSettings().isCloneable()));
            setSuspendable(market.getActiveSellers().stream().anyMatch(s -> s.getSettings().isSuspendable()));
        }

        public Market getMarket() {
            return market_;
        }

        public List<CommodityResource> getCommoditySoldList() {
            return commSoldList_;
        }

        public void setCloneable(boolean cloneable) {
            cloneable_ = cloneable;
        }

        public boolean isCloneable() {
            return cloneable_;
        }

        public void setSuspendable(boolean suspendable) {
            suspendable_ = suspendable;
        }

        public boolean isSuspendable() {
            return suspendable_;
        }

        /**
         * Get the commodity revenue for current super seller.
         *
         * @param index The index in market basket
         * @param util Set to -1 means use current utilization, otherwise, use user setting utilization
         *
         * @return The revenue of a commodity in a market
         */
        public double getRevenue(int index, double util) {
            CommodityResource commResource = getCommoditySoldList().get(index);
            if (commResource == null) {
                logger.error("Cannot find Commodity Specification when estimator tried to get revenue.");
                return -1;
            }
            // calculate utilization value
            if (util == -1d) {
                util = commResource.getQuantity() / commResource.getCapacity();
            }

            // TODO: handle what is being passed for priceComputation in the case of complex pf's
            return util * commResource.getPriceFunction().unitPrice(util, null, null, null, null);
        }

        /**
         * get the a commodity revenue for a market base on current utilization.
         *
         * @param index the commodity index
         *
         * @return the revenue of a commodity in a market
         */
        public double getRevenue(int index) {
            return getRevenue(index, -1d);
        }

        /**
         * Calculate the revenues of the market superSeller.
         *
         * @param market The market for which to compute superSeller revenues
         */
        public void calcSuperSellerRevenues(Market market) {
            // Calculate revenue for this market
            for (int i = 0; i < market.getBasket().size(); i++) {
                CommodityResource commResource = getCommoditySoldList().get(i);
                // Put revenue into super Seller
                commResource.setRevenue(getRevenue(i));
            }
        }

        /**
         * Get desire ROI for this super Seller.
         *
         * @param maxDesired when true use max desired ROI, otherwise use min desired ROI
         * @return max or min desired ROI
         */
        public double getDesiredROI(boolean maxDesired) {
            double desiredUtil;
            double desiredRevenue = 0d;
            // Get desired Utilization based on any seller in the market
            Trader modelSeller = market_.getActiveSellers().iterator().next();
            desiredUtil = maxDesired
                    ? modelSeller.getSettings().getMaxDesiredUtil()
                    : modelSeller.getSettings().getMinDesiredUtil();
            // Calculate desired revenues for super Seller
            for (int i = 0; i < market_.getBasket().size(); i++) {
                desiredRevenue += getRevenue(i, desiredUtil);
            }
            // Calculate desired expenses (min expenses if max=true, max expenses if max=false)
            // provision/suspension must be driven by the sellersAvailableForPlacement
            double expense = market_.getActiveSellersAvailableForPlacement().stream()
                    .map(s -> {
                        IncomeStatement incomeStatement =
                                ledger_.getTraderIncomeStatements().get(s.getEconomyIndex());
                        return maxDesired
                            ? incomeStatement.getMinDesiredExpenses()
                            : incomeStatement.getMaxDesiredExpenses();
                    })
                    .reduce((x, y) -> x + y)
                    .get();
            return expense == 0d ? -1 : desiredRevenue / expense;
        }

        /**
         * Get current ROI of superSeller.
         *
         * @return current ROI
         */
        public double getCurrROI() {
            double totalMarketExp = getTotalMarketExpenses(market_);
            return totalMarketExp == 0 ? -1 : getCommoditySoldList().stream()
                .map(commRes -> commRes.getRevenue()).reduce((x, y) -> x + y).get() / totalMarketExp;
        }

        /**
         * Get the total Expenses of all sellers in the market.
         *
         * @param market The market for which total expenses are calculated
         * @return Total expenses of all sellers in the market
         */
        public double getTotalMarketExpenses(Market market) {
            double totalExp = 0;
            for (Trader s: market.getActiveSellersAvailableForPlacement()) {
                totalExp += ledger_.getTraderIncomeStatements().get(s.getEconomyIndex()).getExpenses();
                if (Double.isInfinite(totalExp)) {
                    break;
                }
            }
            return totalExp;
        }

        /**
         * Update capacity and revenues of superSeller when a seller is added
         * removed from the market.
         *
         * @param seller seller added/removed
         * @param add true if seller is added, false if seller is removed
         */
        public void updateCapacityAndRevenues(Trader seller, boolean add) {
            for (int index = 0; index < getCommoditySoldList().size(); index++) {
                CommodityResource commResource = getCommoditySoldList().get(index);
                double capacityDiff = seller.getCommoditySold(commResource.getCommoditySpecification())
                                .getEffectiveCapacity();
                if (add) {
                    commResource.increaseCapacityBy(capacityDiff);
                } else {
                    commResource.decreaseCapacityBy(capacityDiff);
                }
                commResource.setRevenue(getRevenue(index));
            }
        }
    }

    /**
     * A commodity of a {@link SuperSeller}.
     *
     */
    class CommodityResource implements Comparable<CommodityResource> {
        private CommoditySpecification commSpec_;
        private PriceFunction priceFunc_;
        private double quantity_ = 0;
        private double peakQuantity_ = 0;
        private double capacity_ = 0;
        private double revenue_ = 0;
        private double maxCapacity_ = 0;
        private Trader candidateClone_ = null;
        private boolean considerCommodity_ = true;

        private PriorityQueue<Trader> inactiveCandidateHeap_ = new PriorityQueue<>((t1, t2) -> {
            double c1 = t1.getCommoditiesSold().get(t1.getBasketSold().indexOf(commSpec_)).getCapacity();
            double c2 = t2.getCommoditiesSold().get(t2.getBasketSold().indexOf(commSpec_)).getCapacity();
            return c1 < c2 ? 1 : c1 == c2 ? 0 : -1;
        });

        private PriorityQueue<Trader> candidateSuspendHeap_ = new PriorityQueue<>((t1, t2) -> {
            double c1 = t1.getCommoditiesSold().get(t1.getBasketSold().indexOf(commSpec_)).getCapacity();
            double c2 = t2.getCommoditiesSold().get(t2.getBasketSold().indexOf(commSpec_)).getCapacity();
            return c1 < c2 ? -1 : c1 == c2 ? 0 : 1;
        });

        public CommoditySpecification getCommoditySpecification() {
            return commSpec_;
        }

        /**
         * If a commodity resource is not a consider commodity
         * we don't consider this commodity resource any more
         * in the clone and suspend decision.
         * Default value is true.
         *
         * @return whether this commodity should be considered
         */
        public boolean isConsiderCommodity() {
            return considerCommodity_;
        }

        public void setIsConsiderCommodity(boolean isConsider) {
            considerCommodity_ = isConsider;
        }

        /**
         * Get price function of this commodity.
         *
         * @return PriceFuction
         */
        public PriceFunction getPriceFunction() {
            return priceFunc_;
        }

        public void setPriceFunction(PriceFunction priceFunction) {
            priceFunc_ = priceFunction;
        }

        /**
         * Get price function of this commodity.
         *
         * @return PriceFunction
         */
        public double getQuantity() {
            return quantity_;
        }

        public void increaseQuantityBy(double increase) {
            quantity_ += increase;
        }

        /**
         * Get peak quantities of this commodity.
         *
         * @return PeakQuantities
         */
        public double getPeakQuantity() {
            return peakQuantity_;
        }

        public void increasePeakQuantityBy(double increase) {
            peakQuantity_ += increase;
        }

        /**
         * Get capacity of this commodity.
         *
         * @return Capacity
         */
        public double getCapacity() {
            return capacity_;
        }

        public void increaseCapacityBy(double increase) {
            capacity_ += increase;
        }

        public void decreaseCapacityBy(double decrease) {
            capacity_ -= decrease;
        }

        /**
         * Get revenue of this commodity.
         *
         * @return Revenue
         */
        public double getRevenue() {
            return revenue_;
        }

        public void increaseRevenue(double increase) {
            revenue_ += increase;
        }

        public void decreaseRevenueBy(double decrease) {
            revenue_ -= decrease;
        }

        public void setRevenue(double revenue) {
            revenue_ = revenue;
        }

        CommodityResource(CommoditySpecification commSpec) {
            commSpec_ = commSpec;
        }

        public PriorityQueue<Trader> getCandidateSuspendHeap() {
            return candidateSuspendHeap_;
        }

        public PriorityQueue<Trader> getInactiveCandidateHeap() {
            return inactiveCandidateHeap_;
        }

        public double getMaxCapacity() {
            return maxCapacity_;
        }

        public void setMaxCapacity(double capacity) {
            maxCapacity_ = capacity;
        }

        public Trader getCandidateClone() {
            return candidateClone_;
        }

        public void setCandidateClone(Trader candidateClone) {
            candidateClone_ = candidateClone;
        }

        @Override
        public int compareTo(CommodityResource commResource) {
            if (isConsiderCommodity() != commResource.isConsiderCommodity()) {
                return isConsiderCommodity() ? 1 : -1;
            } else {
                return (int)Math.signum(getRevenue() - commResource.getRevenue());
            }
        }
    }
}
