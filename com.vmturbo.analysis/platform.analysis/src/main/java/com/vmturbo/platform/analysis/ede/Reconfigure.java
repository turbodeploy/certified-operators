package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionImpl;
import com.vmturbo.platform.analysis.actions.ReconfigureProviderAddition;
import com.vmturbo.platform.analysis.actions.ReconfigureProviderRemoval;
import com.vmturbo.platform.analysis.actions.Utility;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.EconomyConstants;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;
import com.vmturbo.platform.analysis.ede.Provision.MostProfitableBundle;
import com.vmturbo.platform.analysis.ledger.IncomeStatement;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.ledger.Ledger.MostExpensiveCommodityDetails;

/**
 * Provides capability to generate reconfigure provider actions for entities.
 */
public class Reconfigure {

    private Reconfigure() {}

    private static final Logger logger = LogManager.getLogger();

    /**
     * Attempts to reconfigure providers to add commodities.
     *
     * @param economy the economy.
     * @param ledger the ledger.
     * @return newly generated actions.
     */
    public static @NonNull List<@NonNull Action> reconfigureAdditionDecisions(@NonNull Economy economy,
        @NonNull Ledger ledger) {
        List<@NonNull Action> allActions = new ArrayList<>();
        ArrayList<Market> orignalMkts = new ArrayList<>();
        orignalMkts.addAll(economy.getMarkets());
        orignalMkts.sort((m1, m2) -> {
            return Integer.compare(m1.getSellerCount(), m2.getSellerCount()) * -1;
        });
        for (Market market : orignalMkts) {
            try {
                boolean marketContainsReconfigurableSeller = market.getSellers()
                        .anyMatch(seller -> seller.getSettings().isReconfigurable()
                    && Utility.marketContainsReconfigurableSeller(market, economy));
                // Skip market if no reconfigurable sellers.
                if (!marketContainsReconfigurableSeller) {
                    continue;
                }
                Map<Integer, List<Trader>> reconfigCommBuckets = market.getSellers()
                    .collect(Collectors.groupingBy(trader -> {
                        return trader.getReconfigureableCount(economy);
                    }));
                Map<Integer, List<Trader>> sortedBuckets =
                    new TreeMap<>((Comparator<Integer>)(i1, i2) -> i2.compareTo(i1) * -1);
                sortedBuckets.putAll(reconfigCommBuckets);
                for (Map.Entry<Integer, List<Trader>> entry : sortedBuckets.entrySet()) {
                    if (entry.getKey() == 0) {
                        continue;
                    }
                    int failSafe = 0;
                    for (;;) {
                        failSafe++;
                        if (economy.getForceStop()) {
                            return allActions;
                        }

                        if (failSafe > 100) {
                            logger.warn("Continuous loop in Reconfigure. Breaking out."
                                + "Bucket size: {}. First Trader: {}", entry.getKey(),
                                entry.getValue().get(0));
                            break;
                        }

                        boolean successfulEvaluation = false;
                        List<@NonNull Action> actions = new ArrayList<>();
                        ledger.calculateExpAndRevForSellersInMarket(economy, market);
                        MostProfitableBundle pb = Provision.findBestTraderToEngage(market, ledger, economy, entry.getValue(), true);
                        Trader mostProfitableTrader = pb.getMostProfitableTrader();
                        MostExpensiveCommodityDetails mostExpensiveCommodityDetails = pb.getMostExpensiveCommodityDetails();
                        if (mostProfitableTrader != null && mostExpensiveCommodityDetails != null) {
                            double origRoI = ledger.getTraderIncomeStatements().get(
                                    mostProfitableTrader.getEconomyIndex()).getROI();
                            double oldRevenue = ledger.getTraderIncomeStatements().get(
                                    mostProfitableTrader.getEconomyIndex()).getRevenues();
                            successfulEvaluation = tryCommodityAddition(sortedBuckets,
                                    ledger, mostProfitableTrader, economy, actions, market, pb, origRoI,
                                    mostExpensiveCommodityDetails, oldRevenue);
                            if (successfulEvaluation) {
                                allActions.addAll(actions);
                                continue;
                            }
                        }
                        if (!successfulEvaluation) {
                            break;
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
        return allActions;
    }

    /**
     * Try to add ReconfigureCommodityAddition actions to hosts that do not contain the
     * reconfigurable commodities of the mostProfitableTrader.
     *
     * @param sortedBuckets - The mapping of traders in market and amount of reconfigurable commodities they have.
     * @param ledger - The ledger.
     * @param mostProfitableTrader - Most profitable trader in the market.
     * @param economy - The economy.
     * @param actions - The list of actions.
     * @param market - The Market.
     * @param pb - The most profitable bundle in the market.
     * @param origRoI - the old roi of the most profitable trader.
     * @param mostExpensiveCommodityDetails - the most expensive commodity info.
     * @param oldRevenue - the old revenue.
     * @return boolean if we are able to add the reconfigurable commodity successfully to a trader.
     */
    private static boolean tryCommodityAddition(Map<Integer, List<Trader>> sortedBuckets,
        Ledger ledger, Trader mostProfitableTrader, Economy economy, List<Action> actions,
        Market market, MostProfitableBundle pb, double origRoI,
        MostExpensiveCommodityDetails mostExpensiveCommodityDetails, double oldRevenue) {
        for (Map.Entry<Integer, List<Trader>> innerEntry : sortedBuckets.entrySet()) {
            PriorityQueue<Trader> pq = sortByLeastProfitableTraderToEngage(ledger, innerEntry.getValue(),
                mostProfitableTrader.getBasketSold().stream()
                    .filter(commSpec -> economy.getSettings()
                        .getReconfigureableCommodities().contains(commSpec.getBaseType()))
                    .map(CommoditySpecification::getType)
                    .collect(Collectors.toList()));
            Trader leastProfitableTrader;
            while ((leastProfitableTrader = pq.poll()) != null) {
                List<Action> actionsToAdd = Lists.newArrayList();
                Basket leastProfitableBasket = leastProfitableTrader.getBasketSold();
                List<CommoditySpecification> commSpecList = mostProfitableTrader.getBasketSold().stream()
                    .filter(commSpec -> economy.getSettings()
                        .getReconfigureableCommodities().contains(commSpec.getBaseType()))
                    .collect(Collectors.toList());
                commSpecList.removeIf(commSpec -> leastProfitableBasket.contains(commSpec));
                Map<CommoditySpecification, CommoditySold> commMap = commSpecList.stream()
                    .collect(Collectors.toMap(Function.identity(),
                    commSpec -> mostProfitableTrader.getCommoditySold((CommoditySpecification)commSpec)));

                List<ReconfigureProviderAddition> additionActions = Lists.newArrayList();
                for (Entry<CommoditySpecification, CommoditySold> entry : commMap.entrySet()) {
                    Map<CommoditySpecification, CommoditySold> comm = new HashMap<CommoditySpecification, CommoditySold>() {{
                        put(entry.getKey(), entry.getValue());
                    }};
                    ReconfigureProviderAddition commAddAction = new ReconfigureProviderAddition(economy,
                        (TraderWithSettings)leastProfitableTrader, comm).take();
                    actionsToAdd.add(commAddAction);
                    additionActions.add(commAddAction);
                }
                actionsToAdd.addAll(Provision.placementAfterProvisionAction(economy, market, mostProfitableTrader));
                if (!Provision.evaluateAcceptanceCriteria(economy, ledger, origRoI, pb, leastProfitableTrader)) {
                    Lists.reverse(actionsToAdd).forEach(axn -> axn.rollback());
                    additionActions.clear();
                    actionsToAdd.clear();
                } else {
                    for (ReconfigureProviderAddition commAddAction : additionActions) {
                        logger.info(mostProfitableTrader.getDebugInfoNeverUseInCode() + " triggered "
                            + "RECONFIGURE_PROVIDER_ADDITION of "
                            + leastProfitableTrader.getDebugInfoNeverUseInCode()
                            + " for commodity " + commAddAction.getReconfiguredCommodities().keySet()
                                .stream().map(CommoditySpecification::getDebugInfoNeverUseInCode)
                                .collect(Collectors.toList())
                            + " due to commodity : "
                            + mostExpensiveCommodityDetails.getCommoditySpecification()
                                .getDebugInfoNeverUseInCode());
                        ((ActionImpl)commAddAction).setImportance(oldRevenue - ledger
                            .getTraderIncomeStatements().get(mostProfitableTrader
                                .getEconomyIndex()).getRevenues());
                    }
                    actions.addAll(actionsToAdd);
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Create a queue of least profitable trader to engage checking that the
     * reconfigurable commodities list to add is not all found on the Trader.
     * This queue is ordered by the ROI of the trader is order to be able to use the trader with
     * the least ROI to try to add a reconfigurable commodity to for an overutilized
     * reconfigurable trader.
     *
     * @param ledger - The ledger.
     * @param bucket - The list of traders.
     * @param commTypesToAdd - The reconfigurable commodities to add.
     * @return the sorted queue.
     */
    private static PriorityQueue<Trader> sortByLeastProfitableTraderToEngage(Ledger ledger,
        List<Trader> bucket, List<Integer> commTypesToAdd) {
        PriorityQueue<Trader> additionCandidatesHeap = new PriorityQueue<>((t1, t2) -> {
            IncomeStatement is1 = ledger.getTraderIncomeStatements().get(t1.getEconomyIndex());
            IncomeStatement is2 = ledger.getTraderIncomeStatements().get(t2.getEconomyIndex());
            double c1 = is1.getROI();
            double c2 = is2.getROI();
            return c1 > c2 ? 1 : c1 == c2 ? 0 : -1;
        });
        for (Trader seller : bucket) {
            if (seller.getSettings().isReconfigurable()
                && !seller.getBasketSold().stream()
                    .map(CommoditySpecification::getType)
                    .collect(Collectors.toList())
                    .containsAll(commTypesToAdd)
                && ledger.getTraderIncomeStatements().get(seller.getEconomyIndex()).getROI()
                    < ledger.getTraderIncomeStatements().get(seller.getEconomyIndex()).getMinDesiredROI()) {
                additionCandidatesHeap.offer(seller);
            }
        }
        return additionCandidatesHeap;
    }

    /**
     * Attempts to reconfigure providers to remove commodities.
     *
     * @param economy the economy.
     * @param ledger the ledger.
     * @return newly generated actions.
     */
    public static @NonNull List<@NonNull Action> reconfigureRemovalDecisions(@NonNull Economy economy,
        @NonNull Ledger ledger) {
        List<@NonNull Action> allActions = new ArrayList<>();
        int round = 0;
        try {
            while (round < 3) {
                Set<Trader> reconfigurableTraders = new HashSet<>();
                ledger.calculateExpRevForTradersInEconomy(economy);
                // adjust utilThreshold to maxDesiredUtil*utilTh of the seller. Thereby preventing moves
                // that force utilization to exceed maxDesiredUtil*utilTh
                Suspension.adjustUtilThreshold(economy, true);

                for (Market market : economy.getMarkets()) {
                    try {
                        if (economy.getForceStop()) {
                            return allActions;
                        }
                        boolean marketContainsReconfigurableSeller = market.getSellers()
                                .anyMatch(seller -> seller.getSettings().isReconfigurable()
                            && Utility.marketContainsReconfigurableSeller(market, economy));
                        // Skip market if no reconfigurable sellers.
                        if (!marketContainsReconfigurableSeller) {
                            continue;
                        }
                        List<Trader> reconfigureCandidates = Lists.newArrayList(market.getActiveSellersAvailableForPlacement());
                        for (Trader seller : reconfigureCandidates) {
                            if (seller.getReconfigureableCount(economy) > 0 && seller.getSettings().isReconfigurable()) {
                                reconfigurableTraders.add(seller);
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
                for (Trader t : reconfigurableTraders) {
                    List<CommoditySpecification> commSpecList = t.getBasketSold().stream()
                        .filter(commSpec -> economy.getSettings()
                            .getReconfigureableCommodities().contains(commSpec.getBaseType()))
                        .collect(Collectors.toList());
                    for (CommoditySpecification cs : commSpecList) {
                        allActions.addAll(tryCommodityRemoval(t, economy, ledger, cs));
                    }
                }
                round++;
            }
        } catch (Exception e) {
            logger.error(EconomyConstants.EXCEPTION_MESSAGE, "Reconfigure", e.getMessage(), e);
            Suspension.adjustUtilThreshold(economy, false);
        }
        return allActions;
    }

    /**
     * Attempt to remove a reconfigurable comm spec from a trader. If the reconfigurable commodity's
     * consumers are not able to move to and fit in other providers then we rollback the actions.
     *
     * @param t The trader.
     * @param economy The economy.
     * @param ledger The ledger.
     * @param cs The comm spec to remove.
     *
     * @return The actions.
     */
    private static List<Action> tryCommodityRemoval(Trader t, Economy economy, Ledger ledger,
        CommoditySpecification cs) {
        List<@NonNull Action> removeCommodityActions = new ArrayList<>();
        Map<CommoditySpecification, CommoditySold> comm = new HashMap<CommoditySpecification, CommoditySold>() {{
            put(cs, t.getCommoditySold(cs));
        }};
        Action commRemovalAction = new ReconfigureProviderRemoval(economy, (TraderWithSettings)t,
                comm).take();
        removeCommodityActions.add(commRemovalAction);
        removeCommodityActions.addAll(Placement.runPlacementsTillConverge(
            economy, t.getCustomers().stream()
                .filter(sl -> sl.getBasket().contains(cs))
                .collect(Collectors.toList()),
            ledger, true, EconomyConstants.RECONFIGURE_REMOVAL_PHASE).getActions());
        if (t.getCustomers().stream().filter(sl -> sl.getBasket().contains(cs)).count() > 0) {
            Lists.reverse(removeCommodityActions).forEach(axn -> axn.rollback());
            return new ArrayList<>();
        }
        logger.info(t.getDebugInfoNeverUseInCode() + " triggered "
                + "RECONFIGURE_PROVIDER_REMOVAL of " + cs.getDebugInfoNeverUseInCode());
        return removeCommodityActions;
    }
}