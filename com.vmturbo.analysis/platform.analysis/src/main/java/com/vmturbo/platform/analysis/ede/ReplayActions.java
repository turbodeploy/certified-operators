package com.vmturbo.platform.analysis.ede;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ledger.Ledger;
import com.vmturbo.platform.analysis.protobuf.CommunicationDTOs.SuspensionsThrottlingConfig;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * This class keeps actions from an analysis run and metadata needed to replay them
 * in a subsequent run.
 */
public class ReplayActions {

    static final Logger logger = LogManager.getLogger(ReplayActions.class);

    // The actions that are to be replayed
    private @NonNull List<Action> actions_ = new LinkedList<>();
    // The traders which could not be suspended
    private @NonNull Set<Trader> rolledBackSuspensionCandidates_ = new HashSet<>();
    // The trader to OID map needed for translating traders between economies
    private @NonNull BiMap<@NonNull Trader, @NonNull Long> traderOids_ = HashBiMap.create();

    public List<Action> getActions() {
        return actions_;
    }

    public void setActions(List<Action> actions) {
        actions_ = actions;
    }

    public Set<Trader> getRolledBackSuspensionCandidates() {
        return rolledBackSuspensionCandidates_;
    }

    public void setRolledBackSuspensionCandidates(Set<Trader> rolledBackSuspensionCandidates) {
        rolledBackSuspensionCandidates_ = rolledBackSuspensionCandidates;
    }

    public BiMap<Trader, Long> getTraderOids() {
        return traderOids_;
    }

    public void setTraderOids(BiMap<Trader, Long> traderOids) {
        traderOids_ = traderOids;
    }

    /**
     * Replay Actions from earlier run on the new {@link Economy}.
     *
     * @param economy The {@link Economy} in which actions are to be replayed
     * @param ledger current ledger
     */
    public void replayActions(Economy economy, Ledger ledger) {
        Topology topology = economy.getTopology();
        LinkedList<Action> actions = new LinkedList<>();
        List<Action> deactivateActions = actions_.stream()
                        .filter(action -> action.getType() == ActionType.DEACTIVATE)
                        .collect(Collectors.toList());
        actions.addAll(tryReplayDeactivateActions(deactivateActions, economy, ledger));
        actions_.removeAll(deactivateActions);
        actions_.forEach(a -> {
            try {
                if (a.getType() == ActionType.MOVE) {
                    Move oldAction = (Move)a;
                    // Target shopping list is movable and destination trader
                    // can accept customer, generate move action
                    Trader newTargetTrader = translateTrader(oldAction.getDestination(), economy, "Move");
                    ShoppingList newShoppingList = translateShoppingList(oldAction.getTarget(), economy, topology);
                    if (newShoppingList.isMovable() && newTargetTrader.getSettings().canAcceptNewCustomers()) {
                        Move m = new Move(economy, newShoppingList, newTargetTrader);
                        // move will fail if supplier has changed handling actions already taken
                        m.take();
                        actions.add(m);
                    }
                } else if (a.getType() == ActionType.RESIZE) {
                    Resize oldAction = (Resize)a;
                    Trader newSellingTrader = translateTrader(oldAction.getSellingTrader(), economy, "Resize");
                    CommoditySold newSoldCommodity = newSellingTrader.getCommoditySold(
                                    oldAction.getResizedCommoditySpec());
                    // new commodity of trader still resizable, then take the action
                    if (newSoldCommodity.getSettings().isResizable()) {
                        Resize r = new Resize(economy, newSellingTrader,
                                        oldAction.getResizedCommoditySpec(), newSoldCommodity,
                                        newSellingTrader.getBasketSold().indexOf(
                                                        oldAction.getResizedCommoditySpec()),
                                        oldAction.getNewCapacity());
                        r.take();
                        actions.add(r);
                    }
                } else if (a.getType() == ActionType.PROVISION_BY_DEMAND) {
                    ProvisionByDemand oldAction = (ProvisionByDemand)a;
                    // Model seller of provision by demand action is still cloneable,
                    // then take the action
                    Trader newTrader = translateTrader(oldAction.getModelSeller(), economy, "ProvisionByDemand");
                    if (newTrader.getSettings().isCloneable()) {
                        ProvisionByDemand pbd = new ProvisionByDemand(economy,
                                        translateShoppingList(oldAction.getModelBuyer(), economy, topology),
                                        newTrader);
                        pbd.take();
                        Long oid = oldAction.getOid();
                        topology.addProvisionedTrader(pbd.getProvisionedSeller(), oid);
                        topology.getEconomy().getMarketsAsBuyer(pbd.getProvisionedSeller()).keySet()
                                        .stream().forEach(topology::addProvisionedShoppingList);
                        actions.add(pbd);
                    }
                } else if (a.getType() == ActionType.PROVISION_BY_SUPPLY) {
                    ProvisionBySupply oldAction = (ProvisionBySupply)a;
                    // Model seller of provision by supply action is still cloneable,
                    // then take the action
                    Trader newTrader = translateTrader(oldAction.getModelSeller(), economy, "ProvisionBySupply");
                    if (newTrader.getSettings().isCloneable()) {
                        ProvisionBySupply pbs = new ProvisionBySupply(economy, newTrader,
                                        oldAction.getReason());
                        pbs.take();
                        Long oid = oldAction.getOid();
                        topology.addProvisionedTrader(pbs.getProvisionedSeller(), oid);
                        topology.getEconomy().getMarketsAsBuyer(pbs.getProvisionedSeller()).keySet()
                                        .stream().forEach(topology::addProvisionedShoppingList);
                        actions.add(pbs);
                    }
                } else if (a.getType() == ActionType.ACTIVATE) {
                    Activate oldAction = (Activate)a;
                    // Model seller of activate action should be cloneable
                    Trader newTrader = translateTrader(oldAction.getModelSeller(), economy, "Activate2");
                    if (newTrader.getSettings().isCloneable()) {
                        Activate act = new Activate(economy,
                                        translateTrader(oldAction.getTarget(), economy, "Activate1"),
                                        oldAction.getSourceMarket(),
                                        newTrader, oldAction.getReason());
                        act.take();
                        actions.add(act);
                    }
                } else if (a.getType() == ActionType.RECONFIGURE) {
                    Reconfigure oldAction = (Reconfigure)a;
                    Reconfigure reconf = new Reconfigure(economy,
                               translateShoppingList(oldAction.getTarget(), economy, topology));
                    // nothing to do
                    reconf.take();
                    actions.add(reconf);
                } else if (a.getType() == ActionType.COMPOUND_MOVE) {
                    CompoundMove oldAction = (CompoundMove)a;
                    Trader newTrader = translateTrader(oldAction.getActionTarget(), economy,
                                    "CompoundMove");
                    // If trader shopTogether is false, no need to replay compound move
                    if (newTrader.getSettings().isShopTogether()) {
                        List<Move> oldMoves = oldAction.getConstituentMoves();
                        List<ShoppingList> shoppingLists = new LinkedList<>();
                        List<Trader> destinationTraders = new LinkedList<>();
                        boolean movable = true;
                        for (Move move : oldMoves) {
                            if (!move.getTarget().isMovable()) {
                                movable = false;
                                break;
                            }
                            shoppingLists.add(translateShoppingList(move.getTarget(), economy,
                                            topology));
                            destinationTraders.add(translateTrader(move.getDestination(), economy,
                                            "Move"));
                        }
                        // Movable false on any shopping list, means no compound move
                        if (movable) {
                            CompoundMove compound =
                                        CompoundMove.createAndCheckCompoundMoveWithImplicitSources(
                                                        economy, shoppingLists, destinationTraders);
                            if (compound != null) {
                                compound.take();
                                actions.add(compound);
                            }
                        }
                    }
                } else {
                    logger.warn("uncovered action " + a.toString());
                }
                if (logger.isDebugEnabled()) {
                    logger.debug("replayed " + a.toString());
                }
            } catch (Exception e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Could not replay " + a.toString(), e);
                }
            }
        });
        actions_ = actions;
    }

    /**
     * Try deactivate actions and replay only if we are able to move all customers
     * out of current trader.
     *
     * @param deactivateActions List of potential deactivate actions.
     * @param economy The {@link Economy} in which actions are to be replayed.
     * @param ledger The {@link Ledger} related to current {@link Economy}.
     * @return action list related to suspension of trader.
     */
    private List<Action> tryReplayDeactivateActions(List<Action> deactivateActions, Economy economy,
                    Ledger ledger) {
        List<@NonNull Action> suspendActions = new ArrayList<>();
        if (deactivateActions.isEmpty()) {
            return suspendActions;
        }
        Suspension suspensionInstance = new Suspension();
        // adjust utilThreshold of the seller to maxDesiredUtil*utilTh. Thereby preventing moves
        // that force utilization to exceed maxDesiredUtil*utilTh.
        suspensionInstance.adjustUtilThreshold(economy, true);
        for (Action deactivateAction : deactivateActions) {
            Deactivate oldAction = (Deactivate)deactivateAction;
            Trader newTrader = translateTrader(oldAction.getTarget(), economy, "Deactivate");
            if (isEligibleforSuspensionReplay(newTrader, economy)) {
                if (Suspension.getSuspensionsthrottlingconfig() == SuspensionsThrottlingConfig.CLUSTER) {
                    Suspension.makeCoSellersNonSuspendable(economy, newTrader);
                }
                if (newTrader.getSettings().isControllable()) {
                    suspendActions.addAll(suspensionInstance.deactivateTraderIfPossible(newTrader, economy,
                        ledger, true));
                } else {
                    // If controllable is false, deactivate the trader without checking criteria
                    // as entities may not be able to move out of the trader with controllable false.
                    List<Market> marketsAsSeller = economy.getMarketsAsSeller(newTrader);
                    Deactivate replayedSuspension = new Deactivate(economy, newTrader,
                                    !marketsAsSeller.isEmpty() ? marketsAsSeller.get(0) : null);
                    replayedSuspension.take();
                    // Any orphan suspensions generated will be added to the replayed suspension's
                    // subsequent actions list.
                    suspensionInstance
                        .suspendOrphanedCustomers(economy, replayedSuspension);
                    suspendActions.addAll(replayedSuspension.getAllActions());
                }
            }
        }
        //reset the above set utilThreshold.
        suspensionInstance.adjustUtilThreshold(economy, false);
        return suspendActions;
    }

    /**
     * Check for conditions that qualify trader (like trader state, sole provider etc.)
     * for replay of suspension.
     * @param trader to replay suspension for.
     * @param economy to which trader belongs.
     * @return true, if trader qualifies for replay of suspension.
     */
    private boolean isEligibleforSuspensionReplay(Trader trader, Economy economy) {
        return trader != null
            && trader.getSettings().isSuspendable()
            && trader.getState().isActive()
            // If trader is sole provider in any market, we don't want to replay the suspension.
            // Consider scenario where we replay suspension for PM1 but there was a new placement policy
            // for VM1 (currently on PM2) to move on PM1. We end recommending a reconfigure action because
            // PM1 becomes inactive due to replay of suspension.
            && economy.getMarketsAsSeller(trader).stream()
                .noneMatch(market -> market.getActiveSellers().size() == 1);
    }

    /**
     * Translate the list of rolled-back suspension candidate traders to the given {@link Economy}.
     *
     * @param newEconomy The {@link Economy} in which actions are to be replayed
     * @param newTopology The {@link Topology} for the given {@link Economy}
     */
    public void translateRolledbackTraders(Economy newEconomy, Topology newTopology) {
        Set<Trader> newTraders = new HashSet<>();
        rolledBackSuspensionCandidates_.forEach(t -> {
            Trader newTrader = translateTrader(t, newEconomy, "translateTraders");
            if (newTrader != null) {
                newTraders.add(newTrader);
            }
        });
        rolledBackSuspensionCandidates_ = newTraders;
    }

    /**
     * Translate the given trader to the one in new {@link Economy}.
     *
     * @param trader The trader for which we want to find the corresponding trader
     *               in new {@link Economy}
     * @param newEconomy The {@link Economy} in which actions are to be replayed
     * @param callerName A tag used by caller, useful in logging
     * @return Trader in new Economy or null if it fails to translate it
     */
    public @Nullable Trader translateTrader(Trader trader, Economy newEconomy,
                                            String callerName) {
        Topology newTopology = newEconomy.getTopology();
        Long oid = traderOids_.get(trader);
        Trader newTrader = newTopology.getTraderOids().inverse().get(oid);
        if (newTrader == null) {
            logger.info("Could not find trader with oid " + oid + " " + callerName + " " +
                         ((trader != null) ? trader.getDebugInfoNeverUseInCode() : "nullTrader"));
        }
        return newTrader;
    }

    /**
     * Translate the given ShoppingList to the new Economy.
     *
     * @param oldTarget The ShoppingList in the old Economy
     * @param newEconomy The {@link Economy} in which actions are to be replayed
     * @param newTopology The {@link Topology} for the given {@link Economy}
     * @return The ShoppingList in the new Economy or null if it fails to translate it
     */
    public @Nullable ShoppingList translateShoppingList(ShoppingList oldTarget,
                                        Economy newEconomy, Topology newTopology) {
        Basket basket = oldTarget.getBasket();
        double[] requestedQuantities = oldTarget.getQuantities();
        double[] requestedPeakQuantities = oldTarget.getQuantities();
        Trader buyer = oldTarget.getBuyer();
        Trader newBuyer = translateTrader(buyer, newEconomy, "translateShoppingList");
        if (newBuyer != null) {
            Set<ShoppingList> shoppingLists = newEconomy.getMarketsAsBuyer(newBuyer).keySet();
            for (ShoppingList shoppingList : shoppingLists) {
                if (shoppingList.getBasket().equals(basket)
                                && Arrays.equals(shoppingList.getQuantities(), requestedQuantities)
                                && Arrays.equals(shoppingList.getPeakQuantities(),
                                                requestedPeakQuantities)) {
                    return shoppingList;

                }
            }
        }
        throw new IllegalArgumentException("Unable to translate shopping list " + basket.toString());
    }

    /**
     * Translate the CommoditySold to the one in new Economy.
     *
     * @param newSellingTrader The Trader in the new Economy
     * @param oldResizedCommoditySpec The CommoditySpecification for the Trader in old Economy
     * @param oldResizedCommodity The Commodity sold by Trader in old Economy
     * @param newEconomy The {@link Economy} in which actions are to be replayed
     * @param newTopology The {@link Topology} for the given {@link Economy}
     * @return CommoditySold in the new Economy or null if it fails to translate it
     */
    public @Nullable CommoditySold translateCommoditySold(Trader newSellingTrader,
                    @Nullable CommoditySpecification oldResizedCommoditySpec,
                                        @Nullable CommoditySold oldResizedCommodity,
                                        Economy newEconomy, Topology newTopology) {
        CommoditySpecification newResizedCommoditySpec = oldResizedCommoditySpec;
        CommoditySold sold = newSellingTrader.getCommoditySold(newResizedCommoditySpec);
        return sold;
    }
}
