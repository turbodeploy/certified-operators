package com.vmturbo.platform.analysis.ede;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.vmturbo.platform.analysis.actions.Action;
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
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * This class keeps actions from an analysis run and metadata needed to replay them
 * in a subsequent run.
 */
public class ReplayActions {

    static final Logger logger = Logger.getLogger(ReplayActions.class);

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
     * Replay Actions from earlier run on the new {@link Economy}
     *
     * @param economy The {@link Economy} in which actions are to be replayed
     */
    public void replayActions(Economy economy) {
        Topology topology = economy.getTopology();
        LinkedList<Action> actions = new LinkedList<>();
        actions_.forEach(a -> {
            try {
                if (a instanceof Move) {
                    Move oldAction = (Move) a;
                    Move m = new Move(economy,
                         translateShoppingList(oldAction.getTarget(), economy, topology),
                         translateTrader(oldAction.getDestination(), economy, "Move"));
                    // move will fail if supplier has changed handling actions already taken
                    m.take();
                    actions.add(m);
                } else if (a instanceof Resize) {
                    Resize oldAction = (Resize) a;
                    Trader newSellingTrader = translateTrader(oldAction.getSellingTrader(),
                                                              economy, "Resize");
                    Resize r = new Resize(economy, newSellingTrader,
                     oldAction.getResizedCommoditySpec(),
                     newSellingTrader.getCommoditySold(oldAction.getResizedCommoditySpec()),
                     newSellingTrader.getBasketSold().indexOf(oldAction.getResizedCommoditySpec()),
                           oldAction.getNewCapacity());
                    r.take();
                    actions.add(r);
                } else if (a instanceof ProvisionByDemand) {
                    ProvisionByDemand oldAction = (ProvisionByDemand) a;
                    ProvisionByDemand pbd = new ProvisionByDemand(economy,
                       translateShoppingList(oldAction.getModelBuyer(), economy, topology),
                       translateTrader(oldAction.getModelSeller(), economy, "ProvisionByDemand"));
                    pbd.take();
                    Long oid = oldAction.getOid();
                    topology.addProvisionedTrader(pbd.getProvisionedSeller(), oid);
                    topology.getEconomy().getMarketsAsBuyer(pbd.getProvisionedSeller()).keySet()
                            .stream().forEach(topology::addProvisionedShoppingList);
                    actions.add(pbd);
                } else if (a instanceof ProvisionBySupply) {
                    ProvisionBySupply oldAction = (ProvisionBySupply) a;
                    ProvisionBySupply pbs = new ProvisionBySupply(economy,
                                 translateTrader(oldAction.getModelSeller(), economy, "ProvisionBySupply"));
                    pbs.take();
                    Long oid = oldAction.getOid();
                    topology.addProvisionedTrader(pbs.getProvisionedSeller(), oid);
                    topology.getEconomy().getMarketsAsBuyer(pbs.getProvisionedSeller()).keySet()
                            .stream().forEach(topology::addProvisionedShoppingList);
                    actions.add(pbs);
                } else if (a instanceof Activate) {
                    Activate oldAction = (Activate) a;
                    Activate act = new Activate(economy,
                         translateTrader(oldAction.getTarget(), economy, "Activate1"),
                         oldAction.getSourceMarket(),
                         translateTrader(oldAction.getModelSeller(), economy, "Activate2"));
                    act.take();
                    actions.add(act);
                } else if (a instanceof Deactivate) {
                    Deactivate oldAction = (Deactivate) a;
                    Deactivate deact = new Deactivate(economy,
                           translateTrader(oldAction.getTarget(), economy, "Deactivate"),
                           oldAction.getSourceMarket());
                    deact.take();
                    actions.add(deact);
                } else if (a instanceof Reconfigure) {
                    Reconfigure oldAction = (Reconfigure) a;
                    Reconfigure reconf = new Reconfigure(economy,
                               translateShoppingList(oldAction.getTarget(), economy, topology));
                    // nothing to do
                    reconf.take();
                    actions.add(reconf);
                } else if (a instanceof CompoundMove) {
                        CompoundMove oldAction = (CompoundMove) a;
                        List<Move> oldMoves = oldAction.getConstituentMoves();
                        List<ShoppingList> shoppingLists = new LinkedList<>();
                        List<Trader> destinationTraders = new LinkedList<>();
                        for (Move move : oldMoves) {
                            shoppingLists.add(translateShoppingList(move.getTarget(),
                                                                    economy, topology));
                            destinationTraders.add(translateTrader(move.getDestination(),
                                                                   economy, "Move"));
                        }
                        CompoundMove compound = new CompoundMove(economy, shoppingLists, destinationTraders);
                        compound.take();
                        actions.add(compound);
                } else {
                    logger.info("uncovered action " + a.toString());
                }
                logger.info("replayed " + a.toString());
            } catch(Exception e) {
                logger.info("Could not replay " + a.toString());
            }
        });
        actions_ = actions;
    }

    /**
     * Translate the list of rolledback suspension candidate traders to the given {@link Economy}
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
     * Translate the given trader to the one in new {@link Economy}
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
     * Translate the given ShoppingList to the new Economy
     *
     * @param oldTarget The ShoppingList in the old Economy
     * @param newEconomy The {@link Economy} in which actions are to be replayed
     * @param newTopology The {@link Topology} for the given {@link Economy}
     * @return The ShoppingList in the new Economy or null if it fails to translate it
     */
    public @Nullable ShoppingList translateShoppingList(ShoppingList oldTarget,
                                        Economy newEconomy, Topology newTopology) {
        Basket basket = oldTarget.getBasket();
        Trader buyer = oldTarget.getBuyer();
        Trader newBuyer = translateTrader(buyer, newEconomy, "translateShoppingList");
        if (newBuyer != null) {
            Set<ShoppingList> shoppingLists = newEconomy.getMarketsAsBuyer(newBuyer).keySet();
            for (ShoppingList shoppingList : shoppingLists) {
                if (shoppingList.getBasket().equals(basket)) {
                    return shoppingList;
                }
            }
        }
        return null;
    }

    /**
     * Translate the CommoditySold to the one in new Economy
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
