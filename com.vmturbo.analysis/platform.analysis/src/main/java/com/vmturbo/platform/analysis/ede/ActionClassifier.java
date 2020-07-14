package com.vmturbo.platform.analysis.ede;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;

public class ActionClassifier {

    static final Logger logger = LogManager.getLogger(ActionClassifier.class);

    final private @NonNull Economy simulationEconomy_;
    private int executable_ = 0;
    // List of Traders whose actions should be forced to executable = true.
    private Set<Trader> forceEnable_ = new HashSet<>();

    public int getExecutable() {
        return executable_;
    }

    public ActionClassifier(@NonNull Economy economy) throws IOException, ClassNotFoundException {
        simulationEconomy_ = economy.simulationClone();
    }

    /**
     * Mark actions as non-executable
     *
     * @param actions The list of actions to be classified.
     */
    public void classify(@NonNull List<Action> actions) {
        // Step 1 - mark actions we know to be non-executable
        markSuspensionsNonEmptyTradersNonExecutable(actions);

        // Step 2 - mark actions we know to be executable
        markResizeUpsExecutable(actions);
        markResizeDownsExecutable(actions);
        // This must be called after all other suspension classification occurs, because it could
        // potentially enable actions for previously disabled suspension actions.
        markSuspensionsInAtomicSegments(actions);

        // Step 3 - determine if move actions are executable or not
        classifyAndMarkMoves(actions);

        stats(actions);
    }

    /**
     * An atomic segment represents a segment of the supply chain that provisions and suspends as a
     * unit, and the bottom of the segment is the controllable entity.  This atomic segment is
     * defined in the probe by setting the providerMustClone attribute in the entity.  Since these
     * suspended entities are linked, they are exempt from the requirement that the entity's
     * customer list be empty before suspension.  Previously run classifiers here use the "no
     * customers" test when deciding whether to mark an action as not executable.  This classifier
     * runs last and marks the actions associated with these bottoms of atomic segments as
     * executable.
     *
     * An example of this is an Application -> Container -> ContainerPod segment in a Kubernetes
     * cluster, where the ContainerPod is lowest level controllable entity.  In this case, we
     * would want the Application and Container to not be executable, but we would want the
     * ContainerPod at the bottom to be executable.
     *
     * @param actions  List of actions to be classified
     */
    private void markSuspensionsInAtomicSegments(final List<Action> actions) {
        if (!forceEnable_.isEmpty()) {
            actions.stream()
                .filter(a -> a instanceof Deactivate && forceEnable_.contains(a.getActionTarget()))
                .forEach(a -> a.setExecutable(true));
        }
    }

    /**
     * Print a summary of number of executable and non-executable actions
     *
     * @param actions The list of actions that has been classified.
     */
    private void stats(@NonNull List<Action> actions) {
        int executable = 0;
        int nonExecutable = 0;
        int executableMove = 0;
        int nonExecutableMove = 0;
        int executableResize = 0;
        int nonExecutableResize = 0;

        for (Action a : actions) {
            if (a.isExecutable()) {
                executable++;
            } else {
                nonExecutable++;
            }
            if (a instanceof Move) {
                if (a.isExecutable()) {
                    executableMove++;
                } else {
                    nonExecutableMove++;
                }
            }
            if (a instanceof Resize) {
                if (a.isExecutable()) {
                    executableResize++;
                } else {
                    nonExecutableResize++;
                }
            }
        }
        logger.info("Classifier: " + executable + " " + nonExecutable + " " +
                        + executableMove + " " + nonExecutableMove + " " +
                        + executableResize + " " + nonExecutableResize);
        executable_ = executable;
    }

    /**
     * Mark suspensions of non-empty traders as non-executable
     *
     * @param actions The list of actions to be classified.
     */
    private void markSuspensionsNonEmptyTradersNonExecutable(@NonNull List<Action> actions) {
        Set<Trader> checked = new HashSet<>();
        actions.stream().filter(a -> a instanceof Deactivate).forEach(a -> {
            try {
                Deactivate s = (Deactivate)a;
                Trader suspensionCandidate = s.getActionTarget();
                if (!suspensionCandidate.getSettings().isProviderMustClone()) {
                    if (Suspension.sellerHasNonDaemonCustomers(suspensionCandidate)) {
                        s.setExecutable(false);
                    }
                } else {
                    // When providerMustClone is set, we only want the bottom of the atomic
                    // segment to be executable.  Mark this action as not executable and add the
                    // Trader at the bottom to the list of traders whose actions must be forced
                    // to executable.
                    s.setExecutable(false);
                    List<Trader> tradersToCheck = new ArrayList<>();
                    tradersToCheck.add(suspensionCandidate);
                    while (!tradersToCheck.isEmpty()) {
                        Trader trader = tradersToCheck.remove(0);
                        if (!checked.add(trader)) {
                            // Already processed from this trader down, so skip it
                            continue;
                        }
                        if (trader.getSettings().isProviderMustClone()) {
                            for (Map.Entry<ShoppingList, Market> entry :
                                    s.getEconomy().getMarketsAsBuyer(trader).entrySet()) {
                                @Nullable Trader provider = entry.getKey().getSupplier();
                                // If provider has already been checked, no need to check it again.
                                if (provider != null && !checked.contains(provider)) {
                                    tradersToCheck.add(provider);
                                }
                            }
                        } else if (!trader.getSettings().isDaemon()) {
                            // We are at the bottom of the chain, so suspensions associated with
                            // this trader should be executable. If this is a daemon, it is being
                            // suspended due to its supplier suspending (which makes it an orphan),
                            // so we do not want to force these to executable.
                            forceEnable_.add(trader);
                        }
                    }
                }

                // Check for customers on simulation target and mark not executable if any are present.
                Trader simSuspensionCandidate = lookupTraderInSimulationEconomy(suspensionCandidate);
                if (simSuspensionCandidate != null &&
                        Suspension.sellerHasNonDaemonCustomers(simSuspensionCandidate)) {
                    s.setExecutable(false);
                }
            } catch (Exception ex) {
                a.setExecutable(true);
                printLogMessageInDebugForExecutableFlag(a, ex);
            }
        });
    }

    /**
     * Mark resize downs as executable
     *
     * @param actions The list of actions to be classified.
     */
    private void markResizeDownsExecutable(@NonNull List<Action> actions) {
        actions.stream().filter(a -> a instanceof Resize).forEach(a -> {
            try {
                Resize r = (Resize)a;
                if (r.getNewCapacity() < r.getOldCapacity()) {
                    r.setExecutable(true);
                }
            } catch (Exception ex) {
                a.setExecutable(true);
                printLogMessageInDebugForExecutableFlag(a, ex);
            }

        });
    }

    /**
     * Mark resize ups as executable
     *
     * @param actions The list of actions to be classified.
     */
    private void markResizeUpsExecutable(@NonNull List<Action> actions) {
        actions.stream().filter(a -> a instanceof Resize).forEach(a -> {
            try {
                Resize r = (Resize)a;
                if (r.getNewCapacity() > r.getOldCapacity()) {
                    r.setExecutable(true);
                }
            } catch (Exception ex) {
                a.setExecutable(true);
                printLogMessageInDebugForExecutableFlag(a, ex);
            }
        });
    }

    /**
     * Mark moves as executable if they can be successfully simulated in the clone of the Economy.
     *
     * @param actions The list of actions to be classified.
     */
    private void classifyAndMarkMoves(@NonNull List<Action> actions) {
        actions.forEach(a -> {
            if (a instanceof Move) {
                classifyAndMarkMove((Move)a);
            } else if (a instanceof CompoundMove){
                boolean isCompMoveExecutable = true;
                for (Move mv : ((CompoundMove) a).getConstituentMoves()) {
                    classifyAndMarkMove(mv);
                    isCompMoveExecutable &= mv.isExecutable();
                }
                a.setExecutable(isCompMoveExecutable);
            }
        });
    }

    /**
     * Mark move as executable if it can be successfully simulated in the clone of the Economy.
     *
     * @param move The {@link Move} to be classified.
     */
    private void classifyAndMarkMove (Move move) {
        try {
            Trader currentSupplierCopy = move.getSource() != null
                            ? lookupTraderInSimulationEconomy(move.getSource()) : null;
            Trader newSupplierCopy = lookupTraderInSimulationEconomy(move.getDestination());
            if (newSupplierCopy == null || newSupplierCopy.isClone()) {
                move.setExecutable(false);
                return;
            }
            ShoppingList targetCopy = findTargetInEconomyCopy(move.getTarget());
            if (targetCopy == null) {
                move.setExecutable(false);
                return;
            }
            for (Integer baseType : move.getTarget().getModifiableUnquotedCommoditiesBaseTypeList()) {
                targetCopy.addModifiableUnquotedCommodityBaseType(baseType);
            }
            final double[] quote = EdeCommon.quote(simulationEconomy_, targetCopy, newSupplierCopy,
                            Double.POSITIVE_INFINITY, false).getQuoteValues();
            if (quote[0] < Double.POSITIVE_INFINITY) {
                move.simulateChangeDestinationOnly(simulationEconomy_, currentSupplierCopy,
                                newSupplierCopy, targetCopy);
                move.setExecutable(true);
            } else {
                move.setExecutable(false);
            }
        } catch (Exception ex) {
            move.setExecutable(true);
            printLogMessageInDebugForExecutableFlag(move, ex);
        }
    }

    private void printLogMessageInDebugForExecutableFlag(Action a, Exception e) {
        String additionalInfo = a.getActionTarget() != null
                        ? a.getActionTarget().getDebugInfoNeverUseInCode() : a.toString();
        ActionType actionType = a.getType();
        logger.error("Setting executable true for " + actionType + " target : " + additionalInfo
                        + ". Error message: " + e.toString(), e);
    }
    /**
     * Find the corresponding {@link Trader} in the cloned {@link Economy}.
     *
     * @param realTrader The {@link Trader} in the market Economy.
     * @return The corresponding {@link Trader} in the cloned Economy.
     */
    private @Nullable Trader lookupTraderInSimulationEconomy(Trader realTrader) {
        List<Trader> tradersCopy = simulationEconomy_.getTraders();
        int traderIndex = realTrader.getEconomyIndex();
        return traderIndex < tradersCopy.size() ? tradersCopy.get(traderIndex) : null;
    }

    /**
     * Find the corresponding target in the cloned {@link Economy}.
     *
     * @param target The ShoppingList in the market Economy.
     * @return The corresponding ShoppingList in the cloned Economy.
     */
    private @Nullable ShoppingList findTargetInEconomyCopy(ShoppingList target) {
        Trader buyer = target.getBuyer();
        Trader buyerCopy = lookupTraderInSimulationEconomy(buyer);
        // When classifying a move for a cloned entity, we wont find it in the simulationEconomy
        if (buyerCopy == null) {
            return null;
        }
        Set<ShoppingList> shoppingLists = simulationEconomy_.getMarketsAsBuyer(buyerCopy).keySet();
        for (ShoppingList shoppingList: shoppingLists) {
            if (shoppingList.getShoppingListId().equals(target.getShoppingListId())) {
                return shoppingList;
            }
        }
        return null;
    }
}
