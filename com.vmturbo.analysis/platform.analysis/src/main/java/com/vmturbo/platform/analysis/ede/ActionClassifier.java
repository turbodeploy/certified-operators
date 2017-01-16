package com.vmturbo.platform.analysis.ede;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;

public class ActionClassifier {

    static final Logger logger = Logger.getLogger(ActionClassifier.class);

    private @NonNull Economy simulationEconomy_;

    public ActionClassifier(@NonNull Economy economy) throws IOException, ClassNotFoundException {
        simulationEconomy_ = economy.cloneEconomy(economy);
    }

    /**
     * Mark actions as non-executable
     *
     * @param actions The list of actions to be classified.
     */
    public void classify(@NonNull List<Action> actions) {

        // Step 1 - mark actions we know to be non-executable
        markProvisionsNonExecutable(actions);
        markSuspensionsNonEmptyTradersNonExecutable(actions);
        markResizeUpsNonExecutable(actions);

        // Step 2 - mark actions we know to be executable
        markResizeDownsExecutable(actions);
        markSuspensionsEmptyTradersExecutable(actions);

        // Step 3 - determine if move actions are executable or not
        classifyAndMarkMoves(actions);

        stats(actions);
        return;
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
    }

    /**
     * Mark Provision actions as non-executable
     *
     * @param actions The list of actions to be classified.
     */
    private void markProvisionsNonExecutable(@NonNull List<Action> actions) {
        actions.stream().filter(a -> a instanceof ProvisionBySupply || a instanceof ProvisionByDemand)
                        .forEach(p -> p.setExecutable(false));
    }

    /**
     * Mark suspensions of non-empty traders as non-executable
     *
     * @param actions The list of actions to be classified.
     */
    private void markSuspensionsNonEmptyTradersNonExecutable(@NonNull List<Action> actions) {
        actions.stream().filter(a -> a instanceof Deactivate)
                        .forEach(a -> {
                                          Deactivate s = (Deactivate) a;
                                          Trader suspensionCandidate = s.getActionTarget();
                                          if (suspensionCandidate.getCustomers() != null &&
                                                          !suspensionCandidate.getCustomers().isEmpty()) {
                                              s.setExecutable(false);
                                          }
                                      });
    }

    /**
     * Mark suspension of empty traders as executable
     *
     * @param actions The list of actions to be classified.
     */
    private void markSuspensionsEmptyTradersExecutable(@NonNull List<Action> actions) {
        actions.stream().filter(a -> a instanceof Deactivate)
        .forEach(a -> {
                          Deactivate s = (Deactivate) a;
                          Trader suspensionCandidate = s.getActionTarget();
                          if (suspensionCandidate.getCustomers() == null ||
                                          suspensionCandidate.getCustomers().isEmpty()) {
                              s.setExecutable(true);
                          }
                      });
    }

    /**
     * Mark resize downs as executable
     *
     * @param actions The list of actions to be classified.
     */
    private void markResizeDownsExecutable(@NonNull List<Action> actions) {
        actions.stream().filter(a -> a instanceof Resize)
        .forEach(a -> {
                          Resize r = (Resize) a;
                          if (r.getNewCapacity() < r.getOldCapacity()) {
                              r.setExecutable(true);
                          }
                      });
    }

    /**
     * Mark resize ups as non-executable
     *
     * @param actions The list of actions to be classified.
     */
    private void markResizeUpsNonExecutable(@NonNull List<Action> actions) {
        actions.stream().filter(a -> a instanceof Resize)
        .forEach(a -> {
                          Resize r = (Resize) a;
                          if (r.getNewCapacity() > r.getOldCapacity()) {
                              r.setExecutable(false);
                          }
                      });
    }

    /**
     * Mark moves as executable if they can be successfully simulated in the clone of the Economy.
     *
     * @param actions The list of actions to be classified.
     */
    private void classifyAndMarkMoves(@NonNull List<Action> actions) {
        actions.stream().filter(a -> a instanceof Move).forEach(m -> {
            Move move = (Move) m;

            Trader currentSupplierCopy = lookupTraderInSimulationEconomy(move.getSource());
            Trader newSupplierCopy = lookupTraderInSimulationEconomy(move.getDestination());
            if (newSupplierCopy == null) {
                move.setExecutable(false);
                return;
            }
            ShoppingList targetCopy = findTargetInEconomyCopy(move.getTarget());
            if (targetCopy == null) {
                move.setExecutable(false);
                return;
            }

            final double[] quote = EdeCommon.quote(simulationEconomy_, targetCopy, newSupplierCopy, Double.POSITIVE_INFINITY, false);
            if (quote[0] < Double.POSITIVE_INFINITY) {
                move.simulateChangeDestinationOnly(simulationEconomy_, currentSupplierCopy, newSupplierCopy, targetCopy);
                move.setExecutable(true);
            } else {
                move.setExecutable(false);
            }
        });
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
        if (traderIndex < tradersCopy.size()) {
            return tradersCopy.get(traderIndex);
        } else {
            return null;
        }
    }

    /**
     * Find the corresponding target in the cloned {@link Economy}.
     *
     * @param target The ShoppingList in the market Economy.
     * @return The corresponding ShoppingList in the cloned Economy.
     */
    private @Nullable ShoppingList findTargetInEconomyCopy(ShoppingList target) {
        Basket basket = target.getBasket();
        Trader buyer = target.getBuyer();
        Trader buyerCopy = lookupTraderInSimulationEconomy(buyer);
        Set<ShoppingList> shoppingLists = simulationEconomy_.getMarketsAsBuyer(buyerCopy).keySet();
        for (ShoppingList shoppingList: shoppingLists) {
            if (shoppingList.getBasket().equals(basket)) {
                return shoppingList;
            }
        }
        return null;
    }
}
