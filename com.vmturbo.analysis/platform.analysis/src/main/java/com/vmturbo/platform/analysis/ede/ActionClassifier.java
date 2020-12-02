package com.vmturbo.platform.analysis.ede;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ActionType;
import com.vmturbo.platform.analysis.actions.CompoundMove;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionBase;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;

public class ActionClassifier {

    static final Logger logger = LogManager.getLogger(ActionClassifier.class);

    // The current economy
    private final @NonNull Economy economy_;
    // The cloned economy
    private final @NonNull Economy simulationEconomy_;
    private int executable_ = 0;
    // List of Traders whose actions should be forced to executable = true.
    private Set<Trader> forceEnable_ = new HashSet<>();

    public int getExecutable() {
        return executable_;
    }

    public ActionClassifier(@NonNull Economy economy) throws IOException, ClassNotFoundException {
        this.economy_ = economy;
        simulationEconomy_ = economy.simulationClone();
    }

    /**
     * Mark actions as non-executable
     *
     * @param actions The list of actions to be classified.
     * @param economy The economy the actions belong to.
     */
    public void classify(@NonNull List<Action> actions, Economy economy) {
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

        // Step 4 - determine if provision actions are executable or not
        markProvisions(actions);

        if (!economy.getExceptionTraders().isEmpty()) {
            logger.error("There were {} entities with exceptions during analysis",
                economy.getExceptionTraders().size());
        }
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
                    .filter(Deactivate.class::isInstance)
                    .filter(a -> forceEnable_.contains(a.getActionTarget())
                            // If this is a daemon, it is being suspended due to its supplier
                            // suspending (which makes it an orphan), so we do not want to force these to executable.
                            && !a.getActionTarget().getSettings().isDaemon())
                    // We are at the bottom of the chain, so suspensions associated with
                    // this trader should be executable.
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
        int executableProvision = 0;
        int nonExecutableProvision = 0;
        int executableSuspension = 0;
        int nonExecutableSuspension = 0;

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
            if (a instanceof ProvisionBase) {
                if (a.isExecutable()) {
                    executableProvision++;
                } else {
                    nonExecutableProvision++;
                }
            }
            if (a instanceof Deactivate) {
                if (a.isExecutable()) {
                    executableSuspension++;
                } else {
                    nonExecutableSuspension++;
                }
            }
        }
        logger.info("Classifier:"
                + " Total: " + executable + " " + nonExecutable
                + " Move: " + executableMove + " " + nonExecutableMove
                + " Resize: " + executableResize + " " + nonExecutableResize
                + " Provision: " + executableProvision + " " + nonExecutableProvision
                + " Suspension: " + executableSuspension + " " + nonExecutableSuspension);
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
                    forceEnable_.addAll(findBottomOfAtomicSegment(checked, suspensionCandidate));
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
     * Find the bottom trader on the providerMustClone chain.
     *
     * @param checked traders that have already been visited
     * @param trader  the starting trader
     * @return the bottom trader
     */
    private Set<Trader> findBottomOfAtomicSegment(@NonNull Set<Trader> checked,
                                                  @NonNull final Trader trader) {
        Set<Trader> bottomTraders = new HashSet<>();
        Queue<Trader> toCheck = new ArrayDeque<>();
        toCheck.add(trader);
        while (!toCheck.isEmpty()) {
            Trader current = toCheck.remove();
            if (!checked.add(current)) {
                // Already processed from this trader down, so skip it
                continue;
            }
            if (!current.getSettings().isProviderMustClone()) {
                // We found the bottom
                bottomTraders.add(current);
                continue;
            }
            // Add the providers to check list
            economy_.getMarketsAsBuyer(current).keySet().stream()
                    .map(ShoppingList::getSupplier)
                    .filter(Objects::nonNull)
                    .filter(provider -> !checked.contains(provider))
                    .forEach(toCheck::add);
        }
        return bottomTraders;
    }

    /**
     * Mark provision actions.
     *
     * <p>If any provision action has the clone sitting on the bottom of the
     * providerMustClone chain, and either of the following two conditions are met,
     * then mark this action as non-executable:
     * - The clone has a supplier that itself is cloned. For example, during SLO driven scaling,
     *   market may place a cloned pod (which sits on the bottom of the providerMustClone chain) on
     *   a cloned node. When pod provision is automated, but the node provision is not, we need to
     *   mark the pod provision non-executable to avoid accumulation of pending pods in the cluster.
     *   Even if the node provision is automated, that action usually takes much longer than pod
     *   provision (i.e., may even span multiple market analysis cycle). During that period, it is
     *   still desirable to mark those pod provisions as non-executable.
     * - The clone causes an existing supplier to have an infinite quote on the simulation economy
     *
     * @param actions the list of actions to be classified
     */
    private void markProvisions(@NonNull List<Action> actions) {
        Set<Trader> checked = new HashSet<>();
        // A set holding bottom traders on the providerMustClone chain
        Set<Trader> bottomTradersToClone = new HashSet<>();
        actions.stream()
                .filter(ProvisionBase.class::isInstance)
                .map(ProvisionBase.class::cast)
                .map(ProvisionBase::getProvisionedSeller)
                .filter(Objects::nonNull)
                .filter(clone -> clone.getSettings().isProviderMustClone())
                .forEach(clone -> bottomTradersToClone
                        .addAll(findBottomOfAtomicSegment(checked, clone)));
        // Mark the provision actions whose provisioned sellers are bottom traders
        actions.stream()
                .filter(ProvisionBase.class::isInstance)
                .map(ProvisionBase.class::cast)
                .filter(clone -> bottomTradersToClone.contains(clone.getProvisionedSeller()))
                .forEach(this::markProvision);
    }

    /**
     * Mark a provision action.
     *
     * <p>Mark a provision as non-executable if:
     * - The clone has a supplier that itself is cloned, or
     * - The clone causes an existing supplier to have an infinite quote on the simulation economy
     *
     * @param provision the provision action to be classified
     */
    private void markProvision(@NonNull ProvisionBase provision) {
        final Trader clonedTrader = provision.getProvisionedSeller();
        final Set<ShoppingList> shoppingListsWithSupplier =
                economy_.getMarketsAsBuyer(clonedTrader).keySet().stream()
                        .filter(sl -> sl.getSupplier() != null).collect(Collectors.toSet());
        if (shoppingListsWithSupplier.isEmpty()) {
            // None of the shopping lists has a supplier, this shouldn't happen, guard just in case.
            logger.warn("Provisioned trader {} does not have a supplier. "
                    + "Disable the provision.", clonedTrader);
            provision.setExecutable(false);
            return;
        }
        // We only check shopping list with supplier for now.
        // TODO: Currently, the cloned SL may not always have a supplier. This can happen when the
        //  cloned SL of a container pod requests for quota commodity which should be provided by
        //  namespace entities, but currently namespace entities do not participate in market
        //  placement. This will be addressed in OM-63075.
        if (shoppingListsWithSupplier.stream()
                .map(ShoppingList::getSupplier).anyMatch(Trader::isClone)) {
            // At least one supplier is a clone
            logger.info("Provisioned trader {} is hosted on provisioned supplier. "
                    + "Disable the provision.", clonedTrader);
            provision.setExecutable(false);
            return;
        }
        // All shopping lists are placed on existing supplier
        for (ShoppingList shoppingList : shoppingListsWithSupplier) {
            final Trader supplier = shoppingList.getSupplier();
            // Find the copy of the supplier in the simulation economy
            final Trader supplierCopy = lookupTraderInSimulationEconomy(supplier);
            if (supplierCopy == null) {
                logger.warn("Provisioned trader {}'s supplier {} does not have a copy in"
                        + " simulation economy.", clonedTrader, supplier);
                continue;
            }
            final double[] quote = EdeCommon.quote(simulationEconomy_, shoppingList,
                    supplierCopy, Double.POSITIVE_INFINITY, false).getQuoteValues();
            if (quote[0] >= Double.POSITIVE_INFINITY) {
                // If the clone causes infinite quote on an existing supplier, then disable it
                logger.info("Provisioned trader {} on supplier {} causes infinite quote. "
                        + "Disable the provision.", clonedTrader, supplier);
                provision.setExecutable(false);
                return;
            }
            // Simulate the effect of the clone on the supplierCopy. For simplicity and
            // performance concerns, we are not actually creating a clone of the provisioned
            // trader and shopping list in the simulation economy, we just update the sold
            // quantity on the supplierCopy.
            Move.updateQuantities(simulationEconomy_, shoppingList, supplierCopy,
                    UpdatingFunctionFactory.ADD_COMM, true);
            if (logger.isTraceEnabled()) {
                logger.info("Provisioned trader [{} modelled after {}]'s shopping list {}"
                                + " can be placed on supplier {}.",
                        clonedTrader, provision.getModelSeller(),
                        shoppingList.getBasket().toDebugString(), supplier);
            }
        }
        logger.debug("Provision of trader {} is executable", clonedTrader);
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
            // context may not be cloned in simulationClone method as it was cloning the context
            // before the placement run. But in migration plans, only when start running placement,
            // context will be attached to buying traders, so targetCopy in migration plan has to
            // get its context here from move action.
            Optional<Context> context = move.getTarget().getBuyer().getSettings().getContext();
            if (context.isPresent()) {
                targetCopy.getBuyer().getSettings().setContext(context.get());
            }
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
