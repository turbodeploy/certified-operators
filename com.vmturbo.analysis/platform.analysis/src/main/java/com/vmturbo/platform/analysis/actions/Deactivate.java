package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import com.google.common.hash.Hashing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * An action to deactivate an active {@link Trader trader}.
 */
public class Deactivate extends StateChangeBase { // inheritance for code reuse

    private static final Logger logger = LogManager.getLogger();
    private List<ShoppingList> removedShoppingLists = new ArrayList<>();

    // Constructors

    /**
     * Constructs a new Deactivate action with the specified target.
     *
     * @param target The trader that will be deactivated as a result of taking {@code this} action.
     * @param triggeringBasket The basket bought of the market that benefits from deactivating
     *                         target or the basket sold by the trader that doesn't sell in any
     *                         market.
     */
    public Deactivate(@NonNull Economy economy, @NonNull Trader target,
                      @NonNull Basket triggeringBasket) {
        super(economy, target, triggeringBasket);
    }

    // Methods

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder()
                        // TODO (Vaptistis): do I also need to send the market or basket?
                        .append("<action type=\"deactivate\" target=\"")
                        .append(oid.apply(getTarget())).append("\" />").toString();
    }

    /**
     * Takes a deactivate action to change the state of a trader from active to inactive.
     */
    @Override
    public @NonNull Action take() {
        super.take();
        Trader target = getTarget();
        checkArgument(target.getState().isActive());

        try {
            target.getCustomers().stream()
                .map(ShoppingList::getBuyer)
            .filter(trader -> trader.getSettings().isResizeThroughSupplier())
            .forEach(trader -> {
                    getEconomy().getMarketsAsBuyer(trader).keySet().stream()
                        .filter(shoppingList -> shoppingList.getSupplier() == target)
                        .forEach(sl -> {
                            // Generate the resize actions for matching commodities between
                            // the model seller and the resizeThroughSupplier trader.
                            getSubsequentActions().addAll(Utility.resizeCommoditiesOfTrader(
                                                                                    getEconomy(),
                                                                                    target,
                                                                                    sl, false));
                        });
            });
        } catch (Exception e) {
            logger.error("Error in Deactivate for resizeThroughSupplier Trader Capacity "
                            + "Resize when suspending "
                                + target.getDebugInfoNeverUseInCode(), e);
        }

        // If this trader has providerMustClone set, suspend this trader's suppliers as well.
        GuaranteedBuyerHelper.suspendProviders(this);
        removedShoppingLists.addAll(
            GuaranteedBuyerHelper.removeShoppingListForGuaranteedBuyers(getEconomy(), target));
        target.changeState(TraderState.INACTIVE);
        return this;
    }

    /**
     * Rolls back a deactivate action to change the state of a trader from active to inactive.
     */
    @Override
    public @NonNull Deactivate rollback() {
        super.rollback();
        Trader trader = getTarget();
        checkArgument(!trader.getState().isActive());
        if (logger.isTraceEnabled() || trader.isDebugEnabled()) {
            logger.info("Rolling back deactivate for {" + trader.getDebugInfoNeverUseInCode() + "}");
        }
        trader.changeState(TraderState.ACTIVE);
        List<ShoppingList> slsBetweenGuaranteedBuyersAndSuspendedTrader =
                GuaranteedBuyerHelper.getSlsWithGuaranteedBuyers(removedShoppingLists);
        Map<Trader, Set<ShoppingList>> slsSponsoredByGuaranteedBuyer =
                GuaranteedBuyerHelper.getAllSlsSponsoredByGuaranteedBuyer(getEconomy(),
                        slsBetweenGuaranteedBuyersAndSuspendedTrader);
        GuaranteedBuyerHelper.addNewSlAndAdjustExistingSls(getEconomy(),
                slsBetweenGuaranteedBuyersAndSuspendedTrader, slsSponsoredByGuaranteedBuyer,
                trader, true);
        removedShoppingLists.clear();
        return this;
    }

    @Override
    public @NonNull Deactivate port(@NonNull final Economy destinationEconomy,
            @NonNull final Function<@NonNull Trader, @NonNull Trader> destinationTrader,
            @NonNull final Function<@NonNull ShoppingList, @NonNull ShoppingList>
                                                                        destinationShoppingList) {
        return new Deactivate(destinationEconomy, destinationTrader.apply(getTarget()),
            getTriggeringBasket());
    }

    /**
     * Returns whether {@code this} action respects constraints and can be taken.
     *
     * <p>Currently a deactivate is considered valid iff the target trader is suspendable and
     * active.</p>
     */
    @Override
    public boolean isValid() {
        return getTarget().getSettings().isSuspendable() && getTarget().getState().isActive();
    }

    // TODO: update description and reason when we create the corresponding matrix.
    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                    @NonNull Function<@NonNull Trader, @NonNull String> name,
                    @NonNull IntFunction<@NonNull String> commodityType,
                    @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Deactivate ");
        appendTrader(sb, getTarget(), uuid, name);
        sb.append(".");

        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                    @NonNull Function<@NonNull Trader, @NonNull String> name,
                    @NonNull IntFunction<@NonNull String> commodityType,
                    @NonNull IntFunction<@NonNull String> traderType) {
        return new StringBuilder().append("Because of insufficient demand for ")
                                  .append(getTriggeringBasket()).append(".").toString();
                                  // TODO: print basket in human-readable form.
    }

    /**
     * Tests whether two Deactivate actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Deactivate this,@ReadOnly Object other) {
        if (!(other instanceof Deactivate)) {
            return false;
        }
        Deactivate otherDeactivate = (Deactivate)other;
        return otherDeactivate.getEconomy() == getEconomy()
                        && otherDeactivate.getTarget() == getTarget()
                        && otherDeactivate.getTriggeringBasket().equals(getTriggeringBasket());
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
            .putInt(getTarget().hashCode()).putInt(getTriggeringBasket().hashCode()).hash().asInt();
    }

    @Override
    public ActionType getType() {
        return ActionType.DEACTIVATE;
    }
} // end Deactivate class
