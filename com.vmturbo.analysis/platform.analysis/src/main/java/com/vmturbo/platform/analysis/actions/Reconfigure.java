package com.vmturbo.platform.analysis.actions;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import com.google.common.hash.Hashing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.Placement;

/**
 * An action to reconfigure a {@link ShoppingList}.
 */
public class Reconfigure extends MoveBase implements Action { // inheritance for code reuse
    // Fields
    static final Logger logger = LogManager.getLogger(Placement.class);

    // A set of commodities from shopping list which can not be satisfied by any seller.
    private Set<CommoditySpecification> unavailableCommodities = new HashSet<>();
    // Constructors

    /**
     * Constructs a new reconfigure action with the specified target and economy.
     *
     * @param economy The economy containing target.
     * @param target The shopping list of the trader that needs reconfiguration.
     */
    public Reconfigure(@NonNull Economy economy, @NonNull ShoppingList target) {
        super(economy, target, target.getSupplier());
        findUnavailableCommodities();
    }


    // Methods

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder()
            // TODO: is it enough to send the buyer or is the basket needed as well?
            // TODO: How do we serialize null sources?
            .append("<action type=\"reconfigure\" target=\"").append(oid.apply(getTarget().getBuyer()))
            .append("\" source=\"").append(oid.apply(getSource()))
            .append("\" />")
            .toString();
    }

    /**
     * Take this action, then for all scaling group peers, generate and take Reconfigure actions.
     * Place those Reconfigures onto the subsequent actions list inside this Reconfigure.
     * @return This reconfigure action.  Any subsequent Reconfigure actions will be placed in
     * subsequentActions_.
     */
    @Override
    public @NonNull Reconfigure take() {
        internalTake();
        Economy economy = getEconomy();
        List<ShoppingList> peers = economy.getPeerShoppingLists(getTarget());
        for (ShoppingList shoppingList : peers) {
            logger.info("Synthesizing Reconfigure for {} in scaling group {}",
                shoppingList.getBuyer(), shoppingList.getBuyer().getScalingGroupId());
            Reconfigure reconfigure = new Reconfigure(economy, shoppingList);
            getSubsequentActions().add(reconfigure.internalTake()
                .setImportance(Double.POSITIVE_INFINITY));
        }
        return this;
    }

    private @NonNull Reconfigure internalTake() {
        super.take();
        // Nothing can be done automatically
        return this;
    }

    @Override
    public @NonNull Reconfigure rollback() {
        getSubsequentActions()
            .forEach(reconfigure -> ((Reconfigure)reconfigure).internalRollback());
        internalRollback();
        getSubsequentActions().clear();
        return this;
    }

    @Override
    @Pure
    public @NonNull Action port(@NonNull final Economy destinationEconomy,
            @NonNull final Function<@NonNull Trader, @NonNull Trader> destinationTrader,
            @NonNull final Function<@NonNull ShoppingList, @NonNull ShoppingList>
                                                                        destinationShoppingList) {
        return new Reconfigure(destinationEconomy, destinationShoppingList.apply(getTarget()));
    }

    /**
     * Returns whether {@code this} action respects constraints and can be taken.
     *
     * <p>Currently a reconfigure is always considered valid.</p>
     */
    // TODO: do we need to add conditions?
    @Override
    public boolean isValid() {
        return true;
    }

    private @NonNull Reconfigure internalRollback() {
        super.rollback();
        // Nothing to roll back!
        return this;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Change constraints of ");
        appendTrader(sb, getTarget().getBuyer(), uuid, name);
        sb.append(" or ");

        if (getSource() != null) {
            sb.append(traderType.apply(getSource().getType()));
            sb.append("s");
        } else {
            sb.append("[need supply chain to fill in correct trader type]");
        }

        // TODO: add provision alternative.
        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        if (getSource() == null) { // No current provider
            sb.append("Unable to start ");
            appendTrader(sb, getTarget().getBuyer(), uuid, name);
            sb.append(" because no [need supply chain to fill in correct trader type] can ");
            sb.append("satisfy the following combination of constraints: ");
            sb.append(getTarget().getBasket()); // TODO: add more cases and substitute commodity types.
        } else {
            appendTrader(sb, getTarget().getBuyer(), uuid, name);
            sb.append(" is currently placed on a ").append(traderType.apply(getSource().getType()));
            sb.append("that does not satisfy the ");
            sb.append(getTarget().getBasket()); // TODO: substitute commodity types.
            sb.append("constraint(s)");
            // TODO: add text in case of a violated utilization upper bound.
            sb.append(" and there is currently no ").append(traderType.apply(getSource().getType()));
            sb.append(" that can satisfy them.");
        }

        return sb.toString();
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getTarget().getBuyer();
    }

    /**
     * Tests whether two Reconfigure actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Reconfigure this, @ReadOnly Object other) {
        if (!(other instanceof Reconfigure)) {
            return false;
        }
        Reconfigure otherReconfigure = (Reconfigure)other;
        return otherReconfigure.getEconomy() == getEconomy()
                        && otherReconfigure.getTarget() == getTarget();
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getTarget().hashCode()).hash()
                        .asInt();
    }

    @Override
    public ActionType getType() {
        return ActionType.RECONFIGURE;
    }

    /**
     * Finds all commodities that are not available on source but are requested by shopping list.
     * If the source does not exist, returns all the commodities bought by shopping list.
     *
     * @return A set of unavailable commodity specifications
     */
    private Set<CommoditySpecification> findUnavailableCommodities() {
        // When the shopping list is not empty and it does not have a supplier,
        // put all the commodities into reconfigure action's unavailableCommodities.
        if (getSource() == null) {
            unavailableCommodities = getTarget().getBasket().stream().collect(Collectors.toSet());
            return unavailableCommodities;
        }
        for (CommoditySpecification c : getTarget().getBasket()) {
            if (!getSource().getBasketSold().contains(c)) {
                unavailableCommodities.add(c);
            }
        }
        return unavailableCommodities;
    }

    /**
     * Returns a set of commodities asked by the shopping list but can not be satisfied by any seller.
     *
     * @return A set of unavailable commodities.
     */
    public Set<CommoditySpecification> getUnavailableCommodities() {
        return unavailableCommodities;
    }

} // end Reconfigure class
