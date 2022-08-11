package com.vmturbo.platform.analysis.actions;

import java.util.function.DoubleBinaryOperator;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;
import static com.google.common.base.Preconditions.checkArgument;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

/**
 * An action to move a {@link ShoppingList} from one supplier to another.
 */
public class Move extends MoveBase implements Action { // inheritance for code reuse
    // Fields
    private final @Nullable Trader destination_;

    // Constructors

    /**
     * Constructs a new move action with the specified target and economy.
     *
     * @param economy The economy containing target and destination.
     * @param target The shopping list that will move.
     * @param destination The trader, target is going to move to.
     */
    public Move(@NonNull Economy economy, @NonNull ShoppingList target, @Nullable Trader destination) {
        this(economy,target,target.getSupplier(),destination);
    }

    /**
     * Constructs a new move action with the specified target and economy.
     *
     * @param economy The economy containing target and destination.
     * @param target The shopping list that will move.
     * @param source The trader, target is going to move from. Note that this argument is mostly
     *               needed when combining actions. Another version of the constructor infers it
     *               from <b>target</b>.
     * @param destination The trader, target is going to move to.
     */
    public Move(@NonNull Economy economy, @NonNull ShoppingList target,
                @Nullable Trader source, @Nullable Trader destination) {
        super(economy,target,source);
        destination_ = destination;
    }

    // Methods

    /**
     * Returns the destination of {@code this} move. i.e. the trader, target will move to.
     */
    @Pure
    public @Nullable Trader getDestination(@ReadOnly Move this) {
        return destination_;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        StringBuilder sb = new StringBuilder()
            // TODO: is it enough to send the buyer or is the basket needed as well?
            .append("<action type=\"move\" target=\"").append(oid.apply(getTarget().getBuyer()));
        if (getSource() != null) {
            sb.append("\" source=\"").append(oid.apply(getSource()));
        }
        sb.append("\" destination=\"").append(oid.apply(destination_)).append("\" />");
        return sb.toString();
    }

    @Override
    public @NonNull Move take() {
        super.take();
        if (getSource() != getDestination()) {
            getTarget().move(destination_);
            updateQuantities(getEconomy(), getTarget(), getSource(), (sold, bought) -> Math.max(0, sold - bought));
            updateQuantities(getEconomy(), getTarget(), getDestination(), (sold, bought) -> sold + bought);
        }
        return this;
    }

    @Override
    public @NonNull Move rollback() {
        super.rollback();
        if (getSource() != getDestination()) {
            getTarget().move(getSource());
            updateQuantities(getEconomy(), getTarget(), getDestination(), (sold, bought) -> Math.max(0, sold - bought));
            updateQuantities(getEconomy(), getTarget(), getSource(), (sold, bought) -> sold + bought);
        }
        return this;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        if (getSource() != null) {
            if (destination_ != null) { // Move
                sb.append("Move ");
                appendTrader(sb, getTarget().getBuyer(), uuid, name);
                sb.append(" from ");
                appendTrader(sb, getSource(), uuid, name);
                sb.append(" to ");
                appendTrader(sb, getDestination(), uuid, name);
                sb.append(".");
            } else {// Unplace
                sb.append("Unplace ");
                appendTrader(sb, getTarget().getBuyer(), uuid, name);
                sb.append(" from ");
                appendTrader(sb, getSource(), uuid, name);
                sb.append(".");
            }
        } else {
            if (destination_ != null) { // Start
                sb.append("Start ");
                appendTrader(sb, getTarget().getBuyer(), uuid, name);
                sb.append(" on ");
                appendTrader(sb, getDestination(), uuid, name);
                sb.append(".");
            } else {
                return "Error in post-processing actions! This action should have been removed!";
            }
        }

        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        if (getSource() != null) {
            if (destination_ != null) {
                return "To provide a better placement."; // TODO: cover move conditions here!
            } else { // Unplace
                sb.append("To suspend ");
                appendTrader(sb, getSource(), uuid, name);
                sb.append(".");
            }
        } else {
            if (destination_ != null) { // Start
                sb.append("Because ");
                appendTrader(sb, getTarget().getBuyer(), uuid, name);
                sb.append(" was previously unplaced");
            } else {
                return "Error in post-processing actions! This action should have been removed!";
            }
        }

        return sb.toString();
    }

    /**
     * Updates the quantities and peak quantities sold by a specified seller as a result of a buyer
     * becoming or seizing to be a customer of the seller.
     *
     * <p>
     *  A binary operator is used to predict the change in quantities and peak quantities sold
     *  caused by the move.
     * </p>
     *
     * <p>
     *  Quantities are updated on a "best guess" basis: there is no guarantee that when the move is
     *  actually executed in the real environment, the quantities will change in that way.
     * </p>
     *
     * @param basketBought The basket bought by the buyer. It must match shopping list.
     * @param shoppingList The shopping list that will be moved.
     * @param traderToUpdate The seller whose commodities sold will be updated.
     * @param defaultCombinator A binary operator (old quantity sold, quantity bought) -> new quantity sold.
     */
    // TODO: should we cover moves of inactive traders?
    public static void updateQuantities(@NonNull UnmodifiableEconomy economy,
                    @NonNull ShoppingList shoppingList,
            @Nullable Trader traderToUpdate, @NonNull DoubleBinaryOperator defaultCombinator) {
        @NonNull Basket basketBought = economy.getMarket(shoppingList).getBasket();

        if (traderToUpdate != null) {
            final @NonNull @ReadOnly Basket basketSold = traderToUpdate.getBasketSold();
            for (int boughtIndex = 0, soldIndex = 0 ; boughtIndex < basketBought.size() ; ++boughtIndex) {
                final int soldIndexBackUp = soldIndex;
                while (soldIndex < basketSold.size()
                        && !basketBought.get(boughtIndex).isSatisfiedBy(basketSold.get(soldIndex))) {
                    ++soldIndex;
                }

                if (soldIndex == basketSold.size()) { // if not found
                    soldIndex = soldIndexBackUp;
                } else {
                    final @NonNull CommoditySold commodity = traderToUpdate.getCommoditiesSold().get(soldIndex);
                    final double[] quantities = updatedQuantities(economy, defaultCombinator,
                        shoppingList.getQuantity(boughtIndex), shoppingList.getPeakQuantity(boughtIndex),
                        traderToUpdate, soldIndex, false);
                    commodity.setQuantity(quantities[0]);
                    commodity.setPeakQuantity(quantities[1]);
                    ++soldIndex;
                }
            }
        }
    }

    @Pure // The contents of the array are deterministic but the address of the array isn't...
    public static double[] updatedQuantities(@NonNull UnmodifiableEconomy economy, @NonNull DoubleBinaryOperator defaultCombinator,
            double quantityBought, double peakQuantityBought, @NonNull Trader traderToUpdate, int soldIndex,
            boolean incoming) {
        final CommoditySpecification specificationSold = traderToUpdate.getBasketSold().get(soldIndex);
        final CommoditySold commoditySold = traderToUpdate.getCommoditiesSold().get(soldIndex);

        DoubleBinaryOperator explicitCombinator = economy.getQuantityFunctions().get(specificationSold);
        if (explicitCombinator == null) { // if there is no explicit combinator, use default one.
            return new double[]{defaultCombinator.applyAsDouble(commoditySold.getQuantity(), quantityBought),
                                defaultCombinator.applyAsDouble(commoditySold.getPeakQuantity(), peakQuantityBought)};
        } if (incoming) {
            return new double[]{explicitCombinator.applyAsDouble(commoditySold.getQuantity(), quantityBought),
                                explicitCombinator.applyAsDouble(commoditySold.getPeakQuantity(), peakQuantityBought)};
        } else {
            // this loop is used when we use a combinator that is "max" for example, when we move out of this trader, we wouldnt know the initial value
            // that was replaced by the current quantity. For example, max(5,12), we wont know what 12 replaced.
            // Find the quantities bought by all shopping lists and calculate the quantity sold.
            double combinedQuantity = 0.0; // TODO: generalize default value
            double combinedPeakQuantity = 0.0; // if/when needed.
            for (ShoppingList customer : traderToUpdate.getCustomers()) {
                // TODO: this needs to be changed to something that takes matching but unequal
                // commodities into account.
                int specIndex = economy.getMarket(customer).getBasket().indexOf(specificationSold);
                if (specIndex >= 0) {
                    combinedQuantity = explicitCombinator.applyAsDouble(combinedQuantity,
                                                                        customer.getQuantity(specIndex));
                    combinedPeakQuantity = explicitCombinator.applyAsDouble(combinedPeakQuantity,
                                                                            customer.getPeakQuantity(specIndex));
                }
            }

            return new double[]{combinedQuantity,combinedPeakQuantity};
        }
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Object getCombineKey() {
        return Lists.newArrayList(Move.class, getTarget());
    }

    @Override
    @Pure
    public @Nullable @ReadOnly Action combine(@NonNull Action action) {
        // Assume the argument is a Move of the same target, otherwise we are not supposed to get here.
        // Also assume a consistent sequence of actions, i.e. this.getDestination() == action.getSource().
        Move move = (Move) action;
        checkArgument(getTarget().equals(move.getTarget()));
        if (move.getDestination() == getSource()) { // Moves cancel each other
            return null;
        } else {
            Move newMove = new Move(getEconomy(), getTarget(), getSource(), move.getDestination());
            return newMove;
        }
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getTarget().getBuyer();
    }

    /**
     * Tests whether two Move actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Move this,@ReadOnly Object other) {
        if (other == null || !(other instanceof Move)) {
            return false;
        }
        Move otherMove = (Move)other;
        return otherMove.getEconomy() == getEconomy() && otherMove.getTarget() == getTarget()
                        && otherMove.getSource() == getSource()
                        && otherMove.getDestination() == getDestination();
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getTarget().hashCode())
                        .putInt(getSource() == null ? 0 : getSource().hashCode())
                        .putInt(getDestination() == null ? 0 : getDestination().hashCode()).hash()
                        .asInt();
    }

} // end Move class
