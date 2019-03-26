package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO.MoveContext;
import com.vmturbo.platform.analysis.utilities.FunctionalOperator;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;

/**
 * An action to move a {@link ShoppingList} from one supplier to another.
 */
public class Move extends MoveBase implements Action { // inheritance for code reuse
    // Fields
    private final @Nullable Trader destination_;
    private final Optional<MoveContext> context_;

    // Constructors

    /**
     * Constructs a new move action with the specified target and economy.
     *
     * @param economy The economy containing target and destination.
     * @param target The shopping list that will move.
     * @param destination The trader, target is going to move to.
     */
    public Move(@NonNull Economy economy, @NonNull ShoppingList target, @Nullable Trader destination) {
        this(economy,target,target.getSupplier(),destination, Optional.empty());
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
        this(economy,target,source,destination, Optional.empty());
    }

    /**
     * Constructs a new move action
     *
     * @param economy The economy containing target and destination.
     * @param target The shopping list that will move.
     * @param source The trader, target is going to move from. Note that this argument is mostly
     *               needed when combining actions. Another version of the constructor infers it
     *               from <b>target</b>.
     * @param destination The trader, target is going to move to.
     * @param context Additional information about the Move,
     *               like the region that has the lowest cost.
     */
    public Move(@NonNull Economy economy, @NonNull ShoppingList target,
                @Nullable Trader source, @Nullable Trader destination,
                Optional<MoveContext> context) {
        super(economy,target,source);
        destination_ = destination;
        context_ = context;
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
            updateQuantities(getEconomy(), getTarget(), getSource(), FunctionalOperatorUtil.SUB_COMM);
            updateQuantities(getEconomy(), getTarget(), getDestination(), FunctionalOperatorUtil.ADD_COMM);
        }
        return this;
    }

    @Override
    public @NonNull Move rollback() {
        super.rollback();
        if (getSource() != getDestination()) {
            getTarget().move(getSource());
            updateQuantities(getEconomy(), getTarget(), getDestination(), FunctionalOperatorUtil.SUB_COMM);
            updateQuantities(getEconomy(), getTarget(), getSource(), FunctionalOperatorUtil.ADD_COMM);
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
     * @param defaultCombinator A ternary operator (old quantity sold, quantity bought, third parameter)
     *       -> new quantity sold. will be used only if there is no explicitCombinator for the commodity
     */
    // TODO: should we cover moves of inactive traders?
    public static void updateQuantities(@NonNull UnmodifiableEconomy economy,
                    @NonNull ShoppingList shoppingList,
            @Nullable Trader traderToUpdate, @NonNull FunctionalOperator defaultCombinator) {
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
                        shoppingList, boughtIndex, traderToUpdate, soldIndex, false, true);
                    commodity.setQuantity(quantities[0]);
                    commodity.setPeakQuantity(quantities[1]);
                    ++soldIndex;
                }
            }
        }
    }

    /**
     * Returns the updated quantity and peak quantity sold by a specified seller as a result of a buyer
     * becoming a customer of the seller.
     *
     * <p>
     *  A turnary operator is used to predict the change in quantities and peak quantities sold
     * </p>
     *
     * <p>
     *  Quantities are updated on a "best guess" basis: there is no guarantee that when the move is
     *  actually executed in the real environment, the quantities will change in that way.
     * </p>
     *
     * @param economy is the {@link UnmodifiableEconomy} where the traders are present
     * @param defaultCombinator A turnary operator (old quantity sold, quantity bought, third parameter)
     *       -> new quantity sold. will be used only if there is no explicitCombinator for the commodity
     * @param quantityBought amount of a particular commodity that the {@link Trader} shops for from the seller
     * @param peakQuantityBought peak amount of a particular commodity that the buyer shops for from the seller
     * @param traderToUpdate The seller whose commodities sold will be updated.
     * @param soldIndex is the index of the soldCommodity
     * @param incoming is a boolean flag that specifies if the trader is moving in(TRUE) or moving out
     * @param take is true when the action is being taken and is false when we simulate the move while getting the quote
     *
     */
    @Pure // The contents of the array are deterministic but the address of the array isn't...
    public static double[] updatedQuantities(@NonNull UnmodifiableEconomy economy,
                                             @NonNull FunctionalOperator defaultCombinator,
                                             ShoppingList sl, int boughtIndex,
                                             @NonNull Trader traderToUpdate, int soldIndex,
                                             boolean incoming, boolean take) {
        final CommoditySpecification specificationSold = traderToUpdate.getBasketSold().get(soldIndex);
        final CommoditySold commoditySold = traderToUpdate.getCommoditiesSold().get(soldIndex);

        FunctionalOperator explicitCombinator = commoditySold.getSettings().getUpdatingFunction();
        if (explicitCombinator == null) { // if there is no explicit combinator, use default one.
            return defaultCombinator.operate(sl, boughtIndex, commoditySold, traderToUpdate, economy, take, 0);
        } if (incoming) {
            // include quantityBought to the current used of the corresponding commodity
            return explicitCombinator.operate(sl, boughtIndex, commoditySold, traderToUpdate, economy, take, 0);
        } else {
            // this loop is used when we use a combinator that is "max" for example, when we move out of this trader, we wouldnt know the initial value
            // that was replaced by the current quantity. For example, max(5,12), we wont know what 12 replaced.
            // Find the quantities bought by all shopping lists and calculate the quantity sold.

            // TODO: change the definition of updateQuantities to prevent re-computation overhead several times
            // while moving in

            // incomingSl is TRUE when the VM is moving in and false when moving out
            boolean incomingSl = traderToUpdate.getCustomers().contains(sl);
            double overhead = commoditySold.getQuantity();
            for (ShoppingList customer : traderToUpdate.getCustomers()) {
                int commIndex = customer.getBasket().indexOf(specificationSold);
                if (customer != sl && commIndex != -1) {
                    // subtract the used value of comm in question of all the customers but the
                    // incoming shoppingList from the current used value of the sold commodity
                    overhead -= customer.getQuantity(commIndex);
                }
            }
            // when a sl is moving out, it will not be part of the seller's customers. In that case,
            // we get the used value of the comm in question bought by the sl and remove from overhead
            if (!incomingSl) {
                int commIndex = sl.getBasket().indexOf(specificationSold);
                if (commIndex != -1) {
                    // subtract the used value of comm in question of all the customers but the
                    // incoming shoppingList from the current used value of the sold commodity
                    overhead -= sl.getQuantity(commIndex);
                }
            }

            // set used and peakUsed to 0 when starting for the seller
            double sellerOrigUsed = commoditySold.getQuantity();
            double sellerOrigPeak = commoditySold.getPeakQuantity();
            commoditySold.setQuantity(0).setPeakQuantity(0);
            // updating the numConsmers of a commodity when the move action is being taken
            int numConsumers = commoditySold.getNumConsumers();
            if (take) {
                if (incomingSl) {
                    commoditySold.setNumConsumers(numConsumers + 1);
                // check if numConsumers is greater than 0. It can be 0 for bicliques
                } else if (numConsumers > 0) {
                    commoditySold.setNumConsumers(numConsumers - 1);
                }
            }

            // TODO: Currently, whenever there is an explicit combinator, we neglect overhead
            // because of the used recomputation we could have explicitCombinators for which we
            // dont want to neglect overhead. We need to handle these cases.
            for (ShoppingList customer : traderToUpdate.getCustomers()) {
                // TODO: this needs to be changed to something that takes matching but unequal
                // commodities into account.
                int specIndex = customer.getBasket().indexOf(specificationSold);
                if (specIndex >= 0) {
                    double[] tempUpdatedQnty = explicitCombinator.operate(customer, specIndex,
                                    commoditySold, traderToUpdate, economy, take, overhead);
                    commoditySold.setQuantity(tempUpdatedQnty[0]).setPeakQuantity(tempUpdatedQnty[1]);
                }
            }
            double[] combinedQuantity = new double[]{
                    // OM-40472 - handle the case where we are rolling back from one to zero
                    // customers. In this case, the explicitCombinator will not be called to
                    // update the commoditySold. Therefore, if there are no customers when
                    // we reach this point, we should return the overhead. We limit it to >= 0
                    // to maintain the previous behavior of returning commoditySold.getQuantity
                    // after being reset above to 0
                    traderToUpdate.getCustomers().isEmpty() ?
                            Math.max(overhead, 0) : commoditySold.getQuantity(),
                    commoditySold.getPeakQuantity()
            };
            if (explicitCombinator == FunctionalOperatorUtil.AVG_COMMS) {
                if (incomingSl) {
                    combinedQuantity[0] = Math.max(combinedQuantity[0], sellerOrigUsed);
                    combinedQuantity[1] = Math.max(combinedQuantity[1], sellerOrigPeak);
                } else {
                    combinedQuantity[0] = Math.min(combinedQuantity[0], sellerOrigUsed);
                    combinedQuantity[1] = Math.min(combinedQuantity[1], sellerOrigPeak);
                }
            }
            return new double[]{combinedQuantity[0],combinedQuantity[1]};
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

    /**
     * Simulate the move on a copy of the Economy. It changes the destination quantities
     * but does not modify those at the source.
     *
     * @param economy The economy containing target and destination.
     * @param source The trader, target is going to move from.
     * @param destination The trader, target is going to move to.
     * @param target The shopping list that will move.
     * @return The {@link Move} object.
     */
    public  @NonNull Move simulateChangeDestinationOnly(@NonNull Economy economy,
                               @NonNull Trader source, @NonNull Trader destination,
                               @NonNull ShoppingList target) {
        if (source != destination) {
            target.move(destination);
            updateQuantities(economy, target, destination, FunctionalOperatorUtil.ADD_COMM);
        }
        return this;
    }

    @Override
    public ActionType getType() {
        return ActionType.MOVE;
    }

    public Optional<MoveContext> getContext() {
        return context_;
    }
} // end Move class
