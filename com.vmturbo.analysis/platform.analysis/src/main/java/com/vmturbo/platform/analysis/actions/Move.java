package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;
import static com.vmturbo.platform.analysis.ede.Placement.initiateQuoteMinimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ScalingGroupPeerInfo;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.Placement;
import com.vmturbo.platform.analysis.ede.QuoteMinimizer;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.MoveTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.Context;
import com.vmturbo.platform.analysis.translators.AnalysisToProtobuf;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunction;
import com.vmturbo.platform.analysis.updatingfunction.UpdatingFunctionFactory;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityContext;

/**
 * An action to move a {@link ShoppingList} from one supplier to another.
 */
public class Move extends MoveBase implements Action { // inheritance for code reuse
    private static final Logger logger = LogManager.getLogger(Placement.class);
    // Fields
    private final @Nullable Trader destination_;
    private final Optional<Context> context_;
    // Contains new assigned capacities for bought commodities, generated in Quote, which is used
    // to update shoppingList's assigned capacities when action is taken
    private final Collection<CommodityContext> commodityContexts_;
    // Contains old and new values for assigned capacities for bought commodities in the
    // shoppingList that need to be resized as a part of the action
    private final List<MoveTO.CommodityContext> resizeCommodityContexts_ = new ArrayList<>();
    // Constructors

    /**
     * Constructs a new move action with the specified target and economy.
     *
     * @param economy     The economy containing target and destination.
     * @param target      The shopping list that will move.
     * @param destination The trader, target is going to move to.
     */
    public Move(@NonNull Economy economy, @NonNull ShoppingList target, @Nullable Trader destination) {
        this(economy, target, target.getSupplier(), destination, Optional.empty(), null);
    }

    /**
     * Constructs a new move action with the specified target and economy.
     *
     * @param economy     The economy containing target and destination.
     * @param target      The shopping list that will move.
     * @param source The trader, target is going to move from. Note that this argument is mostly
     *               needed when combining actions. Another version of the constructor infers it
     *               from <b>target</b>.
     * @param destination The trader, target is going to move to.
     */
    public Move(@NonNull Economy economy, @NonNull ShoppingList target,
                @Nullable Trader source, @Nullable Trader destination) {
        this(economy, target, source, destination, Optional.empty(), null);
    }

    /**
     * Constructs a new move action
     *
     * @param economy       The economy containing target and destination.
     * @param target        The shopping list that will move.
     * @param source The trader, target is going to move from. Note that this argument is mostly
     *               needed when combining actions. Another version of the constructor infers it
     *               from <b>target</b>.
     * @param destination   The trader, target is going to move to.
     * @param context Additional information about the Move,
     *               like the region that has the lowest cost.
     */
    public Move(@NonNull Economy economy, @NonNull ShoppingList target,
                @Nullable Trader source, @Nullable Trader destination,
                Optional<Context> context) {
        super(economy, target, source);
        destination_ = destination;
        context_ = context;
        commodityContexts_ = null;
    }


    /**
     * Constructs a new move action.
     *
     * @param economy       The economy containing target and destination.
     * @param target        The shopping list that will move.
     * @param source The trader, target is going to move from. Note that this argument is mostly
     *               needed when combining actions. Another version of the constructor infers it
     *               from <b>target</b>.
     * @param destination   The trader, target is going to move to.
     * @param context Additional information about the Move,
     *               like the region that has the lowest cost.
     * @param commodityContexts contains commodity demand values that may have been adjusted during
     *                      Placement.
     */
    public Move(@NonNull Economy economy, @NonNull ShoppingList target,
                @Nullable Trader source, @Nullable Trader destination,
                Optional<Context> context, @Nullable Collection<CommodityContext> commodityContexts) {
        super(economy, target, source);
        destination_ = destination;
        context_ = context;
        commodityContexts_ = commodityContexts;
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

    /**
     * Take this action and move consumer from source to destination.
     */
    private @NonNull Move internalTake() {
        super.take();
        final ShoppingList shoppingList = getTarget();
        if (getSource() != getDestination() ||
                isContextChangeValid(shoppingList.getBuyer())) {
            // unplacing the sl
            shoppingList.move(null);
            updateQuantities(getEconomy(), shoppingList, getSource(),
                    UpdatingFunctionFactory.SUB_COMM, false);
            // moving sl to destination
            shoppingList.move(destination_);
            updateQuantities(getEconomy(), shoppingList, getDestination(),
                    UpdatingFunctionFactory.ADD_COMM, true);
            shoppingList.setContext(getContext().orElse(null));
        }
        Optional.ofNullable(commodityContexts_)
            .ifPresent(contexts -> contexts.forEach(context -> {
                final CommoditySpecification commoditySpecification =
                    context.getCommoditySpecification();
                final int baseType = commoditySpecification.getBaseType();
                final double newCapacityOnSeller = context.getNewCapacityOnSeller();
                    if (context.isCommodityDecisiveOnSeller()) {
                        final Double oldCapacity = shoppingList.getAssignedCapacity(baseType);
                        if (oldCapacity != null
                            && Double.compare(newCapacityOnSeller, oldCapacity) != 0) {
                            resizeCommodityContexts_.add(
                                MoveTO.CommodityContext.newBuilder()
                                    .setSpecification(AnalysisToProtobuf
                                    .commoditySpecificationTO(commoditySpecification))
                                    .setOldCapacity(oldCapacity.floatValue())
                                    .setNewCapacity((float)newCapacityOnSeller)
                                    .build());
                        }
                    }
                    shoppingList.addAssignedCapacity(baseType, newCapacityOnSeller);
                }
            ));
        return this;
    }

    public List<MoveTO.CommodityContext> getResizeCommodityContexts() {
        return resizeCommodityContexts_;
    }

    /**
     * Checks if the buyer context equals this move's context.
     *
     * @param buyer Buyer whose Context is checked.
     * @return True only if both contexts are present and they are different, thus change is valid.
     */
    public boolean isContextChangeValid(Trader buyer) {
        if (!getContext().isPresent()) {
            return false;
        }
        if (buyer == null || !buyer.getSettings().getContext().isPresent()) {
            return false;
        }
        final Context moveContext = getContext().get();
        final com.vmturbo.platform.analysis.economy.Context buyerContext =
                buyer.getSettings().getContext().get();
        return !buyerContext.equalsDto(moveContext);
    }

    /**
     * Take this action, then for all scaling group peers, generate and take Move actions.
     * Place those Moves onto the subsequent actions list inside this Move.
     * @return This move action.  Any subsequent Move actions will be placed in subsequentActions_.
     */
    @Override
    public @NonNull Move take() {
        Economy economy = getEconomy();
        ScalingGroupPeerInfo info = economy.getScalingGroupPeerInfo(getTarget());
        List<ShoppingList> peers = info.getPeers(getTarget());
        internalTake();
        for (ShoppingList shoppingList : peers) {
            logger.trace("Synthesizing Move for {} in scaling group {}",
                shoppingList.getBuyer(), shoppingList.getBuyer().getScalingGroupId());
            // we do not care about the current provider and need to force a move
            QuoteMinimizer minimizer = getConsistentQuote(economy, shoppingList, getDestination());
            final Trader cheapestSeller = minimizer.getBestSeller();
            final Optional<Context> optionalQuoteContext = minimizer.getBestQuote().getContext();
            final double savings =
                minimizer.getCurrentQuote().getQuoteValue() - minimizer.getTotalBestQuote();
            // we will prevent actions to and from the same TP. But we would still generate action to the same CBTP with
            // the same coverage. We would need to handle these actions outside M2
            if (!(minimizer.getBestSeller() == shoppingList.getSupplier() &&
                    optionalQuoteContext.isPresent() &&
                    optionalQuoteContext.get().getFamilyBasedCoverageList().isEmpty())) {
                final Move move = new Move(economy, shoppingList, shoppingList.getSupplier(),
                        cheapestSeller, optionalQuoteContext);
                // Add peer Move to subsequent actions list.
                getSubsequentActions().add(move.internalTake().setImportance(savings));
            }
        }
        // All moves have been taken, so the scaling group is now matched (i.e., all the same size).
        info.setConsistentlySized(true);
        return this;
    }

    /**
     * Rollback move action from destination to source.
     */
    private void internalRollback() {
        super.rollback();
        if (getSource() != getDestination()) {
            getTarget().move(getSource());
            updateQuantities(getEconomy(), getTarget(), getDestination(), UpdatingFunctionFactory.SUB_COMM, false);
            updateQuantities(getEconomy(), getTarget(), getSource(), UpdatingFunctionFactory.ADD_COMM, true);
        }
    }

    /**
     * Rollback this action and any Moves in the subsequent actions list. The subsequent Moves will
     * be rolled back in reserve order of the original take(), followed by the rollback of this
     * Move.
     * @return This move action.
     */
    @Override
    public @NonNull Move rollback() {
        getSubsequentActions().forEach(move -> ((Move)move).internalRollback());
        internalRollback();
        getSubsequentActions().clear();
        return this;
    }

    @Override
    @Pure
    public @NonNull Move port(@NonNull final Economy destinationEconomy,
            @NonNull final Function<@NonNull Trader, @NonNull Trader> destinationTrader,
            @NonNull final Function<@NonNull ShoppingList, @NonNull ShoppingList>
                                                                        destinationShoppingList) {
        return new Move(destinationEconomy, destinationShoppingList.apply(getTarget()),
            getSource() == null ? null : destinationTrader.apply(getSource()),
            getDestination() == null ? null : destinationTrader.apply(getDestination()),
            getContext());
    }

    /**
     * Returns whether {@code this} action respects constraints and can be taken.
     *
     * <p>Currently a move is considered valid iff the target shopping list is movable, the
     * destination trader can accept new customers and the source trader is where the target
     * shopping list is currently placed.
     * </p>
     */
    // TODO: are those checks enough?
    @Override
    public boolean isValid() {
        return getTarget().isMovable()
            && (getDestination() == null || getDestination().getSettings().canAcceptNewCustomers())
            // TODO: for replay this kind of makes sense but in general?
            && getSource() == getTarget().getSupplier();
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
     * @param economy           The economy <b>traderToUpdate</b> participates in.
     * @param shoppingList      The shopping list that will be moved.
     * @param traderToUpdate    The seller whose commodities sold will be updated.
     * @param defaultCombinator Default update function when it is not set for a commodity.
     * @param isIncoming        Is the shoppingList moving into the traderToUdpate? True if it is
     *                          moving in, false if it is moving out.
     */
    public static void updateQuantities(@NonNull UnmodifiableEconomy economy,
                                        @NonNull ShoppingList shoppingList,
                                        @Nullable Trader traderToUpdate,
                                        @NonNull UpdatingFunction defaultCombinator,
                                        boolean isIncoming) {
        updateQuantities(economy, shoppingList, traderToUpdate, defaultCombinator, null, isIncoming);
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
     * Quantities are updated on a "best guess" basis: there is no guarantee that when the move is
     * actually executed in the real environment, the quantities will change in that way.
     * </p>
     *
     * @param economy           The economy <b>traderToUpdate</b> participates in.
     * @param shoppingList      The shopping list that will be moved.
     * @param traderToUpdate    The seller whose commodities sold will be updated.
     * @param defaultCombinator Default update function when it is not set for a commodity.
     * @param currentSLs        A set of shopping list that belongs to the same guaranteed buyer.
     *                          Only set during horizontal scale.
     * @param isIncoming        Is the shoppingList moving into the traderToUdpate? True if it is
     *                          moving in, false if it is moving out.
     */
    // TODO: should we cover moves of inactive traders?
    public static void updateQuantities(@NonNull UnmodifiableEconomy economy,
                                        @NonNull ShoppingList shoppingList,
                                        @Nullable Trader traderToUpdate,
                                        @NonNull UpdatingFunction defaultCombinator,
                                        @Nullable final Set<ShoppingList> currentSLs,
                                        boolean isIncoming) {
        if (traderToUpdate == null) {
            return;
        }
        @NonNull Basket basketBought = shoppingList.getBasket();
        final @NonNull @ReadOnly Basket basketSold = traderToUpdate.getBasketSold();
        for (int boughtIndex = 0, soldIndex = 0; boughtIndex < basketBought.size(); ++boughtIndex) {
            final int soldIndexBackUp = soldIndex;
            while (soldIndex < basketSold.size()
                    && !basketBought.get(boughtIndex).equals(basketSold.get(soldIndex))) {
                ++soldIndex;
            }

            if (soldIndex == basketSold.size()) { // if not found
                soldIndex = soldIndexBackUp;
            } else {
                final @NonNull CommoditySold commodity = traderToUpdate.getCommoditiesSold().get(soldIndex);
                final double[] quantities = updatedQuantities(economy, defaultCombinator,
                    shoppingList, boughtIndex, traderToUpdate, soldIndex, true,
                    currentSLs, isIncoming);
                commodity.setQuantity(quantities[0]);
                commodity.setPeakQuantity(quantities[1]);
                ++soldIndex;
            }
        }
    }

    /**
     * Returns the updated quantity and peak quantity sold by a specified seller as a result of a
     * buyer becoming or seizing to be a customer of the seller.
     *
     * <p>
     * A ternary operator is used to predict the change in quantities and peak quantities sold
     * </p>
     *
     * <p>
     * Quantities are updated on a "best guess" basis: there is no guarantee that when the move is
     * actually executed in the real environment, the quantities will change in that way.
     * </p>
     *
     * @param economy           The {@link UnmodifiableEconomy} where the traders are present.
     * @param defaultCombinator Default update function when it is not set for a commodity.
     * @param traderToUpdate    The seller whose commodities sold will be updated.
     * @param soldIndex         The index of the soldCommodity.
     * @param take              True when the action is being taken, FALSE when we simulate the
     *                          move while getting the quote.
     * @param currentSLs        A set of shopping list that belongs to the same guaranteed buyer.
     *                          Only set during horizontal scale.
     * @param isIncoming        Is the shoppingList moving into the traderToUdpate? True if it is
     *                          moving in, false if it is moving out.
     */
    @Pure // The contents of the array are deterministic but the address of the array isn't...
    public static double[] updatedQuantities(@NonNull UnmodifiableEconomy economy,
                                             @NonNull UpdatingFunction defaultCombinator,
                                             ShoppingList sl, int boughtIndex,
                                             @NonNull Trader traderToUpdate, int soldIndex,
                                             boolean take, @Nullable final Set<ShoppingList> currentSLs,
                                             final boolean isIncoming) {
        final CommoditySold commoditySold = traderToUpdate.getCommoditiesSold().get(soldIndex);
        if (take) {
            // updating the numConsumers of a commodity when the move action is being taken
            final int numConsumers = commoditySold.getNumConsumers();
            commoditySold.setNumConsumers(isIncoming ? numConsumers + 1
                    : numConsumers <= 1 ? 0 : numConsumers - 1);
        }
        UpdatingFunction explicitCombinator = commoditySold.getSettings().getUpdatingFunction();
        if (explicitCombinator == null) {
            // if there is no explicit combinator, use default one.
            return defaultCombinator.operate(sl, boughtIndex, commoditySold, traderToUpdate,
                    economy, take, currentSLs, isIncoming);
        }
        return explicitCombinator.operate(sl, boughtIndex, commoditySold, traderToUpdate,
            economy, take, currentSLs, isIncoming);
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
        Move move = (Move)action;
        checkArgument(getTarget().equals(move.getTarget()));
        // If this.destination == this.source, it means that we never moved (This can happen in
        // case of group leaders of ASGs). So combine the actions.
        if (getDestination() != getSource() &&
            move.getDestination() == getSource()) { // Moves cancel each other
            return null;
        } else {
            Move newMove = new Move(getEconomy(), getTarget(), getSource(), move.getDestination(),
                move.getContext());
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
    public boolean equals(@ReadOnly Move this, @ReadOnly Object other) {
        if (!(other instanceof Move)) {
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
     * @param economy     The economy containing target and destination.
     * @param source      The trader, target is going to move from.
     * @param destination The trader, target is going to move to.
     * @param target      The shopping list that will move.
     * @return The {@link Move} object.
     */
    public @NonNull Move simulateChangeDestinationOnly(@NonNull Economy economy,
                                                       @NonNull Trader source, @NonNull Trader destination,
                                                       @NonNull ShoppingList target) {
        if (source != destination) {
            target.move(destination);
            updateQuantities(economy, target, destination, UpdatingFunctionFactory.ADD_COMM, true);
        }
        return this;
    }

    @Override
    public ActionType getType() {
        return ActionType.MOVE;
    }

    public Optional<Context> getContext() {
        return context_;
    }

    /**
     * The purpose of this method is to get the savings for non-leader scaling group members.
     * @param economy the economy
     * @param shoppingList shopping list to get a quote for
     * @param destination template. The quote will use only this destination CBTP or related TPs.
     * @return quote minimizer
     */
    private static QuoteMinimizer getConsistentQuote(Economy economy, ShoppingList shoppingList,
                                                     Trader destination) {
        // If there are no sellers in the market, the buyer is misconfigured, but this method
        // will not be called in that case.
        final @NonNull List<@NonNull Trader> sellers = getPeerSellers(economy, destination,
                shoppingList);
        if (logger.isTraceEnabled()) {
            logger.trace("PSL Sellers for shoppingList: " + shoppingList.toString());
            for (Trader trader : sellers) {
                if (AnalysisToProtobuf.replaceNewSupplier(shoppingList, economy, trader) != null) {
                    logger.trace("PSL Seller: " +
                        trader.toString());
                }
            }
        }
        return initiateQuoteMinimizer(economy, sellers, shoppingList, null,
                                        0 /* ignored because cache == null */);
    }

    /**
     * Get all valid sellers for peers in a scaling group.  Valid sellers in this case are the
     * set of all valid TPs that sell the basket that the consumer requests
     * @param economy that the leader {@link ShoppingList} is a part of
     * @param seller possible destination
     * @param buyer is the leader whose peers we need to compute
     * @return peer traders of a given groupLeader
     */
    static List<Trader> getPeerSellers(Economy economy, Trader seller, ShoppingList buyer) {
        List<Trader> mutableSellers = new ArrayList<>();
        mutableSellers.add(seller);
        final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>>
            shoppingListsInMarket = economy.getMarketsAsBuyer(seller).entrySet();
        if (!shoppingListsInMarket.isEmpty()) {
            Market market = shoppingListsInMarket.iterator().next().getValue();
            List<Trader> sellers = market.getActiveSellers();
            mutableSellers.addAll(sellers);
            mutableSellers.retainAll(economy.getMarket(buyer).getActiveSellers());
        }
        return mutableSellers;
    }

} // end Move class
