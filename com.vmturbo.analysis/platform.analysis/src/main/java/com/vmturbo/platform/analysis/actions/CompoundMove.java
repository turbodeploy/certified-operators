package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * An action to group multiple {@link Move}s that should happen atomically by {@link ShoppingList}s
 * of the same {@link Trader}.
 */
public class CompoundMove extends ActionImpl {

    static final Logger logger = LogManager.getLogger(CompoundMove.class);

    // Fields
    private final @NonNull List<@NonNull Move> moves_;

    // Constructors

    /**
     * Constructs a new compound move action with the specified shopping lists, economy and
     * destinations. The constructor is private, because it can create a compound move with
     * empty constituent moves, thus it is used in createAndCheckCompoundMoveWithExplicitSources()
     * which returns null and prints a diagnostic message in logs in such a case.
     *
     * @param economy The economy containing <b>shoppingLists</b> and <b>destinations</b>.
     * @param shoppingLists The shopping lists that should move atomically.
     * @param destinations The new suppliers for the shopping lists in the same order as the
     *                     shopping lists appear in the respective collection.
     */
    private CompoundMove(@NonNull Economy economy, @NonNull Collection<@Nullable ShoppingList> shoppingLists,
                        @NonNull Collection<@Nullable Trader> destinations) {
        this(economy,shoppingLists,shoppingLists.stream().map(ShoppingList::getSupplier)
             .collect(Collectors.toList()),destinations);
    }

    /**
     * Constructs a new compound move action with the specified shopping lists, economy, sources and
     * destinations.  The constructor is private, because it can create a compound move with
     * empty constituent moves, thus it is used in createAndCheckCompoundMoveWithImplicitSources()
     * which returns null and prints a diagnostic message in logs in such a case.
     *
     * <p>
     *  Same as {@link #CompoundMove(Economy, Collection, Collection)}, but let's you explicitly
     *  specify the sources of the moves instead of getting them from the shopping lists.
     * </p>
     *
     * @param economy Same as for {@link #CompoundMove(Economy, Collection, Collection)}.
     * @param shoppingLists Same as for {@link #CompoundMove(Economy, Collection, Collection)}.
     * @param sources The current suppliers for the shopping lists, at the time the move would be
     *                executed, in the same order as the latter appear in
     *                {@link Economy#getMarketsAsBuyer(Trader)}.
     *                Must be the same size as <b>destinations</b> and not empty.
     * @param destinations Same as for {@link #CompoundMove(Economy, Collection, Collection)}.
     */
    private CompoundMove(@NonNull Economy economy, @NonNull Collection<@Nullable ShoppingList> shoppingLists,
            @NonNull Collection<@Nullable Trader> sources, @NonNull Collection<@Nullable Trader> destinations) {
        super(economy);

        checkArgument(shoppingLists.size() == destinations.size(), "shoppingLists.size() = "
                    + shoppingLists.size() + ", destinations.size() = " + destinations.size());
        checkArgument(shoppingLists.size() == sources.size(), "shoppingLists.size() = "
                    + shoppingLists.size() + ", sources.size() = " + sources.size());
        @NonNull List<@NonNull Move> moves = new ArrayList<>();

        Iterator<ShoppingList> shoppingListIter = shoppingLists.iterator();
        Iterator<Trader> sourceIter = sources.iterator();
        Iterator<Trader> destinationIter = destinations.iterator();
        while (shoppingListIter.hasNext()) {
            // create move action only if the source and destination are different
            ShoppingList sl = shoppingListIter.next();
            Trader source = sourceIter.next();
            Trader destination = destinationIter.next();
            if (destination != null && !destination.equals(source)) {
                moves.add(new Move(economy, sl, source, destination));
            }
        }

        moves_ = Collections.unmodifiableList(moves);
    }

    // Getters

    /**
     * Returns an unmodifiable list of the moves that comprise {@code this} compound move.
     */
    @Pure
    public @NonNull List<@NonNull Move> getConstituentMoves(@ReadOnly CompoundMove this) {
        return moves_;
    }

    // Methods

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("<action type=\"compound move\" >");
        for (Move move : moves_) {
            sb.append("    ").append(move.serialize(oid)).append('\n');
        }
        sb.append("</action>");

        return sb.toString();
    }

    @Override
    public @NonNull CompoundMove take() {
        super.take();
        moves_.forEach(Move::take);
        return this;
    }

    @Override
    public @NonNull CompoundMove rollback() {
        super.rollback();
        moves_.forEach(Move::rollback);
        return this;
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return moves_.get(0).getActionTarget();
    }

    @Override
    @Pure
    public @NonNull @ReadOnly Object getCombineKey() {
        return Lists.newArrayList(CompoundMove.class, getActionTarget());
    }

    @Override
    @Pure
    public @Nullable @ReadOnly Action combine(@NonNull Action action) {
        // examples of compoundMoves and their combined result:
        // 1. compoundMove1 contains move sl1 from t1 to t2, sl2 from t4 to t5, compoundMove2
        // contains move sl1 from t2 to t3, sl2 from t5 to t6, then the result of compoundMove.combine
        // will be a single compoundMove that contains move sl1 from t1 to t3,, sl2 from t4 to t6
        // 2. compoundMove1 contains move sl1 from t1 to t2, compoundMove2 contains move sl2 from t3 to t4,
        // then the result of compoundMove.combine will be a single compoundMove contains move sl1 from t1
        // to t2 and move sl2 from t3 to t4.
        checkArgument(action.getType() == ActionType.COMPOUND_MOVE);
        CompoundMove other = (CompoundMove)action;
        List<@NonNull Move> combinedMoveResult = new ArrayList<>();
        // create maps with shopping list to constitute move, it is a helper
        // data structure to find matching shopping list of the two compoundMoves
        Map<@NonNull ShoppingList, @NonNull Move> slPerMoveMap1 =
                        new HashMap<@NonNull ShoppingList, @NonNull Move>();
        Map<@NonNull ShoppingList, @NonNull Move> slPerMoveMap2 =
                        new HashMap<@NonNull ShoppingList, @NonNull Move>();
        moves_.forEach(m1 -> slPerMoveMap1.put(m1.getTarget(), m1));
        other.getConstituentMoves().forEach(m2 -> slPerMoveMap2.put(m2.getTarget(), m2));

        for (Entry<ShoppingList, Move> entry1 : slPerMoveMap1.entrySet()) {
            @NonNull
            ShoppingList sl1 = entry1.getKey();
            @NonNull
            Move move1 = entry1.getValue();
            if (slPerMoveMap2.containsKey(sl1)) {
                // a shopping list is found in both compoundMoves, we should merge them
                @NonNull
                Move move2 = slPerMoveMap2.get(sl1);
                @Nullable
                Move combined = (Move)move1.combine(move2);
                // if merge result is null, meaning two moves canceled each other
                if (combined != null) {
                    // we put the combined move into the result list
                    combinedMoveResult.add(combined);
                }
            } else {
                // a shopping list is found in first compoundMove but not in the second compoundMove
                // we keep the move and put into result list
                combinedMoveResult.add(move1);
            }
        }
        for (Entry<ShoppingList, Move> entry2 : slPerMoveMap2.entrySet()) {
            @NonNull
            ShoppingList sl2 = entry2.getKey();
            @NonNull
            Move move2 = entry2.getValue();
            if (!slPerMoveMap1.containsKey(sl2)) {
                // a shopping list is found in second compoundMove but not in the first compoundMove
                // we keep it and put into result list
                combinedMoveResult.add(move2);
            }
        }

        if (combinedMoveResult.isEmpty()) {
            return null;
        } else {
            // NOTE: we should pass all four params instead of three in to compoundMove
            // constructor, because the move is taken, so the source of the move is not
            // the supplier of shopping list any more
            return CompoundMove.createAndCheckCompoundMoveWithExplicitSources(
                    combinedMoveResult.get(0).getEconomy(),
                    combinedMoveResult.stream().map(Move::getTarget)
                            .collect(Collectors.toList()),
                    combinedMoveResult.stream().map(Move::getSource)
                            .collect(Collectors.toList()),
                    combinedMoveResult.stream().map(Move::getDestination)
                            .collect(Collectors.toList()));
        }
    }

    /**
     * Tests whether two {@link CompoundMove}s are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly CompoundMove this, @ReadOnly Object other) {
        if(other == null || !(other instanceof CompoundMove))
            return false;
        return moves_.equals(((CompoundMove)other).moves_);
    }

    @Override
    @Pure
    public int hashCode() {
        return moves_.hashCode();
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Perform the following moves atomically:\n");
        for (Move move : moves_) {
            sb.append("    ").append(move.debugDescription(uuid, name, commodityType, traderType)).append('\n');
        }

        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("To improve the state of the environment. The reasons for individual moves follow:\n");
        for (Move move : moves_) {
            sb.append("    ").append(move.debugReason(uuid, name, commodityType, traderType)).append('\n');
        }

        return sb.toString();
    }

    @Override
    public ActionType getType() {
        return ActionType.COMPOUND_MOVE;
    }

    public static @Nullable CompoundMove createAndCheckCompoundMoveWithExplicitSources(
                            @NonNull Economy economy,
                            @NonNull Collection<@Nullable ShoppingList> shoppingLists,
                            @NonNull Collection<@Nullable Trader> sources,
                            @NonNull Collection<@Nullable Trader> destinations) {

        CompoundMove compoundMove = new CompoundMove(economy, shoppingLists, sources, destinations);
        if (!compoundMove.getConstituentMoves().isEmpty()) {
            return compoundMove;
        } else {
            StringBuilder errorMsg = new StringBuilder("A compound move with no " +
                    "constituent actions was generated. ");
            errorMsg.append("Current Suppliers: ");
            for (Trader supplier : sources) {
                errorMsg.append(supplier.getDebugInfoNeverUseInCode()).append(" ");
            }
            errorMsg.append("Best Suppliers: ");
            for (Trader supplier : destinations) {
                errorMsg.append(supplier.getDebugInfoNeverUseInCode()).append(" ");
            }
            logger.error(errorMsg.toString());
            return null;
        }
    }

    public static @Nullable CompoundMove createAndCheckCompoundMoveWithImplicitSources(
            @NonNull Economy economy,
            @NonNull Collection<@Nullable ShoppingList> shoppingLists,
            @NonNull Collection<@Nullable Trader> destinations) {

        CompoundMove compoundMove = new CompoundMove(economy, shoppingLists, destinations);
        if (!compoundMove.getConstituentMoves().isEmpty()) {
            return compoundMove;
        } else {
            StringBuilder errorMsg = new StringBuilder("A compound move with no " +
                    "constituent actions was generated. ");
            errorMsg.append("Current Suppliers: ");
            for (ShoppingList shoppingList : shoppingLists) {
                errorMsg.append(shoppingList.getSupplier().getDebugInfoNeverUseInCode()).
                        append(" ");
            }
            errorMsg.append("Best Suppliers: ");
            for (Trader supplier : destinations) {
                errorMsg.append(supplier.getDebugInfoNeverUseInCode()).append(" ");
            }
            logger.error(errorMsg.toString());
            return null;
        }
    }

} // end CompoundMove class
