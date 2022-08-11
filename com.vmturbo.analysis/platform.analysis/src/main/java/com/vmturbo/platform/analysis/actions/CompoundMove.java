package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.google.common.collect.Lists;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * An action to group multiple {@link Move}s that should happen atomically by {@link ShoppingList}s
 * of the same {@link Trader}.
 */
public class CompoundMove extends ActionImpl {
    // Fields
    private final @NonNull List<@NonNull Move> moves_;

    // Constructors

    /**
     * Constructs a new compound move action with the specified shopping lists, economy and
     * destinations.
     *
     * @param economy The economy containing <b>shoppingLists</b> and <b>destinations</b>.
     * @param shoppingLists The shopping lists that should move atomically.
     * @param destinations The new suppliers for the shopping lists in the same order as the
     *                     shopping lists appear in the respective collection.
     */
    public CompoundMove(@NonNull Economy economy, @NonNull Collection<@Nullable ShoppingList> shoppingLists,
                        @NonNull Collection<@Nullable Trader> destinations) {
        this(economy,shoppingLists,shoppingLists.stream().map(ShoppingList::getSupplier)
             .collect(Collectors.toList()),destinations);
    }

    /**
     * Constructs a new compound move action with the specified shopping lists, economy, sources and
     * destinations.
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
    public CompoundMove(@NonNull Economy economy, @NonNull Collection<@Nullable ShoppingList> shoppingLists,
            @NonNull Collection<@Nullable Trader> sources, @NonNull Collection<@Nullable Trader> destinations) {
        checkArgument(shoppingLists.size() == destinations.size(), "shoppingLists.size() = "
                    + shoppingLists.size() + ", destinations.size() = " + destinations.size());
        checkArgument(shoppingLists.size() == sources.size(), "shoppingLists.size() = "
                    + shoppingLists.size() + ", sources.size() = " + sources.size());
        @NonNull List<@NonNull Move> moves = new ArrayList<>();

        Iterator<ShoppingList> shoppingListIter = shoppingLists.iterator();
        Iterator<Trader> sourceIter = sources.iterator();
        Iterator<Trader> destinationIter = destinations.iterator();
        while (shoppingListIter.hasNext()) {
            moves.add(new Move(economy, shoppingListIter.next(), sourceIter.next(), destinationIter.next()));
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
        // Assume the argument is a CompoundMove of the same target, otherwise we are not supposed
        // to get here. Also assume a consistent sequence of actions, i.e.
        // this.getDestination() == action.getSource().
        // TODO: do we need to combine with scalar moves?

        CompoundMove other = (CompoundMove)action;
        checkArgument(moves_.size() == other.moves_.size());

        if (IntStream.range(0, moves_.size()).allMatch(i->moves_.get(i).getSource() == other.moves_.get(i).getDestination())) {
            return null;
        } else {
            return new CompoundMove(moves_.get(0).getEconomy(),
                                    moves_.stream().map(Move::getTarget).collect(Collectors.toList()),
                                    moves_.stream().map(Move::getSource).collect(Collectors.toList()),
                              other.moves_.stream().map(Move::getDestination).collect(Collectors.toList()));
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

} // end CompoundMove class
