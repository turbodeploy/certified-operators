package com.vmturbo.platform.analysis.economy;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;

/**
 * A trading place where a particular basket of goods is sold and bought.
 *
 * <p>
 *  A {@code Market} is associated with a {@link Basket} and comprises a list of buyers and sellers
 *  trading that particular basket.
 * </p>
 */
public final class Market {
    // Fields

    private final @NonNull Basket basket_; // see #getBasket()
    private final @NonNull List<@NonNull BuyerParticipation> buyers_ = new ArrayList<>(); // see #getBuyers()
    // TODO (Vaptistis): consider making sellers_ a Set.
    private final @NonNull List<@NonNull Trader> sellers_ = new ArrayList<>(); // see #getSellers()

    // Cached data

    // Cached unmodifiable view of the buyers_ list.
    private final @NonNull List<@NonNull BuyerParticipation> unmodifiableBuyers_ = Collections.unmodifiableList(buyers_);
    // Cached unmodifiable view of the sellers_ list.
    private final @NonNull List<@NonNull Trader> unmodifiableSellers_ = Collections.unmodifiableList(sellers_);

    // Constructors

    /**
     * Constructs an empty Market and attaches the given basket.
     *
     * @param basketToAssociate The basket to associate with the new market. It it referenced and
     *                          not copied.
     */
    Market(@NonNull Basket basketToAssociate) {
        basket_ = basketToAssociate;
    }

    // Methods

    /**
     * Returns the associated {@link Basket}.
     *
     * <p>
     *  All buyers in the market buy that basket.
     * </p>
     */
    @Pure
    public @NonNull Basket getBasket(@ReadOnly Market this) {
        return basket_;
    }

    /**
     * Returns an unmodifiable list of sellers participating in {@code this} {@code Market}.
     *
     * <p>
     *  A {@link Trader} participates in the market as a seller iff he is active and the basket he
     *  is selling satisfies the one associated with the market.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull Trader> getSellers(@ReadOnly Market this) {
        return unmodifiableSellers_;
    }

    /**
     * Adds a new seller to {@code this} market. If he is already in the market the results are
     * undefined.
     *
     * <p>
     *  The trader's own {@link TraderWithSettings#getMarketsAsSeller() markets as seller} list is
     *  updated.
     * </p>
     *
     * @param newSeller The new trader to add to the market as a seller. His basket sold must match
     *                  the market's one and he must be active. He should not be already in the market.
     * @return {@code this}
     */
    @Deterministic
    @NonNull Market addSeller(@NonNull TraderWithSettings newSeller) {
        checkArgument(getBasket().isSatisfiedBy(newSeller.getBasketSold()));
        checkArgument(newSeller.getState().isActive());

        sellers_.add(newSeller);
        newSeller.getMarketsAsSeller().add(this);

        return this;
    }

    /**
     * Removes an existing seller from {@code this} market. If he was not in {@code this} market in
     * the first place, the results are undefined.
     *
     * <p>
     *  The trader's own {@link TraderWithSettings#getMarketsAsSeller() markets as seller} list is
     *  updated.
     * </p>
     *
     * <p>
     *  Note that if there are buyer participations in the market buying from sellerToRemove, they
     *  are left unchanged. That's because it's legal from a buyer to temporarily buy from a seller
     *  in another market if e.g. an access commodity has been removed, but the recommendation to
     *  move the buyer hasn't been taken yet.
     * </p>
     *
     * @param sellerToRemove The existing trader that should seize selling in this market.
     * @return {@code this}
     */
    @Deterministic
    @NonNull Market removeSeller(@NonNull TraderWithSettings sellerToRemove) {
        checkArgument(sellers_.remove(sellerToRemove));
        sellerToRemove.getMarketsAsSeller().remove(this);

        return this;
    }

    /**
     * Returns an unmodifiable list of buyers participating in {@code this} {@code Market}.
     *
     * <p>
     *  A {@link Trader} participates in the market as a buyer iff he is active and he buys at least
     *  one basket equal to the one associated with the market. He may appear more than once in the
     *  returned list as different participations iff he buys that basket more than once.
     * </p>
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull BuyerParticipation> getBuyers(@ReadOnly Market this) {
        return unmodifiableBuyers_;
    }

    /**
     * Adds a new buyer to {@code this} market or an existing one again.
     *
     * <p>
     *  This will make the trader buy {@code this} market's basket if he wasn't, or buy it again
     *  if he was. That implies modification of his {@link TraderWithSettings#getMarketsAsBuyer()
     *  markets as buyer} map.
     * </p>
     *
     * @param newBuyer The new trader to add to the market as a buyer. He must be active.
     *                 If his economy index is incorrect, the results are undefined.
     * @return The buyer participation that was created for the buyer.
     */
    // TODO: consider adding an extra argument for supplier
    @NonNull BuyerParticipation addBuyer(@NonNull TraderWithSettings newBuyer) {
        checkArgument(newBuyer.getState().isActive());

        BuyerParticipation newParticipation = new BuyerParticipation(newBuyer, basket_.size());
        buyers_.add(newParticipation);
        newBuyer.getMarketsAsBuyer().put(this, newParticipation);

        return newParticipation;
    }

    /**
     * Removes an existing buyer participation from {@code this} market. If it was not in
     * {@code this} market in the first place, the results are undefined.
     * The trader's own {@link TraderWithSettings#getMarketsAsBuyer() markets as seller} map is
     * updated.
     *
     * @param participationToRemove The existing buyer participation that should be removed from
     *                              {@code this} market. It should be in the market.
     * @return {@code this}
     */
    @NonNull Market removeBuyerParticipation(@NonNull BuyerParticipation participationToRemove) {
        checkArgument(buyers_.remove(participationToRemove));
        participationToRemove.move(null);
        ((TraderWithSettings)participationToRemove.getBuyer()).getMarketsAsBuyer().remove(this, participationToRemove);

        return this;
    }

} // end Market class
