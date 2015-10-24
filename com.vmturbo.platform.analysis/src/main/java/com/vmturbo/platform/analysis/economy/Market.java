package com.vmturbo.platform.analysis.economy;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
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
// TODO: consider making markets agnostic to the economy by hoisting a few methods to the economy
// class. If that is not an option consider making market a non-static inner class of the economy.
public final class Market {
    // Fields

    private final @NonNull Basket basket_; // see #getBasket()
    private final @NonNull Economy economy_; // see #getEconomy()
    // All active Traders buying this market's basket. Some may appear more than once as different
    // participations.
    private final @NonNull List<@NonNull BuyerParticipation> buyers_ = new ArrayList<>();
    // All active Traders selling a basket that matches this market's.
    private final @NonNull List<@NonNull Trader> sellers_ = new ArrayList<>();

    // Constructors

    /**
     * Constructs and empty Market inside a given Economy and attaches the given basket.
     *
     * @param enclosingEconomy  The economy inside which the market will be created.
     * @param basketToAssociate The basket to associate with the new market. It it referenced and
     *                          not copied.
     */
    public Market(@NonNull Economy enclosingEconomy, @NonNull Basket basketToAssociate) {
        economy_ = enclosingEconomy;
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
     * Returns the enclosing {@link Economy}.
     *
     * <p>
     *  Each market is created and maintained inside a unique economy.
     * </p>
     */
    @Pure
    public @NonNull Economy getEconomy(@ReadOnly Market this) {
        return economy_;
    }

    /**
     * Returns an unmodifiable list of sellers participating in {@code this} {@code Market}.
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull Trader> getSellers(@ReadOnly Market this) {
        return Collections.unmodifiableList(sellers_);
    }

    @Deterministic
    @NonNull Market addSeller(@NonNull Trader newSeller) {
        sellers_.add(newSeller);
        return this;
    }

    @Deterministic
    @NonNull Market removeSeller(@NonNull Trader sellerToRemove) {
        sellers_.remove(sellerToRemove);
        return this;
    }

    /**
     * Returns an unmodifiable list of buyers participating in {@code this} {@code Market}.
     */
    @Pure
    public @NonNull @ReadOnly List<@NonNull BuyerParticipation> getBuyers(@ReadOnly Market this) {
        return Collections.unmodifiableList(buyers_);
    }

    // TODO: consider adding an extra argument for supplier
    @NonNull BuyerParticipation addBuyer(@NonNull TraderWithSettings newBuyer) {
        BuyerParticipation newParticipation = new BuyerParticipation(newBuyer.getEconomyIndex(),
            BuyerParticipation.NO_SUPPLIER, basket_.size());
        buyers_.add(newParticipation);

        return newParticipation;
    }

    @NonNull Market removeBuyerParticipation(@NonNull BuyerParticipation participationToRemove) {
        buyers_.remove(participationToRemove);
        // TODO: move participation to null
        return this;
    }

    /**
     * Returns the supplier of the given buyer participation in {@code this} market or {@code null}
     * if there is no such supplier.
     *
     * @param participation The buyer participation for which we query the supplier.
     * @return The supplier or {@code null} if the given buyer participation is currently not buying
     *          the corresponding basket from anyone.
     */
    @Pure
    public @Nullable @ReadOnly Trader getSupplier(@ReadOnly Market this, @NonNull @ReadOnly BuyerParticipation participation) {
        return participation.getSupplierIndex() == BuyerParticipation.NO_SUPPLIER
            ? null : getEconomy().getTraders().get(participation.getSupplierIndex());
    }

} // end Market class
