package com.vmturbo.platform.analysis.economy;

import java.util.List;

import org.checkerframework.checker.javari.qual.PolyRead;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Deterministic;
import org.checkerframework.dataflow.qual.Pure;


/**
 * An entity that trades goods in a {@link Market}.
 *
 * <p>
 *  It can participate in multiple markets either as a seller or a buyer and even participate in a
 *  single market multiple times (though never as a buyer and seller simultaneously). The latter can
 *  happen e.g. if a buyer buys multiple storage commodities. It is also possible that a Trader
 *  buys from another trader that is not in the current market, if a policy is created that is not
 *  enforced initially.
 * </p>
 */
public interface Trader {
    /**
     * Returns the basket sold by {@code this} seller.
     */
    @Pure
    @NonNull @ReadOnly Basket getBasketSold(@ReadOnly Trader this);

    /**
     * Returns an unmodifiable list of the commodities {@code this} trader is selling.
     */
    @Pure
    @NonNull @ReadOnly List<@NonNull @ReadOnly CommoditySold> getCommoditiesSold(@ReadOnly Trader this);

    /**
     * Returns the commodity sold by {@code this} trader that corresponds to the given
     * {@link CommoditySpecification}.
     *
     * @param specification The commodity specification specifying the commodity sold that should be
     *         returned.
     * @return The commodity sold by {@code this} trader that corresponds to the given commodity
     *         specification, or {@code null} iff !{@link #getBasketSold()}.
     *         {@link Basket#contains(CommoditySpecification) contains(specification)}
     */
    @Pure
    @PolyRead CommoditySold getCommoditySold(@PolyRead Trader this, @NonNull @ReadOnly CommoditySpecification specification);

    // TODO: consider making addCommoditySold and removeCommoditySold throw in cases they now return
    // null. Same for the corresponding Basket methods.

    /**
     * Adds a new commodity to the list of commodities sold by {@code this} seller and returns it.
     *
     * @param newSpecification The type of the new commodity. It will be added to {@code this}
     *          seller's basket, or ignored if it already exists.
     * @return The new commodity that was created and added, or {@code null} if it already existed.
     */
    @Deterministic
    CommoditySold addCommoditySold(@NonNull @ReadOnly CommoditySpecification newSpecification);

    /**
     * Removes an existing commodity from the list of commodities sold by {@code this} seller.
     *
     * <p>
     *  A commodity sold by a single trader is uniquely identified by its type.
     *  Both the list of commodities sold and the basket sold are updated.
     * </p>
     *
     * @param specificationToRemove The specification of the commodity that needs to be removed.
     *              It will be removed from {@code this} seller's basket, or ignored if it was never
     *              in the basket.
     * @return The removed commodity sold, or {@code null} if it wasn't in the basket.
     */
    @Deterministic // in the sense that for the same referents of this and typeToRemove the result will
    // be the same. Calling this two times on the same topology will produce different results
    // because the topology is modified.
    CommoditySold removeCommoditySold(@NonNull @ReadOnly CommoditySpecification specificationToRemove);

   // May need to add methods to add/remove baskets bought later...

    // May need to add some reference to the associated reservation later...

    /**
     * The {@link TraderSettings settings} controlling {@code this} trader's behavior.
     */
    @Pure
    @NonNull TraderSettings getSettings();

    /**
     * Returns the type of the trader.
     *
     * <p>
     *  Its a numerical representation of the type. An ID of sorts that may e.g. correspond to
     *  "physical machine" or "storage", but the correspondence is not important to the economy and
     *  kept (potentially) outside the economy. It is non-negative.
     * </p>
     */
    @Pure
    int getType();

    /**
     * Returns the current {@link TraderState state} of {@code this} trader.
     */
    @Pure
    @NonNull TraderState getState();

} // end interface Trader
