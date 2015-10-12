package com.vmturbo.platform.analysis.economy;

import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;


/**
 * An entity that that trades goods in a {@link Market}.
 *
 * <p>
 *  It can participate in multiple markets either as a seller or a buyer and even participate in a
 *  single market multiple times (though never as a buyer and seller simultaneously). The later can
 *  happen e.g. if a buyer buys multiple storage commodities. It is also possible that a Merchant
 *  buys from another trader that is not in the current market, if a policy is created that is not
 *  enforced initially.
 * </p>
 */
public interface Trader {
    /**
     * An unmodifiable list of the baskets of goods {@code this} trader is buying, or needs to buy.
     */
    @NonNull List<@NonNull Basket> getBasketsBought();

    /**
     * An unmodifiable list of the commodities {@code this} trader is selling.
     */
    @NonNull List<@NonNull UnmodifiableCommoditySold> getCommoditiesSold();

    // May need to add some reference to the associated reservation later...

    /**
     * The {@link TraderSettings settings} controlling {@code this} trader's behavior.
     */
    @NonNull TraderSettings getSettings();

    /**
     * The {@link TraderType type} of {@code this} trader.
     */
    @NonNull TraderType getType();

    /**
     * Returns the current {@link TraderState state} of {@code this} trader.
     */
    @NonNull TraderState getState();

    /**
     * Sets the value of the <b>state</b> field.
     *
     * <p>
     *  Has no observable side-effects except setting the above field.
     * </p>
     *
     * @param state the new value for the field.
     * @return {@code this}
     */
    @NonNull Trader setState(TraderState state);

} // end interface Merchant
