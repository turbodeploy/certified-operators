package com.vmturbo.platform.analysis.actions;

import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Trader;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

/**
 * An action to resize a {@link CommoditySold commodity sold} of a {@link Trader trader}.
 */
public class Resize implements Action {
    // Fields
    private final @NonNull Trader sellingTrader_;
    private final @NonNull CommoditySpecification resizedCommodity_;
    private final double oldCapacity_; // needed for rolling back.
    private final double newCapacity_;

    // Constructors

    /**
     * Constructs a new resize action with the specified attributes and an inferred old capacity.
     *
     * @param sellingTrader The trader that sells the commodity that will be resized.
     * @param resizedCommodity The commodity specification of the commodity that will be resized.
     * @param newCapacity The capacity of the commodity after the resize action is taken.
     */
    public Resize(@NonNull Trader sellingTrader, @NonNull CommoditySpecification resizedCommodity,
                  double newCapacity) {
        this(sellingTrader,resizedCommodity,sellingTrader.getCommoditySold(resizedCommodity).getCapacity(),newCapacity);
    }

    /**
     * Constructs a new resize action with the specified attributes.
     *
     * @param sellingTrader The trader that sells the commodity that will be resized.
     * @param resizedCommodity The commodity specification of the commodity that will be resized.
     * @param oldCapacity The capacity of the commodity before the resize action is taken.
     *                    Note that this argument is mostly needed when combining actions.
     *                    Another version of the constructor infers it from <b>sellingTrader</b> and
     *                    <b>resizedCommodity</b>.
     * @param newCapacity The capacity of the commodity after the resize action is taken.
     */
    public Resize(@NonNull Trader sellingTrader, @NonNull CommoditySpecification resizedCommodity,
                  double oldCapacity, double newCapacity) {
        checkArgument(sellingTrader.getBasketSold().indexOf(resizedCommodity) >= 0,
                      "resizedCommodity =  " + resizedCommodity);
        checkArgument(oldCapacity >= 0, "oldCapacity = " + oldCapacity);
        checkArgument(newCapacity >= 0, "newCapacity = " + newCapacity);

        sellingTrader_ = sellingTrader;
        resizedCommodity_ = resizedCommodity;
        oldCapacity_ = oldCapacity;
        newCapacity_ = newCapacity;
    }

    // Methods

    /**
     * Returns the trader whose commodity will be resized by {@code this} action.
     */
    @Pure
    public @Nullable Trader getSellingTrader(@ReadOnly Resize this) {
        return sellingTrader_;
    }

    /**
     * Returns the commodity specification of the commodity that will be resized by {@code this}
     * action.
     */
    @Pure
    public @Nullable CommoditySpecification getResizedCommodity(@ReadOnly Resize this) {
        return resizedCommodity_;
    }

    /**
     * Returns the capacity of the resized commodity before {@code this} action was taken.
     */
    @Pure
    public double getOldCapacity(@ReadOnly Resize this) {
        return oldCapacity_;
    }

    /**
     * Returns the capacity of the resized commodity after {@code this} action was taken.
     */
    @Pure
    public double getNewCapacity(@ReadOnly Resize this) {
        return newCapacity_;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder()
            .append("<action type=\"resize\" sellingTrader=\"").append(oid.apply(getSellingTrader()))
            .append("\" commoditySpecification=\"").append(getResizedCommodity())
            .append("\" oldCapacity=\"").append(getOldCapacity())
            .append("\" newCapacity=\"").append(getNewCapacity()).append("\" />").toString();
    }

    @Override
    public @NonNull Resize take() {
        getSellingTrader().getCommoditySold(getResizedCommodity()).setCapacity(getNewCapacity());
        return this;
    }

    @Override
    public @NonNull Resize rollback() {
        getSellingTrader().getCommoditySold(getResizedCommodity()).setCapacity(getOldCapacity());
        return this;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Resize ").append(commodityType.apply(getResizedCommodity().getType())).append(" of ");
        appendTrader(sb, getSellingTrader(), uuid, name);
        sb.append(getNewCapacity() > getOldCapacity() ? " up" : " down");
        sb.append(" from ").append(getOldCapacity()).append(" to ").append(getNewCapacity()).append(".");

        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        // TODO: update this when we settle on the reason messages for resize actions.
        if(getNewCapacity() > getOldCapacity())
            return "To ensure performance.";
        else
            return "To improve efficiency.";
    }

} // end Resize class
