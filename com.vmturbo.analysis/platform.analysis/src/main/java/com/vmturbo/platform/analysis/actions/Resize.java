package com.vmturbo.platform.analysis.actions;

import static com.google.common.base.Preconditions.checkArgument;
import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.Resizer;

/**
 * An action to resize a {@link CommoditySold commodity sold} of a {@link Trader trader}.
 */
public class Resize extends ActionImpl {
    // Fields
    private final @NonNull Economy economy_;
    private final @NonNull Trader sellingTrader_;
    private final @NonNull CommoditySpecification resizedCommoditySpec_;
    private final @NonNull CommoditySold resizedCommodity_;
    private final int soldIndex_;
    private final double oldCapacity_; // needed for rolling back.
    private final double newCapacity_;

    // Constructors

    /**
     * Constructs a new resize action with the specified attributes and an inferred
     * commodity sold and commodity sold index.
     *
     * @param economy The economy containing target and destination.
     * @param sellingTrader The trader that sells the commodity that will be resized.
     * @param resizedCommoditySpec The commodity specification of the commodity that will be resized.
     * @param oldCapacity The capacity of the commodity before the resize action is taken.
     * @param newCapacity The capacity of the commodity after the resize action is taken.
     */
    public Resize(@NonNull Economy economy, @NonNull Trader sellingTrader,
                  @NonNull CommoditySpecification resizedCommoditySpec,
                  double oldCapacity, double newCapacity) {
        this(economy,sellingTrader,resizedCommoditySpec,sellingTrader.getCommoditySold(
             resizedCommoditySpec),sellingTrader.getBasketSold().indexOf(resizedCommoditySpec),
             oldCapacity,newCapacity);
    }

    /**
     * Constructs a new resize action with the specified attributes and an inferred old capacity,
     * commodity sold and commodity sold index.
     *
     * @param economy The economy containing target and destination.
     * @param sellingTrader The trader that sells the commodity that will be resized.
     * @param resizedCommoditySpec The commodity specification of the commodity that will be resized.
     * @param newCapacity The capacity of the commodity after the resize action is taken.
     */
    public Resize(@NonNull Economy economy, @NonNull Trader sellingTrader,
                  @NonNull CommoditySpecification resizedCommoditySpec, double newCapacity) {
        this(economy,sellingTrader,resizedCommoditySpec,sellingTrader.getCommoditySold(
             resizedCommoditySpec),sellingTrader.getBasketSold().indexOf(resizedCommoditySpec),
             sellingTrader.getCommoditySold(resizedCommoditySpec).getCapacity(),newCapacity);
    }

    /**
     * Constructs a new resize action with the specified attributes and an inferred old capacity,
     * commodity sold and commodity sold index.
     *
     * @param economy The economy containing target and destination.
     * @param sellingTrader The trader that sells the commodity that will be resized.
     * @param resizedCommoditySpec The commodity specification of the commodity that will be resized.
     * @param resizedCommodity The commodity that will be resized.
     * @param soldIndex The index of the resized commodity specification in seller's basket.
     * @param newCapacity The capacity of the commodity after the resize action is taken.
     */
    public Resize(@NonNull Economy economy, @NonNull Trader sellingTrader,
                  @NonNull CommoditySpecification resizedCommoditySpec,
                  @NonNull CommoditySold commoditySold, int soldIndex, double newCapacity) {
        this(economy,sellingTrader,resizedCommoditySpec,commoditySold,soldIndex,
             sellingTrader.getCommoditySold(resizedCommoditySpec).getCapacity(),newCapacity);
    }

    /**
     * Constructs a new resize action with the specified attributes.
     *
     * @param economy The economy containing target and destination.
     * @param sellingTrader The trader that sells the commodity that will be resized.
     * @param resizedCommoditySpec The commodity specification of the commodity that will be resized.
     * @param resizedCommodity The commodity that will be resized.
     * @param soldIndex The index of the resized commodity.
     * @param oldCapacity The capacity of the commodity before the resize action is taken.
     *                    Note that this argument is mostly needed when combining actions.
     *                    Another version of the constructor infers it from <b>sellingTrader</b> and
     *                    <b>resizedCommodity</b>.
     * @param newCapacity The capacity of the commodity after the resize action is taken.
     */
    public Resize(@NonNull Economy economy, @NonNull Trader sellingTrader,
                  @NonNull CommoditySpecification resizedCommoditySpec, CommoditySold
                  resizedCommodity, int soldIndex,double oldCapacity, double newCapacity) {
        checkArgument(sellingTrader.getBasketSold().indexOf(resizedCommoditySpec) >= 0,
                      "resizedCommodity =  " + resizedCommoditySpec);
        checkArgument(oldCapacity >= 0, "oldCapacity = " + oldCapacity);
        checkArgument(newCapacity >= 0, "newCapacity = " + newCapacity);

        economy_ = economy;
        sellingTrader_ = sellingTrader;
        resizedCommoditySpec_ = resizedCommoditySpec;
        resizedCommodity_ = resizedCommodity;
        soldIndex_ = soldIndex;
        oldCapacity_ = oldCapacity;
        newCapacity_ = newCapacity;
    }

    // Methods

    /**
     * Returns the economy of {@code this} resize. i.e. the economy containing
     * resized commodity.
     */
    @Pure
    public @NonNull Economy getEconomy(@ReadOnly Resize this) {
        return economy_;
    }

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
    public @Nullable CommoditySpecification getResizedCommoditySpec(@ReadOnly Resize this) {
        return resizedCommoditySpec_;
    }

    /**
     * Returns the commodity that will be resized by {@code this}
     * action.
     */
    @Pure
    public @Nullable CommoditySold getResizedCommodity(@ReadOnly Resize this) {
        return resizedCommodity_;
    }

    /**
     * Returns the sold index of the commodity sold by the trader.specification of the
     * commodity that will be resized by {@code this}.
     */
    @Pure
    public int getSoldIndex(@ReadOnly Resize this) {
        return soldIndex_;
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
            .append("\" commoditySpecification=\"").append(getResizedCommoditySpec())
            .append("\" oldCapacity=\"").append(getOldCapacity())
            .append("\" newCapacity=\"").append(getNewCapacity()).append("\" />").toString();
    }

    @Override
    public @NonNull Resize take() {
        return take(true);
    }

    /**
     * Simulate effect of taking actions.
     *
     * @param basedOnHistorical Is this action based on historical quantity? The simulation
     * on dependent commodities changes based on the parameter.
     * @return {@code this}
     */
    public @NonNull Resize take(boolean basedOnHistorical) {
        super.take();
        Resizer.resizeDependentCommodities(getEconomy(), getSellingTrader(), getResizedCommodity(),
                                   getSoldIndex(), getNewCapacity(), basedOnHistorical);
        getSellingTrader().getCommoditySold(getResizedCommoditySpec()).setCapacity(getNewCapacity());
        return this;
    }

    @Override
    public @NonNull Resize rollback() {
        super.rollback();
        getSellingTrader().getCommoditySold(getResizedCommoditySpec()).setCapacity(getOldCapacity());
        return this;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Resize ").append(commodityType.apply(getResizedCommoditySpec().getType())).append(" of ");
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

    @Override
    @Pure
    public @NonNull @ReadOnly Object getCombineKey() {
        return Lists.newArrayList(Resize.class, getSellingTrader(), getResizedCommoditySpec());
    }

    @Override
    @Pure
    public @Nullable @ReadOnly Action combine(@NonNull Action action) {
        // Check that the argument is a Resize of the same target and same commodity spec,
        // otherwise we are not supposed to get here.
        // Also check that this is a consistent sequence of actions, i.e.
        // this.getNewCapacity() == action.getOldCapacity().
        Resize resize = (Resize) action;
        checkArgument(getSellingTrader().equals(resize.getSellingTrader()));
        checkArgument(getResizedCommoditySpec().equals(resize.getResizedCommoditySpec()));
        if (resize.getNewCapacity() == getOldCapacity()) { // the resizes cancel each other
            return null;
        } else {
            Resize newResize = new Resize(getEconomy(), getSellingTrader(),
                        getResizedCommoditySpec(), getResizedCommodity(), getSoldIndex(),
                        getOldCapacity(), resize.getNewCapacity());
            return newResize;
        }
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getSellingTrader();
    }

    /**
     * Tests whether two Resize actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly Resize this,@ReadOnly Object other) {
        if (other == null || !(other instanceof Resize)) {
            return false;
        }
        Resize otherResize = (Resize)other;
        return otherResize.getSellingTrader() == getSellingTrader()
                        && otherResize.getResizedCommoditySpec().equals(getResizedCommoditySpec())
                        && otherResize.getOldCapacity() == getOldCapacity()
                        && otherResize.getNewCapacity() == getNewCapacity();
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getSellingTrader().hashCode())
                        .putInt(getResizedCommoditySpec().hashCode()).putDouble(getOldCapacity())
                        .putDouble(getNewCapacity()).hash().asInt();
    }

    @Override
    public ActionType getType() {
        return ActionType.RESIZE;
    }
} // end Resize class
