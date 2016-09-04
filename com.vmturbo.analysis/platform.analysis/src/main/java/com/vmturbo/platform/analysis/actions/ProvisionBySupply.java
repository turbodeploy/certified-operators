package com.vmturbo.platform.analysis.actions;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.economy.ShoppingList;

import com.google.common.hash.Hashing;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

/**
 * An action to provision a new {@link Trader seller} using another {@link Trader seller} as the
 * template.
 */
public class ProvisionBySupply extends ActionImpl {
    // Fields
    private final @NonNull Economy economy_;
    private final @NonNull Trader modelSeller_;
    private @Nullable Trader provisionedSeller_;
    // TODO: may need to add a 'triggering buyer' for debugReason...

    // Constructors

    /**
     * Constructs a new ProvisionBySupply action with the specified attributes.
     *
     * @param economy The economy in which the seller will be provisioned.
     * @param modelSeller The seller that should be used as a template for the new seller.
     */
    public ProvisionBySupply(@NonNull Economy economy, @NonNull Trader modelSeller) {
        economy_ = economy;
        modelSeller_ = modelSeller;
    }
    // Methods

    /**
     * Returns the economy in which the new seller will be added.
     */
    @Pure
    public @NonNull Economy getEconomy(@ReadOnly ProvisionBySupply this) {
        return economy_;
    }

    /**
     * Returns the model buyer that should be satisfied by the new seller.
     */
    @Pure
    public @NonNull Trader getModelSeller(@ReadOnly ProvisionBySupply this) {
        return modelSeller_;
    }

    /**
     * Returns the seller that was added as a result of taking {@code this} action.
     *
     * <p>
     *  It will be {@code null} before the action is taken and/or after it is rolled back.
     * </p>
     */
    @Pure
    public @Nullable Trader getProvisionedSeller(@ReadOnly ProvisionBySupply this) {
        return provisionedSeller_;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder().append("<action type=\"provisionBySupply\" modelSeller=\"")
            .append(oid.apply(getModelSeller())).append("\" />").toString();
    }

    @Override
    public @NonNull Action take() {
        super.take();
        List<ShoppingList> shoppingLists = GuaranteedBuyerHelper.findShoppingListForGuaranteedBuyer(getEconomy(),
                                                getModelSeller());
        // if there is a guaranteed buyer, find the commodities it buys, and for those commodities
        // create a new CommSpec with a new commodityType. This map returned is used to update the
        // commodities that the clone sells and what the guaranttedBuyer buys
        Map<CommoditySpecification, CommoditySpecification> commToReplaceMap = shoppingLists.size() != 0 ?
                        GuaranteedBuyerHelper.createCommSpecWithNewKeys(shoppingLists.get(0)) : null;
        // use the commToReplaceMap to transform the basket that the clone sells. eg, make the clone
        // allocation commodities with new keys
        provisionedSeller_ = getEconomy().addTrader(getModelSeller().getType(), TraderState.ACTIVE,
                                shoppingLists.size() != 0 ? GuaranteedBuyerHelper.transformBasket(commToReplaceMap
                                        , getModelSeller().getBasketSold()) : getModelSeller().getBasketSold());

        // Copy trader settings
        provisionedSeller_.getSettings().setCloneable(getModelSeller().getSettings().isCloneable());
        provisionedSeller_.getSettings().setSuspendable(getModelSeller().getSettings().isSuspendable());
        provisionedSeller_.getSettings().setMinDesiredUtil(getModelSeller().getSettings().getMinDesiredUtil());
        provisionedSeller_.getSettings().setMaxDesiredUtil(getModelSeller().getSettings().getMaxDesiredUtil());
        provisionedSeller_.getSettings().setGuaranteedBuyer(getModelSeller().getSettings().isGuaranteedBuyer());

        // Add basket(s) bought
        for (@NonNull Entry<@NonNull ShoppingList, @NonNull Market> entry
                : getEconomy().getMarketsAsBuyer(getModelSeller()).entrySet()) {
            ShoppingList shoppingList = getEconomy().addBasketBought(getProvisionedSeller(),
                                                                entry.getValue().getBasket());
            if (!entry.getKey().isMovable()) {
                shoppingList.move(entry.getKey().getSupplier());
                Move.updateQuantities(getEconomy(), shoppingList, shoppingList.getSupplier(),
                                (sold, bought) -> sold + bought);
            }

            // Copy movable attribute
            shoppingList.setMovable(entry.getKey().isMovable());

            // Copy quantities bought
            for (int i = 0 ; i < entry.getValue().getBasket().size() ; ++i) {
                shoppingList.setQuantity(i, entry.getKey().getQuantity(i));
                shoppingList.setPeakQuantity(i, entry.getKey().getPeakQuantity(i));
            }
        }

        // Update commodities sold
        for (int i = 0 ; i < getModelSeller().getBasketSold().size() ; ++i) {
            // TODO: also copy overhead

            // Copy commodity sold attributes
            getProvisionedSeller().getCommoditiesSold().get(i).setCapacity(
                getModelSeller().getCommoditiesSold().get(i).getCapacity());
            getProvisionedSeller().getCommoditiesSold().get(i).setThin(
                getModelSeller().getCommoditiesSold().get(i).isThin());

            // Copy commodity sold settings
            getProvisionedSeller().getCommoditiesSold().get(i).getSettings().setCapacityIncrement(
                getModelSeller().getCommoditiesSold().get(i).getSettings().getCapacityIncrement());
            getProvisionedSeller().getCommoditiesSold().get(i).getSettings().setCapacityLowerBound(
                getModelSeller().getCommoditiesSold().get(i).getSettings().getCapacityLowerBound());
            getProvisionedSeller().getCommoditiesSold().get(i).getSettings().setCapacityUpperBound(
                getModelSeller().getCommoditiesSold().get(i).getSettings().getCapacityUpperBound());
            getProvisionedSeller().getCommoditiesSold().get(i).getSettings().setUtilizationUpperBound(
                getModelSeller().getCommoditiesSold().get(i).getSettings().getUtilizationUpperBound());
            getProvisionedSeller().getCommoditiesSold().get(i).getSettings().setResizable(
                getModelSeller().getCommoditiesSold().get(i).getSettings().isResizable());
            getProvisionedSeller().getCommoditiesSold().get(i).getSettings().setPriceFunction(
                getModelSeller().getCommoditiesSold().get(i).getSettings().getPriceFunction());
        }

        // if the trader being cloned is a provider for a gauranteedBuyer, then the clone should
        // be a provider for that guranteedBuyer as well
        if (commToReplaceMap != null) {
            GuaranteedBuyerHelper.storeGuaranteedbuyerInfo(shoppingLists, provisionedSeller_,
                            new Basket(commToReplaceMap.values()));
        }
        getProvisionedSeller().setDebugInfoNeverUseInCode(
                getModelSeller().getDebugInfoNeverUseInCode()
                + " clone #"
                + getProvisionedSeller().getEconomyIndex()
        );
        return this;
    }

    @Override
    public @NonNull Action rollback() {
        super.rollback();
        getEconomy().removeTrader(provisionedSeller_);
        provisionedSeller_ = null;

        return this;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();

        sb.append("Provision a new ").append(traderType.apply(getModelSeller().getType())).append(" similar to ");
        appendTrader(sb, getModelSeller(), uuid, name);
        sb.append(".");

        return sb.toString();
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        return new StringBuilder()
            .append("No ").append(traderType.apply(getModelSeller().getType()))
            .append(" has enough leftover capacity for [buyer].").toString();
        // TODO: update when we create the recommendation matrix for provisioning and possibly
        // create additional classes for provisioning actions.
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getProvisionedSeller();
    }

    /**
     * Tests whether two ProvisionBySupply actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly ProvisionBySupply this,@ReadOnly Object other) {
        if (other == null || !(other instanceof ProvisionBySupply)) {
            return false;
        }
        ProvisionBySupply otherProvisionBySupply = (ProvisionBySupply)other;
        return otherProvisionBySupply.getEconomy() == getEconomy()
                        && otherProvisionBySupply.getModelSeller() == getModelSeller()
                        // if the provisioned seller is null, we should expect
                        // getProvisionedSeller() is null so that null==null returns true
                        && otherProvisionBySupply
                                        .getProvisionedSeller() == getProvisionedSeller();
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with
     * {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getModelSeller().hashCode())
                        .putInt(getProvisionedSeller() == null ? 0
                                        : getProvisionedSeller().hashCode())
                        .hash().asInt();
    }
} // end ProvisionBySupply class
