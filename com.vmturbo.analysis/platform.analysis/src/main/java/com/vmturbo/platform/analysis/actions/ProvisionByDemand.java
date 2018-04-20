package com.vmturbo.platform.analysis.actions;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.hash.Hashing;

import com.vmturbo.platform.analysis.actions.GuaranteedBuyerHelper.BuyerInfo;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommodityResizeSpecification;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;

/**
 * An action to provision a new {@link Trader seller} using another {@link Trader buyer} as the
 * template.
 */
public class ProvisionByDemand extends ActionImpl {
    // Fields
    private final @NonNull Economy economy_;
    private final @NonNull ShoppingList modelBuyer_; // TODO: also add source market? Desired state?
    private final @NonNull Trader modelSeller_;
    private @Nullable Trader provisionedSeller_;
    private long oid_;
    // a map from commodity base type to its new capacity which will satisfy the demand
    private @NonNull Map<@NonNull Integer, @NonNull Double> commodityNewCapacityMap_ =
                    new HashMap<>();

    // Constructors

    /**
     * Constructs a new ProvisionByDemand action with the specified attributes.
     *
     * @param economy The economy in which the seller will be provisioned.
     * @param modelBuyer The shopping list that should be satisfied by the new seller.
     */
    public ProvisionByDemand(@NonNull Economy economy, @NonNull ShoppingList modelBuyer,
                    @NonNull Trader modelSeller) {
        economy_ = economy;
        modelBuyer_ = modelBuyer;
        // provisionByDemand means create an copy of modelSeller and increase capacity of certain
        // commodities, in case the modelSeller is itself a clone, go all the way back to the
        // original modelSeller to simplify action handling by entities outside M2 that are not
        // necessarily aware of cloned traders
        modelSeller_ = economy.getCloneOfTrader(modelSeller);
    }

    // Methods

    /**
     * Returns the economy in which the new seller will be added.
     */
    @Pure
    public @NonNull Economy getEconomy(@ReadOnly ProvisionByDemand this) {
        return economy_;
    }

    /**
     * Returns the model buyer that should be satisfied by the new seller.
     */
    @Pure
    public @NonNull ShoppingList getModelBuyer(@ReadOnly ProvisionByDemand this) {
        return modelBuyer_;
    }

    /**
     * Returns the seller that was added as a result of taking {@code this} action.
     *
     * <p>
     *  It will be {@code null} before the action is taken and/or after it is rolled back.
     * </p>
     */
    @Pure
    public @Nullable Trader getProvisionedSeller(@ReadOnly ProvisionByDemand this) {
        return provisionedSeller_;
    }

    /**
     * Returns the seller that is used as a model in taking {@code this} action.
     */
    @Pure
    public @Nullable Trader getModelSeller(@ReadOnly ProvisionByDemand this) {
        return modelSeller_;
    }

    /**
     * Returns the modifiable map from the commodity base type to its new capacity after generating
     * a provisionByDemand decision.
     */
    @Pure
    public @NonNull Map<@NonNull Integer, @NonNull Double> getCommodityNewCapacityMap() {
        return commodityNewCapacityMap_;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder().append("<action type=\"provisionByDemand\" modelBuyer=\"")
            .append(oid.apply(getModelBuyer().getBuyer())).append("\" />").toString();
        // TODO: should I send the shopping list and basket bought instead?
    }

    @Override
    public @NonNull Action take() {
        super.take();
        // a list of shopping list sponsored by guaranteed buyers that consume only the model seller
        List<ShoppingList> guaranteedBuyerSlsOnModelSeller = GuaranteedBuyerHelper
                        .findSlsBetweenSellerAndGuaranteedBuyer(getEconomy(), getModelSeller());
        // a map of each guaranteed buyer to all shopping lists that it sponsors
        Map<Trader, Set<ShoppingList>> allSlsSponsoredByGuaranteedBuyer = GuaranteedBuyerHelper
                        .getAllSlsSponsoredByGuaranteedBuyer(getEconomy(), guaranteedBuyerSlsOnModelSeller);
        Basket basketSold = getModelSeller().getBasketSold();
        provisionedSeller_ = getEconomy().addTraderByModelSeller(getModelSeller(), TraderState.ACTIVE,
                                        basketSold, getModelSeller().getCliques());
        provisionedSeller_.setCloneOf(modelSeller_);
        // traders cloned through provisionByDemand are marked non-cloneable by default
        provisionedSeller_.getSettings().setSuspendable(getModelSeller().getSettings().isSuspendable());
        provisionedSeller_.getSettings().setMinDesiredUtil(getModelSeller().getSettings().getMinDesiredUtil());
        provisionedSeller_.getSettings().setMaxDesiredUtil(getModelSeller().getSettings().getMaxDesiredUtil());
        provisionedSeller_.getSettings().setGuaranteedBuyer(getModelSeller().getSettings().isGuaranteedBuyer());
        provisionedSeller_.getSettings().setMandatorySupplier(getModelSeller().getSettings().isMandatorySupplier());

        // adding commodities to be bought by the provisionedSeller and resizing them
        // TODO: we don't have a case for provisionByDemand a trader with mandatory seller as supplier
        // maybe we need to handle it in future
        getEconomy().getMarketsAsBuyer(modelSeller_).keySet().forEach(shoppingList -> {
            // Note that because we are cloning, this does not require that we populate a new market
            // with sellers. There must be an existing market for sellers.
            ShoppingList sl = getEconomy().addBasketBought(provisionedSeller_, shoppingList
                .getBasket());
            // Copy movable attribute
            sl.setMovable(shoppingList.isMovable());
        });

        List<CommoditySold> commSoldList = provisionedSeller_.getCommoditiesSold();
        for (CommoditySpecification cs : basketSold) {
            // retrieve dependent commodities to be resized
            List<CommodityResizeSpecification> typeOfCommsBought = getEconomy().getResizeDependency
                                                                        (cs.getBaseType());
            if (typeOfCommsBought == null || typeOfCommsBought.isEmpty()) {
                continue;
            }

            getEconomy().getMarketsAsBuyer(provisionedSeller_).keySet().forEach(sl -> {
                for (CommodityResizeSpecification typeOfCommBought : typeOfCommsBought) {
                    int boughtIndex = sl.getBasket().indexOfBaseType(typeOfCommBought
                        .getCommodityType());
                    // if comm not present in shoppingList, indexOfBaseType < 0
                    if (boughtIndex < 0) {
                        continue;
                    }
                    // increase the amount of dependent commodity bought
                    sl.getQuantities()[boughtIndex] = Math.max(sl.getQuantities()
                                    [boughtIndex],commSoldList.get(basketSold.indexOf(cs))
                        .getQuantity());
                }
            });
        }

        TraderSettings sellerSettings = getProvisionedSeller().getSettings();
        double desiredUtil = (sellerSettings.getMaxDesiredUtil() + sellerSettings
                                    .getMinDesiredUtil()) / 2;
        // resize the commodities sold by the clone to fit the buyer
        for (CommoditySpecification commSpec : getModelBuyer().getBasket()) {
            int indexOfCommSold = basketSold.indexOf(commSpec.getType());
            int indexOfCommBought = getModelBuyer().getBasket().indexOf(commSpec);
            CommoditySold modelCommSold = modelSeller_.getCommoditiesSold().get(indexOfCommSold);
            double initialCapSold = modelCommSold.getCapacity();
            double overhead = Utility.calculateCommodityOverhead(getModelSeller(), commSpec,
                            getEconomy());
            // the new capacity should be equal to the old capacity or scaled capacity which
            // considers overhead that is a factor of the bought commodity
            double newCapacity = Math.max((getModelBuyer().getPeakQuantity(indexOfCommBought) + overhead) /
                            (desiredUtil * modelCommSold.getSettings().getUtilizationUpperBound()),
                            initialCapSold);
            if (newCapacity > initialCapSold) {
                // commodityNewCapacityMap_  keeps information about commodity sold and its
                // new capacity, if there are several commodities of same base type, pick the
                // biggest capacity.
                int baseType = basketSold.get(indexOfCommSold).getBaseType();
                commodityNewCapacityMap_.put(baseType,commodityNewCapacityMap_
                                .containsKey(baseType) ? Math.max(commodityNewCapacityMap_
                                                .get(baseType), newCapacity) : newCapacity);
            }
            CommoditySold provCommSold = provisionedSeller_.getCommoditiesSold().get(indexOfCommSold);
            provCommSold.setCapacity(newCapacity);
            provCommSold.setQuantity(modelCommSold.getQuantity());
            provCommSold.getSettings().setUtilizationUpperBound(modelCommSold.getSettings()
                                                                .getUtilizationUpperBound());
            provCommSold.getSettings().setOrigUtilizationUpperBound(modelCommSold.getSettings()
                                                                    .getUtilizationUpperBound());
            provCommSold.getSettings().setPriceFunction(modelCommSold.getSettings()
                                                                    .getPriceFunction());
            provCommSold.getSettings().setUpdatingFunction(modelCommSold.getSettings()
                                                                    .getUpdatingFunction());
        }
        // Set the capacities of all commodities sold by the provisioned seller
        // ,which are not in the model buyer's basket, to be the same as those of model seller
        // because these don't need a resize
        for (CommoditySpecification commSpec : getModelSeller().getBasketSold()) {
            if (!getModelBuyer().getBasket().contains(commSpec)) {
                int indexOfCommSold = basketSold.indexOf(commSpec.getType());
                CommoditySold modelCommSold = modelSeller_.getCommoditiesSold().get(indexOfCommSold);
                CommoditySold provCommSold = provisionedSeller_.getCommoditiesSold().get(indexOfCommSold);
                provCommSold.setCapacity(modelCommSold.getCapacity());
            }
        }
        Utility.adjustOverhead(getModelSeller(), getProvisionedSeller(), getEconomy());
        // if the trader being cloned is a provider for a gauranteedBuyer, then the clone should
        // be a provider for that guranteedBuyer as well
        if (guaranteedBuyerSlsOnModelSeller.size() != 0) {
            List<BuyerInfo> guaranteedBuyerInfoList = GuaranteedBuyerHelper
                            .storeGuaranteedbuyerInfo(guaranteedBuyerSlsOnModelSeller, allSlsSponsoredByGuaranteedBuyer,
                                                      provisionedSeller_);
            GuaranteedBuyerHelper.processGuaranteedbuyerInfo(getEconomy(), guaranteedBuyerInfoList);
        }
        getProvisionedSeller().setDebugInfoNeverUseInCode(
            getModelSeller().getDebugInfoNeverUseInCode()
                + " clone #"
                + getProvisionedSeller().getEconomyIndex()
        );
        // traders provisioned by demand should NOT be cloneable. They exist to handle cases where
        // none of the sellers has enough capacity to satisfy a particular demand
        provisionedSeller_.getSettings().setCloneable(false);
        return this;
    }

    @Override
    public @NonNull Action rollback() {
        super.rollback();
        GuaranteedBuyerHelper.removeShoppingListForGuaranteedBuyers(getEconomy(), provisionedSeller_);
        getEconomy().removeTrader(provisionedSeller_);
        provisionedSeller_ = null;
        commodityNewCapacityMap_.clear();
        return this;
    }

    @Override
    public @NonNull String debugDescription(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                            @NonNull Function<@NonNull Trader, @NonNull String> name,
                                            @NonNull IntFunction<@NonNull String> commodityType,
                                            @NonNull IntFunction<@NonNull String> traderType) {
        return new StringBuilder()
            .append("Provision a new ").append(traderType.apply(getModelBuyer().getBuyer().getType()/* Substitute correct type */))
            .append(" with the following characteristics: ").toString(); // TODO: print characteristics
    }

    @Override
    public @NonNull String debugReason(@NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                       @NonNull Function<@NonNull Trader, @NonNull String> name,
                                       @NonNull IntFunction<@NonNull String> commodityType,
                                       @NonNull IntFunction<@NonNull String> traderType) {
        final @NonNull StringBuilder sb = new StringBuilder();
        sb.append("No ").append(traderType.apply(getModelBuyer().getBuyer().getType()/* Substitute correct type */))
          .append(" has enough capacity for ");
        appendTrader(sb, getModelBuyer().getBuyer(), uuid, name);
        sb.append(".");
        // TODO: update when we create the recommendation matrix for provisioning and possibly
        // create additional classes for provisioning actions.

        return sb.toString();
    }

    @Override
    public @NonNull Trader getActionTarget() {
        return getProvisionedSeller();
    }

    /**
     * Tests whether two ProvisionByDemand actions are equal field by field.
     */
    @Override
    @Pure
    public boolean equals(@ReadOnly ProvisionByDemand this,@ReadOnly Object other) {
        if (other == null || !(other instanceof ProvisionByDemand)) {
            return false;
        }
        ProvisionByDemand otherProvisionByDemand = (ProvisionByDemand)other;
        return otherProvisionByDemand.getEconomy() == getEconomy()
                        && otherProvisionByDemand.getModelBuyer() == getModelBuyer()
                        // maybe modelSeller can be different?
                        && otherProvisionByDemand.getModelSeller() == getModelSeller()
                        // if the provisioned seller is null, we should expect
                        // getProvisionedSeller() is null so that null==null returns true
                        && (otherProvisionByDemand
                                        .getProvisionedSeller() == getProvisionedSeller());
    }

    /**
     * Use the hashCode of each field to generate a hash code, consistent with
     * {@link #equals(Object)}.
     */
    @Override
    @Pure
    public int hashCode() {
        return Hashing.md5().newHasher().putInt(getEconomy().hashCode())
                        .putInt(getModelBuyer().hashCode()).putInt(getModelSeller().hashCode())
                        .putInt(getProvisionedSeller() == null ? 0
                                        : getProvisionedSeller().hashCode())
                        .hash().asInt();
    }

    /**
     * Save the oid of the provisioned trader for use in replaying action
     *
     * @param oid The oid of the provisioned trader
     */
    public void setOid(@NonNull Long oid) {
        oid_ = oid;
    }

    /**
     * Return the saved oid for the provisioned trader
     *
     * @return oid of the provisioned trader
     */
    public Long getOid() {
        return oid_;
    }

    @Override
    public ActionType getType() {
        return ActionType.PROVISION_BY_DEMAND;
    }
} // end ProvisionByDemand class