package com.vmturbo.platform.analysis.actions;

import static com.vmturbo.platform.analysis.actions.Utility.appendTrader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.dataflow.qual.Pure;

import com.google.common.hash.Hashing;

import com.vmturbo.platform.analysis.actions.GuaranteedBuyerHelper.BuyerInfo;
import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderSettings;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ede.BootstrapSupply;
import com.vmturbo.platform.analysis.utilities.FunctionalOperatorUtil;

/**
 * An action to provision a new {@link Trader seller} using another {@link Trader seller} as the
 * template.
 */
public class ProvisionBySupply extends ProvisionBase implements Action {

    // Fields
    private static final Logger logger = LogManager.getLogger();
    private @NonNull Map<CommoditySpecification, CommoditySpecification> commSoldToReplaceMap_;
    private @NonNull CommoditySpecification reasonCommodity;
    // a list of actions triggered by taking provisionBySupply action
    private List<@NonNull Action> subsequentActions_ = new ArrayList<>();
    // TODO: may need to add a 'triggering buyer' for debugReason...

    // Constructors

    /**
     * Constructs a new ProvisionBySupply action with the specified attributes.
     *
     * @param economy The economy in which the seller will be provisioned.
     * @param modelSeller The seller that should be used as a template for the new seller.
     * @param commCausingProvision commodity that led to activation
     */
    public ProvisionBySupply(@NonNull Economy economy, @NonNull Trader modelSeller,
                    @Nullable CommoditySpecification commCausingProvision) {
        // provisionBySupply means create an exact copy of modelSeller, in case the modelSeller
        // is itself a clone, go all the way back to the original modelSeller to simplify action
        // handling by entities outside M2 that are not necessarily aware of cloned traders
        super(economy, economy.getCloneOfTrader(modelSeller));
        commSoldToReplaceMap_ = new HashMap<>();
        reasonCommodity = commCausingProvision;
    }

    /**
     * Constructs a new ProvisionBySupply action with the specified attributes.
     *
     * @param economy The economy in which the seller will be provisioned.
     * @param modelSeller The seller that should be used as a template for the new seller.
     * @param commToReplaceMap the mapping for commodity specification to new commodity specification
     */
    public ProvisionBySupply(@NonNull Economy economy, @NonNull Trader modelSeller,
                    Map<CommoditySpecification, CommoditySpecification> commToReplaceMap,
                    CommoditySpecification mostProfitableCommoditySpecification) {
        // provisionBySupply means create an exact copy of modelSeller, in case the modelSeller
        // is itself a clone, go all the way back to the original modelSeller to simplify action
        // handling by entities outside M2 that are not necessarily aware of cloned traders
        super(economy, economy.getCloneOfTrader(modelSeller));
        commSoldToReplaceMap_ = commToReplaceMap;
        reasonCommodity = mostProfitableCommoditySpecification;
    }
    // Methods

    /**
     * Returns the actions that was triggered after taking {@code this} action
     * @return a list of actions followed by {@code this}
     */
    @Pure
    public @NonNull List<Action> getSubsequentActions() {
       return subsequentActions_;
    }

    @Override
    public @NonNull String serialize(@NonNull Function<@NonNull Trader, @NonNull String> oid) {
        return new StringBuilder().append("<action type=\"provisionBySupply\" modelSeller=\"")
            .append(oid.apply(getModelSeller())).append("\" />").toString();
    }

    @Override
    public CommoditySpecification getReason() {
        return reasonCommodity;
     }

    @Override
    public @NonNull Action take() {
        super.take();
        // a list of shopping list sponsored by guaranteed buyers that consume only the model seller
        List<ShoppingList> slBetweenModelSellerAndGuaranteedBuyer = GuaranteedBuyerHelper
                        .findSlsBetweenSellerAndGuaranteedBuyer(getModelSeller());
        // a map of each guaranteed buyer to all shopping lists that it sponsors
        Map<Trader, Set<ShoppingList>> allSlsSponsoredByGuaranteedBuyer = GuaranteedBuyerHelper
                        .getAllSlsSponsoredByGuaranteedBuyer(getEconomy(),
                                                             slBetweenModelSellerAndGuaranteedBuyer);
        setProvisionedSeller(getEconomy()
                        .addTraderByModelSeller(getModelSeller(), TraderState.ACTIVE,
                                                Utility.transformBasket(commSoldToReplaceMap_,
                                                                        getModelSeller().getBasketSold()),
                                                getModelSeller().getCliques()));
        getProvisionedSeller().setCloneOf(getModelSeller());
        // Copy trader settings
        TraderSettings copySettings = getProvisionedSeller().getSettings();
        copySettings.setCloneable(getModelSeller().getSettings().isCloneable());
        copySettings.setSuspendable(getModelSeller().getSettings().isSuspendable());
        copySettings.setMinDesiredUtil(getModelSeller().getSettings().getMinDesiredUtil());
        copySettings.setMaxDesiredUtil(getModelSeller().getSettings().getMaxDesiredUtil());
        copySettings.setGuaranteedBuyer(getModelSeller().getSettings().isGuaranteedBuyer());
        copySettings.setProviderMustClone(getModelSeller().getSettings().isProviderMustClone());

        List<Trader> unPlacedClones = new ArrayList<>();
        // Add basket(s) bought
        for (@NonNull Entry<@NonNull ShoppingList, @NonNull Market> entry
                : getEconomy().getMarketsAsBuyer(getModelSeller()).entrySet()) {
            // populate commToReplaceMap with those of the commodities bought by the current
            // shopping list that requires substitution. Here the new commSpec will be used mainly
            // for the sl of the provisionedSeller and the basket sold of the provisionedSeller's
            // supplier
            @NonNull final ShoppingList shoppingList = entry.getKey();
            Map<CommoditySpecification, CommoditySpecification> commToReplaceMap =
                            Utility.createCommSpecWithNewKeys(shoppingList);
            // replace those of the commodities bought that require substitution
            Basket provisionedSellerSlBasket = Utility.transformBasket(commToReplaceMap,
                                                               shoppingList.getBasket());
            ShoppingList provisionedSellerSl = getEconomy()
                            .addBasketBought(getProvisionedSeller(), provisionedSellerSlBasket);
            // Copy quantities bought
            // TODO: if the model seller has a guaranteed buyer, the quantity bought by the
            // provisionedSeller may change since we will adjust the quantity of guaranteed buyer
            // shopping list, which in turns can change the quantity of provisionedSeller commSold
            // and then affect the quantity bought of raw material, which is the provisionedSellerSl
            // but we don't have a clear understanding on how to change it so we just copy the same
            // value for now
            for (int i = 0 ; i < entry.getValue().getBasket().size() ; ++i) {
                provisionedSellerSl.setQuantity(i, shoppingList.getQuantity(i));
                provisionedSellerSl.setPeakQuantity(i, shoppingList.getPeakQuantity(i));
            }
            Trader currentSupplier = shoppingList.getSupplier();
            // if the modelSeller needs to clone the supplier, then clone the supplier to
            // place the provisionedSeller on it
            if (currentSupplier != null && getModelSeller().getSettings().isProviderMustClone()) {
                ProvisionBySupply provisionedSupplier = new ProvisionBySupply(getEconomy(),
                                currentSupplier, commToReplaceMap,
                                getReason());
                subsequentActions_.add(provisionedSupplier.take());
                subsequentActions_.addAll(provisionedSupplier.getSubsequentActions());
                // move the sl of the provisionedSeller directly to the newly cloned mandatorySeller
                Trader clonedMandatorySupplier = provisionedSupplier.getProvisionedSeller();
                provisionedSellerSl.move(clonedMandatorySupplier);
                Move.updateQuantities(getEconomy(), provisionedSellerSl,
                                      provisionedSellerSl.getSupplier(),
                                      FunctionalOperatorUtil.ADD_COMM);
                unPlacedClones.add(clonedMandatorySupplier);
            } else if (!shoppingList.isMovable()) {
                // place the provisionedSeller on the same supplier as modelSeller
                provisionedSellerSl.move(shoppingList.getSupplier());
                Move.updateQuantities(getEconomy(), provisionedSellerSl,
                                      provisionedSellerSl.getSupplier(),
                                      FunctionalOperatorUtil.ADD_COMM);
            } else {
                // If the new clone does not need its provider to clone and it is movable,
                // add it to unPlacedClones set and run bootstrap to place it.
                unPlacedClones.add(getProvisionedSeller());
            }

            // Copy movable attribute, it has to be set since bootstrap checks it
            provisionedSellerSl.setMovable(shoppingList.isMovable());
            runBootstrapToPlaceClones(unPlacedClones);
        }

        // Generate Capacity Resize actions on resizeThroughSupplier traders whose Provider is
        // cloning.
        try {
            getModelSeller().getCustomers().stream()
                .map(ShoppingList::getBuyer)
            .filter(trader -> trader.getSettings().isResizeThroughSupplier())
            .forEach(trader -> {
                    getEconomy().getMarketsAsBuyer(trader).keySet().stream()
                        .filter(shoppingList -> shoppingList.getSupplier() == getModelSeller())
                        .forEach(sl -> {
                            // Generate the resize actions for matching commodities between
                            // the model seller and the resizeThroughtSupplier trader.
                            getSubsequentActions().addAll(Utility.resizeCommoditiesOfTrader(
                                                                                    getEconomy(),
                                                                                    getModelSeller(),
                                                                                    sl));
                });
            });
        } catch (Exception e) {
            logger.error("Error in ProvisionBySupply for resizeThroughSupplier Trader Capacity "
                            + "Resize when provisioning "
                                + getModelSeller().getDebugInfoNeverUseInCode(), e);
        }

        // Update commodities sold
        for (int i = 0 ; i < getModelSeller().getBasketSold().size() ; ++i) {
            // TODO: also copy overhead
            int indexOfProvisionedSellerCommSold;
            CommoditySpecification modelCommSpec = getModelSeller().getBasketSold().get(i);
            if (commSoldToReplaceMap_.containsKey(modelCommSpec)) {
                indexOfProvisionedSellerCommSold = getProvisionedSeller().getBasketSold()
                                .indexOf(commSoldToReplaceMap_.get(modelCommSpec));
            } else {
                indexOfProvisionedSellerCommSold = i;
            }
            CommoditySold provCommSold = getProvisionedSeller().getCommoditiesSold()
                            .get(indexOfProvisionedSellerCommSold);
            CommoditySold modelCommSold = getModelSeller().getCommoditiesSold().get(i);
            // Copy commodity sold attributes
            provCommSold.setCapacity(modelCommSold.getCapacity());
            provCommSold.setQuantity(modelCommSold.getQuantity());
            provCommSold.setThin(modelCommSold.isThin());

            // Copy commodity sold settings
            provCommSold.getSettings().setCapacityIncrement(
                modelCommSold.getSettings().getCapacityIncrement());
            provCommSold.getSettings().setCapacityLowerBound(
                modelCommSold.getSettings().getCapacityLowerBound());
            provCommSold.getSettings().setCapacityUpperBound(
                modelCommSold.getSettings().getCapacityUpperBound());
            provCommSold.getSettings().setUtilizationUpperBound(
                modelCommSold.getSettings().getUtilizationUpperBound());
            provCommSold.getSettings().setOrigUtilizationUpperBound(
                modelCommSold.getSettings().getOrigUtilizationUpperBound());
            provCommSold.getSettings().setResizable(
                modelCommSold.getSettings().isResizable());
            provCommSold.getSettings().setPriceFunction(
                modelCommSold.getSettings().getPriceFunction());
            provCommSold.getSettings().setUpdatingFunction(
               modelCommSold.getSettings().getUpdatingFunction());
        }

        // adjust the quantity of provisionedSeller, if it sells economy.getCommsToAdjustOverhead()
        // keep only the overhead in the commSold, otherwise, set the quantity and peak quantity to 0
        Utility.adjustOverhead(getModelSeller(), getProvisionedSeller(), getEconomy());
        // if the trader being cloned is a provider for a guaranteedBuyer, then the clone should
        // be a provider for that guaranteedBuyer as well
        if (slBetweenModelSellerAndGuaranteedBuyer.size() != 0) {
            List<BuyerInfo> guaranteedBuyerInfoList = GuaranteedBuyerHelper
                            .storeGuaranteedbuyerInfo(slBetweenModelSellerAndGuaranteedBuyer,
                                                      allSlsSponsoredByGuaranteedBuyer,
                                                      getProvisionedSeller());
            GuaranteedBuyerHelper.processGuaranteedbuyerInfo(getEconomy(), guaranteedBuyerInfoList);
        }

        getProvisionedSeller().setDebugInfoNeverUseInCode(
                getModelSeller().getDebugInfoNeverUseInCode()
                + " clone #"
                + getProvisionedSeller().getEconomyIndex()
        );
        return this;
    }

    /**
     * Run bootstrap logic to place the newly cloned traders, including provisionedSeller and
     * its supplier.
     *
     * @param unPlacedClones a list of cloned traders that are unplaced
     */
    private void runBootstrapToPlaceClones(List<Trader> unPlacedClones) {
        List<Action> actions = new ArrayList<>();
        for (Trader trader : unPlacedClones) {
            // for each trader, we create a map to hold the sl may require provisions
            Map<ShoppingList, Long> slsThatNeedProvBySupply = new HashMap<>();
            if (trader.getSettings().isShopTogether()) {
                actions.addAll(BootstrapSupply
                               .shopTogetherBootstrapForIndividualBuyer(getEconomy(), trader,
                                                                        slsThatNeedProvBySupply));
                actions.addAll(BootstrapSupply
                               .processCachedShoptogetherSls(getEconomy(), slsThatNeedProvBySupply));
            } else {
                Set<Entry<ShoppingList, Market>> slByMkt = getEconomy()
                                .getMarketsAsBuyer(trader).entrySet();
                slByMkt.stream().forEach(e ->  {
                    actions.addAll(BootstrapSupply
                                   .nonShopTogetherBootStrapForIndividualBuyer(getEconomy(),
                                                                               e.getKey(),
                                                                               e.getValue(),
                                                                               slsThatNeedProvBySupply));
                    actions.addAll(BootstrapSupply.processSlsThatNeedProvBySupply(getEconomy(),
                                                                   slsThatNeedProvBySupply));
                });
            }
        }
        subsequentActions_.addAll(actions);
    }

    @Override
    public @NonNull Action rollback() {
        super.rollback();
        GuaranteedBuyerHelper.removeShoppingListForGuaranteedBuyers(getEconomy(),
                getProvisionedSeller());
        getEconomy().removeTrader(getProvisionedSeller());
        getSubsequentActions().forEach(a -> {
            if (a instanceof ProvisionBase) {
                getEconomy().removeTrader(((ProvisionBase)a).getProvisionedSeller());
            } else if (a instanceof Resize && a.isExtractAction()) {
                a.rollback();
            }
        });
        commSoldToReplaceMap_.clear();
        subsequentActions_.clear();
        setProvisionedSeller(null);
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

    @Override
    public ActionType getType() {
        return ActionType.PROVISION_BY_SUPPLY;
    }

} // end ProvisionBySupply class
