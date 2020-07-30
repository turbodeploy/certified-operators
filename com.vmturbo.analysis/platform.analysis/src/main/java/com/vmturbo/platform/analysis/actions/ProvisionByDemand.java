package com.vmturbo.platform.analysis.actions;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntFunction;

import com.google.common.hash.Hashing;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

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
public class ProvisionByDemand extends ProvisionBase implements Action {

    // Fields
    private static final Logger logger = LogManager.getLogger();
    private final @NonNull ShoppingList modelBuyer_; // TODO: also add source market? Desired state?
    // a map from commodity base type to its new capacity which will satisfy the demand
    private @NonNull Map<@NonNull Integer, @NonNull Double> commodityNewCapacityMap_ =
                    new HashMap<>();
    // Constructors

    /**
     * Constructs a new ProvisionByDemand action with the specified attributes.
     *
     * <p>provisionByDemand means create an copy of modelSeller and increase capacity of certain
     * commodities, in case the modelSeller is itself a clone, go all the way back to the
     * original modelSeller to simplify action handling by entities outside M2 that are not
     * necessarily aware of cloned traders
     *
     * <p>We also need to find the corresponding shopping list of the original buyer.
     * The reason is that sometimes the model buyer is a cloned entity and it can be suspended later.
     * If so, the model buyer won't appear in the shoppingListOid map of AnalysisToProtobuf#actionTO,
     * which will lead to a NPE. See OM-60571.
     *
     * @param economy The economy in which the seller will be provisioned.
     * @param modelBuyer The shopping list that should be satisfied by the new seller.
     */
    public ProvisionByDemand(@NonNull Economy economy, @NonNull ShoppingList modelBuyer,
                    @NonNull Trader modelSeller) {
        super(economy, economy.getCloneOfTrader(modelSeller));
        // Find the shopping list of the original entity.
        // It's not possible that the shopping list of the original entity doesn't exist.
        // For safety, use the given modelBuyer if it cannot be found.
        modelBuyer_ = getOriginalShoppingList(economy, modelBuyer);
        logger.debug("Use {} as modelBuyer instead of {}.",
            () -> modelBuyer_.getDebugInfoNeverUseInCode() == null ? ""
                : modelBuyer_.getDebugInfoNeverUseInCode(),
            () -> modelBuyer.getDebugInfoNeverUseInCode() == null ? ""
                : modelBuyer.getDebugInfoNeverUseInCode());
    }

    // Methods

    /**
     * Find the shopping list of the original entity.
     * We are able to do it because getMarketsAsBuyer is a linked map so order is well defined.
     *
     * @param economy The economy in which the seller will be provisioned.
     * @param modelBuyer The shopping list that should be satisfied by the new seller.
     * @return the shopping list of the original entity
     * @throws NoSuchElementException if no shopping list exists
     */
    private ShoppingList getOriginalShoppingList(@NonNull Economy economy, @NonNull ShoppingList modelBuyer) {
        final Set<ShoppingList> cloned =
            economy.getMarketsAsBuyer(modelBuyer.getBuyer()).keySet();
        final Iterator<ShoppingList> originIterator =
            economy.getMarketsAsBuyer(economy.getCloneOfTrader(modelBuyer.getBuyer())).keySet().iterator();
        for (ShoppingList shoppingList : cloned) {
            if (shoppingList == modelBuyer) {
                return originIterator.next();
            }
            originIterator.next();
        }
        throw new NoSuchElementException("Cannot find the original shopping list of "
            + (modelBuyer.getDebugInfoNeverUseInCode() == null ? ""
            : modelBuyer.getDebugInfoNeverUseInCode()));
    }

    /**
     * Returns the model buyer that should be satisfied by the new seller.
     */
    @Pure
    public @NonNull ShoppingList getModelBuyer(@ReadOnly ProvisionByDemand this) {
        return modelBuyer_;
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
                        .findSlsBetweenSellerAndGuaranteedBuyer(getModelSeller());
        // a map of each guaranteed buyer to all shopping lists that it sponsors
        Map<Trader, Set<ShoppingList>> allSlsSponsoredByGuaranteedBuyer = GuaranteedBuyerHelper
                        .getAllSlsSponsoredByGuaranteedBuyer(getEconomy(), guaranteedBuyerSlsOnModelSeller);
        Basket basketSold = getModelSeller().getBasketSold();
        setProvisionedSeller(getEconomy().addTraderByModelSeller(getModelSeller(), TraderState.ACTIVE,
                                        basketSold, getModelSeller().getCliques()));
        getProvisionedSeller().setCloneOf(getModelSeller());
        // traders cloned through provisionByDemand are marked non-cloneable by default
        TraderSettings copySettings = getProvisionedSeller().getSettings();
        copySettings.setSuspendable(true);
        copySettings.setMinDesiredUtil(getModelSeller().getSettings().getMinDesiredUtil());
        copySettings.setMaxDesiredUtil(getModelSeller().getSettings().getMaxDesiredUtil());
        copySettings.setGuaranteedBuyer(getModelSeller().getSettings().isGuaranteedBuyer());
        copySettings.setProviderMustClone(getModelSeller().getSettings().isProviderMustClone());

        // adding commodities to be bought by the provisionedSeller and resizing them
        // TODO: we don't have a case for provisionByDemand a trader with mandatory seller as supplier
        // maybe we need to handle it in future
        getEconomy().getMarketsAsBuyer(getModelSeller()).keySet().forEach(shoppingList -> {
            // Note that because we are cloning, this does not require that we populate a new market
            // with sellers. There must be an existing market for sellers.
            ShoppingList sl = getEconomy().addBasketBought(getProvisionedSeller(), shoppingList
                .getBasket());
            // Copy movable attribute
            sl.setMovable(shoppingList.isMovable());
        });

        List<CommoditySold> commSoldList = getProvisionedSeller().getCommoditiesSold();
        for (CommoditySpecification cs : basketSold) {
            // retrieve dependent commodities to be resized
            List<CommodityResizeSpecification> typeOfCommsBought = getEconomy().getResizeDependency
                                                                        (cs.getBaseType());
            if (typeOfCommsBought == null || typeOfCommsBought.isEmpty()) {
                continue;
            }

            getEconomy().getMarketsAsBuyer(getProvisionedSeller()).keySet().forEach(sl -> {
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
            CommoditySold modelCommSold = getModelSeller().getCommoditiesSold().get(indexOfCommSold);
            double initialCapSold = modelCommSold.getCapacity();
            double[] overheads = Utility.calculateCommodityOverhead(getModelSeller(), commSpec,
                            getEconomy());
            // the new capacity should be equal to the old capacity or scaled capacity which
            // considers overhead that is a factor of the bought commodity
            double newCapacity = Math.max(Math.max(
                            getModelBuyer().getPeakQuantity(indexOfCommBought) + overheads[1],
                                getModelBuyer().getQuantity(indexOfCommBought) + overheads[0])
                            / (desiredUtil * modelCommSold.getSettings().getUtilizationUpperBound()),
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
            CommoditySold provCommSold = getProvisionedSeller().getCommoditiesSold().get(indexOfCommSold);
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
                CommoditySold modelCommSold = getModelSeller().getCommoditiesSold().get(indexOfCommSold);
                CommoditySold provCommSold = getProvisionedSeller().getCommoditiesSold().get(indexOfCommSold);
                provCommSold.setCapacity(modelCommSold.getCapacity());
            }
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
                            // the model seller and the resizeThroughSupplier trader.
                            getSubsequentActions().addAll(Utility.resizeCommoditiesOfTrader(
                                getEconomy(), getModelSeller(), sl, true,
                                    commodityNewCapacityMap_.keySet()));
                });
            });
        } catch (Exception e) {
            logger.error("Error in ProvisionByDemand for resizeThroughSupplier Trader Capacity "
                            + "Resize when provisioning "
                                + getModelSeller().getDebugInfoNeverUseInCode(), e);
        }

        Utility.adjustOverhead(getModelSeller(), getProvisionedSeller(), getEconomy());
        // if the trader being cloned is a provider for a guaranteedBuyer, then the clone should
        // be a provider for that guaranteedBuyer as well
        if (guaranteedBuyerSlsOnModelSeller.size() != 0) {
            List<BuyerInfo> guaranteedBuyerInfoList = GuaranteedBuyerHelper
                            .storeGuaranteedbuyerInfo(guaranteedBuyerSlsOnModelSeller, allSlsSponsoredByGuaranteedBuyer,
                                                      getProvisionedSeller());
            GuaranteedBuyerHelper.processGuaranteedbuyerInfo(getEconomy(), guaranteedBuyerInfoList);
        }
        getProvisionedSeller().setDebugInfoNeverUseInCode(
            getModelSeller().getDebugInfoNeverUseInCode()
                + " clone #"
                + getProvisionedSeller().getEconomyIndex()
        );
        // traders provisioned by demand should NOT be cloneable. They exist to handle cases where
        // none of the sellers has enough capacity to satisfy a particular demand
        getProvisionedSeller().getSettings().setCloneable(false);
        return this;
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
        setProvisionedSeller(null);
        commodityNewCapacityMap_.clear();
        return this;
    }

    @Override
    public @NonNull ProvisionByDemand port(@NonNull final Economy destinationEconomy,
            @NonNull final Function<@NonNull Trader, @NonNull Trader> destinationTrader,
            @NonNull final Function<@NonNull ShoppingList, @NonNull ShoppingList>
                                                                        destinationShoppingList) {
        return new ProvisionByDemand(destinationEconomy,
            destinationShoppingList.apply(getModelBuyer()),
            destinationTrader.apply(getModelSeller()));
    }

    /**
     * Returns whether {@code this} action respects constraints and can be taken.
     *
     * <p>Currently a provision-by-demand is considered valid iff the model seller is cloneable.</p>
     */
    @Override
    public boolean isValid() {
        return getModelSeller().getSettings().isCloneable();
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
        Utility.appendTrader(sb, getModelBuyer().getBuyer(), uuid, name);
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
        if (!(other instanceof ProvisionByDemand)) {
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

    @Override
    public ActionType getType() {
        return ActionType.PROVISION_BY_DEMAND;
    }
} // end ProvisionByDemand class
