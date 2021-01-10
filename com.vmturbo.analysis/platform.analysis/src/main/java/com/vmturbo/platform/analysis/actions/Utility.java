package com.vmturbo.platform.analysis.actions;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.BootstrapSupply;

/**
 * Comprises a number of static utility methods used by many {@link Action} implementations.
 */
public final class Utility {

    static final Logger logger = LogManager.getLogger(Utility.class);
    private static final Double MIN_CAPACITY_RTS = 0.0001;

    // Methods

    /**
     * Appends a human-readable string identifying a trader to a string builder in the form
     * "name [oid] (#index)".
     *
     * @param builder The {@link StringBuilder} to which the string should be appended.
     * @param trader The {@link Trader} for which to append the identifying string.
     * @param uuid A function from {@link Trader} to trader UUID.
     * @param name A function from {@link Trader} to human-readable trader name.
     */
    public static void appendTrader(@NonNull StringBuilder builder, @NonNull Trader trader,
                                    @NonNull Function<@NonNull Trader, @NonNull String> uuid,
                                    @NonNull Function<@NonNull Trader, @NonNull String> name) {
        builder.append(name.apply(trader)).append(" [").append(uuid.apply(trader)).append("] (#")
               .append(trader.getEconomyIndex()).append(")");
    }

    /**
     * For every commodity sold by a newly provisioned seller which may incur overhead, initialize
     * the used value to a computed overhead. Commodity overhead is computed through the model seller
     * by subtracting the quantities bought by all its buyers from the total quantity if is selling
     *
     * @param modelSeller this is the {@link Trader} that the new clone is based out off
     * @param provisionedSeller is the newly cloned {@link Trader}
     * @param economy that the Traders are a part of
     */
    public static void adjustOverhead(Trader modelSeller, Trader provisionedSeller,
                    Economy economy) {
        int soldIndex = 0;
        for (CommoditySpecification specSold : provisionedSeller.getBasketSold()) {
            double[] overheads = calculateCommodityOverhead(modelSeller, specSold, economy);
            CommoditySold cs = provisionedSeller.getCommoditiesSold().get(soldIndex);
            cs.setQuantity(overheads[0]).setPeakQuantity(overheads[1]);
            soldIndex++;
        }
    }

    /**
     * Get overhead for a commoditySpecification sold of a trader
     *
     * @param trader this is the {@link Trader} for whom the overhead is to be calculated
     * @param specSold the spec for which the overhead is to be calculated
     * @param economy  that the Trader is a part of
     *
     * @return avg and peak overheads.
     */
    public static double[] calculateCommodityOverhead(Trader trader, CommoditySpecification specSold,
                    Economy economy) {
        double overhead = 0;
        double overHeadPeak = 0;
        if (economy.getCommsToAdjustOverhead().contains(specSold)) {
            int soldIndex = trader.getBasketSold().indexOf(specSold);
            if (soldIndex != -1) {
                // Overhead is the seller's comm sold quantity - sum of quantities of all the
                // customers of this seller
                overhead = trader.getCommoditiesSold().get(soldIndex).getQuantity();
                overHeadPeak = trader.getCommoditiesSold().get(soldIndex).getPeakQuantity();
                for(ShoppingList sl : trader.getCustomers()) {
                    int boughtIndex = sl.getBasket().indexOf(specSold);
                    if (boughtIndex != -1) {
                        overhead -= sl.getQuantity(boughtIndex);
                        overHeadPeak -= sl.getPeakQuantity(boughtIndex);
                    }
                }
            }
        }
        return new double[] {Math.max(overhead, 0), Math.max(overHeadPeak, 0)};
    }

    /**
     * Creates a map of newly generated {@link CommoditySpecification}s that are sold
     * by the clone in place of the commodities sold by the modelSeller
     *
     * @param sl shopping list who may be cloned
     * @return a map where the key and values are a {@link CommoditySpecification}s with same
     *              "baseType" but different "commodityType"
     */
    public static Map<CommoditySpecification, CommoditySpecification>
                            createCommSpecWithNewKeys (ShoppingList sl) {
        Map<CommoditySpecification, CommoditySpecification> newCommSoldMap = new HashMap<>();
        // Note: we start assign type based from Integer.MAX_VALUE and keep decrementing from this
        Iterator<@NonNull @ReadOnly CommoditySpecification> iter = sl.getBasket().iterator();
        while(iter.hasNext()) {
            CommoditySpecification cs = iter.next();
            if (!newCommSoldMap.containsKey(cs) && cs.isCloneWithNewType()) {
                // change commType and add commSpecs into the basket
                newCommSoldMap.put(cs, new CommoditySpecification(cs.getBaseType(),
                                                                  cs.isCloneWithNewType()));
            }
        }
        return newCommSoldMap;
    }

    /**
     * A helper method to create a new {@link Basket} containing all the
     * {@link CommoditySpecification}s present in origBasketSold except the ones present in
     * commToReplaceMap that are to be replaced by the new ones with unique commodityType (that
     * represents new key)
     *
     * <p>
     *  There may be a new market being created in economy due to the new basket.
     * </p>
     *
     * @param commToReplaceMap is a map where the key is a {@link CommoditySpecification} with
     *  a "commodityType" that need to be replaced by the new {@link CommoditySpecification}
     *  represented by the corresponding value.
     * @param origBasket original basket bought or sold
     * @return a new {@link Basket} that contains the commodities that are replaced based on
     *  commToReplaceMap
     */
    public static Basket transformBasket(Map<CommoditySpecification, CommoditySpecification>
                                         commToReplaceMap, Basket origBasket) {
        // replacing the old commSpecs with the ones in the commToReplaceMap. eg application
        // commodity should be replaced with a new type when cloning the sl or seller contains an
        // application commodity
        Basket newBasket = new Basket(origBasket);
        for (Entry<CommoditySpecification, CommoditySpecification> entry : commToReplaceMap
                        .entrySet()) {
            newBasket = newBasket.remove(entry.getKey());
            newBasket = newBasket.add(entry.getValue());
        }
        return newBasket;
    }

    /**
     * Check if Trader is resizeThroughSupplier trader. If it is, then check that the trader
     * has a supplier of its own that provides the needed commodities for the buyer shopping list.
     * This is necessary in order to determine if this trader is a valid candidate for generating
     * additional supply to meet the shopping list's needs.
     *
     * @param trader The resizeThroughSupplier Trader.
     * @param buyerShoppingList Shopping List that may buy from seller.
     * @param economy The current economy.
     * @return Whether this trader is a valid candidate for generating supply.
     */
    public static boolean resizeThroughSupplier(Trader trader, ShoppingList buyerShoppingList,
                    Economy economy) {
        return trader.getSettings().isResizeThroughSupplier()
            && providerOfSellerSellsCommodityHavingInfiniteQuote(trader, buyerShoppingList, economy);
    }

    /**
     * Generate capacity resize for commodities sold by resizeThroughSupplier Trader that are
     * provided by Trader's own supplier.
     *
     * @param economy Economy of resizeThroughSupplier Trader
     * @param seller Seller providing to the resizeThroughSupplier Trader
     * @param buyerSL ShoppingList of the resizeThroughSupplier Trader
     * @param up boolean for resize up or down
     * @param relatedReasons the related reasons for the resize
     *
     * @return List of resize actions.
     */
    public static List<Resize> resizeCommoditiesOfTrader(Economy economy, Trader seller,
                    ShoppingList buyerSL, boolean up, Set<Integer> relatedReasons) {
        List<Resize> resizeList = new ArrayList<>();
        Trader resizeThroughSupplierTrader = buyerSL.getBuyer();
        buyerSL.getBasket().forEach(buyerCS -> {
            CommoditySold buyerCommoditySold = resizeThroughSupplierTrader.getCommoditySold(buyerCS);
            // Scale the capacity of the commodities of the resizeThroughSupplier trader with the
            // capacity of the commodity sold by the supplier of the trader as the added amount.
            if (buyerCommoditySold != null && buyerCommoditySold.getSettings().isResizable()) {
                CommoditySold sellerCommoditySold = seller.getCommoditySold(buyerCS);
                // Utilization Upper Bound can change in suspension adjustUtilThreshold.
                double origEffectiveCapacity = sellerCommoditySold.getSettings().getOrigUtilizationUpperBound()
                        * sellerCommoditySold.getCapacity();
                double newCapacity = buyerCommoditySold.getCapacity()
                    + (up ? origEffectiveCapacity
                            : -origEffectiveCapacity);
                // TODO Needs to be improved for scenarios that can lead to the negative capacity:
                // See OM-59512.
                if (!up && newCapacity <= 0) {
                    logger.warn("Deactivating supplier {} of trader {}, would lead to negative or 0 "
                        + "capacity for commodity {}. Capping at {}.", seller.getDebugInfoNeverUseInCode(),
                            resizeThroughSupplierTrader.getDebugInfoNeverUseInCode(),
                            buyerCS.getDebugInfoNeverUseInCode(), MIN_CAPACITY_RTS);
                    // Generate the Resize down but cap at a small capacity above 0 so that we can
                    // keep suspending unnecessary hosts if there is host reservation set.
                    // If there there are consumers that now won't be able to place because of this
                    // lower capacity, then the resize and host suspension will be rolled back.
                    Resize resizeAction = new Resize(economy, resizeThroughSupplierTrader, buyerCS,
                        buyerCommoditySold,
                        resizeThroughSupplierTrader.getBasketSold().indexOf(buyerCS),
                        MIN_CAPACITY_RTS);
                    resizeAction.take();
                    resizeAction.enableExtractAction();
                    resizeAction.getResizeTriggerTraders().put(seller, relatedReasons);
                    resizeList.add(resizeAction);
                } else {
                    Resize resizeAction = new Resize(economy, resizeThroughSupplierTrader, buyerCS,
                        buyerCommoditySold,
                        resizeThroughSupplierTrader.getBasketSold().indexOf(buyerCS),
                        newCapacity);
                    resizeAction.take();
                    resizeAction.enableExtractAction();
                    resizeAction.getResizeTriggerTraders().put(seller, relatedReasons);
                    resizeList.add(resizeAction);
                }
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("ResizeThroughSupplier Trader SL {} Commodity {} is not resizable",
                        buyerSL.getDebugInfoNeverUseInCode(), buyerCS.getDebugInfoNeverUseInCode());
                }
            }
        });
        return resizeList;
    }

    /**
     * Determine if the seller of a shopping list that gets an infinite quote has a provider of
     * one of its own shopping lists that is selling the same commodity that the buyerShoppingList
     * needs.
     * For example:
     * VM on Storage gets an infinite quote due to StorageAmount. Check that the Storage's Supplier
     * (potentially a host) is selling the it StorageAmount before returning true.
     *
     * @param seller Seller for shoppingList that gets an infinite quote.
     * @param buyerShoppingList Shopping List that may buy from seller.
     * @param economy The current economy.
     * @return Whether the potential seller for the shopping list can provide the needed
     * commodities through its supplier.
     */
    public static boolean providerOfSellerSellsCommodityHavingInfiniteQuote(Trader seller,
                    ShoppingList buyerShoppingList, Economy economy) {
        List<CommoditySpecification> commSpecs = BootstrapSupply.findCommSpecsWithInfiniteQuote(seller,
            buyerShoppingList, economy);
        Basket neededCommsBasket = new Basket(commSpecs);
        return economy.getMarketsAsBuyer(seller).keySet().stream()
            .anyMatch(sl ->
                sl.getSupplier() != null
                && sl.getSupplier().getSettings().isCloneable()
                && neededCommsBasket.isSatisfiedBy(sl.getSupplier().getBasketSold()))
            && sellerCommoditiesAreResizable(commSpecs, seller);
    }

    /**
     * Check that the selling trader's specified commodities are resizable.
     *
     * @param commSpecList List of commodity specifications to check.
     * @param seller Trader whose commodities are being checked.
     * @return Whether the selling trader's specified commodities are resizable.
     */
    public static boolean sellerCommoditiesAreResizable(List<CommoditySpecification> commSpecList,
                    Trader seller) {
        return commSpecList.stream()
                .noneMatch(cs -> !seller.getCommoditySold(cs).getSettings().isResizable());
    }

    /**
     * Provision enough supply to support the shopping list placing on the seller by considering the
     * commodities leading to infinite quotes, the shopping list consumption and the added capacity
     * per provision.
     *
     * @param economy Economy shopping list is in.
     * @param seller Seller directly selling to the shopping list.
     * @param csList CommoditySpecification list causing infinite quotes for the shopping list.
     * @param sl ShoppingList of trader consuming from seller.
     * @param allActions Actions list for adding new actions to it.
     * @return List of provisioned traders.
     */
    public static List<Trader> provisionSufficientSupplyForResize(Economy economy, Trader seller,
                    List<CommoditySpecification> csList, ShoppingList sl, List<Action> allActions) {
        if (logger.isDebugEnabled()) {
            logger.debug("---Provision Sufficient Supply---"
                + "\nSeller: {}" + "\nShoppingList: {}" + "\nShoppingListProvider: {}"
                    + "\nInfiniteCommSpecsList: {}",
                seller.getDebugInfoNeverUseInCode(), sl.getDebugInfoNeverUseInCode(),
                    sl.getSupplier() != null ? sl.getSupplier().getDebugInfoNeverUseInCode() : null,
                        csList.stream().map(CommoditySpecification::getDebugInfoNeverUseInCode)
                            .collect(Collectors.toList()).toString());
        }
        boolean sellerIsSupplier = sl.getSupplier() == seller;
        int mostNeededSellers = 0;
        CommoditySpecification commSpec = null;
        Trader provisionableProvider = null;
        List<Trader> provisionedSupply = new ArrayList<>();
        Basket infiniteCommsBasket = new Basket(csList);
        // Find provider of seller to increase capacities of commodities of seller trader that are
        // highly utilized and therefore giving the buyer shopping list an infinite quote.
        for (ShoppingList shoppingList : economy.getMarketsAsBuyer(seller).keySet()) {
            // shopping list suppliers can be null if for example the entity is in maintenenace/uknown/failover/not_monitored state.
            // This is because such entites are not sent to the market.
            if (shoppingList.getSupplier() == null || !shoppingList.getSupplier().getSettings().isCloneable()) {
                continue;
            }
            if (infiniteCommsBasket.isSatisfiedBy(shoppingList.getSupplier().getBasketSold())) {
                provisionableProvider = shoppingList.getSupplier();
                break;
            }
        }
        if (provisionableProvider == null) {
            return provisionedSupply;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("ProvisionableProvider: {}", provisionableProvider
                            .getDebugInfoNeverUseInCode());
        }
        // Go through the commodities that need to be resized and find the one that needs the most
        // provisions and the count of how many provisions are needed so we generate enough supply
        // for the shopping list.
        // Example:
        // sl: VM shopping list
        // seller: vSanStorage trader
        // provisionableProvider: Host trader
        // The formula is:
        // neededClones = Ceiling of (excess usage / effective capacity added per entity) =
        //                Ceiling of ((sellerUsage
        //                              + buyerUsage if not currently on seller
        //                              - effective capacity of seller)
        //                              / (provisionableProvider capacity
        //                              * util threshold of seller))
        for (CommoditySpecification cs : csList) {
            CommoditySold commSold = seller.getCommoditySold(cs);
            int neededClones = (int) Math.ceil(((commSold.getQuantity()
                                + (sellerIsSupplier ? 0 : sl.getQuantities()[sl.getBasket().indexOf(cs)])
                                    - commSold.getEffectiveCapacity())
                        / (provisionableProvider.getCommoditySold(cs).getEffectiveCapacity()
                            * commSold.getSettings().getUtilizationUpperBound())));
            if (logger.isDebugEnabled()) {
                logger.debug("CommoditySpecification: {}" + "\nNeededClones: {}"
                    + "\nCommSold Quantity: {}" + "\nSellerIsSupplier: {}"
                    + "\nSL Quantity Bought: {}" + "\nCommSold EffectiveCapacity: {}"
                    + "\nProvisionableProvider EffectiveCapacity: {}",
                    cs.getDebugInfoNeverUseInCode(), neededClones, commSold.getQuantity(),
                    sellerIsSupplier, sl.getQuantities()[sl.getBasket().indexOf(cs)],
                    commSold.getEffectiveCapacity(),
                    provisionableProvider.getCommoditySold(cs).getEffectiveCapacity());
            }
            if (neededClones > mostNeededSellers) {
                mostNeededSellers = neededClones;
                commSpec = cs;
            }
        }
        if (commSpec == null) {
            return provisionedSupply;
        }
        // Generate provision actions based on the calculated count and add them to the allActions
        // list. Each provision should generate capacity resizes for at least the commodities in csList
        // that are giving infinite quotes to the shopping list. The Resizes are added to the allActions
        // through being in the subsequent actions of the provision action.
        for (int x = 0; x < mostNeededSellers; x++) {
            ProvisionBySupply provisionAction = new ProvisionBySupply(economy, provisionableProvider, commSpec);
            provisionAction.take();
            provisionAction.setImportance(Double.POSITIVE_INFINITY);
            allActions.add(provisionAction);
            provisionedSupply.add(provisionAction.getProvisionedSeller());
            allActions.addAll(provisionAction.getSubsequentActions());
        }
        return provisionedSupply;
    }

    /**
     * Check whether the trader is provider of ResizeThroughSupplier Trader.
     *
     * @param provider The trader to check.
     * @return Whether the trader is provider of ResizeThroughSupplier Trader.
     */
    public static boolean isProviderOfResizeThroughSupplierTrader(Trader provider) {
        return provider.getCustomers().stream().map(ShoppingList::getBuyer)
            .anyMatch(trader -> trader.getSettings().isResizeThroughSupplier());
    }

    /**
     * Gets the resize through supplier traders from the would be provider.
     *
     * @param provider The trader to check.
     * @return Set of the resize through supplier traders consuming from the provider.
     */
    public static Set<Trader> getResizeThroughSupplierTradersFromProvider(Trader provider) {
        return provider.getCustomers().stream()
            .map(ShoppingList::getBuyer)
                .filter(trader -> trader.getSettings().isResizeThroughSupplier())
                    .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    /**
     * Check if the sl is of resize through supplier trader and not movable.
     *
     * @param sl The ShoppingList to check.
     * @return boolean of answer.
     */
    public static boolean isUnmovableRTSShoppingList(ShoppingList sl) {
        return sl.getBuyer().getSettings().isResizeThroughSupplier()
            && !sl.isMovable();
    }

    /**
     * Check if the trader that is suspending is the last provider of a resizeThroughSupplier
     * Trader which would then need to suspend as well.
     *
     * @param economy The economy.
     * @param isProviderOfResizeThroughSupplier Quick exit if trader is not provider of RTS.
     * @param resizeThroughSuppliers relevant RTS. Currently there should just be 1 RTS consumer of a Trader.
     * @param guaranteedBuyerSls GB SL consumers of Trader that is suspending which should include the RTS SL.
     * @param deactivate The deactivate action of the Trader providing to the RTS Trader.
     * @param suspendActions The Suspend related actions around deactivate action to this point.
     */
    public static void checkRTSToSuspend(Economy economy, boolean isProviderOfResizeThroughSupplier,
        Set<Trader> resizeThroughSuppliers, List<ShoppingList> guaranteedBuyerSls,
        Deactivate deactivate, List<Action> suspendActions) {
        if (isProviderOfResizeThroughSupplier && !resizeThroughSuppliers.isEmpty()) {
            // Ideally there should just be 1 resize through supplier consuming from a trader.
            for (Trader resizeThroughSupplierToCheck : resizeThroughSuppliers) {
                Optional<ShoppingList> shoppingList = guaranteedBuyerSls.stream()
                    .filter(sl -> sl.getBuyer() == resizeThroughSupplierToCheck).findFirst();
                if (shoppingList.isPresent()) {
                    if (economy.getMarketsAsBuyer(resizeThroughSupplierToCheck).keySet().stream()
                        .filter(sl -> {
                            Trader supplier = sl.getSupplier();
                            return supplier != null && supplier.getState().isActive()
                                && shoppingList.get().getBasket().equals(sl.getBasket());
                    }).count() < 1) {
                        if (resizeThroughSupplierToCheck.getCustomers().isEmpty()) {
                            Deactivate deactivateAction = new Deactivate(economy,
                                resizeThroughSupplierToCheck, resizeThroughSupplierToCheck.getBasketSold());
                            deactivateAction.setExecutable(false);
                            Action taken = deactivateAction.take();
                            deactivate.getSubsequentActions().add(taken);
                            suspendActions.add(taken);
                        }
                    }
                }
            }
        }
    }

    /**
     * Check if the Market contains a seller that has reconfigurable commodities.
     *
     * @param m The Market.
     * @param e The Economy.
     *
     * @return If the market contains reconfigurable seller or not.
     */
    public static boolean marketContainsReconfigurableSeller(Market m, Economy e) {
        return m.getActiveSellers().stream().anyMatch(trader -> trader.getReconfigureableCount(e) > 0)
            || m.getInactiveSellers().stream().anyMatch(trader -> trader.getReconfigureableCount(e) > 0);
    }
} // end Utility class
