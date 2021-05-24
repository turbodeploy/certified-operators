package com.vmturbo.platform.analysis.utilities;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Context;
import com.vmturbo.platform.analysis.economy.Context.BalanceAccount;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.QuoteMinimizer;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.ComputeTierCostDTO.ComputeResourceDependency;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CostTuple.DependentCostTuple.DependentResourceOption;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseServerTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.DatabaseTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.StorageTierCostDTO.StorageResourceCost;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs;
import com.vmturbo.platform.analysis.translators.ProtobufToAnalysis;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.CapacityLimitation;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.RangeBasedResourceDependency;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.RatioBasedResourceDependency;
import com.vmturbo.platform.analysis.utilities.CostFunctionFactoryHelper.SelectedStorageAndCost;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityCloudQuote;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityContext;
import com.vmturbo.platform.analysis.utilities.Quote.CommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.CostUnavailableQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InfiniteDependentComputeCommodityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InsufficientCapacityQuote;
import com.vmturbo.platform.analysis.utilities.Quote.InsufficientCommodity;
import com.vmturbo.platform.analysis.utilities.Quote.LicenseUnavailableQuote;
import com.vmturbo.platform.analysis.utilities.Quote.MutableQuote;

/**
 * The factory class to construct cost function.
 */
public class CostFunctionFactory {

    private static final Logger logger = LogManager.getLogger();

    private static final double riCostDeprecationFactor = 0.00001;

    /**
     * A utility method to extract base and dependent commodities.
     *
     * @param dependencyDTOs the DTO represents the base and dependent commodity relation
     * @return a mapping for commodities with dependency relation
     */
    public static Map<CommoditySpecification, CommoditySpecification>
                    translateComputeResourceDependency(List<ComputeResourceDependency> dependencyDTOs) {
        Map<CommoditySpecification, CommoditySpecification> dependencyMap = new HashMap<>();
        dependencyDTOs
                .forEach(dto -> dependencyMap
                        .put(ProtobufToAnalysis.commoditySpecification(dto.getBaseResourceType()),
                             ProtobufToAnalysis.commoditySpecification(dto.getDependentResourceType())));
        return dependencyMap;
    }

    /**
     * Scan commodities in the shopping list to find any that exceed capacities in the seller.
     *
     * @param sl the shopping list
     * @param seller the templateProvider that supplies the resources
     * @return If all commodities in the shopping list fit within the seller, returns a number less than zero.
     *         If some commodity does not fit, returns the boughtIndex of the first commodity in that shopping list's
     *         basket that cannot fit within the seller.
     */
    public static MutableQuote insufficientCommodityWithinSellerCapacityQuote(ShoppingList sl, Trader seller, int couponCommodityBaseType) {
        // check if the commodities bought comply with capacity limitation on seller
        if (!sl.getBasket().isSatisfiedBy(seller.getBasketSold())) {
            // buyer basket not satisfied by seller providing quote
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        int boughtIndex = 0;
        Basket basket = sl.getBasket();
        final double[] quantities = sl.getQuantities();
        List<CommoditySold> commsSold = seller.getCommoditiesSold();
        List<InsufficientCommodity> insufficientCommodities = new ArrayList<>();


        for (int soldIndex = 0; boughtIndex < basket.size();
                        boughtIndex++, soldIndex++) {
            CommoditySpecification basketCommSpec = basket.get(boughtIndex);

            // TODO Make this a list if we have more commodities to skip
            // Skip the coupon commodity
            if (basketCommSpec.getBaseType() == couponCommodityBaseType) {
                continue;
            }
            // Find corresponding commodity sold. Commodities sold are ordered the same way as the
            // basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
            while (!basketCommSpec.equals(seller.getBasketSold().get(soldIndex))) {
                soldIndex++;
            }
            double soldCapacity = commsSold.get(soldIndex).getCapacity();
            if (quantities[boughtIndex] > soldCapacity) {
                logMessagesForSellerCapacityValidation(sl, seller, quantities[boughtIndex],
                    basketCommSpec, soldCapacity);
                insufficientCommodities.add(new InsufficientCommodity(basketCommSpec, soldCapacity));
            }
        }
        if (insufficientCommodities.isEmpty()) {
            return new CommodityQuote(seller);
        } else {
            return new InsufficientCapacityQuote(seller, sl.getBasket().size(), insufficientCommodities);
        }
    }

    /**
     * Logs messages if the logger's trace is enabled or the seller/buyer of shopping list
     * have their debug enabled.
     *
     * @param sl the shopping list whose capacity is being validated.
     * @param seller the seller
     * @param quantityBought the quantity of commodity requested
     * @param boughtCommSpec the commodity spec of the commodity requested
     * @param soldCapacity the capacity of the commodity of the seller
     */
    private static void logMessagesForSellerCapacityValidation(ShoppingList sl,
                    Trader seller, double quantityBought, CommoditySpecification boughtCommSpec,
                    double soldCapacity) {
        if (logger.isTraceEnabled() || seller.isDebugEnabled()
                        || sl.getBuyer().isDebugEnabled()) {
            logger.debug("{} requested amount {} of {} not within {} capacity ({})",
                            sl, quantityBought,
                            boughtCommSpec.getDebugInfoNeverUseInCode(),
                            seller, soldCapacity);
        }
    }

    /**
     * Validate if the sum of netTpUsed and ioTpUsed is within the netTpSold capacity
     *
     * @param sl the shopping list
     * @param seller the templateProvider that supplies the resources
     * @param comm1Type is the type of 1st commodity
     * @param comm2Type is the type of 2nd commodity
     * @return A quote for the dependent compute commodities. An infinite quote indicates that the
     *         dependent compute commodities do not comply with the maximum capacity.
     */
    private static MutableQuote getDependentComputeCommoditiesQuote(ShoppingList sl,
                                                                    Trader seller,
                                                                    int comm1Type,
                                                                    int comm2Type) {
        int comm1BoughtIndex = sl.getBasket().indexOf(comm1Type);
        int comm2BoughtIndex = sl.getBasket().indexOf(comm2Type);
        int comm1SoldIndex = seller.getBasketSold().indexOf(comm1Type);
        double comm1BoughtQuantity = comm1BoughtIndex != -1 ? sl.getQuantity(comm1BoughtIndex) : 0;
        double comm2BoughtQuantity = comm2BoughtIndex != -1 ? sl.getQuantity(comm2BoughtIndex) : 0;
        double boughtSum = comm1BoughtQuantity + comm2BoughtQuantity;

        double comm1SoldCapacity = seller.getCommoditiesSold().get(comm1SoldIndex).getCapacity();
        boolean isValid = boughtSum <= comm1SoldCapacity;
        logMessagesForDependentComputeCommValidation(isValid, seller, sl.getBuyer(), sl,
                        comm1BoughtIndex, comm2BoughtIndex, comm1BoughtQuantity, comm2BoughtQuantity,
                        comm1SoldIndex, comm1SoldCapacity);
        return isValid ? CommodityQuote.zero(seller) :
            new InfiniteDependentComputeCommodityQuote(seller, sl.getBasket().get(comm1BoughtIndex),
                sl.getBasket().get(comm2BoughtIndex), comm1SoldCapacity, comm1BoughtQuantity,
                comm2BoughtQuantity);
    }

    /**
     * Logs messages if the logger's trace is enabled or the seller/buyer of shopping list
     * have their debug enabled.
     */
    private static void logMessagesForDependentComputeCommValidation(boolean isValid,
                    Trader seller, Trader buyer, ShoppingList sl, int comm1BoughtIndex,
                    int comm2BoughtIndex, double comm1BoughtQuantity, double comm2BoughtQuantity,
                    int comm1SoldIndex, double comm1SoldCapacity) {
        if (!isValid && (logger.isTraceEnabled() || seller.isDebugEnabled()
                        || buyer.isDebugEnabled())) {
            CommoditySpecification comm1Sold = seller.getBasketSold().get(comm1SoldIndex);
            logger.debug("{} bought ({}) + {} bought ({}) by {} is greater than {} capacity ({}) "
                            + "sold by {}", comm1BoughtIndex != -1
                                            ? sl.getBasket().get(comm1BoughtIndex)
                                                            .getDebugInfoNeverUseInCode()
                                            : "null",
                            comm1BoughtQuantity, comm2BoughtIndex != -1
                                            ? sl.getBasket().get(comm2BoughtIndex)
                                                            .getDebugInfoNeverUseInCode()
                                            : "null",
                            comm2BoughtQuantity, sl, comm1Sold != null
                                            ? comm1Sold.getDebugInfoNeverUseInCode()
                                            : "null",
                            comm1SoldCapacity, seller);
        }
    }

    /**
     * Calculate the discounted compute cost, based on available RIs discount on a cbtp.
     *
     * @param buyer {@link ShoppingList} associated with the vm that is requesting price
     * @param seller {@link Trader} the cbtp that the buyer asks a price from
     * @param cbtpResourceBundle {@link CbtpCostDTO} associated with the selling cbtp
     * @param economy {@link Economy}
     * @param costTable containing costTuples indexed by location id, account id and license
     *                  commodity type.
     *
     * @return the cost given by {@link CostFunction}
     */
    public static MutableQuote calculateDiscountedComputeCost(ShoppingList buyer, Trader seller,
                                                              CbtpCostDTO cbtpResourceBundle,
                                                              UnmodifiableEconomy economy,
                                                              CostTable costTable,
                                                              final int licenseBaseType) {
        long groupFactor = buyer.getGroupFactor();
        final Context buyerContext = buyer.getBuyer().getSettings().getContext().get();
        final int licenseCommBoughtIndex = buyer.getBasket().indexOfBaseType(licenseBaseType);
        if (costTable.getAccountIds().isEmpty()) {
            // empty cost table, return infinity to not place entity on this seller
            logger.warn("No cost information found for seller {}, return infinity quote",
                    seller.getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        if (licenseCommBoughtIndex == -1) {
            // when there is no license for the shopping list, return infinity quote
            // NOTE: we assume that on prem entities have to contain LicenseAccessCommodity
            logger.warn("No license commodity found for buyer {}, return infinity quote",
                    buyer.getBuyer().getDebugInfoNeverUseInCode());
            return new CommodityQuote(seller, Double.POSITIVE_INFINITY);
        }
        final int licenseTypeKey = buyer.getBasket().get(licenseCommBoughtIndex).getType();
        final CostTuple costTuple = retrieveCbtpCostTuple(buyerContext, cbtpResourceBundle,
                costTable, licenseTypeKey);
        if (costTuple == null) {
            if (logger.isTraceEnabled() || seller.isDebugEnabled()
                    || buyer.getBuyer().isDebugEnabled()) {
                logger.info("VM {} with context {} is not in scope of discounted tier {} with " +
                                "context {}", buyer.getDebugInfoNeverUseInCode(), buyerContext,
                        seller.getDebugInfoNeverUseInCode(),
                        cbtpResourceBundle.getCostTupleListList());
            }
            return CostFunctionFactoryHelper.getNoCostMutableQuote(seller, buyer,
                    buyerContext.getRegionId(), buyerContext.getBalanceAccount());
        }

        Trader destTP = findMatchingTpForCalculatingCost(economy, seller, buyer);

        // destTP is becoming null because minimizer has no best seller.
        // This can happen in 2 cases,
        // 1) when the budget of a business account is too low and then the quote in
        // budgetDepletionRiskBasedQuoteFunction is infinity
        // 2) when none of the mutableSellers can fit the buyer
        // so when it gets here, minimizer has no best seller.
        if (destTP == null) {
            return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null, null);
        }

        // Get the cost of the template matched with the vm. If this buyer represents a
        // consistent scaling group, the template cost will be for all members of the
        // group (the cost will be multiplied by the group factor)
        MutableQuote destTPQuote = QuoteFunctionFactory.computeCost(buyer, destTP, false, economy);
        Optional<EconomyDTOs.Context> context = destTPQuote.getContext();
        double templateCostForBuyer = destTPQuote.getQuoteValue();

        // for VMs placed on a CBTP, we need the price on the associated TP. We need this since we return infinite
        // price on the CBTP when it is the current supplier.
        Trader currentTP = findMatchingTpForCalculatingCost(economy, buyer.getSupplier(), buyer);
        double costOnCurrentSupplier = QuoteFunctionFactory.computeCost(buyer, currentTP, false, economy)
                .getQuoteValue();

        // If we are sizing up to use a RI and we only allow resizes up to templates
        // that cost less than : (riCostFactor * cost at current supplier). If it is more, we prevent
        // such a resize to avoid cases like forcing small VMs to use large unused RIs.
        if (economy.getSettings().hasDiscountedComputeCostFactor()
                && templateCostForBuyer > economy.getSettings().getDiscountedComputeCostFactor() * costOnCurrentSupplier) {
            if (logger.isTraceEnabled() || seller.isDebugEnabled()
                    || buyer.getBuyer().isDebugEnabled()) {
                logger.info("Ignoring supplier : {} (cost : {}) for shopping list {} because "
                                + "it has cost {} times greater than current supplier {} (cost : {}).",
                                destTP.getDebugInfoNeverUseInCode(), templateCostForBuyer,
                                buyer.getDebugInfoNeverUseInCode(),
                                economy.getSettings().getDiscountedComputeCostFactor(),
                                buyer.getSupplier(), costOnCurrentSupplier);
            }
            return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null, null);
        }

        // The capacity of a coupon commodity sold by a template provider reflects the number of
        // coupons associated with the template it represents. This number is the requested amount
        // of coupons by a matching vm.
        int indexOfCouponCommByTp = destTP.getBasketSold()
                        .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        CommoditySold couponCommSoldByTp =
                destTP.getCommoditiesSold().get(indexOfCouponCommByTp);

        // If seller is the CBTP that the VM is currently on, check how many coupons are already covered.
        double currentCoverage = buyer.getTotalAllocatedCoupons(economy, seller);
        // if the currentCouponCoverage satisfies the requestedAmount for the new template, request 0 coupons
        double requestedCoupons = Math.max(0, couponCommSoldByTp.getCapacity() * (groupFactor > 0 ? groupFactor : 1));

        // Calculate the number of available coupons from cbtp
        int indexOfCouponCommByCbtp = seller.getBasketSold()
                        .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        int indexOfCouponCommBought = buyer.getBasket()
                .indexOfBaseType(cbtpResourceBundle.getCouponBaseType());
        CommoditySold couponCommSoldByCbtp =
                        seller.getCommoditiesSold().get(indexOfCouponCommByCbtp);

        // if the groupLeader is asking for cost, reqlinquish all coupons and provide quote for the group
        double availableCoupons =
                couponCommSoldByCbtp.getCapacity() - couponCommSoldByCbtp.getQuantity();
        if (groupFactor > 0) {
            // when leader shops, relinquish group's coverage
            availableCoupons += currentCoverage;
        } else if (seller == buyer.getSupplier()) {
            // when peer shops consider the coupons it used from the supplier. If we dont do that, VM might end up not
            // finding the coupon and might move out of CBTP. We did not do this before before peer always relinquished
            // before it shopped
            availableCoupons += buyer.getQuantity(indexOfCouponCommBought);
        }
        double singleVmTemplateCost = templateCostForBuyer / (groupFactor > 0 ? groupFactor : 1);

        double discountedCost = 0;
        double numCouponsToPayFor = 0;

        if (availableCoupons > 0) {

            if (couponCommSoldByTp.getCapacity() != 0) {
                final double matchingTpCouponCapacity = couponCommSoldByTp.getCapacity();
                final double templateCostPerCoupon = singleVmTemplateCost / matchingTpCouponCapacity;
                // Assuming 100% discount for the portion of requested coupons satisfied by the CBTP
                numCouponsToPayFor = Math.max(0, (requestedCoupons - availableCoupons));


                final double numCouponsCovered = requestedCoupons - numCouponsToPayFor;
                final double cbtpResellerRate = calculateCbtpResellerPrice(costTuple, destTP, singleVmTemplateCost);
                final double cbtpResellerCost = cbtpResellerRate * (numCouponsCovered / matchingTpCouponCapacity);

                discountedCost = numCouponsToPayFor * templateCostPerCoupon + cbtpResellerCost;

            } else {
                logger.warn("Coupon commodity sold by {} has 0 capacity.",
                        destTP.getDebugInfoNeverUseInCode());
                discountedCost = Double.POSITIVE_INFINITY;
            }
        } else {
            numCouponsToPayFor = requestedCoupons;
            // In case that there isn't discount available, avoid preferring a cbtp that provides
            // no discount on tp of the matching template.
            discountedCost = Double.POSITIVE_INFINITY;
        }
        // coverage in the moveContext is on the group level for the group leader
        if (buyer.getGroupFactor() > 0) {
            return new CommodityCloudQuote(seller, discountedCost, context.get(),
                    requestedCoupons, requestedCoupons - numCouponsToPayFor, economy);
        } else {
            return new CommodityCloudQuote(seller, discountedCost, context.get(),
                    buyer.getTotalRequestedCoupons(economy, seller),
                    currentCoverage + requestedCoupons - numCouponsToPayFor, economy);
        }
    }


    /**
     * Calculates the price for a CBTP, based on reselling TP commodities. The price calculation takes
     * into account a general reseller fee as part of the CBTP, which may represent the nominal fee
     * used to "size" CBTPs, as well as a core-based license fee based on the destination TP.
     *
     * @param costTuple The {@link CostTuple} of the CBTP
     * @param matchingTp The destination template provider the CBTP will resell
     * @param matchingTpPrice the on demand cost of the matching TP
     * @return The price/rate of the CBTP. This price does not take into account the coupons bought
     * on the CBTP and is therefore representative of the rate, not cost.
     */
    private static double calculateCbtpResellerPrice(@NonNull CostTuple costTuple,
                                                     @NonNull Trader matchingTp,
                                                     double matchingTpPrice) {

        double corePrice = 0.0;

        if (costTuple.hasCoreCommodityType() && costTuple.getCoreCommodityType() >= 0) {
            final int indexOfCoreCommodity =
                    matchingTp.getBasketSold().indexOf(costTuple.getCoreCommodityType());

            final CommoditySold coreCommSoldByMatchingTp = (indexOfCoreCommodity  >= 0) ?
                    matchingTp.getCommoditiesSold().get(indexOfCoreCommodity) : null;

            if (coreCommSoldByMatchingTp != null) {
                final long coreCapacity = Math.round(coreCommSoldByMatchingTp.getCapacity());
                corePrice = costTuple.getPriceByNumCoresMap().getOrDefault(coreCapacity, 0.0);
            }
        }

        // If there is no core price, then return a very small fraction of the on-demand price of
        // the matching TP.
        // The CBTP cost tuple itself will have a cost which corresponds with the cost of the
        // largest tier in that RI's family. But it does not make sense to look at that price since
        // this Shopping list could be going to some other tier in that family. So we should be
        // looking at the on demand price of that matching tier.
        // So we return a price which will be a fraction of the price of matching TP.
        if (corePrice == 0.0) {
            return matchingTpPrice * riCostDeprecationFactor;
        } else {
            return corePrice;
        }
    }

    /**
     * Find the TP that the trader best fits on.
     *
     * @param economy {@link Economy}
     * @param seller {@link Trader} the cbtp/tp that the buyer asks price from
     * @param buyer {@link ShoppingList} associated with the vm that is requesting price
     *
     * @return the Trader that the buyer best fits on.
     */
    private static Trader findMatchingTpForCalculatingCost(UnmodifiableEconomy economy, Trader seller, ShoppingList buyer) {
        if (seller == null) {
            return null;
        }
        // Match the vm with a template in order to:
        // 1) Estimate the number of coupons requested by the vm
        // 2) Determine the template cost the discount should apply to
        // Get the market for the cbtp. sellers in this market are Template Providers
        final @NonNull @ReadOnly Set<Entry<@NonNull ShoppingList, @NonNull Market>>
                shoppingListsInMarket = economy.getMarketsAsBuyer(seller).entrySet();
        if (shoppingListsInMarket.isEmpty()) {
            // if TP doesnt have any underlying markets where it shops, return TP as the seller
            return seller;
        }
        Market market = shoppingListsInMarket.iterator().next().getValue();
        Set<Trader> sellers = market.getActiveSellers();
        List<Trader> mutableSellers = new ArrayList<Trader>();
        mutableSellers.addAll(sellers);
        mutableSellers.retainAll(economy.getMarket(buyer).getActiveSellers());
        if (mutableSellers.isEmpty()) {
            // seller is a TP
            return seller;
        }

        // else, seller is a CBTP so find the best TP
        // Get cheapest quote, that will be provided by the matching template
        final QuoteMinimizer minimizer = mutableSellers.stream().collect(
                () -> new QuoteMinimizer(economy, buyer), QuoteMinimizer::accept,
                QuoteMinimizer::combine);
        return minimizer.getBestSeller();
    }
    /**
     * Retrieves a CostTuple from the provided CostTable using cbtpScopeIds, region/zone and
     * license commodity index from the buyerContext.
     *
     * @param buyerContext for which the CostTuple is being retrieved.
     * @param cbtpCostDTO CBTP cost DTO.
     * @param costTable from which the CostTuple is being retrieved.
     * @return costTuple from the CostTable or {@code null} if not found.
     */
    @Nullable
    @VisibleForTesting
    protected static CostTuple retrieveCbtpCostTuple(
            final @Nullable Context buyerContext,
            final @Nonnull CbtpCostDTO cbtpCostDTO,
            final @Nonnull CostTable costTable,
            final int licenseTypeKey) {
        final BalanceAccount balanceAccount = buyerContext.getBalanceAccount();
        final long priceId = balanceAccount.getPriceId();
        final long regionId = buyerContext.getRegionId();
        final long zoneId = buyerContext.getZoneId();

        // Match CbtpCostDTO based on billing family ID (if present) or business account ID
        final Set<Long> cbtpScopeIds = ImmutableSet.copyOf(cbtpCostDTO.getScopeIdsList());
        if (cbtpScopeIds.contains(balanceAccount.getParentId())
                || cbtpScopeIds.contains(balanceAccount.getId())) {
            // costTable will be indexed using zone id, if the CBTP is scoped to a zone.
            // Otherwise the costTable will be indexed using the region id.
            CostTuple costTuple = costTable.getTuple(regionId, priceId, licenseTypeKey);
            if (costTuple != null) {
                return costTuple;
            }
            costTuple = costTable.getTuple(zoneId, priceId, licenseTypeKey);
            if (costTuple != null) {
                return costTuple;
            }
        }

        return null;
    }

    /**
     * Returns the cheapest tuple in the CostTable.
     *
     * @param costTable the information for pricing
     * @param licenseTypeKey the type of license access commodity
     * @return the cheapest tuple in the CostTable
     */
    private static CostTuple getCheapestTuple(@Nonnull final CostTable costTable, final int licenseTypeKey) {
        double cheapestCost = Double.MAX_VALUE;
        CostTuple cheapestTuple = null;
        for (long id : costTable.getAccountIds()) {
            // The cheapest cost for a given license and ba in all region is kept in the costTable.
            // The key of that cheapest cost tuple is {NO_VALUE, businessAccountId, licenseCommodityType}
            CostTuple tuple = costTable.getTuple(CostTable.NO_VALUE, id, licenseTypeKey);
            if (tuple != null) {
                cheapestTuple = tuple.getPrice() < cheapestCost ? tuple : cheapestTuple;
            }
        }
        return cheapestTuple;
    }

    /**
     * Calculates the cost of template that a shopping list has matched to.
     *
     * @param seller {@link Trader} that the buyer matched to
     * @param sl is the {@link ShoppingList} that is requesting price
     * @param costTable Table that stores cost by Business Account, license type and Region.
     * @param licenseBaseType The base type of the license commodity the sl contains.
     * @return A quote for the cost given by {@link CostFunction}
     */
    @VisibleForTesting
    protected static MutableQuote calculateComputeAndDatabaseCostQuote(
            Trader seller,
            ShoppingList sl,
            CostTable costTable,
            final int licenseBaseType) {
        final int licenseCommBoughtIndex = sl.getBasket().indexOfBaseType(licenseBaseType);
        final long groupFactor = sl.getGroupFactor();
        final Optional<Context> optionalContext = sl.getBuyer().getSettings().getContext();
        if (!optionalContext.isPresent()) {
            return new CostUnavailableQuote(seller, null, null, null);
        }
        final Context context = optionalContext.get();
        final long regionIdBought = context.getRegionId();
        final BalanceAccount balanceAccount = context.getBalanceAccount();
        if (balanceAccount == null) {
            logger.warn("Business account is not found on seller for shopping list: {}",
                    sl.getDebugInfoNeverUseInCode());
            return CostFunctionFactoryHelper.getNoCostMutableQuote(seller, sl, regionIdBought, null);
        }
        final long accountId = costTable.hasAccountId(balanceAccount.getPriceId()) ?
                balanceAccount.getPriceId() : balanceAccount.getId();

        if (!costTable.hasAccountId(accountId)) {
            logger.debug("Business account id {} is not found on seller: {}, in regionId: {}, for shopping list: {},"
                            + " return infinity compute quote", accountId,
                    seller.getDebugInfoNeverUseInCode(), regionIdBought, sl.getDebugInfoNeverUseInCode());
            return new CostUnavailableQuote(seller, regionIdBought, balanceAccount.getId(),
                    balanceAccount.getPriceId());
        }

        final int licenseTypeKey;
        // NOTE: CostTable.NO_VALUE (-1) is the no license commodity type
        if (licenseCommBoughtIndex == CostTable.NO_VALUE) {
            licenseTypeKey = licenseCommBoughtIndex;
        } else {
            licenseTypeKey = sl.getBasket().get(licenseCommBoughtIndex).getType();
        }

        CostTuple costTuple = costTable.getTuple(regionIdBought, accountId, licenseTypeKey);
        if (costTuple == null) {
            // If the cost tuple is null for the no license case, that means there is no pricing for
            // this region for any license for the template. In this case, we will return an infinite
            // quote rather than looking up the cheapest region as we don't want to support inter-region
            // moves.
            logger.debug("Cost for region {} and license key {} not found in seller {},",
                    regionIdBought, licenseTypeKey,
                    sl.getDebugInfoNeverUseInCode());
            return CostFunctionFactoryHelper.getNoCostMutableQuote(seller, sl, regionIdBought, balanceAccount);
        }

        final Long regionId = costTuple.getRegionId();
        // Cost plus the dependent options cost
        double totalCost = costTuple.getPrice();
        final List<CommodityContext> commodityContexts = new ArrayList<>();

        final double dependentCost = costTuple.getDependentCostTuplesCount() > 0 ?
                getDependentCost(seller, costTuple, sl, commodityContexts) : 0;
        if (Double.isFinite(dependentCost)) {
            totalCost += dependentCost;
            // NOTE: CostTable.NO_VALUE (-1) is the no license commodity type
            return Double.isInfinite(totalCost) && licenseCommBoughtIndex != CostTable.NO_VALUE ?
                    new LicenseUnavailableQuote(seller, sl.getBasket().get(licenseCommBoughtIndex)) :
                    new CommodityCloudQuote(seller, totalCost * (groupFactor > 0 ? groupFactor : 1),
                            regionId, accountId, commodityContexts);
        } else {
            return new CommodityCloudQuote(seller,
                    Double.POSITIVE_INFINITY, regionId, accountId, commodityContexts);
        }
    }

    /**
     * Calculate the 'dependent' cost of template using dependent cost tuples.
     *
     * @param seller seller {@link Trader} that the buyer matched to
     * @param costTuple main {@link CostTuple}
     * @param sl is the {@link ShoppingList} that is requesting price
     * @param commodityContexts Context with commodity data about the Quote
     * @return dependent cost part
     */
    private static double getDependentCost(Trader seller, CostTuple costTuple, ShoppingList sl, List<CommodityContext> commodityContexts) {

        double totalCost = 0;
        List<DependentCostTuple> dependentCostTuplesList = costTuple.getDependentCostTuplesList();
        if (!dependentCostTuplesList.isEmpty()) {
            for (DependentCostTuple dependentCostTuple : dependentCostTuplesList) {
                List<DependentResourceOption> dependentResourceOptions =
                        dependentCostTuple.getDependentResourceOptionsList();
                if (!dependentResourceOptions.isEmpty()) {
                    int dependentResourceType = dependentCostTuple.getDependentResourceType();
                    int dependentResourceIndex = sl.getBasket().indexOf(dependentResourceType);
                    // Skip if you can't find the index.
                    if (dependentResourceIndex == -1) {
                        continue;
                    }
                    double dependentResourceQuantity = sl.getQuantities()[dependentResourceIndex];
                    int baseType = sl.getBasket().get(dependentResourceIndex).getBaseType();
                    Double currentCapacity = sl.getAssignedCapacity(baseType);

                    /*
                     * 1. if isNewSupplier or isCongested, then get max free storage scaling.
                     * 2. If new selection has lower cost then consider new selected Amount. (savings, scale down).
                     * 3. If isNewSupplier and congestion does not exists, then do no change currentSelection to avoid free scaling.
                     */
                    boolean isCongested = dependentResourceQuantity >= (currentCapacity != null ? currentCapacity : Double.MIN_VALUE);
                    double selectedAmount;
                    final boolean isNewSupplier = seller != sl.getSupplier();
                    final SelectedStorageAndCost newSelection = generateDependentSizeAndDependentCost(
                            dependentResourceOptions, dependentResourceQuantity, seller, sl);
                    double cost = 0;
                    if (isNewSupplier || isCongested) {
                        selectedAmount = newSelection.getSelectedAmount();
                        cost = newSelection.getCost();
                    } else {
                        final SelectedStorageAndCost currentSelection = generateDependentSizeAndDependentCost(
                                dependentResourceOptions, currentCapacity, seller, sl);
                        if (newSelection.getCost() < currentSelection.getCost()) {
                            selectedAmount = newSelection.getSelectedAmount();
                            cost = newSelection.getCost();
                        } else {
                            selectedAmount = currentCapacity == null ?
                                    currentSelection.getSelectedAmount() : currentCapacity;
                            cost = currentSelection.getCost();
                        }
                    }
                    // Update the shopping list and keep the selected value.
                    commodityContexts.add(
                            new CommodityContext(sl.getBasket().get(dependentResourceIndex),
                                    selectedAmount, true));
                    totalCost += cost;
                }
            }
        }

        return totalCost;
    }

    @Nonnull
    private static SelectedStorageAndCost generateDependentSizeAndDependentCost(
            @Nonnull Collection<DependentResourceOption> dependentResourceOptions,
            @Nullable Double dependentResourceQuantity,
            @Nonnull  Trader seller,
            @Nonnull  ShoppingList sl) {
        double cost = 0;
        double prevEndRange = 0;
        double endRange = 0;
        long increment = 0;
        double price = 0;
        if (dependentResourceQuantity == null) {
            logger.debug("No assigned capacity found on shopping list : {}. " +
                    "Can not calculate current dependent cost. returning infinite cost.",
                    sl.getDebugInfoNeverUseInCode());
            return new SelectedStorageAndCost(endRange, Double.POSITIVE_INFINITY);
        }
        // Find the right dependent option.
        for (DependentResourceOption dependentResourceOption : dependentResourceOptions) {
            prevEndRange = endRange;
            endRange = dependentResourceOption.getEndRange();
            increment = dependentResourceOption.getAbsoluteIncrement();
            price = dependentResourceOption.getPrice();
            if (increment <= 0) {
                return handleNoIncrementDependentResourceOption(
                        dependentResourceQuantity, seller, sl, endRange, price);
            }
            if (dependentResourceQuantity <= endRange) {
                break;
            } else {
                cost += (endRange - prevEndRange) * price;
            }
        }
        /*
        The bought quantity is less than the maximum endRange because
        insufficientCommodityWithinSellerCapacityQuote takes care of checking
        for bought quantity less than the sold commodity max.
         */
        double selectedAmount = prevEndRange;
        if (dependentResourceQuantity > endRange) {
            logger.debug("dependentResourceQuantity : {} for buyer: {}  was not met by seller {}. Returning infinite"
                            + " cost for this seller.",
                    dependentResourceQuantity, sl.getDebugInfoNeverUseInCode(), seller.getDebugInfoNeverUseInCode());
            return new SelectedStorageAndCost(selectedAmount, Double.POSITIVE_INFINITY);
        }
        // Add the increment in the current option to satisfy the demand.
        // if selectedAmount is 0 - do one increment step to satisfy limits defined in
        // {@link DependentResourceOption}, 0 capacity is not valid
        while (selectedAmount < dependentResourceQuantity || selectedAmount == 0) {
            selectedAmount += increment;
            cost += increment * price;
        }

        return new SelectedStorageAndCost(selectedAmount, cost);
    }

    private static SelectedStorageAndCost handleNoIncrementDependentResourceOption(
            final Double dependentResourceQuantity,
            final Trader seller,
            final ShoppingList sl,
            final double endRange,
            final double price) {
        if (dependentResourceQuantity <= endRange) {
            logger.debug("Seller: {} dependentResourceOptions has 0 incrementIntervals," +
                            "Calculating cost by multiplying capacity {} directly by unit cost.",
                    seller.getDebugInfoNeverUseInCode(), dependentResourceQuantity);
            double cost = dependentResourceQuantity * price;
            return new SelectedStorageAndCost(dependentResourceQuantity, cost);
        } else {
            logger.debug("Seller ID: {}, increment range for dependentResourceOption is 0 " +
                            "and current demand is not satisfied by endRange." +
                            "Returning infinite cost for seller. {}",
                    seller.getDebugInfoNeverUseInCode(), sl.getDebugInfoNeverUseInCode());
            return new SelectedStorageAndCost(endRange, Double.POSITIVE_INFINITY);
        }
    }

    /**
     * Creates {@link CostFunction} for a given seller.
     *
     * @param costDTO the DTO carries the cost information
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunction(CostDTO costDTO) {
        switch (costDTO.getCostTypeCase()) {
            case STORAGE_TIER_COST:
                return createCostFunctionForStorageTier(costDTO.getStorageTierCost());
            case COMPUTE_TIER_COST:
                return createCostFunctionForComputeTier(costDTO.getComputeTierCost());
            case DATABASE_TIER_COST:
                return createCostFunctionForDatabaseTier(costDTO.getDatabaseTierCost());
            case CBTP_RESOURCE_BUNDLE:
                return createResourceBundleCostFunctionForCbtp(costDTO.getCbtpResourceBundle());
            case DATABASE_SERVER_TIER_COST:
                return createCostFunctionForDatabaseServerTier(costDTO.getDatabaseServerTierCost());
            default:
                throw new IllegalArgumentException("input = " + costDTO);
        }
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link ComputeTierCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunctionForComputeTier(ComputeTierCostDTO costDTO) {
        final CostTable costTable = new CostTable(costDTO.getCostTupleListList());
        Map<CommoditySpecification, CommoditySpecification> dependencyMap =
                translateComputeResourceDependency(costDTO.getComputeResourceDepedencyList());
        final int licenseBaseType = costDTO.getLicenseCommodityBaseType();
        CostFunction costFunction = new CostFunction() {
            @Override
            public MutableQuote calculateCost(ShoppingList buyer, Trader seller, boolean validate,
                    UnmodifiableEconomy economy) {

                int couponCommodityBaseType = costDTO.getCouponBaseType();
                final MutableQuote capacityQuote =
                        insufficientCommodityWithinSellerCapacityQuote(buyer, seller, couponCommodityBaseType);
                if (capacityQuote.isInfinite()) {
                    return capacityQuote;
                }

                for (Entry<CommoditySpecification, CommoditySpecification> dependency
                                : dependencyMap.entrySet()) {
                    final MutableQuote dependentComputeCommoditiesQuote =
                            getDependentComputeCommoditiesQuote(buyer, seller, dependency.getKey().getType(),
                                                                dependency.getValue().getType());
                    if (dependentComputeCommoditiesQuote.isInfinite()) {
                        return dependentComputeCommoditiesQuote;
                    }
                }

                return calculateComputeAndDatabaseCostQuote(seller, buyer, costTable, licenseBaseType);
            }
        };
        return costFunction;
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link CbtpCostDTO}
     *
     * @param cbtpResourceBundle the DTO carries the data used to construct cost function for cbtp
     * @return CostFunction
     */
    public static @NonNull CostFunction
                    createResourceBundleCostFunctionForCbtp(CbtpCostDTO cbtpResourceBundle) {
        final CostTable costTable = new CostTable(cbtpResourceBundle.getCostTupleListList());
        int licenseBaseType = cbtpResourceBundle.getLicenseCommodityBaseType();
        CostFunction costFunction = (CostFunction)(buyer, seller, validate, economy) -> {
            if (!validate) {
                // In case that a vm is already placed on a cbtp, We return INFINITE price forcing it to compare pricing on all sellers
                return new CommodityCloudQuote(seller, Double.POSITIVE_INFINITY, null, null, null);
            }
            int couponCommodityBaseType = cbtpResourceBundle.getCouponBaseType();
            final MutableQuote quote =
                insufficientCommodityWithinSellerCapacityQuote(buyer, seller,
                        couponCommodityBaseType);
            if (quote.isInfinite()) {
                return quote;
            }

            return calculateDiscountedComputeCost(buyer, seller, cbtpResourceBundle, economy,
                    costTable, licenseBaseType);
        };

        return costFunction;
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link StorageTierCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function for storage
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunctionForStorageTier(StorageTierCostDTO costDTO) {
        List<StorageResourceCost> resourceCost = costDTO.getStorageResourceCostList();
        // mapping from commodity type to price table, where price table keeps priceData
        // per businessAccount and region.
        final Map<CommoditySpecification, AccountRegionPriceTable> priceDataMap
                = CostFunctionFactoryHelper.translateResourceCostForStorageTier(resourceCost);
        // the set to keep commodity types that are related to tier constraints
        final Set<CommoditySpecification> commTypesWithConstraints = new HashSet<>();
        // Construct constraints representation, meanwhile update commTypesWithConstraints set.
        // the map to keep commodity to its min max capacity limitation
        final Map<CommoditySpecification, CapacityLimitation> commCapacityLimitation
                = CostFunctionFactoryHelper.translateResourceCapacityLimitation(
                costDTO.getStorageResourceLimitationList(), commTypesWithConstraints);
        // ratio capacity constraint between commodities
        final List<RatioBasedResourceDependency> ratioDependencyList
                = CostFunctionFactoryHelper.translateStorageResourceRatioDependency(
                costDTO.getStorageResourceRatioDependencyList(), commTypesWithConstraints);
        // range capacity constraint between commodities
        final List<RangeBasedResourceDependency> rangeDependencyList
                = CostFunctionFactoryHelper.translateStorageResourceRangeDependency(
                costDTO.getStorageResourceRangeDependencyList(), commTypesWithConstraints);

        CostFunction costFunction = new CostFunction() {
            @Override
            public MutableQuote calculateCost(ShoppingList sl, Trader seller,
                                              boolean validate, UnmodifiableEconomy economy) {
                if (seller == null) {
                    return CommodityQuote.zero(seller);
                }
                // When commTypesWithConstraints is empty(seller is Unmanaged Standard or Unmanaged Premium tier),
                // there is no constraint defined by the seller, thus there is no need to analyze shoppingList on the seller.
                if (commTypesWithConstraints.isEmpty()) {
                    return new CostUnavailableQuote(seller, null, null, null);
                }
                // Construct commQuantityMap for sl commodities' demand for commTypesWithConstraints,
                // which is used for constraint check, and will be updated during constraint check.
                Map<CommoditySpecification, Double> commQuantityMap = new HashMap<>();
                Map<CommoditySpecification, Double> commHistoricalQuantityMap = new HashMap<>();
                commTypesWithConstraints.stream().forEach(commType -> {
                    int index = sl.getBasket().indexOf(commType);
                    if (index == -1) {
                        // The sl is from the buyer, which may not buy all resources sold
                        // by the seller, e.g: some VM does not request IOPS sold by IO1.
                        // Constraint check is irrelevant for such commodities, and we skip it.
                        return;
                    }
                    commQuantityMap.put(commType, Math.ceil(sl.getQuantities()[index]));
                    commHistoricalQuantityMap.put(commType, sl.getHistoricalQuantities()[index]);
                });
                // If volume is in Reversibility mode, try to get a quote in Reversibility mode.
                if (!sl.getDemandScalable()) {
                    MutableQuote reversibilityModeQuote = CostFunctionFactoryHelper
                            .calculateStorageTierQuote(sl, seller, commQuantityMap,
                                    commHistoricalQuantityMap, priceDataMap,
                                    commCapacityLimitation, ratioDependencyList,
                                    rangeDependencyList, false, false);
                    // If can get finite quote, directly return.
                    if (reversibilityModeQuote.isFinite()) {
                        return reversibilityModeQuote;
                    }
                }
                // If volume is in Savings mode, try to get a quote in Savings mode without penalty cost.
                // If volume is in Reversibility mode and gets infinite quote in Reversibility, try to get
                // a quote in Savings mode with penalty cost.
                return CostFunctionFactoryHelper.calculateStorageTierQuote(sl, seller,
                        commQuantityMap, commHistoricalQuantityMap, priceDataMap,
                        commCapacityLimitation, ratioDependencyList, rangeDependencyList, true,
                        !sl.getDemandScalable());
            }
        };

        return costFunction;
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link ComputeTierCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunctionForDatabaseTier(DatabaseTierCostDTO costDTO) {
        CostTable costTable = new CostTable(costDTO.getCostTupleListList());
        final int licenseBaseType = costDTO.getLicenseCommodityBaseType();

        CostFunction costFunction = (buyer, seller, validate, economy) -> {
            int couponCommodityBaseType = costDTO.getCouponBaseType();
            final MutableQuote capacityQuote =
                    insufficientCommodityWithinSellerCapacityQuote(buyer, seller, couponCommodityBaseType);
            if (capacityQuote.isInfinite()) {
                return capacityQuote;
            }

            return calculateComputeAndDatabaseCostQuote(seller, buyer, costTable, licenseBaseType);
        };
        return costFunction;
    }

    /**
     * Create {@link CostFunction} by extracting data from {@link ComputeTierCostDTO}
     *
     * @param costDTO the DTO carries the data used to construct cost function
     * @return CostFunction
     */
    public static @NonNull CostFunction createCostFunctionForDatabaseServerTier(
            DatabaseServerTierCostDTO costDTO) {
        CostTable costTable = new CostTable(costDTO.getCostTupleListList());
        final int licenseBaseType = costDTO.getLicenseCommodityBaseType();

        CostFunction costFunction = (buyer, seller, validate, economy) -> {
            int couponCommodityBaseType = costDTO.getCouponBaseType();
            final MutableQuote capacityQuote =
                    insufficientCommodityWithinSellerCapacityQuote(buyer, seller, couponCommodityBaseType);
            if (capacityQuote.isInfinite()) {
                return capacityQuote;
            }

            return calculateComputeAndDatabaseCostQuote(seller, buyer, costTable, licenseBaseType);
        };
        return costFunction;
    }
}
