package com.vmturbo.platform.analysis.utilities;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.analysis.economy.BalanceAccount;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO;
import com.vmturbo.platform.analysis.protobuf.CostDTOs.CostDTO.CbtpCostDTO;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO;
import com.vmturbo.platform.analysis.topology.Topology;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.Map.Entry;

import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.QuoteMinimizer;
import com.vmturbo.platform.analysis.pricefunction.QuoteFunctionFactory;

public class FunctionalOperatorUtil {

    private static final Logger logger = LogManager.getLogger(FunctionalOperatorUtil.class);

    /**
     * Creates {@link CostFunction} for a given seller.
     *
     * @param costDTO the DTO carries the cost information
     * @param updateFunctionDTO contains the updateFunctionType
     * @return CostFunction
     */
    public static @NonNull FunctionalOperator createUpdatingFunction(CostDTO costDTO,
                                                                     UpdatingFunctionTO updateFunctionTO) {
        switch (updateFunctionTO.getUpdatingFunctionTypeCase()) {
            case MAX:
                return createMaxCommUpdatingFunction(costDTO, updateFunctionTO);
            case MIN:
                return createMinCommUpdatingFunction(costDTO, updateFunctionTO);
            case PROJECT_SECOND:
                return createReturnCommBoughtUpdatingFunction(costDTO, updateFunctionTO);
            case DELTA:
                return createAddCommUpdatingFunction(costDTO, updateFunctionTO);
            case AVG_ADD:
                return createAverageCommUpdatingFunction(costDTO, updateFunctionTO);
            case IGNORE_CONSUMPTION:
                return createIgnoreConsumptionUpdatingFunction(costDTO, updateFunctionTO);
            case UPDATE_EXPENSES:
                return createExpenseUpdatingFunction(costDTO, updateFunctionTO);
            case EXTERNAL_UPDATE:
                return createExternalUpdatingFunction(costDTO, updateFunctionTO);
            case UPDATE_COUPON:
                return createCouponUpdatingFunction(costDTO, updateFunctionTO);
            case UPDATINGFUNCTIONTYPE_NOT_SET:
            default:
                return null;
        }
    }

    public static FunctionalOperator createMaxCommUpdatingFunction(CostDTO costDTO,
                                                                   UpdatingFunctionTO updateFunctionTO) {
        return MAX_COMM;
    }

    public static FunctionalOperator createMinCommUpdatingFunction(CostDTO costDTO,
                                                                   UpdatingFunctionTO updateFunctionTO) {
        return MIN_COMM;
    }

    public static FunctionalOperator createReturnCommBoughtUpdatingFunction(CostDTO costDTO,
                                                                            UpdatingFunctionTO updateFunctionTO) {
        return RETURN_BOUGHT_COMM;
    }

    public static FunctionalOperator createAddCommUpdatingFunction(CostDTO costDTO,
                                                                   UpdatingFunctionTO updateFunctionTO) {
        return ADD_COMM;
    }

    public static FunctionalOperator createAverageCommUpdatingFunction(CostDTO costDTO,
                                                                       UpdatingFunctionTO updateFunctionTO) {
        return AVG_COMMS;
    }

    public static FunctionalOperator createSubtractCommUpdatingFunction(CostDTO costDTO,
                                                                        UpdatingFunctionTO updateFunctionTO) {
        return SUB_COMM;
    }

    public static FunctionalOperator createExpenseUpdatingFunction(CostDTO costDTO,
                                                                   UpdatingFunctionTO updateFunctionTO) {
        return UPDATE_EXPENSES;
    }

    public static FunctionalOperator createIgnoreConsumptionUpdatingFunction(CostDTO costDTO,
                                                                             UpdatingFunctionTO updateFunctionTO) {
        return IGNORE_CONSUMPTION;
    }

    public static FunctionalOperator createExternalUpdatingFunction(CostDTO costDTO,
                                                                    UpdatingFunctionTO updateFunctionTO) {
        return EXTERNAL_UPDATING_FUNCTION;
    }

    public static FunctionalOperator createCouponUpdatingFunction(CostDTO costDTO,
                                                                    UpdatingFunctionTO updateFunctionTO) {
        FunctionalOperator UPDATE_COUPON_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead)
                        -> {
                            CbtpCostDTO cbtpResourceBundle = costDTO.getCbtpResourceBundle();
                            // Find the template matched with the buyer
                            final Set<Entry<ShoppingList, Market>>
                            shoppingListsInMarket = economy.getMarketsAsBuyer(seller).entrySet();
                            Market market = shoppingListsInMarket.iterator().next().getValue();
                            List<Trader> sellers = market.getActiveSellers();
                            List<Trader> mutableSellers = new ArrayList<Trader>();
                            mutableSellers.addAll(sellers);
                            mutableSellers.retainAll(economy.getMarket(buyer).getActiveSellers());
                            // Get cheapest quote, that will be provided by the matching template
                            final QuoteMinimizer minimizer = mutableSellers.stream().collect(
                                            () -> new QuoteMinimizer(economy, buyer), QuoteMinimizer::accept,
                                            QuoteMinimizer::combine);
                            Trader matchingTP = minimizer.getBestSeller();

                            if (matchingTP == null) {
                                logger.error("UPDATE_COUPON_COMM cannot find a best seller for:"
                                                + buyer.getBuyer().getDebugInfoNeverUseInCode()
                                                + " which moved to: "
                                                + seller.getDebugInfoNeverUseInCode()
                                                + " mutable sellers: "
                                                + mutableSellers.stream()
                                                                .map(Trader::getDebugInfoNeverUseInCode)
                                                                .collect(Collectors.toList()));
                                return new double[] {commSold.getQuantity(), 0};
                            }

                            if (overhead < 0) {
                                logger.error("The overhead for CouponComm on CBTP " + seller.getDebugInfoNeverUseInCode()
                                + " containing " + seller.getCustomers().size() + " is " + overhead);
                                overhead = 0;
                            }
                            // copy the sold commodity's old used into the quantity attribute when
                            // the attribute was reset while using explicit combinator
                            if (commSold.getQuantity() == 0) {
                                commSold.setQuantity(overhead);
                            }

                            // The capacity of coupon commodity sold by the matching tp holds the
                            // number of coupons associated with the template. This is the number of
                            // coupons consumed by a vm that got placed on a cbtp.
                            int couponCommBaseType = buyer.getBasket().get(boughtIndex).getBaseType();
                            int indexOfCouponCommByTp = matchingTP.getBasketSold()
                                            .indexOfBaseType(couponCommBaseType);
                            CommoditySold couponCommSoldByTp =
                                            matchingTP.getCommoditiesSold().get(indexOfCouponCommByTp);
                            double requestedCoupons = couponCommSoldByTp.getCapacity();
                            double templateCost = QuoteFunctionFactory.computeCost(buyer, matchingTP, false, economy);
                            double availableCoupons = commSold.getCapacity() - commSold.getQuantity();

                            double discountedCost = 0;
                            double discountCoefficient = 0;
                            double allocatedCoupons = 0;
                            if (availableCoupons > 0) {
                                allocatedCoupons = Math.min(requestedCoupons, availableCoupons);
                                buyer.setQuantity(boughtIndex, allocatedCoupons);
                                discountCoefficient = allocatedCoupons / requestedCoupons;
                                discountedCost = ((1 - discountCoefficient) * templateCost) + (discountCoefficient
                                                * ((1 - cbtpResourceBundle.getDiscountPercentage()) * templateCost));
                            }
                            // The cost of vm placed on a cbtp is the discounted cost
                            buyer.setCost(discountedCost);
                            if (logger.isTraceEnabled() || buyer.getBuyer().isDebugEnabled()) {
                                logger.info(buyer.getBuyer().getDebugInfoNeverUseInCode() + " migrated to CBTP "
                                             + seller.getDebugInfoNeverUseInCode() + " offering a dicount of "
                                             + cbtpResourceBundle.getDiscountPercentage() + " on TP "
                                             + matchingTP.getDebugInfoNeverUseInCode() + " at a discountCoeff of"
                                             + discountCoefficient + " with a final discount of "
                                             + discountedCost + " requests " + requestedCoupons + " coupons, allowed "
                                             + allocatedCoupons + " coupons");
                            }
                            /** Increase the used value of coupon commodity sold by cbtp accordingly.
                             * Increase the value by what was allocated to the buyer and not
                             * how much the buyer requested. This is important when we rollback the action.
                             * During rollback, we are only subtracting what buyer is buying (quantity).
                             * This was changed few lines above to what is allocated and not to what was
                             * requested by buyer.
                             */
                            return new double[]{(commSold.getQuantity() + allocatedCoupons), 0};
                        };
                        return UPDATE_COUPON_COMM;
    }

    public static FunctionalOperator ADD_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead)
                    -> new double[]{buyer.getQuantities()[boughtIndex] + commSold.getQuantity(),
                                    buyer.getPeakQuantities()[boughtIndex] + commSold.getPeakQuantity()};

    public static FunctionalOperator SUB_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead)
                    -> new double[]{Math.max(0, commSold.getQuantity() - buyer.getQuantities()[boughtIndex]),
                                    Math.max(0, commSold.getPeakQuantity() - buyer.getPeakQuantities()[boughtIndex])};

    // Return commSoverhead (template-cost) when taking the action and
    // (spent - oldTemplateCost + newTemplateCost) when not taking the action
    public static FunctionalOperator UPDATE_EXPENSES = (buyer, boughtIndex, commSold, seller,
                        economy, take, overhead)
                    -> {
                        BalanceAccount ba = seller.getSettings().getBalanceAccount();
                        if (take) {
                            // updating the action spent when taking it
                            CommoditySold commSoldByCurrSeller = buyer.getSupplier() != null ? buyer.getSupplier()
                                            .getCommoditySold(buyer.getBasket().get(boughtIndex)) : null;
                            double currCost = commSoldByCurrSeller == null ? 0 : commSoldByCurrSeller.getQuantity();
                            ba.setSpent((float)(ba.getSpent() - currCost + commSold.getQuantity()));
                            // do not update the usedValues of the soldCommodities when the action is being taken
                            return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
                        } else {
                            return new double[]{ba.getSpent() - buyer.getQuantities()[boughtIndex] +
                                                commSold.getQuantity(), 0};
                    }};

    // when taking the action, Return commSoldUsed
    // when not taking the action, return 0 if the buyer fits or INFINITY otherwise
    public static FunctionalOperator IGNORE_CONSUMPTION = (buyer, boughtIndex, commSold, seller, economy
                    , take, overhead)
                    -> {if (take) {
                            return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
                        } else {
                            return ((buyer.getQuantities()[boughtIndex] <= commSold.getCapacity()) &&
                                    (buyer.getPeakQuantities()[boughtIndex] <= commSold.getCapacity())) ?
                                    new double[]{0, 0} : new double[]{Double.POSITIVE_INFINITY,
                                                    Double.POSITIVE_INFINITY};
                        }
                    };

    public static FunctionalOperator AVG_COMMS = (buyer, boughtIndex, commSold, seller, economy
                    , take, overhead)
                    -> {
                        // consider just the buyers that consume the commodity as customers
                        CommoditySpecification csBought = buyer.getBasket().get(boughtIndex);
                        long numCustomers = seller.getCustomers().stream().filter(c ->
                                (c.getBasket().indexOf(csBought) != -1)).count();
                        // if we take the move, we have already moved and we dont need to assume a new
                        // customer. If we are not taking the move, we want to update the used considering
                        // an incoming customer. In which case, we need to increase the custoemrCount by 1
                        if (take) {
                            return new double[]{(commSold.getQuantity() * numCustomers +
                                                buyer.getQuantities()[boughtIndex])
                                                /(numCustomers),
                                                (commSold.getPeakQuantity() * numCustomers +
                                                buyer.getPeakQuantities()[boughtIndex])
                                                /(numCustomers)};
                        } else {
                            return new double[]{Math.max(commSold.getQuantity(),
                                        (commSold.getQuantity() * numCustomers
                                        + buyer.getQuantities()[boughtIndex])/(numCustomers + 1)),
                                        Math.max(commSold.getPeakQuantity(),
                                        (commSold.getPeakQuantity() * numCustomers
                                        + buyer.getPeakQuantities()[boughtIndex])/(numCustomers + 1))};
                        }
                    };

    public static FunctionalOperator MAX_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead)
                    -> new double[]{Math.max(buyer.getQuantities()[boughtIndex], commSold.getQuantity()),
                                    Math.max(buyer.getPeakQuantities()[boughtIndex], commSold.getPeakQuantity())};

    public static FunctionalOperator MIN_COMM = (buyer, boughtIndex, commSold, seller, economy, take, overhead)
                    -> new double[]{Math.min(buyer.getQuantities()[boughtIndex], commSold.getQuantity()),
                                    Math.min(buyer.getPeakQuantities()[boughtIndex], commSold.getPeakQuantity())};

    public static FunctionalOperator RETURN_BOUGHT_COMM = (buyer, boughtIndex, commSold, seller, economy
                    , take, overhead)
                    -> new double[]{buyer.getQuantities()[boughtIndex],
                                    buyer.getPeakQuantities()[boughtIndex]};

    public static FunctionalOperator EXTERNAL_UPDATING_FUNCTION =
                    (buyer, boughtIndex, commSold, seller, economy, take, overhead) -> {
                        // If we are moving, external function needs to call place on matrix
                        // interface, else do nothing
                        if (take) {
                            Topology topology = economy.getTopology();
                            // check if topology or shopping list are null
                            // Make sure we do not call place for a clone of buyer or seller
                            if (topology == null || buyer == null || seller.getCloneOf() != -1
                                            || buyer.getBuyer().getCloneOf() != -1) {
                                return new double[] {0, 0};
                            }
                            String topoId = String.valueOf(topology.getTopologyId());
                            Optional<MatrixInterface> interfaceOptional =
                                            TheMatrix.instance(topoId);
                            // Check if interface is present for given topology
                            if (interfaceOptional.isPresent()) {
                                Long buyerOid = topology.getTraderOids().get(buyer.getBuyer());
                                Long sellerOid = topology.getTraderOids().get(seller);
                                // Call Place method on interface to update matrix after placement
                                interfaceOptional.get().place(buyerOid, sellerOid);
                            }
                        }
                        return new double[] {0, 0};
                    };

}
