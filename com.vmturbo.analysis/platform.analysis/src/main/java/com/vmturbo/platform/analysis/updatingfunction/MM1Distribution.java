package com.vmturbo.platform.analysis.updatingfunction;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderWithSettings;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs.UpdatingFunctionTO.MM1Commodity;
import com.vmturbo.platform.sdk.common.util.Pair;

/**
 * Implements the MM1 distribution function.
 */
public class MM1Distribution implements UpdatingFunction {

    private static final Logger logger = LogManager.getLogger();

    private List<MM1Commodity> dependentCommodities;
    /**
     * A factor to cap the usage of a trader below the capacity during distribution.
     */
    private static final double CAPACITY_FACTOR = 0.99F;
    private float minDecreasePct;

    /**
     * Set the dependent commodities for the MM1 distribution function.
     *
     * @param dependentCommodities a list of dependent {@link MM1Commodity} required to compute
     *                             MM1 distribution
     * @return the {@link MM1Distribution} instance
     */
    public MM1Distribution setDependentCommodities(List<MM1Commodity> dependentCommodities) {
        this.dependentCommodities = dependentCommodities;
        return this;
    }

    /**
     * Set the minimum desired quantity drop percentage for the MM1 distribution function.
     *
     * @param minDecreasePct the minimum desired quantity drop percentage for the MM1 distribution
     * @return the {@link MM1Distribution} instance
     */
    public MM1Distribution setMinDecreasePct(final float minDecreasePct) {
        this.minDecreasePct = minDecreasePct;
        return this;
    }

    /**
     * Get the minimum desired quantity drop percentage for the MM1 distribution function.
     *
     * @return the minimum desired quantity drop percentage for the MM1 distribution function
     */
    public float getMinDecreasePct() {
        return minDecreasePct;
    }

    /**
     * Create a distribution function which distributes commodity values based on M/M/1 queuing model.
     * Provision, projected = current x ((N x capacity - N x used)/((N+1) x capacity - N x used)) ^ elasticity
     * Suspension, projected = current x ((N x capacity - N x used)/((N-1) x capacity - N x used)) ^ elasticity
     *
     * <p>A real example. One service consumes one app hosted by a container.
     * The app sells 300(ms) response time. The service requests 2000(ms) response time from the app.
     * The container sells 575(MHz) VCPU. The application requests 515(MHz) VCPU.
     *
     * <p>After one provision, using the MM1 based projection with elasticity 1.0, the service should request
     * Projected = 2000 x (1 x 575 - 515) / (2 x 575 - 515) = 189 (ms) response time from each of the 2 apps.
     * This represents more than 90% drop of the response time after one provision.
     * As a comparison, using the standard distribution without VCPU headroom, the service would request
     * Projected = 2000 x 1 / 2 = 1000 (ms) response time from each of the 2 apps.
     * This represents only a 50% drop of the response time after one provision.
     *
     * @param clonedSL   the cloned {@link ShoppingList} in the case of provision, or
     *                   the {@link ShoppingList} on the suspended {@link Trader} in the
     *                   case of suspension
     * @param index      the index to the commodity specification in a {@link Basket}
     * @param commSold   not used
     * @param seller     the seller being cloned for provision, or the seller being suspended for
     *                   suspension
     * @param economy    not used
     * @param take       not used
     * @param overhead   not used
     * @param currentSLs the current set of {@link ShoppingList} that belong to the same guaranteed
     *                   buyer
     * @return an array containing the updated quantity and peak quantity on the seller
     */
    @Override
    public double[] operate(ShoppingList clonedSL,
                            int index,
                            @Nonnull CommoditySold commSold,
                            @Nonnull Trader seller,
                            @Nonnull UnmodifiableEconomy economy,
                            boolean take,
                            double overhead,
                            @Nullable Set<ShoppingList> currentSLs) {
        if (currentSLs == null) {
            // There is nothing to distribute
            return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
        }
        boolean isProvision = UpdatingFunctionFactory.isProvision(clonedSL, currentSLs);
        // Aggregate quantities across all shopping lists
        final double originalQuantitySum = currentSLs.stream()
                .peek(sl -> {
                    if (logger.isTraceEnabled()) {
                        logger.trace("M/M/1: {} to sum for {}: {}",
                                sl.getBasket().get(index).getDebugInfoNeverUseInCode(),
                                sl.getDebugInfoNeverUseInCode(), sl.getQuantity(index));
                   }
                })
                .mapToDouble(sl -> sl.getQuantity(index)).sum();
        final int originalSize = currentSLs.size();
        final int newSize = originalSize + (isProvision ? 1 : -1);
        final double originalAvgQuantity = originalQuantitySum / originalSize;
        double projectedQuantity = originalAvgQuantity;
        double projectedPeakQuantity = projectedQuantity;
        if (logger.isTraceEnabled()) {
            logger.trace("M/M/1: Average quantity: {}", originalAvgQuantity);
        }
        List<Pair<Integer, Double>> projectedQuantityDependentComms = new ArrayList<>();
        final Set<TraderWithSettings> currentSuppliers = currentSLs.stream()
                .map(ShoppingList::getSupplier)
                .filter(TraderWithSettings.class::isInstance)
                .map(TraderWithSettings.class::cast)
                .collect(Collectors.toSet());
        // Compute the projected quantity
        for (MM1Commodity mm1Commodity : dependentCommodities) {
            final List<Pair<Double, Double>> dependentCommUsedAndCapacity = currentSuppliers.stream()
                    .map(trader -> trader.getBoughtQuantityAndCapacity(mm1Commodity.getCommodityType()))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .peek(uc -> {
                        if (logger.isTraceEnabled()) {
                            logger.trace("M/M/1: Dependent commodity quantity: {}, capacity {} ",
                                    uc.getFirst(), uc.getSecond());
                        }
                    })
                    .collect(Collectors.toList());
            if (dependentCommUsedAndCapacity.isEmpty()) {
                // No dependent commodities, do not distribute
                logger.error("Missing dependent commodity {} for trader {}.",
                        mm1Commodity.getCommodityType(), seller.getDebugInfoNeverUseInCode());
                return new double[]{commSold.getQuantity(), commSold.getPeakQuantity()};
            }
            final double totalCapacity = dependentCommUsedAndCapacity.stream()
                    .mapToDouble(Pair::getSecond).sum();
            double totalUsed = dependentCommUsedAndCapacity.stream()
                    .mapToDouble(Pair::getFirst).sum();
            final double projectedTotalCapacity = totalCapacity * newSize / originalSize;
            // We assume total usage does not change
            double projectedTotalUsed = totalUsed;
            if (logger.isTraceEnabled()) {
                logger.trace("M/M/1: Dependent commodity total capacity: {}",
                        totalCapacity);
                logger.trace("M/M/1: Dependent commodity total used: {}",
                        totalUsed);
                logger.trace("M/M/1: Dependent commodity projected total capacity: {}",
                        projectedTotalCapacity);
                logger.trace("M/M/1: Dependent commodity projected total used: {}",
                        projectedTotalUsed);
            }
            // This is to protect against the rare cases where resource usage may be larger than
            // capacity. This can happen with a container in a kubernetes cluster, where the VCPU
            // usage of a container may be marginally higher than the VCPU limit of the container
            // due to cgroup implementation. As a result, the application may be consuming VCPU
            // larger than the capacity.
            if (totalUsed >= totalCapacity) {
                totalUsed = totalCapacity * CAPACITY_FACTOR;
                if (logger.isTraceEnabled()) {
                    logger.trace("M/M/1: Cap dependent commodity total used to {}",
                            totalUsed);
                }
            }
            // We calculate the projected resource headroom in order to calculate the projected SLO
            // commodity. During suspension, the projected resource usage may go above the capacity.
            // For example, imagine there are two apps:
            // - app1: used 251 MHz, capacity 500 MHz
            // - app2: used 252 MHz, capacity 500 MHz
            // After suspension of app1, the projected app2 becomes: used 503 MHz, capacity 500 MHz.
            // In this case, we need to cap the used to below capacity.
            if (projectedTotalUsed >= projectedTotalCapacity) {
                projectedTotalUsed = projectedTotalCapacity * CAPACITY_FACTOR;
                if (logger.isTraceEnabled()) {
                    logger.trace("M/M/1: Cap dependent commodity projected total used to {}",
                            projectedTotalUsed);
                }
            }
            final double projectedAvgUsed = projectedTotalUsed / newSize;
            projectedQuantityDependentComms.add(new Pair<>(mm1Commodity.getCommodityType(), projectedAvgUsed));
            // Calculate the updated response time based on the total workload M/M/1 formula
            projectedQuantity = projectedQuantity * Math.pow(
                    (totalCapacity - totalUsed) / (projectedTotalCapacity - projectedTotalUsed),
                    mm1Commodity.getElasticity());
            if (logger.isTraceEnabled()) {
                logger.trace("M/M/1: Projection: buyers ({} => {}),"
                                + " total quantity {}, total capacity ({} => {}), Response "
                                + "Time ({} => {})",
                        originalSize, newSize,
                        totalUsed, totalCapacity, projectedTotalCapacity,
                        originalAvgQuantity, projectedQuantity);
            }
            // As of now, peak is the same
            projectedPeakQuantity = projectedQuantity;
        }
        // Distribute
        UpdatingFunctionFactory.distributeOnCurrent(currentSLs, index, projectedQuantity,
                projectedPeakQuantity, projectedQuantityDependentComms);
        if (isProvision) {
            UpdatingFunctionFactory.distributeOnClone(index, clonedSL, projectedQuantity,
                    projectedPeakQuantity, projectedQuantityDependentComms);
        }
        return new double[]{projectedQuantity, projectedPeakQuantity};
    }
}
