package com.vmturbo.platform.analysis.economy;

import java.io.Serializable;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;

import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.updatingfunction.ProjectionFunction;

/**
 * A class representing the ByProducts.
 */
public class ByProducts implements Serializable {

    Map<Integer, ProjectionFunction> byProducts_;

    /**
     * Constructor for the ByProducts.
     *
     * @param byProducts has all the byProduct-ProjectionFunction mapping.
     */
    public ByProducts(@NonNull Map<Integer, ProjectionFunction> byProducts) {
        byProducts_ = byProducts;
    }

    /**
     * Return the rawMaterial to ProjectionFunction mapping.
     *
     * @return mapping between byProduct to ProjectionFunction.
     */
    public Map<Integer, ProjectionFunction> getByProductMap() {
        return byProducts_;
    }

    /**
     * Return the base types of byProducts.
     *
     * @return baseType of byProducts of a commodity.
     */
    public Set<Integer> getByProducts() {
        return byProducts_.keySet();
    }

    /**
     * Return the projection function for a specific byProduct.
     * @param byProduct is the base type of the byProduct.
     * @return the {@link ProjectionFunction} for a specific byProduct.
     */
    @Pure
    public @NonNull Optional<ProjectionFunction> getProjectionFunction(@NonNull Integer byProduct) {
        return Optional.ofNullable(byProducts_.get(byProduct));
    }

    /**
     * Calculates the revenue due to the by product entry passed.
     * @param seller is the entity whose sold commodities we compute revenue for.
     * @param byProductDescriptor has the mapping between byProduct to projectionFunction.
     * @param resizingCommodity is the resizing commodity.
     * @param u is the projected utilization.
     * @param economy is the economy that the trader participates in.
     *
     * @return the revenue due to the byproduct entry passed.
     */
    public static double computeRevenueOfByProduct(Trader seller, Map.Entry<Integer, ProjectionFunction> byProductDescriptor,
                                   CommoditySold resizingCommodity, double u, Economy economy) {
        int soldIndex = seller.getBasketSold().indexOfBaseType(byProductDescriptor.getKey());
        if (soldIndex != -1) {
            CommoditySold byProduct = seller.getCommoditiesSold().get(soldIndex);
            PriceFunction bpPriceFunction = byProduct.getSettings().getPriceFunction();
            double projectedUtil = byProductDescriptor.getValue()
                    .project(seller, resizingCommodity, byProduct).applyAsDouble(u);
            return projectedUtil * bpPriceFunction
                    .unitPrice((projectedUtil), null, seller, resizingCommodity, economy);
        }
        return 0;
    }
}
