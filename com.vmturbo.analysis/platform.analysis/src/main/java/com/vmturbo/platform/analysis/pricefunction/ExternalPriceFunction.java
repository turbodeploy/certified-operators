package com.vmturbo.platform.analysis.pricefunction;

import java.util.Optional;

import com.vmturbo.matrix.component.TheMatrix;
import com.vmturbo.matrix.component.external.MatrixInterface;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.ShoppingList;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.topology.Topology;

/**
 * ExternalPriceFunction.
 */
public class ExternalPriceFunction implements PriceFunction {

    /**
     * The price of one unit of normalized utilization. When a trader wants to
     * buy say 30% utilization, it will be charged 0.3 of the unit price.
     * @param normalizedUtilization the utilization as a percentage of the utilization
     * admissible for the {@link CommoditySold}
     * @param shoppingList is the consumer's shoppingList.
     * @param seller is the {@link Trader} selling the commodity
     * @param cs is the {@link CommoditySold} by the seller
     * @param e is the {@link UnmodifiableEconomy} that the seller resides in
     * @return the price that will be charged for 100% of the capacity for a particular commodity
     *          sold by a seller
     */
    public double unitPrice(double normalizedUtilization, ShoppingList shoppingList, Trader seller, CommoditySold cs,
                            UnmodifiableEconomy e) {
        double price = 0d;
        // Get the topology id from economy
        Topology topo = e.getTopology();
        // Price calculation happens from places which don't require
        // this information so skip calling external pf.
        // Buyer or seller clones will not be in matrix yet.
        // FIXME: Clone creation is not notified to matrix interface
        if (topo == null || shoppingList == null || seller.getCloneOf() != -1
                || shoppingList.getBuyer().getCloneOf() != -1) {
            return price;
        }
        // Use topology id to get matrix interface
        Optional<MatrixInterface> interfaceOptional = TheMatrix.instance(topo.getTopologyId());
        // If consumer passed to price function is null, just return 0 price, as flow
        // price only makes sense in the context of a specific consumer
        if (interfaceOptional.isPresent()) {
            // Find OIds of traders and send them to calculate price
            Long buyerOid = shoppingList.getBuyer().getOid();
            Long sellerOid = seller.getOid();
            price = interfaceOptional.get().calculatePrice(buyerOid, sellerOid);
            // This multiplication done to negate the effect of dividing price by
            // effective capacity during quote computation. EdeCommon.computeCommodityCost() on M2 side
            price = price * cs.getEffectiveCapacity();
        }
        return price;
    }
}