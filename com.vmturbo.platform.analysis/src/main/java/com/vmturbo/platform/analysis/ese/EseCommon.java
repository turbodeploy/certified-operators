package com.vmturbo.platform.analysis.ese;

import java.util.List;

import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;
import com.vmturbo.platform.analysis.recommendations.RecommendationItem;

public class EseCommon {

	/**
	 * Calculate the quote of a seller for a basket bought by a buyer.
	 *
	 * @param buyerParticipation - participation buying specific quantities of the basket commodities
	 * @param - the market trading the basket bought by the buyer participation
	 * @economy - the economy where the market belongs
	 * @seller - the seller that will give the quote
	 */
    public static double calcQuote(BuyerParticipation buyerParticipation, Market market,
                    Economy economy, Trader seller) {
    	//TODO (Apostolos): we have not dealt with equivalent commodities
    	double quote = 0.0;
    	boolean isCurrentSupplier = (economy.getSupplier(buyerParticipation) != null
    	                && economy.getSupplier(buyerParticipation).equals(seller));
    	int commSoldIndex = 0;

    	// get the quantity and peak quantity to buy for each commodity of the basket
    	double [] quantities = buyerParticipation.getQuantities();
    	double [] peakQuantities = buyerParticipation.getPeakQuantities();

    	// go over all commodities in basket
    	for (int index = 0; index < market.getBasket().size(); index++) {
    		CommoditySpecification basketCommSpec =
    				market.getBasket().getCommoditySpecifications().get(index);

    		// Find corresponding commodity sold. Commodities sold are ordered the same way as the
    		// basket commodities, so iterate once (O(N)) as opposed to searching each time (O(NLog(N))
    		List<CommoditySpecification> commSoldSpecs =
    		                seller.getBasketSold().getCommoditySpecifications();
    		while (!basketCommSpec.isSatisfiedBy(commSoldSpecs.get(commSoldIndex))) {
    		    commSoldIndex++;
    		}
    		CommoditySold commSold = seller.getCommoditiesSold().get(commSoldIndex);

    		// add quantities bought by buyer, to quantities already used at seller
    		double capacity = commSold.getCapacity();
    		double utilization = isCurrentSupplier ? commSold.getUtilization() :
    			(quantities[index] + commSold.getQuantity()) / capacity;
    		double peakUtilization = isCurrentSupplier ? commSold.getPeakUtilization() :
    			(peakQuantities[index] + commSold.getPeakQuantity()) / capacity;
    		if (isCurrentSupplier) {
    		    utilization = commSold.getUtilization();
    		    peakUtilization = commSold.getPeakUtilization();
    		}
    		double utilUpperBound = commSold.getSettings().getUtilizationUpperBound();

    		// calculate the price per unit for quantity and peak quantity
    		PriceFunction pf = commSold.getSettings().getPriceFunction();
    		double priceUsed = pf.unitPrice(utilization, utilUpperBound);
    		double pricePeak = pf.unitPeakPrice(utilization, peakUtilization, utilUpperBound);

    		// calculate quote
    		// TODO (Apostolos): consider removing 100 from everywhere in the code. It is redundant
    		quote += 100.0 * (quantities[index] / capacity * priceUsed
    				+ (peakQuantities[index] - quantities[index]) / capacity * pricePeak);

    	}
    	return quote;
    }

    /**
     * Create and return a placement {@link RecommendationItem recommendation item}.
     *
     * @param buyerParticipation - the buyer participation to be moved
     * @param economy - the economy where the Buyer belongs
     * @param newSupplier - the new Seller where we are going to move the Buyer
     */
    public static RecommendationItem recommendPlace(BuyerParticipation buyerParticipation,
                    Economy economy, Market market, Trader newSupplier) {
    	int buyerIndex = economy.getTraders().indexOf(economy.getBuyer(buyerParticipation));
    	Trader currentSupplier = economy.getSupplier(buyerParticipation);
    	int currentSupplierIndex = currentSupplier != null ?
    	                economy.getTraders().indexOf(currentSupplier) : -1;
    	int newSupplierIndex = newSupplier != null ? economy.getTraders().indexOf(newSupplier) : -1;
    	String description = "";
    	String reason = "";

    	// if there is no current supplier, recommend to start buyer in new supplier
    	if (currentSupplier == null) {
    	       description = "Start: " + buyerIndex + " in: " + newSupplierIndex;
    	       reason = "Buyer is currently not placed in any supplier selling " +
    	                market.getBasket().toString();
    	       return(new RecommendationItem(description, reason));
    	}

    	description = "Move: " + buyerIndex + " from: " +  currentSupplierIndex + " to: " +
    	                newSupplierIndex;

    	if (market.getSellers().contains(currentSupplier)) {
    	    reason = "bla bla bla";
    	} else { // current supplier is not part of the market, i.e. not selling what the buyer wants
    	    reason = "Current supplier: " + currentSupplierIndex +
    	                    " is not selling one or more of the following: " +
    	                    market.getBasket().toString();
    	}
    	return(new RecommendationItem(description, reason));
    }

    /**
     * Create and return a reconfigure {@link RecommendationItem recommendation item}.
     *
     * @param buyerParticipation - the buyer participation to be moved
     * @param market - the market where the buyer participation is shopping
     * @param economy - the economy where the Buyer belongs
     */
    public static RecommendationItem recommendReconfigure(BuyerParticipation buyerParticipation,
                    Market market, Economy economy) {
        int buyerIndex = economy.getTraders().indexOf(economy.getBuyer(buyerParticipation));
        String description = "Reconfigure: " + buyerIndex;
        String reason = "There are no suppliers selling: " + market.getBasket().toString();
        return(new RecommendationItem(description, reason));
    }
}
