package com.vmturbo.platform.analysis.ese;


import java.util.ArrayList;
import java.util.List;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.recommendations.RecommendationItem;

public class Placement {

	/**
	 * Return a list of recommendations to optimize the placement of all traders in the economy.
	 *
	 * @param economy - the economy whose traders placement we want to optimize
	 * @param time - the time the placement decision algorithm is invoked
	 */
	public static List<RecommendationItem> placementDecisions(Economy economy, List<StateItem> state,
	                long time) {

		List<RecommendationItem> recommendationList = new ArrayList<RecommendationItem>();

		// iterate over all markets, i.e., all sets of providers selling a specific basket
		for (Market market: economy.getMarkets()) {

			// iterate over all buyers in this market
			for (@NonNull BuyerParticipation buyerParticipation: market.getBuyers()) {

				double quote;
    			Trader buyer = economy.getBuyer(buyerParticipation);
    			Trader currentSupplier = economy.getSupplier(buyerParticipation);
				int buyerIndex = economy.getTraders().indexOf(buyer);
    			int currentSupplierIndex = currentSupplier != null ?
    			                economy.getTraders().indexOf(currentSupplier) : -1;

    			// check if the buyer can move, and can move out of current supplier
    			if (currentSupplierIndex >= 0
    			        && (time < state.get(currentSupplierIndex).getMoveFromOnlyAfterThisTime()
    			            || time < state.get(buyerIndex).getMoveOnlyAfterThisTime())) {
    				continue;
    			}

    			// get cheapest quote
    			double cheapestQuote = Double.MAX_VALUE;
    			Trader cheapestSeller = null;
    			double currentQuote = Double.MAX_VALUE;
    			List<Trader> sellers = market.getSellers();
    			if (sellers == null) {
    			    EseCommon.recommendReconfigure(buyerParticipation, market, economy);
    			    continue;
    			}
    	    	for (Trader seller: sellers) {
    	    		// if we cannot move to this seller and it is not the current supplier, skip it
    	    		int sellerIndex = economy.getTraders().indexOf(seller);
    	    		if (!seller.equals(currentSupplier)
    	    				&& time < state.get(sellerIndex).getMoveToOnlyAfterThisTime()) {
    	    			continue;
    	    		}
    	    		quote = EseCommon.calcQuote(buyerParticipation, market, economy, seller);
    	    		if (seller.equals(currentSupplier)) {
    	    		    currentQuote = quote;
    	    		    if (cheapestQuote == Double.MAX_VALUE) {
    	    		        cheapestQuote = quote;
    	    		        cheapestSeller = seller;
    	    		    }
    	    		}
    	    		// compare quote with minQuote
    	    		if (quote < cheapestQuote) {
    	    			cheapestQuote = quote;
    	    			cheapestSeller = seller;
    	    		}
    	    	}

    	    	// move, and update economy and state
    	    	// TODO: decide how much cheaper the new supplier should be to decide to move
    			if (currentQuote > cheapestQuote) { // + market.getBasket().size() * 2.0) {
    				//TODO (Apostolos): use economy.getSettings().getQuoteFactor() above
    				// create recommendation and add it to the result list
    				recommendationList.add(EseCommon.recommendPlace(buyerParticipation, economy,
    				                market, cheapestSeller));
    				// update the economy to reflect the decision
    				moveTraderAndUpdateQuantitiesSold(buyerParticipation, currentSupplier,
    				                cheapestSeller, market.getBasket(), economy);
    				// update the state
    				int newSellerIndex = economy.getTraders().indexOf(cheapestSeller);
    				// TODO (Apostolos): use economy.getSettings().getPlacementInterval() below
    				long newTime = time + 1200000; // wait two 10 min intervals
    				state.get(newSellerIndex).setSuspendOnlyAfterThisTime(newTime);
    				state.get(buyerIndex).setMoveOnlyAfterThisTime(newTime);
    			}
			}
		}
		return recommendationList;
	}

	/**
	 * Change a buyer to become a customer of a newSupplier, and update the quantities sold by
	 * the new and the current supplier.
	 *
	 * @param buyerParticipation - the buyer participation to be moved to the new supplier
	 * @param currentSupplier - the current supplier of the buyer participation
	 * @param newSupplier - the new supplier of the buyer participation
	 * @param basket - the basket bought by the buyer participation
	 * @param economy - the economy where the buyer and the suppliers belong
	 */
	private static void moveTraderAndUpdateQuantitiesSold(BuyerParticipation buyerParticipation,
	                Trader currentSupplier, Trader newSupplier, Basket basket, Economy economy) {

	    // Add the buyer to the customers of newSupplier, and remove it from the customers of
	    // currentSupplier
	    economy.moveTrader(buyerParticipation, newSupplier);

	    // Go over all commodities in the basket and update quantities in old and new Supplier
	    int currCommSoldIndex = 0;
	    int newCommSoldIndex = 0;
	    double [] quantities = buyerParticipation.getQuantities();
        double [] peakQuantities = buyerParticipation.getPeakQuantities();
	    for (int index = 0; index < basket.size(); index++) {
            CommoditySpecification basketCommSpec = basket.getCommoditySpecifications().get(index);

            // Update current supplier
            if (currentSupplier != null) {
                // Find the corresponding commodity sold in the current supplier.
                List<CommoditySpecification> currCommSoldSpecs =
                            currentSupplier.getBasketSold().getCommoditySpecifications();
                while (!basketCommSpec.isSatisfiedBy(currCommSoldSpecs.get(currCommSoldIndex))) {
                    currCommSoldIndex++;
                }
                CommoditySold currCommSold = currentSupplier.getCommoditiesSold().get(currCommSoldIndex);

                // adjust quantities at current supplier
                // TODO: the following is problematic for commodities such as latency, peaks, etc.
                // TODO: this is the reason for the Math.min, but it is not a good solution
                currCommSold.setQuantity(Math.max(0.0, currCommSold.getQuantity() - quantities[index]));
                currCommSold.setPeakQuantity(Math.max(0.0, currCommSold.getPeakQuantity() - peakQuantities[index]));
            }

            // Update new supplier
            // Find the corresponding commodity sold in the new supplier.
            List<CommoditySpecification> newCommSoldSpecs =
                            newSupplier.getBasketSold().getCommoditySpecifications();

            while (!basketCommSpec.isSatisfiedBy(newCommSoldSpecs.get(newCommSoldIndex))) {
                newCommSoldIndex++;
            }
            CommoditySold newCommSold = newSupplier.getCommoditiesSold().get(newCommSoldIndex);

            // adjust quantities at new supplier
            newCommSold.setQuantity(newCommSold.getQuantity() + quantities[index]);
            newCommSold.setPeakQuantity(newCommSold.getPeakQuantity() + peakQuantities[index]);
        }
	}
}


	/**
	 *
	 */
	// Lets call a 'partition' the set of resources (traders) that are accessible from each
	// other.
	// Every trader buys several baskets of goods, e.g., mem/cpu from a PM and storage amount from
	// a storage. The traders it buys these baskets from have to belong to the same partition,
	// otherwise they wont be accessible from each other.
	// Let's say also that within every partition, we further create 'trader groups' of traders
	// that can satisfy the same basket request. The number of trader groups is equal to the
	// number of baskets the buyer is buying.
	// Finally, for every buyer, we associate the partitions it is buying from, and within each
	// of them the group of traders, one group per relevant basket it is buying.
	//
	// As an example, let's say we have a VM that needs to buy mem and cpu from a PM, and
	// storage amount from two different storages. Let's also say there are two partitions  of PMs
	// and storages, the first containing PM1, PM2, and ST1, ST2, ST3, and the second containing
	// PM3 and ST3 and ST4. Then partition 1 for the VM would contain trader group 1 = {PM1, PM2},
	// trader group 2 = {ST1, ST2, ST3}, and trader group 3 = {ST1, ST2, and ST3}. Partition 2 for
	// the VM would contain trader group 1 = {PM3}, trader group 2 = {ST3, ST4}, and trader
	// group 3 = {ST3, ST4}.
	//
/*	public List<ActionItem> shopTogetherPlacementDecision1(Economy economy, long decisionTime) {

		List<ActionItem> actionList = new ArrayList<ActionItem>();

		// iterate over all groups of buyers shopping together
		for (Trader buyer : economy.getBuyersShoppingTogether()) {
			// TODO (Apostolos): the following is inefficient, but I have ways to optimize it later
			double [][] quotes = new double [buyer.getBuyers().size()][economy.getTraders().size()];
			int basketIndex = 0;

			// go over all buyer participations (baskets), calculate and store the quote for each
			// potential seller
			for (BuyerParticipation buyerParticipation: buyer.getBuyers()) {
				Market market = buyerParticipation.getMarket();
				// calculate the quote for each potential seller
    			Trader currentSupplier = market.getSupplier(buyerParticipation);
				for (Trader seller: market.getSellers()) {
    	    		// if we cannot move to this seller, or it is the current supplier, skip it
    	    		int sellerEconomyIndex = economy.getTraders().indexOf(seller);
    	    		if (seller != currentSupplier
    	    				&& decisionTime < decisionState.get(sellerEconomyIndex).getMoveToOnlyAfterThisTime()) {
    	    			quotes[basketIndex][sellerEconomyIndex] = Double.MAX_VALUE;
    	    		} else {
    	    			quotes[basketIndex][sellerEconomyIndex] =
    	    					calcQuote(buyerParticipation, market, seller);
    	    		}
    	    	}
				basketIndex++;
			}

			// find the minimum total quote of all partitions, i.e., go over all partitions of the
			// sellers, and calculate the total quote of the partition, by adding the minimum for
			// every basket in that partition
			int partitionIndex = 0;
			int numberOfTraderGroups = buyer.getBuyers().size();
			double [] cheapestQuotes = new double [numberOfTraderGroups];
			int [] cheapestSellerMarketIndex = new int [numberOfTraderGroups];
			int totalPartitionQuote = 0;

			for (Partition partition: buyer.getPartitions()) {
				basketIndex = 0;
				int [] cheapestSellerInPartitionMarketIndex = new int [numberOfTraderGroups];
				double minQuote = Double.MAX_VALUE;
				double [] cheapestSellerQuote = new double [partition.getTraderGroups().size()];

				// for every basket, find the min quote of the basket
				for (List<Trader> traderGroup: partition.getTraderGroups()) {
					double quote;
					for (Trader trader: traderGroup.getTraders()) {
						quote = quotes[basketIndex][economy.getTraders().indexOf(trader)];
						if (quote < minQuote && quote > 0) {
							minQuote = quote;
							cheapestSellerInPartitionMarketIndex[basketIndex] =
									economy.getTraders().indexOf(trader);
						}
					}
					totalPartitionQuote += minQuote;
					basketIndex++;
				}
				// if the partition quote is less than the minQuote so far, replace it, and for
				// each basket keep the sellers providing it and the quote they provide (the quote
				// primarily for debugging)
				if (totalPartitionQuote < minQuote) {
					minQuote = totalPartitionQuote;
					for (basketIndex = 0; basketIndex < numberOfTraderGroups; basketIndex++) {
						cheapestSellerMarketIndex[basketIndex] =
								cheapestSellerInPartitionMarketIndex[basketIndex];
						cheapestSellerQuote[basketIndex] =
								quotes[basketIndex][cheapestSellerInPartitionMarketIndex[basketIndex]];

					}
				}
				partitionIndex++;
			}
			// at this point, for each basket we have the cheapesQuote and the cheapestSeller, so
			// we are done
		}
		// TODO (Apostolos): create the actions
		return actionList;
	}
*/

	/**
	 *
	 */
	// In contrast to the previous solution, here the mediation creates access commodities that
	// are sold by traders.
	// Lets call a 'partition' the set of resources (traders) that are accessible from each
	// other.
	// The access commodities that a trader sells correspond to partitions. A trader may be
	// selling one or more of these commodities, depending on how many partitions on belongs to.
	//
	// As an example, sao we have two partitions of PMs and storages, the first containing PM1, PM2,
	// ST1, ST2, and ST3, and the second containing PM3, and ST3 and ST4. Then PM1, PM2, ST1, ST2,
	// and ST3 will be selling an access commodity with a key P1 (partition 1), while PM3, ST3, and
	// ST4 will be selling an access commodity with a key P2 (partition 2). Notice that ST3 is
	// selling two access commodities, one with key P1, the other with key P2.
	//
	// Now assume the mediation is creating multiple baskets for each basket of good, each
	// corresponding to the different partition it may shop from.
	//
	// As an example, say a VM is buying mem and cpu from a PM. In the above example, instead of
	// creating a basket with mem and stAmt, we create two of them, the first is b1 = <mem, cpu,
	// accessCommP1>, and the second is b2 = <mem, cpu, accessCommP2>.
	//
	// Mediation is also associating with the buyer a list of the partitions.
	// Someone - MAGIC DON'T KNOW HOW TO DO IT - that is not the mediation, but somewhere in the
	// market realm, creates buyer participations for each of these multiple baskets with different
	// access commodities, and magically associates them with the particular partition of the buyer.
	//
	// For example, if the PM in the above example is shopping for a PM and two storages, then
	// the PM has partition 1, where we have buyer participation 1 (BP1) buying <mem, cpu, accessCommP1>,
	// BP2 buying <stAmt, accessCommP1>, and BP3 buying <stAmt, accessCommP1>. Similarly in
	// partition 2, we have BP4 buying <mem, cpu, accessCommP2>, BP5 buying <stAmt, accessCommP5>,
	// and BP6 buying <stAmt, accessCommP2>.
	//
/*	public List<ActionItem> shopTogetherPlacementDecision2(Economy economy, long decisionTime) {

		List<ActionItem> actionList = new ArrayList<ActionItem>();

		// iterate over all buyers with shop together baskets
		for (Trader buyer: economy.getBuyersShoppingTogether()) {
			double [] partitionTotalQuote = new double [buyer.getPartitions().size()];
			int [][] indexOfCheapestSeller = new int [buyer.getPartitions().size()]
					[buyer.getPartitions().get(0).size()];
			int partitionIndex = 0;

			// find the minimum total quote for all partitions
			for (Partition partition: buyer.getPartitions()) {
				double totalQuote = 0;
				int quoteIndex = 0;
				// find the combined quote for all buyer participations in the partition
				for (BuyerParticipation buyerParticipation: partition.getBuyerParticipations()) {
					// find the min quote for this buyerParticipation
					Market market = buyerParticipation.getMarket();
	    			Trader currentSupplier = market.getSupplier(buyerParticipation);
	    			double quote;
	    			double minQuote = Double.MAX_VALUE;
	    			Trader cheapestSeller;
					for (Trader seller: market.getSellers()) {
	    	    		// if we cannot move to this seller, or it is the current supplier, skip it
	    	    		int sellerIndex = economy.getTraders().indexOf(seller);
	    	    		if (seller != currentSupplier
	    	    				&& decisionTime < decisionState.get(sellerIndex).getMoveToOnlyAfterThisTime()) {
	    	    			quote = Double.MAX_VALUE;
	    	    		} else {
	    	    			quote = DecisionsCommon.calcQuote(buyerParticipation, market, seller);
	    	    		}
	    	    		if (quote < minQuote) {
	    	    			minQuote = quote;
	    	    			cheapestSeller = seller;
	    	    		}
	    	    	}
					totalQuote += minQuote;
					indexOfCheapestSeller[partitionIndex][quoteIndex] =
							economy.getTraders().indexOf(cheapestSeller);
					quoteIndex++;
				}
				partitionTotalQuote[partitionIndex] = totalQuote;
				partitionIndex++;
			}
		}

		//... chose the best one ...

		return actionList;
	}
*/

