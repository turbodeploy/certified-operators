package com.vmturbo.platform.analysis;

import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;

import com.vmturbo.platform.analysis.economy.Basket;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Market;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.TraderState;
import com.vmturbo.platform.analysis.ese.Ede;
import com.vmturbo.platform.analysis.recommendations.RecommendationItem;
import com.vmturbo.platform.analysis.utilities.M2Utils;
import com.vmturbo.platform.analysis.utilities.M2Utils.TopologyMapping;

/**
 * The Main class for the application.
 *
 * <p>
 *  Currently it only serves to test that the project is build correctly,
 *  but later it will serve as the entry point for the application that
 *  will run alongside OperationsManager to test the Market 2 prototype
 *  on select customers.
 * </p>
 */
public final class Main {

    private static final Logger logger = Logger.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Correct usage: java Main \"topology-to-analyse\"");
            System.exit(0);
        }
        if (args.length > 1) {
            logger.warn("All arguments after the first were ignored!");
        }

        TopologyMapping mapping = M2Utils.loadFile(args[0]);
        Economy economy = mapping.getTopology().getEconomy();
        Ede ede = new Ede();
        List<RecommendationItem> recommendations = ede.createRecommendations(economy);
        logger.info(recommendations.size() + " recommendations");
        for (RecommendationItem recommendation : recommendations) {
            logger.info("What: " + description(recommendation, mapping));
            logger.info("Why:  " + reason(recommendation));
            logger.info("");
        }
    }

    private static String traderString(TopologyMapping mapping, Trader trader) {
        Economy economy = mapping.getTopology().getEconomy();
        int i = economy.getIndex(trader);
        return String.format("%s (#%d)", mapping.getTraderName(i), i);
    }

    /**
     * Create and return a description for the recommendation.
     */
    public static String description(RecommendationItem recommendation, TopologyMapping mapping) {
        final String buyer = traderString(mapping, recommendation.getBuyer());

        if (recommendation.getNewSupplier() == null) { // reconfigure
            return "Reconfigure " + buyer;
        } else if (recommendation.getCurrentSupplier() != null) { // move
            return "Move " + buyer + " from " + traderString(mapping, recommendation.getCurrentSupplier())
                + " to " + traderString(mapping, recommendation.getNewSupplier());
        } else { // start
            return "Start " + buyer + " on " + traderString(mapping, recommendation.getNewSupplier());
        }
    }

    /**
     * Create and return a reason for the recommendation.
     */
    public static String reason(RecommendationItem recommendation) {
        if (recommendation.getNewSupplier() == null) { // reconfigure
            return "There are no suppliers selling: " + recommendation.getMarket().getBasket().toString();
        } else if (recommendation.getCurrentSupplier() != null) { // move
            if (recommendation.getMarket().getSellers().contains(recommendation.getCurrentSupplier())) {
                return "bla bla bla"; // TODO (Apostolos): put something reasonable here!
            } else { // current supplier is not part of the market, i.e. not selling what the buyer wants
                return "Current supplier is not selling one or more of the following: "
                + recommendation.getMarket().getBasket().toString();
            }
        } else { // start
            return "Buyer is currently not placed in any supplier selling "
                        + recommendation.getMarket().getBasket().toString();
        }
    }

    public static void sampleTopology() {
        CommoditySpecification cpu = new CommoditySpecification(0);
        Basket basket1 = new Basket(cpu,new CommoditySpecification(1),
                                    new CommoditySpecification(2),new CommoditySpecification(3));
        Basket basket2 = new Basket(new CommoditySpecification(4),new CommoditySpecification(5));

        Economy economy = new Economy();
        Trader trader1 = economy.addTrader(0, TraderState.ACTIVE, new Basket(), basket1, basket2, basket2);
        Trader trader2 = economy.addTrader(1, TraderState.ACTIVE, basket1);
        Trader trader3 = economy.addTrader(2, TraderState.ACTIVE, basket2);
        Trader trader4 = economy.addTrader(2, TraderState.ACTIVE, basket2);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket1)).get(0), trader2);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket2)).get(0), trader3);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket2)).get(1), trader4);

        trader2.getCommoditySold(cpu).setCapacity(100);
        economy.getCommodityBought(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket1)).get(0),cpu).setQuantity(42);

        for (Trader supplier : economy.getSuppliers(trader1)) {
            System.out.println(supplier.getType());
        }

        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket1)).get(0), null);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket2)).get(0), null);
        economy.moveTrader(economy.getMarketsAsBuyer(trader1).get(economy.getMarket(basket2)).get(1), null);

        for (Map.Entry<Market, BuyerParticipation> entry : economy.getMarketsAsBuyer(trader1).entries()) {
            if (entry.getValue().getSupplier() == null) {
                System.out.print("Trader1 is not currently buying basket ");
                System.out.print(entry.getKey().getBasket());
                System.out.println(" from anyone!");
            }
        }

        economy = new Economy();
        trader1 = economy.addTrader(0, TraderState.ACTIVE, new Basket());
        trader2 = economy.addTrader(1, TraderState.ACTIVE, basket1);
        trader3 = economy.addTrader(2, TraderState.ACTIVE, basket2);
        trader4 = economy.addTrader(2, TraderState.ACTIVE, basket2);
        economy.moveTrader(economy.addBasketBought(trader1, basket1), trader2);
        economy.moveTrader(economy.addBasketBought(trader1, basket2), trader3);
        economy.moveTrader(economy.addBasketBought(trader1, basket2), trader4);
    }

} // end Main class
