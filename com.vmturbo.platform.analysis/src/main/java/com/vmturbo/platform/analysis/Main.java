package com.vmturbo.platform.analysis;

import java.io.FileNotFoundException;
import java.util.List;
import org.apache.log4j.Logger;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.recommendations.RecommendationItem;
import com.vmturbo.platform.analysis.topology.LegacyTopology;
import com.vmturbo.platform.analysis.utilities.M2Utils;

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

        try {
            LegacyTopology topology = M2Utils.loadFile(args[0]);
            Ede ede = new Ede();
            List<RecommendationItem> recommendations = ede.createRecommendations((Economy)topology.getEconomy());
                // TODO: remove cast to Economy!
            logger.info(recommendations.size() + " recommendations");
            for (RecommendationItem recommendation : recommendations) {
                logger.info("What: " + description(recommendation, topology));
                logger.info("Why:  " + reason(recommendation));
                logger.info("");
            }
        } catch (FileNotFoundException e) {
            logger.error(e.toString());
            System.exit(0);
        }
    }

    private static String traderString(LegacyTopology topology, Trader trader) {
        return String.format("%s [%s] (#%d)", topology.getNames().get(trader),
            topology.getUuids().get(trader), topology.getEconomy().getIndex(trader));
    }

    /**
     * Create and return a description for the recommendation.
     */
    public static String description(RecommendationItem recommendation, LegacyTopology topology) {
        final String buyer = traderString(topology, recommendation.getBuyer());

        if (recommendation.getNewSupplier() == null) { // reconfigure
            return "Reconfigure " + buyer;
        } else if (recommendation.getCurrentSupplier() != null) { // move
            return "Move " + buyer + " from " + traderString(topology, recommendation.getCurrentSupplier())
                + " to " + traderString(topology, recommendation.getNewSupplier());
        } else { // start
            return "Start " + buyer + " on " + traderString(topology, recommendation.getNewSupplier());
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

} // end Main class
