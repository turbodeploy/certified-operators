package com.vmturbo.platform.analysis;

import java.io.FileNotFoundException;
import java.util.List;
import org.apache.log4j.Logger;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.ede.Ede;
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
            boolean keepRunning = true;
            int i = 0;
            while (keepRunning) {
                logger.info("Cycle " + (++i));
                Ede ede = new Ede();
                List<Action> actions = ede.generateActions((Economy)topology.getEconomy()); // TODO: remove cast to Economy!
                logger.info(actions.size() + " actions");
                for (Action action : actions) {
                    logger.info("What: " + action.debugDescription(topology.getUuids()::get, topology.getNames()::get,
                        topology.getTraderTypes()::getName, topology.getCommodityTypes()::getName));
                    logger.info("Why: " + action.debugReason(topology.getUuids()::get, topology.getNames()::get,
                        topology.getTraderTypes()::getName, topology.getCommodityTypes()::getName));
                    logger.info("");
                }
                keepRunning = !actions.isEmpty();
            }
        } catch (FileNotFoundException e) {
            logger.error(e.toString());
            System.exit(0);
        }
    }

} // end Main class
