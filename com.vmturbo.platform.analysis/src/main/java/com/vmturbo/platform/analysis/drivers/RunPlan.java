package com.vmturbo.platform.analysis.drivers;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.topology.LegacyTopology;
import com.vmturbo.platform.analysis.utilities.M2Utils;

/**
 * An application that takes the path to a legacy topology file as an argument, runs the loaded
 * Economy as a plan and lists the resulting actions to the standard output.
 */
public final class RunPlan {
    // Fields
    private static final Logger logger = Logger.getLogger(RunPlan.class);

    // Methods
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
            List<Action> allActions = new ArrayList<>();
            boolean keepRunning = true;
            int i = 0;
            while (keepRunning) {
                logger.info("Cycle " + (++i));
                Ede ede = new Ede();
                List<Action> actions = ede.generateActions((Economy)topology.getEconomy()); // TODO: remove cast to Economy!
                logger.info(actions.size() + " actions");
                for (Action action : actions) {
                    logger.info("What: " + action.debugDescription(topology.getUuids()::get, topology.getNames()::get,
                        topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
                    logger.info("Why: " + action.debugReason(topology.getUuids()::get, topology.getNames()::get,
                        topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
                    logger.info("");
                }
                keepRunning = !actions.isEmpty();
                allActions.addAll(actions);
            }
            logger.info("Before collapse : " + allActions.size());
            List<Action> collapsedActions = Action.collapsed(allActions);
            logger.info("After collapse : " + collapsedActions.size());
        } catch (FileNotFoundException e) {
            logger.error(e.toString());
            System.exit(0);
        }
    }

} // end RunPlan class
