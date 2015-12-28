package com.vmturbo.platform.analysis.benchmarks;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.utilities.M2Utils;

/**
 * A benchmark that loads a number of topologies given as arguments and runs the Economic Decision
 * Engine on each one multiple times printing the running times for further analysis.
 *
 * <p>
 *  Sample input:
 *
 *  "topologies/small-customer.repos.topology"
 *  "topologies/peak10-customer.repos.topology"
 *  "topologies/allscript-customer.markets.topology"
 *  "topologies/allscript-customer.repos.topology"
 *  "topologies/barclays1-customer.markets.topology"
 *  "topologies/barclays1-customer.repos.topology"
 *  "topologies/barclays2-customer.markets.topology"
 *  "topologies/barclays2-customer.repos.topology"
 *  "topologies/bkp-mayo-customer.markets.topology"
 *  "topologies/interoute-customer.markets.topology"
 *  "topologies/jndata-customer.markets.topology"
 *  "topologies/jndata-customer.repos.topology"
 */
public final class RunningLoadedTopologies {
    // Fields
    private static final Logger logger = Logger.getLogger(RunningLoadedTopologies.class);

    // Methods
    public static void main(String[] topologies) {
        if (topologies.length < 1) {
            logger.warn("No arguments supplied!");
            logger.warn("Correct usage: java RunningLoadedTopologies \"topology1\"...\"topologyN\".");
            System.exit(0);
        }

        List<Economy> economies = new ArrayList<>(topologies.length);
        logger.setLevel(Level.ERROR);

        for (int i = 0 ; i < topologies.length ; ++i) {
            try {
                System.out.printf("Loading topology %48s:", topologies[i]);
                long start = System.nanoTime();
                Economy economy = M2Utils.loadFile(topologies[i],logger).getTopology().getEconomy();
                economies.add(economy);
                System.out.printf("%,17dns %7s\n", System.nanoTime()-start, economy.getTraders().size());
            }
            catch (FileNotFoundException e) {
                System.out.println(); // to finish the line we were printing when the exception was
                                      // thrown and allow the error to be printed in a new one.
                logger.error(e);
            }
        }

        for (Economy economy : economies) {
            for (int i = 0 ; i < 3 ; ++i) {
                Ede ede = new Ede();
                long start = System.nanoTime();
                ede.createRecommendations(economy);
                System.out.print(System.nanoTime()-start + "\t");
            }
            System.out.println();
        }

        System.out.println("Done!");
    }

} // end RunningLoadedTopologies benchmark
