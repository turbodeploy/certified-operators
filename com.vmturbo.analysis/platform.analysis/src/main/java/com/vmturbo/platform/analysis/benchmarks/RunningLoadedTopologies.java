package com.vmturbo.platform.analysis.benchmarks;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;

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
 *  "topologies/pnc-customer.markets.topology"
 */
public final class RunningLoadedTopologies {
    // Fields
    private static final Logger logger = LogManager.getLogger(RunningLoadedTopologies.class);

    // Methods
    public static void main(String[] topologies) {
        if (topologies.length < 1) {
            logger.warn("No arguments supplied!");
            logger.warn("Correct usage: java RunningLoadedTopologies \"topology1\"...\"topologyN\".");
            System.exit(0);
        }

        List<Economy> economies = new ArrayList<>(topologies.length);
        LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
        Configuration config = ctx.getConfiguration();
        LoggerConfig loggerConfig = config.getLoggerConfig(logger.getName());
        loggerConfig.setLevel(Level.ERROR);
        ctx.updateLoggers();

        for (int i = 0 ; i < topologies.length ; ++i) {
            try {
                System.out.printf("Loading topology %48s:", topologies[i]);
                long start = System.nanoTime();
                Economy economy = (Economy)M2Utils.loadFile(topologies[i],logger).getEconomy(); // TODO: remove cast
                economies.add(economy);
                System.out.printf("%,17dns %7s\n", System.nanoTime()-start, economy.getTraders().size());
            }
            catch (IOException | ParseException | ParserConfigurationException e) {
                System.out.println(); // to finish the line we were printing when the exception was
                                      // thrown and allow the error to be printed in a new one.
                logger.error(e);
            }
        }

        for (Economy economy : economies) {
            for (int i = 0 ; i < 3 ; ++i) {
                Ede ede = new Ede();
                long start = System.nanoTime();
                ede.generateActions(economy, true, true, true, true, "runloadtopo");
                System.out.print(System.nanoTime()-start + "\t");
            }
            System.out.println();
        }

        System.out.println("Done!");
    }

} // end RunningLoadedTopologies benchmark
