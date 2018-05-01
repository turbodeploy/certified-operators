package com.vmturbo.platform.analysis.demos;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.ProvisionBySupply;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.topology.LegacyTopology;
import com.vmturbo.platform.analysis.utilities.M2Utils;

/**
 * This is a demo adding supply and demand to a sample topology using the same infrastructure that
 * will be used by the market algorithms to do the same.
 *
 * <p>
 *  It was created to be shown to Shmuel on 2016-03-03.
 * </p>
 *
 * <p>
 *  Input file saved from 10.10.172.11 in our lab.
 * </p>
 */
public final class AddSupplyAndDemandThroughActions {
    // Fields
    private static final Logger logger = LogManager.getLogger(AddSupplyAndDemandThroughActions.class);

    // Methods

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Correct usage: java AddSupplyAndDemandThroughActions \"topology-to-analyse\"");
            System.exit(0);
        }
        if (args.length > 1) {
            logger.warn("All arguments after the first were ignored!");
        }

        try {
            final int nVMsToAdd = 4;

            LegacyTopology topology = M2Utils.loadFile(args[0]);
            Economy economy = (Economy)topology.getEconomy();
            @NonNull Map<@NonNull Trader, @NonNull String> supplementaryUuids = new HashMap<>(topology.getUuids());
            @NonNull Map<@NonNull Trader, @NonNull String> supplementaryNames = new HashMap<>(topology.getNames());

            // Double Supply
            logger.info("--- Double Supply. ---");
            for (@NonNull @ReadOnly Trader trader : new ArrayList<>(economy.getTraders())) {
                if ("Abstraction:PhysicalMachine".equals(topology.getTraderTypes().getName(trader.getType()))) {
                    final ProvisionBySupply action = new ProvisionBySupply(economy, trader, new CommoditySpecification(0));

                    action.take();
                    supplementaryNames.put(action.getProvisionedSeller(), supplementaryNames.get(action.getModelSeller())+"_clone");
                    supplementaryUuids.put(action.getProvisionedSeller(), supplementaryUuids.get(action.getModelSeller())+"_CL");

                    // Log action
                    logger.info("What: " + action.debugDescription(supplementaryUuids::get, supplementaryNames::get,
                        topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
                    logger.info("Why: " + action.debugReason(supplementaryUuids::get, supplementaryNames::get,
                        topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
                    logger.info("");
                }
            }

            // Add Demand
            logger.info("--- Add " + nVMsToAdd +" VMs. ---");
            Trader modelVM = topology.getUuids().inverse().get("42230438-3374-9038-7352-07e0ba2754c1");
            for (int i = 0 ; i < nVMsToAdd ; ++i) {
                final ProvisionBySupply action = new ProvisionBySupply(economy, modelVM, new CommoditySpecification(0));

                action.take();
                supplementaryNames.put(action.getProvisionedSeller(), supplementaryNames.get(action.getModelSeller())+"_clone-"+i);
                supplementaryUuids.put(action.getProvisionedSeller(), supplementaryUuids.get(action.getModelSeller())+"_CL-"+i);

                // Log action
                logger.info("What: " + action.debugDescription(supplementaryUuids::get, supplementaryNames::get,
                    topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
                logger.info("Why: " + action.debugReason(supplementaryUuids::get, supplementaryNames::get,
                    topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
                logger.info("");
            }

            // Run simulation
            logger.info("--- Run Simulation. ---");
            List<Action> actions;
            int nCycles = 0;
            do {
                logger.info("Cycle " + (++nCycles));
                Ede ede = new Ede();
                actions = ede.generateActions(economy, true, true, true, true,
                                              "addsupplydemand"); // TODO: remove cast to Economy!
                logger.info(actions.size() + " actions");
                for (Action action : actions) {
                    logger.info("What: " + action.debugDescription(supplementaryUuids::get, supplementaryNames::get,
                        topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
                    logger.info("Why: " + action.debugReason(supplementaryUuids::get, supplementaryNames::get,
                        topology.getCommodityTypes()::getName, topology.getTraderTypes()::getName));
                    logger.info("");
                }
            } while(!actions.isEmpty());
        } catch (IOException | ParseException | ParserConfigurationException e) {
            logger.error(e.toString());
            System.exit(0);
        }
    }

} // end AddSupplyAndDemandThroughActions class
