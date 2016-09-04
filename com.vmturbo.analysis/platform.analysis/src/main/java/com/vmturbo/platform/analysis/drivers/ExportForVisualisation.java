package com.vmturbo.platform.analysis.drivers;

import java.io.IOException;
import java.text.ParseException;
import java.util.function.Function;
import java.util.function.IntFunction;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.Logger;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import com.google.common.base.Strings;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.economy.UnmodifiableEconomy;
import com.vmturbo.platform.analysis.ede.Ede;
import com.vmturbo.platform.analysis.topology.LegacyTopology;
import com.vmturbo.platform.analysis.utilities.M2Utils;

/**
 * A simple driver program that runs the placement algorithm on a loaded Economy and exports the
 * data in a form easily loadable to Excel for further processing.
 *
 * <p>
 *  The intention is that the user runs this program with a topology as input and then copies the
 *  output and pastes it in an Excel sheet.
 * </p>
 */
public final class ExportForVisualisation {
    // Fields

    private static final Logger logger = Logger.getLogger(ExportForVisualisation.class);

    // Methods

    public static void main(String[] args) {
        if (args.length < 1) {
            logger.error("Correct usage: java ExportForVisualisation \"topology-to-analyse\"");
            System.exit(0);
        }
        if (args.length > 1) {
            logger.warn("All arguments after the first were ignored!");
        }

        try {
            LegacyTopology topology = M2Utils.loadFile(args[0]);

            System.out.println("Economy\tTrader Index\tTrader UUID\tTrader Name\tTrader Type\t"
                + "Commodity Index\tCommodity Type\tUtilization\tPeak Utilization\tQuantity\t"
                + "Peak Quantity\tCapacity\tEffective Capacity");
            printEconomy(topology.getEconomy(), "Original",topology.getUuids()::get,topology.getNames()::get,
                         topology.getCommodityTypes()::getName,topology.getTraderTypes()::getName);

            Ede ede = new Ede();
            ede.generateActions((Economy)topology.getEconomy(), false, true, true, true); // TODO: remove cast
            printEconomy(topology.getEconomy(), "Optimized",topology.getUuids()::get,topology.getNames()::get,
                         topology.getCommodityTypes()::getName,topology.getTraderTypes()::getName);
        } catch (IOException | ParseException | ParserConfigurationException e) {
            logger.error(e.toString());
            System.exit(0);
        }
    }

    private static void printEconomy(@NonNull UnmodifiableEconomy economy, @NonNull String economyName,
                                     @NonNull Function<@NonNull Trader, @NonNull String> traderUuid,
                                     @NonNull Function<@NonNull Trader, @NonNull String> traderName,
                                     @NonNull IntFunction<@NonNull String> commodityType,
                                     @NonNull IntFunction<@NonNull String> traderType) {
        final String tabSeparatedFieldsLine = Strings.repeat("%s\t", 12) + "%s\n";

        for (@NonNull @ReadOnly Trader trader : economy.getTraders()) {
            for (int i = 0 ; i < trader.getBasketSold().size() ; ++i) {
                System.out.printf(tabSeparatedFieldsLine,
                    economyName,
                    trader.getEconomyIndex(),
                    traderUuid.apply(trader),
                    traderName.apply(trader),
                    traderType.apply(trader.getType()),
                    i,
                    commodityType.apply(trader.getBasketSold().get(i).getType()),
                    trader.getCommoditiesSold().get(i).getUtilization(),
                    trader.getCommoditiesSold().get(i).getPeakUtilization(),
                    trader.getCommoditiesSold().get(i).getQuantity(),
                    trader.getCommoditiesSold().get(i).getPeakQuantity(),
                    trader.getCommoditiesSold().get(i).getCapacity(),
                    trader.getCommoditiesSold().get(i).getEffectiveCapacity()
                );
            }
        }
    }

} // end class ExportForVisualisation
