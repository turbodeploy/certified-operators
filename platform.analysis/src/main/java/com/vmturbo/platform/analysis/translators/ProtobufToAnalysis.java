package com.vmturbo.platform.analysis.translators;

import java.util.function.LongFunction;

import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.actions.Action;
import com.vmturbo.platform.analysis.actions.Activate;
import com.vmturbo.platform.analysis.actions.Deactivate;
import com.vmturbo.platform.analysis.actions.Move;
import com.vmturbo.platform.analysis.actions.ProvisionByDemand;
import com.vmturbo.platform.analysis.actions.Reconfigure;
import com.vmturbo.platform.analysis.actions.Resize;
import com.vmturbo.platform.analysis.economy.BuyerParticipation;
import com.vmturbo.platform.analysis.economy.CommoditySpecification;
import com.vmturbo.platform.analysis.economy.Economy;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.ActionDTOs.ActionTO;
import com.vmturbo.platform.analysis.protobuf.EconomyDTOs.CommoditySpecificationTO;

/**
 * A class containing methods to convert protobuf messages to java classes used by analysis.
 */
public final class ProtobufToAnalysis {
    // Methods
    public static @NonNull CommoditySpecification commoditySpecification(@NonNull CommoditySpecificationTO input) {
        return new CommoditySpecification(input.getType(),input.getQualityLowerBound(),input.getQualityUpperBound());
    }

    public static @NonNull Action action(@NonNull ActionTO input, @NonNull Economy economy,
            LongFunction<BuyerParticipation> participation, LongFunction<Trader> trader) {
        switch (input.getActionSpecializationCase()) {
            case MOVE:
                return new Move(economy, participation.apply(input.getMove().getShoppingList()),
                    trader.apply(input.getMove().getSource()), trader.apply(input.getMove().getDestination()));
            case RECONFIGURE:
                return new Reconfigure(economy, participation.apply(input.getReconfigure().getShoppingList()));
            case ACTIVATE:
                return new Activate(trader.apply(input.getActivate().getTarget()), null/* not supported */);
            case DEACTIVATE:
                return new Deactivate(trader.apply(input.getDeactivate().getTarget()), null/* not supported */);
            case PROVISION_BY_DEMAND:
                return new ProvisionByDemand(economy, participation.apply(input.getProvisionByDemand().getModelBuyer()));
            case PROVISION_BY_SUPPLY:
                return new ProvisionByDemand(economy, participation.apply(input.getProvisionBySupply().getModelSeller()));
            case RESIZE:
                return new Resize(trader.apply(input.getResize().getSellingTrader()),
                    commoditySpecification(input.getResize().getSpecification()),
                    input.getResize().getOldCapacity(),input.getResize().getNewCapacity());
            case ACTIONSPECIALIZATION_NOT_SET:
            default:
                throw new IllegalArgumentException("input = " + input);
        }
    }

} // end ProtobufToAnalysis class
