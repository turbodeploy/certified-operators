package com.vmturbo.platform.analysis.updatingfunction;

import java.io.Serializable;
import java.util.function.DoubleUnaryOperator;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Trader;

/**
 * Serializable interface for projection functions.
 */
@FunctionalInterface
public interface ProjectionFunction extends Serializable {
    /**
     * The projection of byProduct as a function of the drivingCommodity.
     *
     * @param seller is the trader whose sold commodity is to be projected.
     * @param drivingCommodity is a sold commodity.
     * @param byProduct is the dependent sold commodity.
     *
     * @return {@link DoubleUnaryOperator} that returns the used value of the byProduct in terms of the drivingCommodity.
     */
    DoubleUnaryOperator project(Trader seller, CommoditySold drivingCommodity, CommoditySold byProduct);
}