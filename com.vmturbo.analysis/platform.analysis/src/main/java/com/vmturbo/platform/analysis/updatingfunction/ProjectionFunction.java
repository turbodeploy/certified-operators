package com.vmturbo.platform.analysis.updatingfunction;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Trader;

import java.io.Serializable;
import java.util.function.DoubleUnaryOperator;

@FunctionalInterface
public interface ProjectionFunction extends Serializable {
    DoubleUnaryOperator project(Trader seller, CommoditySold drivingCommodity, CommoditySold byProduct);
}