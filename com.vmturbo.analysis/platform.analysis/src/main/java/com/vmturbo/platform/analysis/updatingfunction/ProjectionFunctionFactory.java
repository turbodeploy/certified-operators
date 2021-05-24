package com.vmturbo.platform.analysis.updatingfunction;

import java.util.function.DoubleUnaryOperator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs;

/**
 * The factory class to construct cost function.
 */
public class ProjectionFunctionFactory {

    private static final Logger logger = LogManager.getLogger();

    private ProjectionFunctionFactory() {
    }

    /**
     * Creates {@link ProjectionFunction} for a given commodity with by-product.
     *
     * @param updatingFunctionTO the DTO carries the update function information
     * @return ProjectionFunction
     */
    public static @NonNull ProjectionFunction createProjectionFunction(UpdatingFunctionDTOs.UpdatingFunctionTO updatingFunctionTO) {
        switch (updatingFunctionTO.getUpdatingFunctionTypeCase()) {
            case MM1_DISTRIBUTION:
                // return MM1 projection function.
                return MM1;
            default:
                throw new IllegalArgumentException("input = " + updatingFunctionTO.getUpdatingFunctionTypeCase());
        }
    }

    /**
     * Create {@link ProjectionFunction} for MM1 projection.
     * byProductUtil' = byProductUtil * normalizedResizingCommUtil' * (1 - resizingCommUtil)
     *                                  ----------------------------------------------------
     *                                  resizingCommUtil * (1 - normalizedResizingCommUtil')
     *
     * @return ProjectionFunction
     */
    public static final ProjectionFunction MM1 = new ProjectionFunction() {
        @Override
        public DoubleUnaryOperator project(Trader seller, CommoditySold resizingCommodity, double byProductUtilization) {
            return u -> {
                // 'u' here is going to be a percentile utilization. We do not know the percentile numbers for both the
                // byproduct and the resizingCommodity simultaneously. In this case, we want to have all the terms in
                // the same unit and so we normalize the percentile utilization into a smoothened average utilization representation.
                double normalizedAverageUtil = u * (resizingCommodity.getQuantity()
                        / resizingCommodity.getHistoricalOrElseCurrentQuantity());
                return (byProductUtilization * normalizedAverageUtil) * (1 - resizingCommodity.getUtilization())
                            / (resizingCommodity.getUtilization() * (1 - normalizedAverageUtil));
            };
        }
    };

}
