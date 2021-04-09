package com.vmturbo.platform.analysis.updatingfunction;

import com.vmturbo.platform.analysis.economy.CommoditySold;
import com.vmturbo.platform.analysis.economy.Trader;
import com.vmturbo.platform.analysis.protobuf.UpdatingFunctionDTOs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.util.function.DoubleUnaryOperator;

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
     * byProductUtil' = byProductUtil * resizingCommUtil' * (1 - resizingCommUtil)
     *                                  ------------------------------------------
     *                                  resizingCommUtil * (1 - resizingCommUtil')
     *
     * @return ProjectionFunction
     */
    public static final ProjectionFunction MM1 = new ProjectionFunction() {
        @Override
        public DoubleUnaryOperator project(Trader seller, CommoditySold resizingCommodity, CommoditySold byProduct) {
            return u -> (byProduct.getUtilization() * u * (1 - resizingCommodity.getUtilization()))
                            / (resizingCommodity.getUtilization() * (1 - u));
        }
    };

}
