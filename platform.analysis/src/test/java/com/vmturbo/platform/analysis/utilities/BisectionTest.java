package com.vmturbo.platform.analysis.utilities;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.function.DoubleUnaryOperator;

import com.vmturbo.platform.analysis.utilities.Bisection;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

public class BisectionTest {

    @Test
    public void testBisectionStandardPricingFunction() {
        double expectedRoot = 0.7;
        double target = 11.1111 * expectedRoot; // p(0.7) = 11.11
        double accuracy = 1.0E-2;
        PriceFunction pfunc = PriceFunction.Cache.createStandardWeightedPriceFunction(1.0);
        double intervalBegin = .01;
        double intervalEnd = 0.99;
        int maxIterations = 100;
        try {
            DoubleUnaryOperator func = (u) -> u * pfunc.unitPrice(u) - target;
            double root = Bisection.solve(accuracy, maxIterations, func, intervalBegin, intervalEnd);
            assertEquals(expectedRoot, root, accuracy);
        }
        catch (Exception e) {
            assertTrue(false);
        }
    }
}
