package com.vmturbo.platform.analysis.utilities;

import org.junit.Test;
import static org.junit.Assert.*;

import java.util.function.DoubleBinaryOperator;
import java.util.function.DoubleUnaryOperator;

import com.vmturbo.platform.analysis.utilities.Bisection;
import com.vmturbo.platform.analysis.pricefunction.PriceFunction;

public class BisectionTest {

    @Test
    public void testBisectionStandardPricingFunction() {
        double capacityIncrement = 100;
        double currentQuantity = 1000;
        double expectedRoot = 0.7;
        double expectedCapacity = currentQuantity / expectedRoot;
        double target = 11.1111 * expectedRoot; // p(0.7) = 11.11
        PriceFunction pfunc = PriceFunction.Cache.createStandardWeightedPriceFunction(1.0);
        double intervalBegin = .01;
        double intervalEnd = 0.99;
        int maxIterations = 53;
        try {
            DoubleUnaryOperator func = (u) -> u * pfunc.unitPrice(u) - target;
            DoubleBinaryOperator errorFunction = (x, y) -> currentQuantity / x - currentQuantity / y;
            double root = Bisection.solve(capacityIncrement, errorFunction, maxIterations, func, intervalBegin, intervalEnd);
            double newCapacity = currentQuantity / root;
            assertEquals(expectedCapacity, newCapacity, capacityIncrement);
        }
        catch (Exception e) {
            assertTrue(false);
        }
    }
}
