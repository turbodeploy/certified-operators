package com.vmturbo.platform.analysis.utilities;

/*
 * There is currently an implicit assumption that the ternary combinator we can use takes the
 * number of customers as the third argument
 *
 */
@FunctionalInterface
public interface DoubleTernaryOperator {
    double applyAsDouble(double arg1, double arg2, double arg3);
}