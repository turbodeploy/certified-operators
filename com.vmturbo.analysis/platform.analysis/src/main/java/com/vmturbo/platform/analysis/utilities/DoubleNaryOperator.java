package com.vmturbo.platform.analysis.utilities;
/**
 * convert multiple arguments to a single double.
 * There is currently an implicit assumption that the Nary combinator we can use takes the
 * number of customers as the third argument
 **/
@FunctionalInterface
public interface DoubleNaryOperator {
    /**
     * converts the arguments into a double.
     *
     * @param arg1 the previous quantity
     * @param arg2 the change in capacity or new capacity.
     * @param arg3 number of customers
     * @param arg4 the original capacity.
     * @return combines the arguments based on the operator.
     */
    double applyAsDouble(double arg1, double arg2, double arg3, double arg4);
}