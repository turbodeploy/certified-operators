package com.vmturbo.platform.analysis;

import static com.google.common.base.Preconditions.*;

import java.util.function.Supplier;

/**
 * The Main class for the application.
 *
 * <p>
 *  Currently it only serves to test that the project is build correctly,
 *  but later it will serve as the entry point for the application that
 *  will run alongside OperationsManager to test the Market 2 prototype
 *  on select customers.
 * </p>
 */
public final class Main {

    public static void main(String[] args) {
        Supplier<String> greeter = () -> "Hello Java 8 world!!!";
        System.out.println(greeter.get());
    }

    /**
     * A simple factorial method, whose purpose is to test that Maven runs tests properly.
     *
     * @param n The natural number whose factorial should be returned.
     * @return n!
     */
    public static long factorial(int n) {
        checkArgument(n >= 0);

        long p = 1;
        while (n > 1) {
            p *= n;
            --n;
        }

        return p;
    }
}
