package com.vmturbo.platform.analysis;

import static com.google.common.base.Preconditions.*;

import java.util.function.Supplier;

import org.checkerframework.checker.javari.qual.Mutable;
import org.checkerframework.checker.javari.qual.ReadOnly;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.dataflow.qual.Pure;
import org.checkerframework.dataflow.qual.SideEffectFree;

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
    @SideEffectFree Main() {}

    public static int y = 0;

    private static class Test {
        public double x;

        @Pure int foo(@ReadOnly Test this) {
            y = 2;
            x = 1;
            return y;
        }
    }

    public static void main(String[] args) {
        @NonNull String s = null;
        //final double x = 0;
        //x = 1; // error
        //final Test ft = null;
        //ft = new Test(); // error
        @ReadOnly Test t = null;
        t = new Test();
        t.x = 1;
        Test[][] table = new Test[][]{new Test[10],new Test[5]};
        @ReadOnly Test @ReadOnly [] @ReadOnly [] test = null;
        test = table;
        test[0] = new Test[3];
        test[0][0] = new Test();
        test[0][0].x = 5.0;
        test[0][0].foo();

        System.out.println(t.x);

        Supplier<@Mutable String> greeter = () -> "Hello Java 8 world!!!";
        System.out.println(greeter.get());
    }

    /**
     * A simple factorial method, whose purpose is to test that Maven runs tests properly.
     *
     * @param n The natural number whose factorial should be returned.
     * @return n!
     */
    @Pure
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
