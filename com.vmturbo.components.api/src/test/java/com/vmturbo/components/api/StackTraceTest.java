package com.vmturbo.components.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Optional;

import org.junit.Test;

/**
 * Unit tests for {@link StackTrace} utilities.
 */
public class StackTraceTest {

    /**
     * Test the {@link StackTrace#getCaller()} method.
     */
    @Test
    public void testStackTraceCaller() {
        // One extra level of nesting, or else the "caller" is considered to be the JUnit framework.
        doCallerTest();
    }

    /**
     * Test the {@link StackTrace#getCaller()} method within an Optional.
     */
    @Test
    public void testStackTraceInOptCaller() {
        // One extra level of nesting, or else the "caller" is considered to be the JUnit framework.
        doOptionalCallerTest();
    }

    /**
     * Test the {@link StackTrace#getCallerOutsideClass()} method.
     */
    @Test
    public void testCallerOutsideClass() {
        final TestHelperClass helper = new TestHelperClass();
        helper.run();
    }

    /**
     * Helper utility for the {@link StackTrace#getCallerOutsideClass()} method.
     */
    private static class TestHelperClass {

        void run() {
            // One extra level of nesting. The caller method should pass through this method, up to
            // the caller of run().
            internalRun();
        }

        void internalRun() {
            final StackTraceElement[] stackTrace = new Throwable().getStackTrace();
            final String caller = StackTrace.getCallerOutsideClass();
            final String expected;
            if (stackTrace.length > 3) {
                // The caller of the entry point to the class is 2 methods up, plus one
                // because the stack trace begins inside this method.
                expected = stackTrace[2].getFileName() + ":" + stackTrace[2].getLineNumber();
            } else {
                expected = "";
            }
            assertThat(caller, is(expected));
        }
    }

    private void doOptionalCallerTest() {
        final Optional<Integer> opt = Optional.of(1);
        opt.ifPresent((i) -> {
            final StackTraceElement[] stackTrace = new Throwable().getStackTrace();
            final String caller = StackTrace.getCaller();
            final String expected;
            if (stackTrace.length > 3) {
                // The caller of this method, which is the place where we call "isPresent".
                // We skip the intermediate "Optional.ifPresent()" stack trace element.
                expected = stackTrace[2].getFileName() + ":" + stackTrace[2].getLineNumber();
            } else {
                expected = "";
            }
            assertThat(caller, is(expected));
        });
    }

    private void doCallerTest() {
        final StackTraceElement[] stackTrace = new Throwable().getStackTrace();
        String caller = StackTrace.getCaller();
        final String expected;
        if (stackTrace.length > 2) {
            // The caller of this method.
            expected = stackTrace[1].getFileName() + ":" + stackTrace[1].getLineNumber();
        } else {
            expected = "";
        }
        assertThat(caller, is(expected));
    }
}