package com.vmturbo.components.api;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.junit.Test;

/**
 * Unit tests for the {@link FormattedString} utility.
 */
public class FormattedStringTest {

    /**
     * Test simple formatting.
     */
    @Test
    public void testFormat() {
        test("My left is better than my right", "My {} is better than my {}", "left", "right");
    }

    /**
     * Test that delimiters with a space between them don't count as a placeholder.
     */
    @Test
    public void testFormatSpaceBetweenDelimiters() {
        test("I love spaghetti this - { } much", "I love spaghetti this - { } much");
    }

    /**
     * Test insufficient number of args.
     */
    @Test
    public void testNotEnoughArgs() {
        test("The result of my work is null", "The result of my work is {}");
    }

    /**
     * Test null object argument.
     */
    @Test
    public void testNullArg() {
        Object obj = null;
        test("The result of my work is null", "The result of my work is {}", obj);
    }

    /**
     * Test too many arguments. Ignore the leftovers.
     */
    @Test
    public void testTooManyArgs() {
        test("I get knocked down", "I get knocked {}", "down", "up");
    }

    /**
     * Verify that escaping doesn't affect behaviour. Sorry, no {} in allowed.
     */
    @Test
    public void testEscapedDelimitersDontWork() {
        test("There is no \\escape!", "There is no \\{}!", "escape");
    }

    /**
     * Verify that objects get included correctly.
     */
    @Test
    public void testFormatObject() {
        List<String> obj = Collections.singletonList("foo");
        String objectStr  = obj.toString();
        test("My object is " + objectStr, "My object is {}", obj);
    }

    /**
     * Suppliers throwing random exceptions? Put them in the string!
     */
    @Test
    public void testSupplierException() {
        RuntimeException exception = new RuntimeException("BOO!");
        Supplier<Object> supplier = () -> {
            throw exception;
        };
        String result = new FormattedString("I don't expect {}", supplier).get();
        assertThat(result, is("I don't expect " + exception.toString()));
    }

    /**
     * Utility method to verify a particular case with all the constructors.
     *
     * @param expected The expected string (should be the same regardless of which constructor we use).
     * @param pattern The pattern.
     * @param args The arguments.
     */
    private void test(String expected, String pattern, final Object... args) {
        String formatResult = FormattedString.format(pattern, args);
        assertThat(formatResult, is(expected));

        String resultViaObject = new FormattedString(pattern, args).get();
        assertThat(resultViaObject, is(expected));

        Supplier<Object>[] suppliers = new Supplier[args.length];
        for (int i = 0; i < args.length; ++i) {
            final int idx = i;
            suppliers[idx] = () -> args[idx];
        }
        String resultViaSuppliers = new FormattedString(pattern, suppliers).get();
        assertThat(resultViaSuppliers, is(expected));
    }
}