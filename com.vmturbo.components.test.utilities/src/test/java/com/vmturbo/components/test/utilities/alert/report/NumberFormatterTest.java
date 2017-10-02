package com.vmturbo.components.test.utilities.alert.report;

import static org.junit.Assert.assertEquals;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class NumberFormatterTest {
    private final NumberFormatter formatter = new NumberFormatter();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public static <K, V> Map.Entry<K, V> entry(K key, V value) {
        return new AbstractMap.SimpleEntry<>(key, value);
    }

    final Map<Double, String> formattingTestCases = Collections.unmodifiableMap(Stream.of(
        entry(0.0, "0"),
        entry(1.0, "1"),
        entry(1.4, "1.4"),
        entry(1.49999, "1.49999"),
        entry(1.4999901, "1.5"),
        entry(999.9999, "999.9999"),
        entry(-999.9999, "-999.9999"),

        entry(1_000.0, "1k"),
        entry(1_100.0, "1.1k"),
        entry(1_110.0, "1.11k"),
        entry(1_111.0, "1.11k"),
        entry(11_111.0, "11.11k"),
        entry(111_111.0, "111k"),

        entry(1_111_111.0, "1.11M"),
        entry(11_111_111.0, "11.11M"),

        entry(9_999_999_999.0, "9.99G"),

        entry(99_999_999_999_999.0, "99.99T"),

        entry(9_188_999_999_999_999.0, "9.18P"),
        entry(-9_188_999_999_999_999.0, "-9.18P")
    ).collect(Collectors.toMap(Entry::getKey, Entry::getValue)));

    @Test
    public void testFormatting() {
        formattingTestCases.forEach((testCase, expectedValue) ->
                assertEquals(String.format("Expected format(%f) to equal %s", testCase, expectedValue),
                    expectedValue,
                    formatter.format(testCase))
        );
    }

    private static final class NumberWithSuffix {
        public final double number;
        public final String suffix;

        private NumberWithSuffix(final double number, @Nonnull final String suffix) {
            this.number = number;
            this.suffix = suffix;
        }

        @Override
        public String toString() {
            return number + suffix;
        }
    }

    public static NumberWithSuffix numberSuffix(final double number, @Nonnull final String suffix) {
        return new NumberWithSuffix(number, suffix);
    }

    final Map<NumberWithSuffix, Double> parsingTestCases = Collections.unmodifiableMap(Stream.of(
        entry(numberSuffix(0, "k"), 0.0),
        entry(numberSuffix(1, "k"), 1_000.0),
        entry(numberSuffix(-1, "k"), -1_000.0),
        entry(numberSuffix(0.01, "k"), 10.0),
        entry(numberSuffix(1.1, "k"), 1_100.0),
        entry(numberSuffix(1.11, "k"), 1_110.0),
        entry(numberSuffix(11.11, "k"), 11_110.0),
        entry(numberSuffix(111.1, "k"), 111_100.0),
        entry(numberSuffix(111.1, "kb"), 111_100.0),

        entry(numberSuffix(191.1, "M"), 191_100_000.0),
        entry(numberSuffix(191.1, "mb"), 191_100_000.0),
        entry(numberSuffix(191.1, "Mb"), 191_100_000.0),

        entry(numberSuffix(191.1, "g"), 191_100_000_000.0),
        entry(numberSuffix(191.1, "gb"), 191_100_000_000.0),
        entry(numberSuffix(191.1, "GB"), 191_100_000_000.0),

        entry(numberSuffix(191.1, "t"), 191_100_000_000_000.0),
        entry(numberSuffix(191.1, "tb"), 191_100_000_000_000.0),

        entry(numberSuffix(191.1, "p"), 191_100_000_000_000_000.0),
        entry(numberSuffix(191.1, "pb"), 191_100_000_000_000_000.0),

        entry(numberSuffix(191.1, "e"), 191_100_000_000_000_000_000.0),
        entry(numberSuffix(191.1, "eb"), 191_100_000_000_000_000_000.0),
        entry(numberSuffix(-191.1, "eb"), -191_100_000_000_000_000_000.0),

        entry(numberSuffix(1.1, "s"), 1.1),
        entry(numberSuffix(-1.1, "s"), -1.1),
        entry(numberSuffix(1.1, "sec"), 1.1),
        entry(numberSuffix(1.1, "S"), 1.1),
        entry(numberSuffix(1.1, "SEC"), 1.1),
        entry(numberSuffix(1.1, "second"), 1.1),
        entry(numberSuffix(1.1, "seconds"), 1.1),

        entry(numberSuffix(1.1, "m"), 66.0),
        entry(numberSuffix(1.1, "min"), 66.0),
        entry(numberSuffix(1.1, "minute"), 66.0),
        entry(numberSuffix(2.5, "minutes"), 150.0),

        entry(numberSuffix(1.0, "h"), 3600.0),
        entry(numberSuffix(1.0, "hour"), 3600.0),
        entry(numberSuffix(1.0, "hours"), 3600.0),

        entry(numberSuffix(3.0, "d"), 3.0 * 24 * 60 * 60),
        entry(numberSuffix(3.0, "day"), 3.0 * 24 * 60 * 60)
    ).collect(Collectors.toMap(Entry::getKey, Entry::getValue)));

    @Test
    public void testTransformToBase() {
        parsingTestCases.forEach((testCase, expectedValue) ->
            assertEquals(
                "Expected " + testCase + " to convert to " + expectedValue,
                NumberFormatter.transformToBaseUnitBySuffix(testCase.number, testCase.suffix),
                expectedValue,
                expectedValue * 0.00001
            ));
    }

    @Test
    public void testTransformThrowsException() {
        expectedException.expect(NumberFormatException.class);
        NumberFormatter.transformToBaseUnitBySuffix(1.0, "foo");
    }
}