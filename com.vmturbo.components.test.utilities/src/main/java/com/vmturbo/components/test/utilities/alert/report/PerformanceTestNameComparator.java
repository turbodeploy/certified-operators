package com.vmturbo.components.test.utilities.alert.report;

import java.util.Comparator;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import com.vmturbo.components.test.utilities.alert.MetricMeasurement;

/**
 * Test names follow the scheme "test(feature tested)(size of test)"
 * example: "testDiscoveryAndBroadcast10K" or "test100kActionPlan"
 *
 * We want to sort so that tests with similar features are grouped together
 * but sorted by their sizes. A simple alphabetical sort will not achieve this.
 *
 * For example:
 *
 * testFoo10K
 * testFoo100K
 * testFoo50K
 * testBar50K
 * testBar25K
 *
 * when sorted with a PerformanceTestNameComparator should produce the order
 *
 * testBar25K
 * testBar50K
 * testFoo10K
 * testFoo50K
 * testFoo100K
 *
 * Tests without numbers are simply sorted alphabetically. The numeric suffix (ie "K", "M", "G", etc.)
 * on the test is not currently accounted for in the comparison.
 */
public class PerformanceTestNameComparator implements Comparator<MetricMeasurement> {

    @Override
    public int compare(MetricMeasurement a, MetricMeasurement b) {
        final NameAndNumber nameAndNumberA = NameAndNumber.parse(a.getTestName());
        final NameAndNumber nameAndNumberB = NameAndNumber.parse(b.getTestName());

        int comparison = nameAndNumberA.getName().compareTo(nameAndNumberB.getName());
        if (comparison == 0) {
            comparison = Integer.compare(nameAndNumberA.getNumber(), nameAndNumberB.getNumber());
        }

        return comparison;
    }

    /**
     * Pairs a name with a number, and contains a utility suitable for parsing the two
     * from a typical test name.
     */
    @Immutable
    private static class NameAndNumber {
        private static final Pattern TEST_DESCRIPTION_THEN_SIZE_PATTERN = Pattern.compile("test(\\D+)(\\d+)");
        private static final Pattern TEST_SIZE_THEN_DESCRIPTION_PATTERN = Pattern.compile("test(\\d+)(\\D+)");

        private final String name;
        private final int number;

        private NameAndNumber(@Nonnull final String name, final int number) {
            this.name = Objects.requireNonNull(name);
            this.number = number;
        }

        /**
         * Attempt to parse out name and number from the test name.
         * If the parse fails, the name is set to the full test name and number is set to 0.
         *
         * @param testName The name of the test to parse.
         * @return The {@link PerformanceTestNameComparator.NameAndNumber}
         *         for the testName.
         */
        public static NameAndNumber parse(@Nonnull final String testName) {
            Matcher match = TEST_DESCRIPTION_THEN_SIZE_PATTERN.matcher(Objects.requireNonNull(testName));
            if (match.find() && match.groupCount() == 2) {
                return new NameAndNumber(match.group(1), Integer.parseInt(match.group(2)));
            } else {
                match = TEST_SIZE_THEN_DESCRIPTION_PATTERN.matcher(testName);
                if (match.find() && match.groupCount() == 2) {
                    return new NameAndNumber(match.group(2), Integer.parseInt(match.group(1)));
                } else {
                    return new NameAndNumber(testName, 0);
                }
            }
        }

        @Nonnull
        public String getName() {
            return name;
        }

        public int getNumber() {
            return number;
        }
    }
}
