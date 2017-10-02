package com.vmturbo.components.test.utilities.alert.report;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import org.junit.Test;

import com.vmturbo.components.test.utilities.alert.AlertMetricId;
import com.vmturbo.components.test.utilities.alert.MetricMeasurement;

public class PerformanceTestNameComparatorTest {

    private final PerformanceTestNameComparator comparator = new PerformanceTestNameComparator();

    @Test
    public void testCompareEmpty() {
        List<MetricMeasurement> names = Collections.emptyList();
        names.sort(comparator);
        assertTrue(names.isEmpty());
    }

    @Test
    public void testCompareOneElement() {
        List<MetricMeasurement> names = Collections.singletonList(measurement("Foo123"));
        names.sort(comparator);
        final List<String> sorted = names.stream()
            .map(MetricMeasurement::getTestName)
            .collect(Collectors.toList());

        assertEquals(Collections.singletonList("Foo123"), sorted);
    }

    @Test
    public void testCompareTwoElements() {
        List<MetricMeasurement> names = Arrays.asList(measurement("Foo123"), measurement("Bar123"));
        names.sort(comparator);
        final List<String> sorted = names.stream()
            .map(MetricMeasurement::getTestName)
            .collect(Collectors.toList());

        assertEquals(Arrays.asList("Bar123", "Foo123"), sorted);
    }

    @Test
    public void testCompareNonParseable() {
        assertEquals(-1, comparator.compare(measurement("a"), measurement("b")));
        assertEquals(0, comparator.compare(measurement("a"), measurement("a")));
        assertEquals(1, comparator.compare(measurement("b"), measurement("a")));
    }

    @Test
    public void testCompareParseable() {
        assertEquals(-1, comparator.compare(measurement("testA1"), measurement("testB1")));
        assertEquals(0, comparator.compare(measurement("testA1"), measurement("testA1")));
        assertEquals(1, comparator.compare(measurement("testB1"), measurement("testA1")));
    }

    @Test
    public void testCompareParseableSameNameDifferentNumber() {
        assertEquals(-1, comparator.compare(measurement("testA2"), measurement("testA10")));
        assertEquals(0, comparator.compare(measurement("testA10"), measurement("testA10")));
        assertEquals(1, comparator.compare(measurement("testA10"), measurement("testA2")));
    }

    @Test
    public void testCompareParseableTestNames() {
        final List<MetricMeasurement> names = Arrays.asList(
            measurement("testFoo10K"),
            measurement("testFoo100K"),
            measurement("testFoo50K"),
            measurement("testBar50K"),
            measurement("testBar25K"));
        names.sort(comparator);
        final List<String> sorted = names.stream()
            .map(MetricMeasurement::getTestName)
            .collect(Collectors.toList());

        assertEquals(
            Arrays.asList(
                "testBar25K",
                "testBar50K",
                "testFoo10K",
                "testFoo50K",
                "testFoo100K"),
            sorted
        );
    }

    @Test
    public void testCompareNumberThenDescription() {
        final List<MetricMeasurement> names = Arrays.asList(
            measurement("test10kFoo"),
            measurement("test100kFoo"),
            measurement("test50kFoo"),
            measurement("test50kBar"),
            measurement("test25kBar"));
        names.sort(comparator);
        final List<String> sorted = names.stream()
            .map(MetricMeasurement::getTestName)
            .collect(Collectors.toList());

        assertEquals(
            Arrays.asList(
                "test25kBar",
                "test50kBar",
                "test10kFoo",
                "test50kFoo",
                "test100kFoo"),
            sorted
        );
    }

    @Test
    public void testCompareMixedOrder() {
        final List<MetricMeasurement> names = Arrays.asList(
            measurement("test10kFoo"),
            measurement("test100kFoo"),
            measurement("test50kFoo"),
            measurement("testBar50K"),
            measurement("testBar25K"));
        names.sort(comparator);
        final List<String> sorted = names.stream()
            .map(MetricMeasurement::getTestName)
            .collect(Collectors.toList());

        assertEquals(
            Arrays.asList(
                "testBar25K",
                "testBar50K",
                "test10kFoo",
                "test50kFoo",
                "test100kFoo"),
            sorted
        );
    }

    private MetricMeasurement measurement(@Nonnull final String testName) {
        return new MetricMeasurement(0, 0, 0, 0, AlertMetricId.fromString("a"), testName, "");
    }
}