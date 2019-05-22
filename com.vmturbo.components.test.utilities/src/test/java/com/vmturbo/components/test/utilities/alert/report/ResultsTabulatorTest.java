package com.vmturbo.components.test.utilities.alert.report;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.junit.Test;

import de.vandermeer.asciithemes.a7.A7_Grids;

import com.vmturbo.components.test.utilities.alert.AlertMetricId;
import com.vmturbo.components.test.utilities.alert.MetricMeasurement;
import com.vmturbo.components.test.utilities.alert.MetricMeasurement.AlertStatus;
import com.vmturbo.components.test.utilities.alert.RuleSignature;
import com.vmturbo.components.test.utilities.alert.report.ResultsTabulator.RuleSignatureEntryComparator;

/**
 * Integration tests for the tables produced by a {@link ResultsTabulator}.
 */
public class ResultsTabulatorTest {

    /**
     * Our tests have to use minusBarPlusEquals format so that the table only generates valid ASCII
     * characters. This allows us to include the tables as strings for comparison in the test.
     *
     * The default character set uses unicode and is prettier and easier to read, so use that
     * in production code. However, ReviewBoard does not allow us to upload diffs containing non-ASCII
     * characters so for that reason use ASCII in the strings here.
     */
    private final ResultsTabulator tabulator = new ResultsTabulator(
        Optional.of(A7_Grids.minusBarPlusEquals()));

    @Test
    public void testRenderEmpty() {
        assertEquals(0, tabulator.renderMeasurements(Stream.empty()).count());
    }

    @Test
    public void testRenderOneMeasurement() {
        final String table = tabulator.renderMeasurements(Stream.of(makeMeasurement("metric", "test", "class")))
            .collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                        |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test                 |4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderUndefinedStdDev() {
        final MetricMeasurement measurement = new MetricMeasurement(1.0, 2.0, Optional.empty(), 4.0,
            AlertMetricId.fromString("metric"), "test", "class");
        final String table = tabulator.renderMeasurements(Stream.of(measurement))
            .collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                        |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test                 |4         |1         |2          |undefined |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderAlertStatusImprovement() {
        final MetricMeasurement measurement = new MetricMeasurement(1.0, 2.0, 3.0, 4.0,
            AlertMetricId.fromString("metric"), "test", "class");
        measurement.setAlertStatus(AlertStatus.IMPROVEMENT);
        final String table = tabulator.renderMeasurements(Stream.of(measurement))
            .collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                        |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test                 |4         |1         |2          |3         |improvement|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderAlertStatusRegression() {
        final MetricMeasurement measurement = new MetricMeasurement(4.0, 2.0, 3.0, 4.0,
            AlertMetricId.fromString("metric"), "test", "class");
        measurement.setAlertStatus(AlertStatus.REGRESSION);
        final String table = tabulator.renderMeasurements(Stream.of(measurement))
            .collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                        |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test                 |4         |4         |2          |3         |regression |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderAlertStatusSlaViolation() {
        final MetricMeasurement measurement = new MetricMeasurement(4.0, 2.0, 3.0, 4.0,
            AlertMetricId.fromString("metric/1s"), "test", "class");
        measurement.setAlertStatus(AlertStatus.SLA_VIOLATION);
        final String table = tabulator.renderMeasurements(Stream.of(measurement))
            .collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                        |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test                 |4         |4         |2          |3         |sla violatn|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderTwoMeasurementsSameSignature() {
        final String table = tabulator.renderMeasurements(
            Stream.of(
                makeMeasurement("metric", "testDiscoveryAndBroadcast100K", "class"),
                makeMeasurement("metric", "testDiscoveryAndBroadcast200K", "class")
            )).collect(Collectors.joining("\n\n"));

        assertEquals(
            "+--------------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                                |" + System.lineSeparator() +
            "+-----------------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name                    |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+-----------------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|testDiscoveryAndBroadcast100K|4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+-----------------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|testDiscoveryAndBroadcast200K|4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+-----------------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderTwoMeasurementsDifferentSignature() {
        final String table = tabulator.renderMeasurements(
            Stream.of(
                makeMeasurement("metric1", "test1", "class"),
                makeMeasurement("metric2", "test2", "class")
            )).collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric1)                                                       |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test1                |4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" +
            "\n\n" +
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric2)                                                       |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test2                |4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderLongMeasurementName() {
        final String table = tabulator.renderMeasurements(Stream.of(
                makeMeasurement("metric", "testDiscoveryAndBroadcastLongName200K", "class"))
        ).collect(Collectors.joining("\n\n"));

        assertEquals(
            "+----------------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                                  |" + System.lineSeparator() +
            "+-------------------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name                      |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+-------------------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|testDiscoveryAndBroadcastLongNa|4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "|me200K                         |          |          |           |          |           |" + System.lineSeparator() +
            "+-------------------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderLongDecimalPart() {
        final String table = tabulator.renderMeasurements(
            Stream.of(new MetricMeasurement(1.28935728935629384882, 2.0, 3.0, 4.0,
                AlertMetricId.fromString("metric"), "test", "class"))
        ).collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                        |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test                 |4         |1.28936   |2          |3         |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderLongDigitPart() {
        final String table = tabulator.renderMeasurements(Stream.of(new MetricMeasurement(12893572893562.9384882, 2.0, 3.0, 4.0,
                AlertMetricId.fromString("metric"), "test", "class"))
        ).collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|class (metric: metric)                                                        |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test                 |4         |12.89T    |2          |3         |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderVeryLongTitle() {
        final String table = tabulator.renderMeasurements(Stream.of(
                new MetricMeasurement(12893572893562.9384882, 2.0, 3.0, 4.0,
                    AlertMetricId.fromString("ao_populate_store_duration_seconds_sum{store_type='Live'}"),
                    "test100kActionPlan", "ActionOrchestratorPerformanceTest"))
        ).collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|ActionOrchestratorPerformanceTest (metric:                                    |" + System.lineSeparator() +
            "|ao_populate_store_duration_seconds_sum{store_type='Live'})                    |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test100kActionPlan   |4         |12.89T    |2          |3         |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    /**
     * Tables are sorted alphabetically by their signature.
     *
     * Rows in each table are sorted using a {@link PerformanceTestNameComparator}.
     */
    @Test
    public void testRenderSortsInCorrectOrder() {
        final String table = tabulator.renderMeasurements(Stream.of(
                makeMeasurement("tp_discovery_duration_seconds_sum",
                    "testDiscoveryAndBroadcast100K",
                    "TopologyProcessorPerformanceTest"),
                makeMeasurement("mkt_analysis_duration_seconds_sum",
                    "test200kTopology",
                    "MarketPerformanceTest"),
                makeMeasurement("mkt_analysis_duration_seconds_sum",
                    "test75kTopology",
                    "MarketPerformanceTest"),
                makeMeasurement("tp_discovery_duration_seconds_sum",
                    "testDiscoveryAndBroadcast50K",
                    "TopologyProcessorPerformanceTest")
            )
        ).collect(Collectors.joining("\n\n"));

        assertEquals(
            "+------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|MarketPerformanceTest (metric: mkt_analysis_duration_seconds_sum)             |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name            |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test75kTopology      |4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|test200kTopology     |4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+---------------------+----------+----------+-----------+----------+-----------+" +
            "\n\n" +
            "+--------------------------------------------------------------------------------------+" + System.lineSeparator() +
            "|TopologyProcessorPerformanceTest (metric: tp_discovery_duration_seconds_sum)          |" + System.lineSeparator() +
            "+-----------------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|Test Name                    |Latest    |RecentAvg |BaselineAvg|Std Dev   |AlertStatus|" + System.lineSeparator() +
            "+-----------------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|testDiscoveryAndBroadcast50K |4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+-----------------------------+----------+----------+-----------+----------+-----------+" + System.lineSeparator() +
            "|testDiscoveryAndBroadcast100K|4         |1         |2          |3         |normal     |" + System.lineSeparator() +
            "+-----------------------------+----------+----------+-----------+----------+-----------+",
            table);
    }

    @Test
    public void testRenderStartsWithHeader() {
        final String table = tabulator.renderTestResults(Stream.empty());
        assertTrue(table.startsWith("\n\n" + ResultsTabulator.RESULTS_HEADER));
    }

    @Test
    public void testRenderEndsWithFooter() {
        final String table = tabulator.renderTestResults(Stream.empty());
        assertTrue(table.endsWith(ResultsTabulator.RESULTS_FOOTER));
    }

    @Test
    public void testRenderIncludesDashboardMessage() {
        final String table = tabulator.renderTestResults(Stream.empty());
        assertThat(table, containsString(ResultsTabulator.DASHBOARD_MESSAGE));
    }

    static MetricMeasurement makeMeasurement(@Nonnull final String metric,
                                             @Nonnull final String testMethodName,
                                             @Nonnull final String testClassName) {
        return new MetricMeasurement(1.0, 2.0, 3.0, 4.0,
            AlertMetricId.fromString(metric), testMethodName, testClassName);
    }

    @Test
    public void testMinTableWidth() {
        int rowSum = IntStream.of(ResultsTabulator.MIN_COLUMN_WIDTHS).sum();
        // number of columns used for column separation is 1 + the number of columns.
        int numColumnSeparators = ResultsTabulator.MIN_COLUMN_WIDTHS.length + 1;
        assertEquals(80, rowSum + numColumnSeparators);
    }

    @Test
    public void testMaxTableWidth() {
        int rowSum = IntStream.of(ResultsTabulator.MAX_COLUMN_WIDTHS).sum();
        // number of columns used for column separation is 1 + the number of columns.
        int numColumnSeparators = ResultsTabulator.MAX_COLUMN_WIDTHS.length + 1;
        assertEquals(90, rowSum + numColumnSeparators);
    }

    @Test
    public void testCompareBefore() {
        final RuleSignatureEntryComparator comparator = new RuleSignatureEntryComparator();
        assertEquals(-1, comparator.compare(makeEntry("classA", "metricA"), makeEntry("classB", "metricB")));
    }

    @Test
    public void testCompareAfter() {
        final RuleSignatureEntryComparator comparator = new RuleSignatureEntryComparator();
        assertEquals(1, comparator.compare(makeEntry("classB", "metricB"), makeEntry("classA", "metricA")));
    }

    @Test
    public void testCompareSame() {
        final RuleSignatureEntryComparator comparator = new RuleSignatureEntryComparator();
        assertEquals(0, comparator.compare(makeEntry("class", "metric"), makeEntry("class", "metric")));
    }

    @Nonnull
    private Entry<RuleSignature, List<MetricMeasurement>> makeEntry(@Nonnull final String className,
                                                                    @Nonnull final String metricName) {
        final RuleSignature signature = new RuleSignature(className, AlertMetricId.fromString(metricName));
        @SuppressWarnings("unchecked")
        final Entry<RuleSignature, List<MetricMeasurement>> entry =
            (Entry<RuleSignature, List<MetricMeasurement>>)mock(Entry.class);
        when(entry.getKey()).thenReturn(signature);
        return entry;
    }
}