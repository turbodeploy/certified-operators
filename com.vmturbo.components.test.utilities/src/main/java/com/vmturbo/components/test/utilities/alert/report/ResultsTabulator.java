package com.vmturbo.components.test.utilities.alert.report;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.asciithemes.TA_Grid;
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment;

import com.vmturbo.components.test.utilities.alert.MetricMeasurement;
import com.vmturbo.components.test.utilities.alert.RuleSignature;
import com.vmturbo.components.test.utilities.alert.jira.AlertProcessorForJira;

/**
 * Results are placed in tables grouped by their method signature.
 *
 * Tables are sorted alphabetically by their signature.
 *
 * Rows in each table are sorted using a {@link PerformanceTestNameComparator}.
 *
 * All tables are between 84 and 94 characters wide. Test names longer than 20 characters
 * will cause the table to extend up to 94 characters. Test names longer than 30 characters
 * will wrap so that the table stays at max length of 94 characters but rows begin to span
 * more than one column.
 *
 * Small decimal numbers are truncated to a maximum number of decimal places.
 *
 * Large decimal numbers are converted to use the notation prefixes
 * "k", "M", "G", "T", etc. for kilo, Mega, Giga, Tera, etc.
 */
public class ResultsTabulator {
    /**
     * Table column headings.
     */
    public static final List<String> TABLE_COLUMN_HEADINGS =
        Arrays.asList("Test Name", "Latest", "RecentAvg", "BaselineAvg", "Std Dev", "AlertStatus");

    /**
     * The minimum column widths in the report table for each column.
     * See http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
     */
    public static final int[] MIN_COLUMN_WIDTHS = { 21, 10, 10, 11, 10, 11 };
    public static final int[] MAX_COLUMN_WIDTHS = { 31, 10, 10, 11, 10, 11 };

    /**
     * The column width calculator used to format the table. For details, see
     * http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
     */
    private static final CWC_LongestLine COLUMN_WIDTH_CALCULATOR = new CWC_LongestLine();
    static {
        for (int i = 0; i < MIN_COLUMN_WIDTHS.length; i++) {
            COLUMN_WIDTH_CALCULATOR.add(MIN_COLUMN_WIDTHS[i], MAX_COLUMN_WIDTHS[i]);
        }
    }
    /**
     * A header printed at the top of the results report tables.
     */
    public static final String RESULTS_HEADER =
        "============================= TEST RESULTS START ==============================";

    /**
     * A header printed at the bottom of the results report tables.
     */
    public static final String RESULTS_FOOTER =
        "============================== TEST RESULTS END ===============================";

    public static final String DASHBOARD_MESSAGE =
        "Visit " + AlertProcessorForJira.DEFAULT_METRICS_DASHBOARD_URL + " to view the metrics dashboard.";

    /**
     * For use in formatting numbers added to the tables.
     */
    private final NumberFormatter formatter;

    /**
     * The grid format for the table.
     */
    private final Optional<TA_Grid> gridFormat;

    /**
     * Create a new ResultsTabulator for tabulating measurements produced by tests for
     * use in reports.
     */
    public ResultsTabulator() {
        this(Optional.empty());
    }

    @VisibleForTesting
    ResultsTabulator(@Nonnull final Optional<TA_Grid> gridFormat) {
        this.gridFormat = gridFormat;
        formatter = new NumberFormatter();
    }

    /**
     * Render a stream of tables that group measurements into a readable format.
     *
     * @return A stream of formatted tables rendered as strings.
     */
    public String renderTestResults(@Nonnull final Stream<MetricMeasurement> measurements) {
        return Stream.concat(
            Stream.concat(
                // Prefix with an empty line before the header to format nicely when printed with a logger.
                Stream.of("", RESULTS_HEADER),
                renderMeasurements(measurements)
            ), Stream.of(DASHBOARD_MESSAGE, RESULTS_FOOTER)
        ).collect(Collectors.joining("\n\n"));
    }

    /**
     * Render measurements into tables, grouping them by the signatures of the rules
     * that generated the measurements. Tables are rendered in alphabetical order
     * by the signature of the rule that is associated with the table.
     *
     * @param measurements The measurements to be grouped and rendered into tables.
     * @return A stream of tables.
     */
    public Stream<String> renderMeasurements(@Nonnull final Stream<MetricMeasurement> measurements) {
        Map<RuleSignature, List<MetricMeasurement>> measurementsBySignature =
            measurements.collect(Collectors.groupingBy(metric ->
                new RuleSignature(metric.getTestClassName(), metric.getMetric())));

        return measurementsBySignature.entrySet().stream()
            .sorted(new RuleSignatureEntryComparator())
            .map(entry -> renderTable(entry.getKey(), entry.getValue()));
    }

    /**
     * Render an individual table with each row in the table composed of a measurement
     * entry in the list of input measurements.
     *
     * The rows in the table should be sorted by the names of each test that produced
     * a measurement.
     *
     * @param signature The signature for the rule that caused the measurement to be taken.
     * @param measurements The list of measurements to be entered into the table.
     * @return A table for the measurements suitable for display in a console.
     */
    private String renderTable(@Nonnull final RuleSignature signature,
                              @Nonnull final List<MetricMeasurement> measurements) {
        final AsciiTable table = new AsciiTable();
        gridFormat.ifPresent(format -> table.getContext().setGrid(format));
        table.getRenderer().setCWC(COLUMN_WIDTH_CALCULATOR);

        table.addRule();
        table.addRow(signatureRow(signature));
        table.addRule();
        table.addRow(TABLE_COLUMN_HEADINGS);
        measurements.stream()
            .sorted(new PerformanceTestNameComparator())
            .forEach(measurement -> {
                table.addRule();
                addRow(table, measurement);
            });
        table.addRule();

        // Text alignment for the table must be set AFTER rows are added to it.
        table.setTextAlignment(TextAlignment.LEFT);
        return table.render();
    }

    private void addRow(@Nonnull final AsciiTable table, @Nonnull final MetricMeasurement measurement) {
        table.addRow(
            measurement.getTestName(),
            formatter.format(measurement.getLatestValue()),
            formatter.format(measurement.getRecentAverage()),
            formatter.format(measurement.getBaselineAverage()),
            measurement.getStandardDeviation()
                .map(formatter::format)
                .orElse("undefined"),
            CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_UNDERSCORE, measurement.getAlertStatus().getDescription())
        );
    }

    /**
     * A comparator that compares entries in a map by their RuleSignature.
     */
    @VisibleForTesting
    static class RuleSignatureEntryComparator implements Comparator<Entry<RuleSignature, List<MetricMeasurement>>> {
        @Override
        public int compare(Entry<RuleSignature, List<MetricMeasurement>> a,
                           Entry<RuleSignature, List<MetricMeasurement>> b) {
            return a.getKey().toString().compareTo(b.getKey().toString());
        }
    }

    /**
     * Compose a table row with all nulls except the last which is the signature.
     * Equivalent to Arrays.asList(null, null, ...., signature) where the
     * number of nulls is TABLE_COLUMN_HEADINGS.size() - 1.
     *
     * The reason for this is we want the signature to span the entire row, and
     * nulling out a column merges it with the following column in the table.
     * See http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_02_ColSpan.html.
     *
     * @param signature The signature to be inserted into the row.
     * @return A collection appropriate for the table signature row.
     */
    private Collection<String> signatureRow(@Nonnull final RuleSignature signature) {
        return Stream.concat(
            TABLE_COLUMN_HEADINGS.stream()
                .limit(TABLE_COLUMN_HEADINGS.size() - 1)
                .map(heading -> null),
            Stream.of(signature.toString())
        ).collect(Collectors.toList());
    }
}
