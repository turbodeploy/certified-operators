package com.vmturbo.components.test.utilities.alert;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableMap;

import com.vmturbo.components.test.utilities.alert.report.NumberFormatter;

/**
 * An identifier for metrics in the alerting framework. In the framework the metric name and label
 * set uniquely identify a metric.
 */
@Immutable
public class AlertMetricId {
    /**
     * In a metric with a label - metric_name{label="foo",} - this pattern returns
     * metric_name, and moves the scanner to the "{".
     */
    private static final Pattern METRIC_W_LABEL_PATTERN = Pattern.compile(".*(?=\\{)");

    /**
     * In a metric with a label - metric_name{label="foo",} - with the scanner at "{",
     * this pattern returns the contents of the braces - label="foo",
     */
    private static final Pattern LABELS_PATTERN = Pattern.compile("(?<=\\{).*(?=\\})");

    /**
     * Within a label string returned by {@link AlertMetricId#LABELS_PATTERN}
     * - label="foo",other="bar", this pattern returns the key of the first label - label - and
     * moves the scanner to the "=".
     */
    private static final Pattern LABEL_KEY_PATTERN = Pattern.compile(".*?(?==)");

    /**
     * Within a label string returned by {@link AlertMetricId#LABELS_PATTERN}
     * - label="foo",other="bar", this pattern returns the value of the first label - foo - without
     * the quotes, and moves the scanner to the trailing ",.
     */
    private static final Pattern LABEL_VALUE_PATTERN = Pattern.compile("(?<=\').*?(?=\',?)");

    /**
     * Labels are of the form 'key="value",'. This moves the scanner past the '","
     * at the end of the label.
     */
    private static final Pattern LABEL_SEP_PATTERN = Pattern.compile("\'\\s*,?");

    /**
     * The name of the metric.
     */
    private final String metricName;

    /**
     * The label set for the metric.
     */
    private final Map<String, String> labels;

    /**
     * An optional SLA_VIOLATION (Service Level Agreement) threshold. If no SLA_VIOLATION is specified, alerts will
     * not attempt to check against an SLA_VIOLATION.
     */
    private final Optional<SlaThreshold> slaThreshold;

    @Nonnull
    public static AlertMetricId fromString(@Nonnull final String alertMetricString)
            throws IllegalArgumentException{
        final MetricWithSla metricWithSla = new MetricWithSla(alertMetricString);
        try (final Scanner scanner = new Scanner(metricWithSla.getMetricNameAndTags())) {
            final ImmutableMap.Builder<String, String> kvBuilder = ImmutableMap.builder();

            // Get the sample name if the sample has a label.
            String sampleName = scanner.findInLine(METRIC_W_LABEL_PATTERN);
            if (sampleName == null) {
                // If the sample does not have a label, then the entire token is the name.
                sampleName = scanner.next();
            } else {
                final String labels = scanner.findInLine(LABELS_PATTERN);
                if (labels == null) {
                    throw new IllegalArgumentException("Invalid labels string: " + alertMetricString
                            + ". Format is: metricName{name1='val',name2='val2'...}");
                }
                try (final Scanner labelScanner = new Scanner(labels)) {
                    while (labelScanner.hasNext()) {
                        final String key = StringUtils.strip(
                                labelScanner.findInLine(LABEL_KEY_PATTERN));
                        final String val = StringUtils.strip(
                                labelScanner.findInLine(LABEL_VALUE_PATTERN));
                        if (key == null || val == null) {
                            throw new IllegalArgumentException(
                                    "Badly formatted label " + alertMetricString
                                        + ". Format is: metricName{name1='val',name2='val2'...}");
                        }
                        kvBuilder.put(key, val);
                        // Move the scanner to the beginning of the next label.
                        labelScanner.findInLine(LABEL_SEP_PATTERN);
                    }
                }
            }
            return new AlertMetricId(sampleName, kvBuilder.build(), metricWithSla.getSlaThreshold());
        }
    }

    public Optional<SlaThreshold> getSlaThreshold() {
        return slaThreshold;
    }

    /**
     * Pairs an unparsed metric name with an optional parsed SLA (Service Level Agreement)
     * that has been transformed into base units.
     */
    private static class MetricWithSla {
        /**
         * In a metric with an sla - metric_name_and_tags/5m - this pattern returns
         * metric_name_and_tags, and moves the scanner to the "/".
         */
        private static final String METRIC_SLA_SEPARATOR = "/";

        /**
         * A regex to capture a decimal number followed by an alphabetical units suffix.
         * Examples: 4h (4 hours), 2.4Gb (2.4 gigabytes), 1m (1 minute)
         *
         * The decimal is captured in a named capture group called "digits" and
         * the suffix is captured in a named capture group called "unitsSuffix".
         */
        private static final Pattern SLA_CAPTURE_PATTERN =
            Pattern.compile("(?<digits>[-+]?\\d+(\\.\\d+)?)(?<unitsSuffix>\\w+)");

        private final String metricNameAndTags;
        private final Optional<SlaThreshold> sla;

        public MetricWithSla(@Nonnull final String alertMetricString) {
            int metricSeparatorIndex = alertMetricString.indexOf(METRIC_SLA_SEPARATOR);
            if (metricSeparatorIndex < 0) {
                this.metricNameAndTags = alertMetricString;
                this.sla = Optional.empty();
            } else {
                this.metricNameAndTags = alertMetricString.substring(0, metricSeparatorIndex);
                // Parse the SLA_VIOLATION, skipping past the '/' character
                this.sla = Optional.of(parseSla(alertMetricString.substring(metricSeparatorIndex + 1)));
            }
        }

        @Nonnull
        SlaThreshold parseSla(@Nonnull final String slaString) {
            final Matcher matcher = SLA_CAPTURE_PATTERN.matcher(slaString);
            if (matcher.matches()) {
                return new SlaThreshold(
                    convertToBaseUnitsBySuffix(
                        Double.parseDouble(matcher.group("digits")),
                        matcher.group("unitsSuffix")),
                    slaString
                );
            } else {
                throw new IllegalArgumentException("Badly formatted metric SLA " + slaString + ". " +
                    "Format is: <decimal number><suffix> where suffix may be an SI or time suffix (ie 4.2d for 4.2 days).");
            }
        }

        double convertToBaseUnitsBySuffix(final double digits, @Nonnull final String unitsSuffix) {
            try {
                return NumberFormatter.transformToBaseUnitBySuffix(digits, unitsSuffix);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Badly formatted SLA suffix: \"" + unitsSuffix + "\". " +
                    e.getMessage());
            }
        }

        public String getMetricNameAndTags() {
            return metricNameAndTags;
        }

        public Optional<SlaThreshold> getSlaThreshold() {
            return sla;
        }
    }

    @Nonnull
    public String getMetricName() {
        return metricName;
    }

    @Nonnull
    public Map<String, String> getLabels() {
        return labels;
    }

    private AlertMetricId(@Nonnull final String metricName,
                          @Nonnull final Map<String, String> labels,
                          @Nonnull final Optional<SlaThreshold> slaThresold) {
        this.metricName = Objects.requireNonNull(metricName);
        this.labels = Objects.requireNonNull(labels);
        this.slaThreshold = Objects.requireNonNull(slaThresold);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof AlertMetricId) {
            final AlertMetricId metricId = (AlertMetricId)other;
            return metricName.equals(metricId.metricName) && labels.equals(metricId.labels);
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(metricName, labels);
    }

    @Override
    public String toString() {
        final StringBuilder stringBuilder = new StringBuilder()
            .append(metricName);
        if (!labels.isEmpty()) {
            stringBuilder.append(
                labels.entrySet().stream()
                    .map(entry -> entry.getKey() + "='" + entry.getValue() + "'")
                    .collect(Collectors.joining(",", "{", "}")));
        }
        return stringBuilder.toString();
    }
}
