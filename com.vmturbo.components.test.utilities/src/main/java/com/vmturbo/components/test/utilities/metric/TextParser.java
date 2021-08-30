package com.vmturbo.components.test.utilities.metric;

import java.io.Reader;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Scanner;
import java.util.regex.Pattern;

import javax.annotation.Nonnull;

import org.apache.commons.lang3.StringUtils;

import com.google.common.collect.ImmutableList;

import io.prometheus.client.Collector.MetricFamilySamples;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.Collector.Type;
import io.prometheus.client.exporter.common.TextFormat;

/**
 * A parser to convert text produced by {@link TextFormat} back into {@link MetricFamilySamples}.
 * Given a set of {@link MetricFamilySamples}, writing them via the {@link TextFormat} methods and
 * reading back via {@link TextParser} should produce a set containing all the original
 * {@link MetricFamilySamples}.
 * <p>
 * This parser is not meant (i.e. not tested) to parse text produced by other prometheus client
 * libraries. This means that using it to parse the text format produced by,
 * say, the python client library may not work. See: {@link TextParser#parse004(Reader)}.
 * <p>
 * TODO (roman, April 5 2017): The prometheus Java client library looks like it's planning to
 * provide a method to parse the text format. Once that's done we can get rid of this class
 * and use theirs:
 * https://github.com/prometheus/client_java/issues/203
 */
public class TextParser {

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
     * Within a label string returned by {@link TextParser#LABELS_PATTERN}
     * - label="foo",other="bar", this pattern returns the key of the first label - label - and
     * moves the scanner to the "=".
     */
    private static final Pattern LABEL_KEY_PATTERN = Pattern.compile(".*?(?==)");

    /**
     * Within a label string returned by {@link TextParser#LABELS_PATTERN}
     * - label="foo",other="bar", this pattern returns the value of the first label - foo - without
     * the quotes, and moves the scanner to the trailing ",.
     * <p>
     * The trailing comma is optional, even though at the time of this writing (April 5, 2017)
     * the {@link TextFormat#write004(Writer, Enumeration)}.
     */
    private static final Pattern LABEL_VALUE_PATTERN = Pattern.compile("(?<=\").*?(?=\",?)");

    /**
     * Labels are of the form 'key="value",'. This moves the scanner past the '","
     * at the end of the label.
     * <p>
     * The trailing comma is optional, even though at the time of this writing (April 5, 2017)
     * the {@link TextFormat#write004(Writer, Enumeration)}.
     */
    private static final Pattern LABEL_SEP_PATTERN = Pattern.compile("\",?");

    /**
     * In a HELP line - # HELP metric_name Some doc - with the scanner at the end of metric_name,
     * this pattern captures the documentation (everything remaining in the line).
     */
    private static final Pattern HELP_DOC_PATTERN = Pattern.compile(".*");


    // START Token constants.
    // See https://prometheus.io/docs/instrumenting/exposition_formats/#format-version-0.0.4
    private static final String COMMENT_START_TOKEN = "#";
    private static final String HELP_TOKEN = "HELP";
    private static final String TYPE_TOKEN = "TYPE";
    // END Token constants.

    /**
     * Parse a set of metrics written in the {@link TextFormat#CONTENT_TYPE_004} format, and
     * written with {@link TextFormat#write004(Writer, Enumeration)}.
     *
     * This link describes the base 004 exposition format:
     * https://prometheus.io/docs/instrumenting/exposition_formats/#format-version-0.0.4
     *
     * However, the parsing here relies quite heavily on the specific implementation of
     * {@link TextFormat}. For example, it does not handle untyped metrics, and assumes
     * each metric will have a HELP and a TYPE comment line preceding it.
     *
     * @param reader Reader for the input.
     * @return The list of collected {@link MetricFamilySamples}.
     * @throws TextParseException If something goes wrong with the parsing.
     */
    public static List<MetricFamilySamples> parse004(@Nonnull final Reader reader)
            throws TextParseException {
        try {
            return new TextParser(new Scanner(reader)).parse004();
        } catch (NoSuchElementException e) {
            throw new TextParseException(e);
        }
    }

    /**
     * The scanner to read input from.
     */
    private final Scanner scanner;

    /**
     * The list of metric family samples processed so far.
     */
    private List<MetricFamilySamples> familySamples = new ArrayList<>();

    /**
     * The information of the currently processing metric family. This is considered incomplete
     * until the beginning of the next metric family or the end of the input.
     */
    private MetricFamilyInfo curMetricFamily;


    private TextParser(@Nonnull final Scanner scanner) {
        this.scanner = scanner;
    }

    private List<MetricFamilySamples> parse004() throws TextParseException {
        // Read and process lines at a time. A line is either a comment line or a metric line.
        // The scanner will ignore empty lines.
        //
        // TODO (roman, March 31 2017): Consider changing the parsing to read one metric-family at
        // a time, instead of one line at a time.
        while (scanner.hasNext()) {
            if (scanner.hasNext(COMMENT_START_TOKEN)) {
                readCommentLine();
            } else if (curMetricFamily != null) {
                curMetricFamily.addSample(readMetricSampleLine());
            } else {
                throw new TextParseException("Parser expects HELP line at the start of every metric family.");
            }
        }

        // If we reach the end of the input, the current metric family is finalized.
        if (curMetricFamily != null) {
            familySamples.add(curMetricFamily.finalizeFamily());
        }
        return familySamples;
    }

    private Sample readMetricSampleLine() throws TextParseException {
        final ImmutableList.Builder<String> keys = new ImmutableList.Builder<>();
        final ImmutableList.Builder<String> vals = new ImmutableList.Builder<>();

        // Get the sample name if the sample has a label.
        String sampleName = scanner.findInLine(METRIC_W_LABEL_PATTERN);
        if (sampleName == null) {
            // If the sample does not have a label, then the entire token is the name.
            sampleName = scanner.next();
        } else {
            final String labels = scanner.findInLine(LABELS_PATTERN);
            try (final Scanner labelScanner = new Scanner(labels)) {
                while (labelScanner.hasNext()) {
                    final String key = labelScanner.findInLine(LABEL_KEY_PATTERN);
                    final String value = labelScanner.findInLine(LABEL_VALUE_PATTERN);
                    if (key == null || value == null) {
                        throw new TextParseException(
                                "Badly formatted label for " + curMetricFamily.getName());
                    }
                    keys.add(key);
                    vals.add(value);
                    // Move the scanner to the beginning of the next label.
                    labelScanner.findInLine(LABEL_SEP_PATTERN);
                }
            }
            // The labels pattern doesn't capture the trailing "}" (it uses lookahead), so we move
            // the scanner past it.
            scanner.next();
        }

        // The sample name + labels is followed by the metric value.
        final double metricVal = scanner.nextDouble();

        // There shouldn't be anything other than the newline, and maybe some spaces.
        // However, we need to move the scanner past the newline or else subsequent
        // findInLine calls won't work properly.
        if (scanner.hasNextLine()) {
            scanner.nextLine();
        }

        return new Sample(sampleName, keys.build(), vals.build(), metricVal);
    }

    private void readCommentLine() throws TextParseException {
        // Cut off the comment.
        scanner.next(COMMENT_START_TOKEN);
        if (scanner.hasNext(HELP_TOKEN)) {

            // Read the "HELP" token.
            scanner.next();

            // The HELP token should be followed by the metric name.
            String metricName = scanner.next();

            // Each # HELP line is the start of a new metric family, so when we encounter
            // a # HELP line we can finalize the previous family.
            if (curMetricFamily != null) {
                familySamples.add(curMetricFamily.finalizeFamily());
            }

            // Ditch the old metric family.
            curMetricFamily = new MetricFamilyInfo(metricName);

            // Rest of the line is just the documentation.
            // If there is nothing more in the line this will return null.
            String docs = scanner.findInLine(HELP_DOC_PATTERN);
            curMetricFamily.setDoc(docs == null ? "" : docs);

            // Move to the following line.
            scanner.nextLine();
        } else if (scanner.hasNext(TYPE_TOKEN)) {
            scanner.next();
            final String curMetricName = scanner.next();
            // Sanity check to verify the expected format for metric families.
            if (curMetricFamily == null) {
                throw new TextParseException("# TYPE line should be preceeded by a # HELP line.");
            }
            if (!curMetricName.equals(curMetricFamily.getName())) {
               throw new TextParseException(
                       "Each HELP line should be followed by a TYPE line with the same " +
                               "metric family name.");
            }
            curMetricFamily.setType(Type.valueOf(StringUtils.upperCase(scanner.next())));
            // Skip rest of the line.
            scanner.nextLine();
        } else {
            // Skip the rest of the line - extraneous comments are useless.
            scanner.nextLine();
        }

    }

    /**
     * A utility class to capture the state of the metric family currently being processed.
     * Since metric families span multiple lines (a HELP line, a TYPE line, and a number of
     * metric lines) we need something to track state.
     *
     *
     */
    private static class MetricFamilyInfo {
        private final String name;

        private final SetOnce<Type> type = new SetOnce<>();

        private final SetOnce<String> doc = new SetOnce<>();

        private List<Sample> samples = new ArrayList<>();

        private boolean finalized = false;

        MetricFamilyInfo(@Nonnull final String metricName) {
            this.name = metricName;
        }

        @Nonnull
        String getName() {
            return name;
        }

        void setType(@Nonnull final Type type) {
            checkNotFinalized();
            this.type.set(type);
        }

        void setDoc(@Nonnull final String doc) {
            checkNotFinalized();
            this.doc.set(StringUtils.strip(doc));
        }

        void addSample(@Nonnull final Sample sample) {
            checkNotFinalized();
            this.samples.add(sample);
        }

        @Nonnull
        MetricFamilySamples finalizeFamily() {
            finalized = true;
            return new MetricFamilySamples(name,
                    type.get().orElse(Type.UNTYPED),
                    doc.get().orElse(""),
                    samples);
        }

        private void checkNotFinalized() {
            if (finalized) {
                throw new IllegalStateException("Attempting to modify finalized family info.");
            }
        }
    }

    /**
     * A utility class for a variable that can only be set once.
     * Used to help catch bugs in with improper use of {@link MetricFamilyInfo}.
     *
     * @param <T> The type of the value.
     */
    private static class SetOnce<T extends Object> {
        private T value = null;

        public synchronized void set(T value) {
            if (this.value == null) {
                this.value = value;
            } else {
                throw new IllegalArgumentException("Illegal value update attempt with " + value +
                        "! Value has already been set to " + this.value);
            }
        }

        public synchronized Optional<T> get() {
            return Optional.ofNullable(value);
        }
    }

    /**
     * Exception thrown when parsing text into a set of {@link MetricFamilySamples} fails.
     */
    public static class TextParseException extends Exception {
        private TextParseException(Throwable cause) {
            super(cause);
        }

        private TextParseException(final String msg) {
            super(msg);
        }
    }

}
