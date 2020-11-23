package com.vmturbo.stitching.journal;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.text.NumberFormat;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import javax.annotation.Nonnull;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;

import io.grpc.stub.StreamObserver;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import de.vandermeer.asciitable.AsciiTable;
import de.vandermeer.asciitable.CWC_LongestLine;
import de.vandermeer.asciithemes.a7.A7_Grids;

import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.OperationEndMessage;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.OperationStartMessage;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.PhaseMessage;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.StitchingChanges;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.TargetEntry;
import com.vmturbo.common.protobuf.topology.Stitching.JournalEntry.TopologySizeEntry;
import com.vmturbo.common.protobuf.topology.Stitching.Verbosity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.journal.IStitchingJournal.FormatRecommendation;
import com.vmturbo.stitching.journal.IStitchingJournal.StitchingMetrics;
import com.vmturbo.stitching.journal.IStitchingJournal.StitchingPhase;

/**
 * A recorder for recording entries in the stitching journal.
 * Subclasses record to specific logging entries such as a {@link Logger} or {@link StringBuilder}.
 */
public interface JournalRecorder {
    /**
     * Record the start of a stitching phase.
     * Formatting of the phase message may be adjusted by the specific recorder.
     *
     * @param phase The message marking the phase start.
     */
    void recordPhaseStart(@Nonnull final StitchingPhase phase);

    /**
     * Record the start of an operation.
     * Formatting of the operation may be adjusted by the specific recorder.
     *
     * @param operation The operation being started.
     * @param operationDetails Additional details about the stitching operation being started.
     */
    void recordOperationStart(@Nonnull final JournalableOperation operation,
                              @Nonnull final Collection<String> operationDetails);

    /**
     * Record the end of an operation.
     * Formatting of the operation may be adjusted by the specific recorder.
     *
     * @param operation The operation being ended.
     * @param operationDetails Additional details about the stitching operation being started.
     * @param changesetsGenerated The number of changesets generated by this operation. This number is
     *                            the number PRIOR to grouping changesets even if changeset grouping is
     *                            enabled by filter options.
     * @param changestsIncluded The number of changesets generated by this operation that are actually
     *                          included in this journal. This number is the number PRIOR to grouping
     *                          changesets even if changeset grouping is enabled by filter options. So,
     *                          for example, if grouping is enabled and we enter a changeset that merges
     *                          two pairs of entities and performs an update on an entity, that single
     *                          grouped changeset will count as 3 (2 merges + 1 update).
     * @param emptyChangesets The number of included changests that had no differences and so were empty.
     *                        ie if a changeset sets a commodity capacity to 5, but that capacity was
     *                        already at 5 there will be no differences and we will deem the changeset
     *                        as "empty" even though we still include it.
     * @param duration How long the operation took to run.
     */
    void recordOperationEnd(@Nonnull final JournalableOperation operation,
                            @Nonnull final Collection<String> operationDetails,
                            final int changesetsGenerated,
                            final int changestsIncluded,
                            final int emptyChangesets,
                            @Nonnull final Duration duration);

    /**
     * Record a stitching change.
     * Formatting of the message may be adjusted by the specific recorder.
     *
     * @param preambles The high-level descriptions of the changes made by the changeset.
     * @param details The fine-grained details of the changes made by the changeset.
     * @param verbosity The verbosity at which to record the change. At some combinations
     *                  of verbosity and format either the preamble or details may be skipped
     *                  or formatted differently.
     * @param format The recommended format with which to format the message. At some
     *               combinations of verbosity and format either the preamble or details
     *               may be skipped or formatted differently.
     */
    void recordChangeset(@Nonnull final List<String> preambles,
                         @Nonnull final List<String> details,
                         @Nonnull final Verbosity verbosity,
                         @Nonnull final FormatRecommendation format);

    /**
     * Record an error that occurred during stitching.
     * Formatting of the message may be adjusted by the specific recorder.
     *
     * @param message The error message to record.
     */
    void recordError(@Nonnull final String message);

    /**
     * Record information about the topology associated with the journal entries in this stitching journal.
     *
     * @param topologyInfo information about the topology associated with the journal
     *                     entries in this stitching journal.
     * @param metrics The metrics to be recorded in the journal.
     */
    void recordTopologyInfoAndMetrics(@Nonnull final TopologyInfo topologyInfo,
                                      @Nonnull final StitchingMetrics metrics);

    /**
     * Record information about targets.
     *
     * @param targetEntries The supplier for target entries.
     */
    void recordTargets(@Nonnull final List<TargetEntry> targetEntries);

    /**
     * Record information about the size of the topology.
     *
     * @param topologySizes The size of the topology as defined by a count for the number of each type
     *                      of entity in the topology.
     */
    void recordTopologySizes(@Nonnull final Map<EntityType, Integer> topologySizes);

    /**
     * Record a miscellaneous message.
     */
    void recordMessage(@Nonnull final String message);

    /**
     * Flush the journal to any outputs.
     *
     * For most recorders, this does nothing.
     */
    default void flush() {}

    /**
     * The number of whitespace characters to buffer lines with in a text box.
     */
    int TEXT_BOX_BUFFER_LENGTH = 4;

    String CHANGESET_BARIER =
        "--------------------------------------------------------------------------------";
    String CHANGESET_ENTRY_SEPARATOR = "\n" + CHANGESET_BARIER + "\n";

    /**
     * PrettyPrint a particular message in the center of a line spaced by separators. ie. a line
     * that looks like: '------------------------------ MY MESSAGE ------------------------------'
     *
     * @param message The message to be pretty printed.
     * @param separator The character to use as the separator border (ie '-' or '+' or '*' etc.)
     * @param lineLength The length of the line to pretty print. If the line is shorter than the
     *                   provided length, the separator character will be used to fill out the
     *                   line in a way that centers the message.
     * @return The pretty printed message centered in the line.
     */
    static String prettyPrintCenter(@Nonnull final String message, final char separator,
                                    final int lineLength) {
        int messageLength = message.length();
        int leftPad = Math.max(lineLength - messageLength - 2, 0) / 2;
        int rightPad = Math.max(lineLength - (leftPad + messageLength + 2), 0);

        return StringUtils.repeat(separator, leftPad) + ' '
            + message + ' ' + StringUtils.repeat(separator, rightPad);
    }

    static String prettyPrintCenter(@Nonnull final String message,
                                    final char leftBound, final char rightBound, int lineLength) {
        return leftBound + prettyPrintCenter(message, ' ', lineLength - 2) + rightBound;
    }

    /**
     * Print text in a box. If the necessary, splits the text by whitespace on multiple lines.
     * Note that the textbox is not careful about preserving the whitespace of the original.
     *
     * @param message The message to put in the text box.
     * @param lineLength The line length of the text box.
     * @return A text box wrapping the message. Lines are formatted to be a maximum of the line length
     *         unless they contain a word or words longer than the lineLength.
     */
    static String textBox(@Nonnull final String message, final int lineLength) {
        final int spaceForWords = lineLength - TEXT_BOX_BUFFER_LENGTH;

        final StringBuilder builder = new StringBuilder(message.length() + lineLength * 4);
        final String margin = prettyPrintCenter("", '|', '|', lineLength) + "\n";
        builder.append(StringUtils.repeat('-', lineLength))
            .append("\n")
            .append(margin);

        for (String line : message.split("\n")) {
            final Deque<String> words = new ArrayDeque<>(Arrays.asList(line.split("\\s+")));
            while (!words.isEmpty()) {
                // Build up the next line as long as there is space to add the next word.
                String nextLine = words.removeFirst();
                while (!words.isEmpty() && nextLine.length() + words.peekFirst().length() + 1 < spaceForWords) {
                    nextLine += ' ' + words.removeFirst();
                }

                builder.append(prettyPrintCenter(nextLine, '|', '|', lineLength))
                    .append("\n");
            }
        }

        builder.append(margin)
            .append(StringUtils.repeat('-', lineLength));
        return builder.toString();
    }

    static String humanReadableFormat(@Nonnull final Duration duration) {
        return duration.toString()
            .substring(2)
            .replaceAll("(\\d[HMS])(?!$)", "$1 ")
            .toLowerCase();
    }

    /**
     * A recorder that records basic messages with common formatting.
     */
    abstract class BasicRecorder implements JournalRecorder {
        /**
         * Record a journal message in the recorder.
         *
         * @param message The message to record.
         */
        protected abstract void record(@Nonnull final String message);
        private final Gson gson = ComponentGsonFactory.createGson();
        private final NumberFormat numberFormat = NumberFormat.getInstance();

        /**
         * Table column headings.
         */
        public static final List<String> TARGET_TABLE_COLUMN_HEADINGS =
            Arrays.asList("Target OID", "Target Name", "Type", "EntityCnt");

        /**
         * The minimum column widths in the report table for each column.
         * See http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
         */
        public static final int[] MIN_TARGET_COLUMN_WIDTHS = { 10, 10, 10, 10 };
        public static final int[] MAX_TARGET_COLUMN_WIDTHS = { 20, 30, 20, 15 };

        /**
         * The column width calculator used to format the table. For details, see
         * http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
         */
        private static final CWC_LongestLine TARGET_COLUMN_WIDTH_CALCULATOR = new CWC_LongestLine();
        static {
            for (int i = 0; i < MIN_TARGET_COLUMN_WIDTHS.length; i++) {
                TARGET_COLUMN_WIDTH_CALCULATOR.add(MIN_TARGET_COLUMN_WIDTHS[i], MAX_TARGET_COLUMN_WIDTHS[i]);
            }
        }

        /**
         * Table column headings.
         */
        public static final List<String> ENTITY_TABLE_COLUMN_HEADINGS =
            Arrays.asList("Entity Type", "Count");

        /**
         * The minimum column widths in the report table for each column.
         * See http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
         */
        public static final int[] MIN_ENTITY_COLUMN_WIDTHS = { 20, 20 };
        public static final int[] MAX_ENTITY_COLUMN_WIDTHS = { 40, 40 };

        /**
         * The column width calculator used to format the table. For details, see
         * http://www.vandermeer.de/projects/skb/java/asciitable/examples/AT_07c_LongestLine.html
         */
        private static final CWC_LongestLine ENTITY_COLUMN_WIDTH_CALCULATOR = new CWC_LongestLine();
        static {
            for (int i = 0; i < MIN_ENTITY_COLUMN_WIDTHS.length; i++) {
                ENTITY_COLUMN_WIDTH_CALCULATOR.add(MIN_ENTITY_COLUMN_WIDTHS[i], MAX_ENTITY_COLUMN_WIDTHS[i]);
            }
        }

        @Override
        public void recordPhaseStart(@Nonnull final StitchingPhase phase) {
            record(phase.transitionMessage());
        }

        @Override
        public void recordOperationStart(@Nonnull final JournalableOperation operation,
                                         @Nonnull final Collection<String> operationDetails) {
            record(operationMessage("START: ", operation));
            operationDetails.forEach(detail -> record(JournalRecorder
                .prettyPrintCenter(detail, '-', 80)));
        }

        @Override
        public void recordOperationEnd(@Nonnull final JournalableOperation operation,
                                       @Nonnull final Collection<String> operationDetails,
                                       final int changesetsGenerated,
                                       final int changestsIncluded,
                                       final int emptyChangests,
                                       @Nonnull final Duration duration) {
            record(operationMessage("END: ", operation));
            operationDetails.forEach(detail -> record(JournalRecorder.prettyPrintCenter(detail, '-', 80)));

            if (changesetsGenerated > 0) {
                record(JournalRecorder.prettyPrintCenter(changesetsMessage(changesetsGenerated,
                        changestsIncluded, emptyChangests)
                    + " Took: " + humanReadableFormat(duration), '-', 80) + "\n");
            } else {
                record("");
            }
        }

        @Override
        public void recordChangeset(@Nonnull final List<String> preambles,
                                    @Nonnull final List<String> details,
                                    @Nonnull final Verbosity verbosity,
                                    @Nonnull final FormatRecommendation format) {
            // Skip recording the preamble for compact operations unless Verbosity is set to COMPLETE_VERBOSITY.
            if (format == FormatRecommendation.PRETTY || verbosity == Verbosity.COMPLETE_VERBOSITY) {
                record(JournalRecorder.textBox(preambleString(preambles), 80));
            } else if (verbosity == Verbosity.PREAMBLE_ONLY_VERBOSITY) {
                record(preambleString(preambles));
            }

            // Unless verbosity is set to PREAMBLE_ONLY, be sure to record the details.
            if (verbosity != Verbosity.PREAMBLE_ONLY_VERBOSITY) {
                record(details.stream().collect(Collectors.joining(CHANGESET_ENTRY_SEPARATOR)));
            }
        }

        @Override
        public void recordError(@Nonnull final String message) {
            record(message);
        }

        @Override
        public void recordTopologyInfoAndMetrics(@Nonnull final TopologyInfo topologyInfo,
                                                 @Nonnull final StitchingMetrics metrics) {
            record(prettyPrintCenter("End of stitching journal for", '=', 80));
            record(prettyPrintCenter("Topology ID: " + topologyInfo.getTopologyId(), '=', 80));
            record(prettyPrintCenter("Topology Context ID: " +
                topologyInfo.getTopologyContextId(), '=', 80));
            record(prettyPrintCenter(changesetsMessage(metrics.getTotalChangesetsGenerated(),
                metrics.getTotalChangesetsIncluded(), metrics.getTotalEmptyChangests()) +
                " Took: " + humanReadableFormat(metrics.getTimeTaken()), '=', 80));

            // For plan-variant topologies, include additional information about the topology.
            if (topologyInfo.getTopologyType() != TopologyType.REALTIME) {
                record(gson.toJson(topologyInfo));
            }
        }

        @Override
        public void recordTargets(@Nonnull final List<TargetEntry> targetEntries) {
            final AsciiTable table = asciiTable();

            table.addRule();
            table.addRow(TARGET_TABLE_COLUMN_HEADINGS);
            table.addRule();

            targetEntries.forEach(targetEntry -> {
                table.addRow(targetRow(targetEntry));
                table.addRule();
            });

            record(table.render() + "\n");
        }

        @Override
        public void recordTopologySizes(@Nonnull final Map<EntityType, Integer> topologySizes) {
            final AsciiTable table = asciiTable();

            table.addRule();
            table.addRow(ENTITY_TABLE_COLUMN_HEADINGS);
            table.addRule();

            topologySizes.entrySet().forEach(entityEntry -> {
                table.addRow(entityRow(entityEntry));
                table.addRule();
            });

            int totalEntityCount = topologySizes.values().stream()
                .mapToInt(size -> size)
                .sum();
            table.addRow(Arrays.asList("Total", totalEntityCount));
            table.addRule();

            record(table.render() + "\n");
        }

        @Override
        public void recordMessage(@Nonnull final String message) {
            record(message);
        }

        private AsciiTable asciiTable() {
            final AsciiTable table = new AsciiTable();
            table.getRenderer().setCWC(TARGET_COLUMN_WIDTH_CALCULATOR);
            table.getContext().setGrid(A7_Grids.minusBarPlusEquals());

            return table;
        }

        private String operationMessage(@Nonnull final String message,
                                        @Nonnull final JournalableOperation operation) {
            return JournalRecorder.prettyPrintCenter(message +
                operation.getOperationName(), '-', 80);
        }

        private String changesetsMessage(final int changestsGenerated,
                                        final int changestsIncluded,
                                        final int emptyChangests) {
            final NumberFormat format = NumberFormat.getInstance();
            final String gen = format.format(changestsGenerated);
            final String inc = format.format(changestsIncluded);
            final String empty = format.format(emptyChangests);

            if (emptyChangests > 0) {
                return inc + "/" + gen + " (" + empty + " empty) changesets shown.";
            } else {
                return inc + "/" + gen + " changesets shown.";
            }
        }

        @Nonnull
        private String preambleString(@Nonnull final List<String> preambles) {
            return preambles.stream().collect(Collectors.joining(" and\n"));
        }

        @Nonnull
        private Collection<?> targetRow(@Nonnull final TargetEntry targetEntry) {
            return Arrays.asList(
                targetEntry.getTargetId(),
                targetEntry.getTargetName(),
                targetEntry.getProbeName(),
                numberFormat.format(targetEntry.getEntityCount())
            );
        }

        @Nonnull
        private Collection<?> entityRow(@Nonnull final Map.Entry<EntityType, Integer> entityEntry) {
            return Arrays.asList(
                entityEntry.getKey(),
                numberFormat.format(entityEntry.getValue())
            );
        }
    }

    /**
     * Record journal entries to a {@link Logger}.
     *
     * Note that a logger recorder buffers all its logs until flushed so that the journal
     * can show up as a contiguous segment in the logs.
     */
    class LoggerRecorder extends BasicRecorder {
        private final Logger logger;

        private final StringBuilder stringBuilder;
        private final long flushingCharacterLength;

        /**
         * The default length at which we will flush the buffering StringBuilder for the LoggerRecorder.
         */
        public static final long DEFAULT_FLUSHING_CHARACTER_LENGTH = 1024 * 1024;

        /**
         * Record journal entries to a logger associated with the {@Link StitchingJournal} class.
         */
        public LoggerRecorder() {
            this(LogManager.getLogger(IStitchingJournal.class));
        }

        public LoggerRecorder(@Nonnull final Logger logger) {
            this.logger = Objects.requireNonNull(logger);
            stringBuilder = new StringBuilder(4096);
            flushingCharacterLength = DEFAULT_FLUSHING_CHARACTER_LENGTH;
        }

        public LoggerRecorder(@Nonnull final Logger logger,
                              final long flushingCharacterLength) {
            this.logger = Objects.requireNonNull(logger);
            stringBuilder = new StringBuilder(4096);
            Preconditions.checkArgument(flushingCharacterLength > 1024);
            this.flushingCharacterLength = flushingCharacterLength;
        }

        @Override
        public void record(@Nonnull String entry) {
            stringBuilder.append(entry).append("\n");

            // If the buffer is getting too long, flush and clear it to prevent running
            // ourselves out of memory.
            if (stringBuilder.length() > flushingCharacterLength) {
                flush();
            }
        }

        @Override
        public String toString() {
            return stringBuilder.toString();
        }

        @Override
        public void flush() {
            // Only write anything when we have data to write.
            if (stringBuilder.length() > 0) {
                logger.info(stringBuilder);
                stringBuilder.setLength(0); // Clear the backing stringBuilder
            }
        }

        @Override
        public void recordOperationEnd(@Nonnull JournalableOperation operation,
                                       @Nonnull final Collection<String> operationDetails,
                                       final int changesetsGenerated,
                                       final int changesetsIncluded,
                                       final int emptychangesets,
                                       @Nonnull final Duration duration) {
            super.recordOperationEnd(operation, operationDetails, changesetsGenerated, changesetsIncluded,
                emptychangesets, duration);

            // Flush at the end of an operation
            flush();
        }
    }

    /**
     * Record journal entries to a {@link StringBuilder}.
     */
    class StringBuilderRecorder extends BasicRecorder {
        private final StringBuilder stringBuilder;

        /**
         * Create a new {@link StringBuilderRecorder}
         * that records to a new string builder.
         */
        public StringBuilderRecorder() {
            this(new StringBuilder(4096));
        }

        public StringBuilderRecorder(@Nonnull final StringBuilder stringBuilder) {
            this.stringBuilder = Objects.requireNonNull(stringBuilder);
        }

        @Override
        public void record(@Nonnull String entry) {
            stringBuilder.append(entry).append("\n");
        }

        @Override
        public String toString() {
            return stringBuilder.toString();
        }
    }

    /**
     * Record journal entries to a {@link PrintStream}.
     */
    class PrintStreamRecorder extends BasicRecorder {
        private final PrintStream stream;

        /**
         * Create a new {@link PrintStreamRecorder}
         * that records to {@link System#out}.
         */
        public PrintStreamRecorder() {
            this(System.out);
        }

        public PrintStreamRecorder(@Nonnull final PrintStream stream) {
            this.stream = Objects.requireNonNull(stream);
        }

        @Override
        public void record(@Nonnull String entry) {
            stream.println(entry);
        }
    }

    /**
     * record journal entries to an {@link OutputStream}.
     */
    class OutputStreamRecorder extends BasicRecorder {

        private static final Logger logger = LogManager.getLogger();

        private final OutputStream outputStream;

        private boolean streamAvailable;

        public OutputStreamRecorder(@Nonnull final OutputStream outputStream) {
            this.outputStream = Objects.requireNonNull(outputStream);
            streamAvailable = true;
        }

        @Override
        protected void record(@Nonnull String message) {
            if (streamAvailable) {
                try {
                    outputStream.write((message + "\n").getBytes());
                } catch (IOException e) {
                    logger.error("Unable to write to output stream due to error: ", e);

                    // Abort all future attempts to write to the stream. so that we do not throw
                    // hundreds or thousands of exceptions attempting to perform writes that we
                    // know will fail.
                    streamAvailable = false;
                }
            }
        }
    }

    /**
     * Record journal entries in a compressed format to a {@link ZipOutputStream}.
     *
     * It is up to the caller to both create and close the {@link ZipOutputStream}.
     *
     * TODO: Should each phase be written to its own ZipEntry to reduce memory pressure?
     * TODO: Right now all entries are written to a single ZipEntry which means the full
     *       text log will be stored in memory until the entry is closed.
     */
    class ZipStreamRecorder extends BasicRecorder {
        public static final String JOURNAL_FILE_NAME = "stitching_journal.txt";

        private static final Logger logger = LogManager.getLogger();

        private final ZipOutputStream zipOutputStream;

        private ZipEntry zipEntry;

        /**
         * Create a new {@link ZipStreamRecorder}.
         *
         * @param zipOutputStream The zipped output stream to which journal entries will be written.
         */
        public ZipStreamRecorder(@Nonnull final ZipOutputStream zipOutputStream) {
            this.zipOutputStream = Objects.requireNonNull(zipOutputStream);

            try {
                this.zipEntry = new ZipEntry(JOURNAL_FILE_NAME);
                this.zipOutputStream.putNextEntry(zipEntry);
            } catch (IOException e) {
                // Clear the zipEntry so that attempts to record don't throw unnecessary exceptions.
                this.zipEntry = null;
                logger.error("Unable to add zip entry for stitching journal. ", e);
            }
        }

        @Override
        protected void record(@Nonnull String message) {
            if (zipEntry != null) {
                try {
                    zipOutputStream.write(message.getBytes());
                } catch (IOException e) {
                    logger.error("Unable to write message " + message + " to ZipStreamRecorder.", e);
                }
            } else {
                // We failed to put the zipEntry in the output stream, so attempting to
                // write to it would generate needless exceptions.
                logger.debug("Not writing message to ZipOutputStream due to earlier error.");
            }
        }
    }

    /**
     * Record a stream of {@link JournalEntry} protobuf messages to a protobuf
     * {@link StreamObserver<JournalEntry>}
     */
    class StreamObserverRecorder implements JournalRecorder {

        private final StreamObserver<JournalEntry> streamObserver;

        public StreamObserverRecorder(@Nonnull final StreamObserver<JournalEntry> streamObserver) {
            this.streamObserver = Objects.requireNonNull(streamObserver);
        }

        @Override
        public void recordPhaseStart(@Nonnull StitchingPhase phase) {
            streamObserver.onNext(JournalEntry.newBuilder()
                .setPhaseStartMessage(PhaseMessage.newBuilder()
                    .setPhaseName(phase.name())
                    .setPhaseDescription(phase.getPhaseDescription()))
                .build());
        }

        @Override
        public void recordOperationStart(@Nonnull JournalableOperation operation,
                                         @Nonnull final Collection<String> operationDetails) {
            streamObserver.onNext(JournalEntry.newBuilder()
                .setOperationStartMessage(OperationStartMessage.newBuilder()
                    .setOperationName(operation.getOperationName()))
                .build());
        }

        @Override
        public void recordOperationEnd(@Nonnull JournalableOperation operation,
                                       @Nonnull final Collection<String> operationDetails,
                                       final int changesetsGenerated,
                                       final int changesetsIncluded,
                                       final int emptychangesets,
                                       @Nonnull final Duration duration) {
            streamObserver.onNext(JournalEntry.newBuilder()
                .setOperationEndMessage(OperationEndMessage.newBuilder()
                    .setOperationName(operation.getOperationName())
                    .setMetrics(JournalEntry.StitchingMetrics.newBuilder()
                        .setChangesetsGenerated(changesetsGenerated)
                        .setChangesetsIncluded(changesetsIncluded)
                        .setEmptyChangesetCount(emptychangesets)
                        .setDurationMilliseconds(duration.toMillis())))
                .build());
        }

        @Override
        public void recordChangeset(@Nonnull List<String> preambles,
                                    @Nonnull List<String> details,
                                    @Nonnull Verbosity verbosity,
                                    @Nonnull FormatRecommendation format) {
            final StitchingChanges.Builder stitchingChanges = StitchingChanges.newBuilder();

            // Skip recording the preamble for compact operations unless Verbosity is set to COMPLETE_VERBOSITY.
            if (format == FormatRecommendation.PRETTY || verbosity == Verbosity.COMPLETE_VERBOSITY
                || verbosity == Verbosity.PREAMBLE_ONLY_VERBOSITY) {
                stitchingChanges.addAllPreamble(preambles);
            }

            // Unless verbosity is set to PREAMBLE_ONLY, be sure to record the details.
            if (verbosity != Verbosity.PREAMBLE_ONLY_VERBOSITY) {
                stitchingChanges.addAllChangeDetails(details);
            }

            streamObserver.onNext(JournalEntry.newBuilder()
                .setChangeMessage(stitchingChanges)
                .build());
        }

        @Override
        public void recordError(@Nonnull String message) {
            streamObserver.onNext(JournalEntry.newBuilder()
                .setErrorMessage(message)
                .build());
        }

        @Override
        public void recordTopologyInfoAndMetrics(@Nonnull TopologyInfo topologyInfo,
                                                 @Nonnull final StitchingMetrics metrics) {
            streamObserver.onNext(JournalEntry.newBuilder()
                .setTopologyInfo(topologyInfo)
                .build());
            streamObserver.onNext(JournalEntry.newBuilder()
                .setMetrics(JournalEntry.StitchingMetrics.newBuilder()
                    .setChangesetsGenerated(metrics.getTotalChangesetsGenerated())
                    .setChangesetsIncluded(metrics.getTotalChangesetsIncluded())
                    .setEmptyChangesetCount(metrics.getTotalEmptyChangests())
                    .setDurationMilliseconds(metrics.getTimeTaken().toMillis()))
                .build());
        }

        @Override
        public void recordTargets(@Nonnull final List<TargetEntry> targetEntries) {
            targetEntries.forEach(targetEntry ->
                streamObserver.onNext(JournalEntry.newBuilder().setTargetEntry(targetEntry).build()));
        }

        @Override
        public void recordTopologySizes(@Nonnull Map<EntityType, Integer> topologySizes) {
            topologySizes.forEach((entityType, size) -> {
                streamObserver.onNext(JournalEntry.newBuilder().setTopologySizeEntry(
                    TopologySizeEntry.newBuilder()
                        .setEntityType(entityType.name())
                        .setEntityCount(size)
                        .build())
                    .build());
            });
        }

        @Override
        public void recordMessage(@Nonnull String message) {
            streamObserver.onNext(JournalEntry.newBuilder()
                .setMiscellaneousMessage(message).build());
        }
    }
}
