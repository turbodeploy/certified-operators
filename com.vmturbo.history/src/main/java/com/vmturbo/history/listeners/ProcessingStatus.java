package com.vmturbo.history.listeners;

import static com.vmturbo.history.schema.abstraction.tables.IngestionStatus.INGESTION_STATUS;
import static java.time.temporal.ChronoUnit.HOURS;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.listeners.IngestionStatus.IngestionState;
import com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor;
import com.vmturbo.history.schema.abstraction.tables.records.IngestionStatusRecord;
import com.vmturbo.sql.utils.jooq.JooqUtil;

/**
 * Manage status of live topology processing.
 *
 * <p>The information stored in this structure drives decisions by {@link ProcessingLoop} about
 * what actions to take regarding incoming topology broadcasts.</p>
 */
class ProcessingStatus {
    private static final Logger logger = LogManager.getLogger(ProcessingStatus.class);

    private final int retentionSecs;
    private final int hourlyRollupTimeoutSecs;
    private final DSLContext dsl;
    private final Map<Instant, SnapshotStatus> statusMap = new ConcurrentHashMap<>();
    private Instant lastRepartitionTime = Instant.MIN;

    Instant getLastRepartitionTime() {
        return lastRepartitionTime;
    }

    void setLastRepartitionTime(Instant lastRepartitionTime) {
        this.lastRepartitionTime = lastRepartitionTime;
    }

    ProcessingStatus(TopologyCoordinatorConfig config, DSLContext dsl) {
        this.retentionSecs = config.topologyRetentionSecs();
        this.hourlyRollupTimeoutSecs = config.hourlyRollupTimeoutSecs();
        this.dsl = dsl;
    }

    IngestionStatus expect(TopologyFlavor flavor, TopologyInfo info, String topologyLabel) {
        return getStatus(info).expect(flavor, info, topologyLabel);
    }

    IngestionStatus receive(TopologyFlavor flavor, TopologyInfo info, String topologyLabel) {
        return getStatus(info).receive(flavor, info, topologyLabel);
    }

    void startIngestion(TopologyFlavor flavor, TopologyInfo info, String topologyLabel) {
        getStatus(info).startIngestion(flavor, info, topologyLabel);
    }

    void finishIngestion(TopologyFlavor flavor,
            TopologyInfo info,
            String topologyLabel,
            BulkInserterFactoryStats stats) {
        getStatus(info).finishIngestion(flavor, info, topologyLabel, stats);
    }

    void skip(TopologyFlavor flavor, TopologyInfo info, String topologyLabel) {
        getStatus(info).skip(flavor, info, topologyLabel);
    }

    void failIngestion(TopologyFlavor flavor, TopologyInfo info, String topologyLabel,
            Optional<BulkInserterFactoryStats> partialStats, Exception e) {
        getStatus(info).failIngestion(flavor, info, topologyLabel, partialStats, e);
    }

    void startHourRollup(Instant timestamp) {
        statusMap.get(timestamp).startHourRollup();
    }

    void finishHourRollup(Instant timestamp) {
        statusMap.get(timestamp).finishHourRollup();
    }

    void startDayMonthRollup(Instant timestamp) {
        statusMap.get(timestamp).startDayMonthRollup();
    }

    void finishDayMonthRollup(Instant timestamp) {
        statusMap.get(timestamp).finishDayMonthRollup();
    }

    boolean isProcessing(final TopologyFlavor flavor, final TopologyInfo info) {
        return getStatus(info).getState(flavor) == IngestionStatus.IngestionState.Processing;
    }

    boolean isReceived(final TopologyFlavor flavor, final TopologyInfo info) {
        return getStatus(info).getState(flavor) == IngestionState.Received;
    }

    private SnapshotStatus getStatus(TopologyInfo info) {
        return statusMap.computeIfAbsent(Instant.ofEpochMilli(info.getCreationTime()),
                snapshotTime -> new SnapshotStatus());
    }

    Stream<IngestionStatus> getIngestions(final TopologyFlavor flavor) {
        return getSnapshotTimes()
                .map(statusMap::get)
                .map(status -> status.getIngestion(flavor))
                .filter(Objects::nonNull);
    }

    IngestionStatus getIngestion(TopologyFlavor flavor, TopologyInfo info) {
        return getStatus(info).getIngestion(flavor);
    }

    Stream<SnapshotStatus> getSnapshots() {
        return statusMap.entrySet().stream()
                .sorted(Entry.comparingByKey())
                .map(Entry::getValue);
    }

    Stream<Instant> getSnapshotTimes() {
        return statusMap.keySet().stream()
                .sorted(Comparator.reverseOrder());
    }

    boolean needsDayMonthRollup(final Instant snapshot) {
        final Instant hour = snapshot.truncatedTo(HOURS);
        return statusMap.get(snapshot).needsDayMonthRollup()
                && statusMap.keySet().stream()
                .filter(t -> t.truncatedTo(HOURS) == hour)
                .map(statusMap::get)
                .allMatch(s -> s.isResolved() && !s.needsHourlyRollup());
    }

    boolean needsHourlyRollup(final Instant snapshot) {
        return statusMap.get(snapshot).needsHourlyRollup();
    }

    boolean exceedsHourlyRollupTimeout(final Instant snapshot) {
        return statusMap.get(snapshot).exceedsHourlyRollupTimeout(hourlyRollupTimeoutSecs);
    }

    boolean isAnyIngestionProcessing(final Instant snapshot) {
        return statusMap.get(snapshot).isAnyIngestionProcessing();
    }

    List<IngestionStatus> forceResolved(final Instant snapshot) {
        return statusMap.get(snapshot).forceResolved();
    }

    Stream<Table<?>> getIngestionTablesForHour(final Instant snapshot) {
        Instant hourStart = snapshot.truncatedTo(HOURS);
        Instant hourEnd = hourStart.plus(1, HOURS);
        return getSnapshotTimes()
                .filter(timestamp -> timestamp.isBefore(hourEnd) && !timestamp.isBefore(hourStart))
                .flatMap(this::getIngestionTables);
    }

    Stream<Table<?>> getIngestionTables(Instant snapshot) {
        return statusMap.get(snapshot).getIngestionTables();
    }

    void prune() {
        Instant cutoff = Instant.now().minus(retentionSecs, ChronoUnit.SECONDS);
        final List<Instant> staleSnapshots = statusMap.keySet().stream()
                .filter(t -> t.isBefore(cutoff) && statusMap.get(t).canRemove())
                .collect(Collectors.toList());
        staleSnapshots.forEach(statusMap::remove);
    }

    String getSummary(String indent) {
        Summary summary = new Summary();
        final List<Instant> snapshots = getSnapshotTimes().collect(Collectors.toList());
        Collections.reverse(snapshots);
        for (Instant snapshot : snapshots) {
            final SnapshotStatus status = statusMap.get(snapshot);
            summary.update(snapshot, status.getIngestionSummary(), status.getRollupSummary());
        }
        return summary.getSummary(indent);
    }

    public boolean isEmpty() {
        return statusMap.isEmpty();
    }

    void store() {
        List<IngestionStatusRecord> records = new ArrayList<>();
        for (Entry<Instant, SnapshotStatus> entry : statusMap.entrySet()) {
            final SnapshotStatus status = entry.getValue();
            if (status.isDirty()) {
                String json = status.toJson();
                records.add(new IngestionStatusRecord(entry.getKey().toEpochMilli(), json));
            }
        }
        if (!records.isEmpty()) {
            try {
                InsertSetStep<IngestionStatusRecord> insert = dsl.insertInto(INGESTION_STATUS);
                // need to insert first record by itself in order to get variable of required type
                // for following steps
                InsertSetMoreStep<IngestionStatusRecord> insertValues = insert.set(records.get(0));
                records.stream().skip(1L).forEach(insertValues::set);
                insertValues.onDuplicateKeyUpdate()
                        .set(INGESTION_STATUS.STATUS,
                                JooqUtil.upsertValue(INGESTION_STATUS.STATUS, dsl.dialect()))
                        .execute();
            } catch (DataAccessException e) {
                logger.error("Failed to persist topology processing status", e);
            }
        }
    }

    /**
     * This method fills the processing status with data stored in the database.
     *
     * <p>Database records are stored in a record-per-snapshot fashion. The actual
     * {@link SnapshotStatus} value stored in each record is actaully stored as a JSON
     * serialization of the object value.</p>
     *
     * <p>GSON type adapters for {@link SnapshotStatus} and {@link IngestionStatus} classes
     * are nested within those classes and registered by them on first use of either
     * {@link com.google.gson.Gson#fromJson(Reader, Type)} or {@link Gson#toJson(Object)}.
     * </p>
     */
    @VisibleForTesting
    void load() {
        statusMap.clear();
        // Load the records on a separate thread so that we don't block the rest of the
        // component startup process. We could do this in a retrying manner later.
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> {
            try {
                dsl.selectFrom(INGESTION_STATUS)
                        .forEach(record -> {
                            final Instant snapshotTime =
                                    Instant.ofEpochMilli(
                                            record.get(INGESTION_STATUS.SNAPSHOT_TIME));
                            SnapshotStatus status = SnapshotStatus.fromJson(record.getStatus());
                            statusMap.put(snapshotTime, status);
                        });
            } catch (Exception e) {
                logger.error("Failed to load saved topology processing status", e);
            }
        });
        executor.shutdown();
    }

    /**
     * Creates a readable summary of the currnet processing status, suitable for logging.
     */
    private static class Summary {
        private static final SimpleDateFormat HOUR_LINE_FORMAT = new SimpleDateFormat(
                "yyyy-MM-dd HH");
        private static final SimpleDateFormat MINITE_LINE_FORMAT = new SimpleDateFormat("mm ");

        private String hourLine = "|";
        private String minuteLine = "|";
        private String ingestionsLine = "|";
        private String rollupLine = "|";
        private Instant currentSnapshot = null;

        public void update(Instant snapshot, String ingestionSummary, String rollupSummary) {
            if (isNewHour(snapshot)) {
                if (currentSnapshot != null) {
                    normalizeLengths(true);
                }
                hourLine = hourLine + HOUR_LINE_FORMAT.format(Timestamp.from(snapshot));
            }
            minuteLine += MINITE_LINE_FORMAT.format(Timestamp.from(snapshot)) + "|";
            ingestionsLine += ingestionSummary + "|";
            rollupLine += rollupSummary + "|";
            currentSnapshot = snapshot;
            normalizeLengths(false);
        }

        String getSummary(String indent) {
            normalizeLengths(true);
            return String.join("\n", Arrays.asList(
                    indent + "Hour:       " + hourLine,
                    indent + "Minute:     " + minuteLine,
                    indent + "Ingestions: " + ingestionsLine,
                    indent + "Rollups:    " + rollupLine
            ));
        }

        private boolean isNewHour(final Instant snapshot) {
            return currentSnapshot == null
                    || currentSnapshot.truncatedTo(HOURS).isBefore(snapshot.truncatedTo(HOURS));
        }

        private void normalizeLengths(boolean includeHourLine) {
            minuteLine = unbar(minuteLine);
            ingestionsLine = unbar(ingestionsLine);
            rollupLine = unbar(rollupLine);
            int maxLength = getMaxLength(minuteLine, ingestionsLine, rollupLine,
                    includeHourLine ? unbar(hourLine) : "");
            minuteLine = padTo(minuteLine, maxLength) + "|";
            ingestionsLine = padTo(ingestionsLine, maxLength) + "|";
            rollupLine = padTo(rollupLine, maxLength) + "|";
            if (includeHourLine) {
                hourLine = padTo(unbar(hourLine), maxLength) + "|";
            }
        }

        private String unbar(String s) {
            return s.endsWith("|") ? s.substring(0, s.length() - 1) : s;
        }

        private int getMaxLength(String... strings) {
            return Stream.of(strings)
                    .map(String::length)
                    .max(Integer::compareTo)
                    .orElse(0);
        }

        private String padTo(String s, int length) {
            final int padding = length - s.length();
            if (padding > 0) {
                s = s + StringUtils.repeat(" ", padding);
            }
            return s;
        }
    }
}
