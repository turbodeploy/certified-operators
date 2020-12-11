package com.vmturbo.history.listeners;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonParser;
import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import com.google.protobuf.util.JsonFormat;
import org.jooq.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo.Builder;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.listeners.IngestionStatus.IngestionState;
import com.vmturbo.history.listeners.IngestionStatus.IngestionStatusTypeAdapter;
import com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor;

/**
 * {@link SnapshotStatus} keeps track of all the information having to do with processing of
 * topologies with a specific snapshot (creation) time.
 */
class SnapshotStatus {
    private final Instant createdOn;
    private Instant hourRollupStart;
    private Duration hourRollupDuration;
    private Instant dayMonthRollupStart;
    private Duration dayMonthRollupDuration;
    private AtomicBoolean dirty = new AtomicBoolean(false);
    private TopologyInfo info;

    private Map<TopologyFlavor, IngestionStatus> ingestions = new ConcurrentHashMap<>();

    SnapshotStatus() {
        this.createdOn = Instant.now();
        for (TopologyFlavor flavor : TopologyFlavor.values()) {
            ingestions.put(flavor, new IngestionStatus(this));
        }
    }

    boolean isResolved() {
        return Stream.of(TopologyFlavor.values())
                .allMatch(f -> ingestions.get(f) != null && ingestions.get(f).isResolved());
    }

    boolean isAnyIngestionProcessing() {
        return Stream.of(TopologyFlavor.values())
                .anyMatch(f -> ingestions.get(f) != null
                        && ingestions.get(f).getState() == IngestionState.Processing);
    }

    List<IngestionStatus> forceResolved() {
        return ingestions.values().stream()
                .map(IngestionStatus::forceResolved)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    boolean needsHourlyRollup() {
        return hourRollupDuration == null && isResolved();
    }

    boolean exceedsHourlyRollupTimeout(final int hourlyRollupTimeoutSecs) {
        final Instant cutoff = createdOn.plus(hourlyRollupTimeoutSecs, ChronoUnit.SECONDS);
        return hourRollupDuration == null && Instant.now().isAfter(cutoff);

    }

    boolean needsDayMonthRollup() {
        return isResolved() && dayMonthRollupDuration == null;
    }

    IngestionStatus expect(TopologyFlavor flavor, TopologyInfo info, String label) {
        setTopologyInfo(info);
        ingestions.get(flavor).withLabel(label).expect();
        return ingestions.get(flavor);
    }

    IngestionStatus receive(TopologyFlavor flavor, TopologyInfo info, String label) {
        setTopologyInfo(info);
        ingestions.get(flavor).withLabel(label).receive();
        return ingestions.get(flavor);
    }

    void startIngestion(TopologyFlavor flavor, TopologyInfo info, String label) {
        setTopologyInfo(info);
        ingestions.get(flavor).withLabel(label).startIngestion();
    }

    void finishIngestion(TopologyFlavor flavor,
            TopologyInfo info,
            String topologyLabel,
            BulkInserterFactoryStats stats) {
        setTopologyInfo(info);
        ingestions.get(flavor).withLabel(topologyLabel).finishIngestion(stats);
    }

    void skip(TopologyFlavor flavor, TopologyInfo info, String label) {
        setTopologyInfo(info);
        ingestions.get(flavor).withLabel(label).skip();
    }

    void failIngestion(TopologyFlavor flavor, TopologyInfo info, String label,
            Optional<BulkInserterFactoryStats> partialStats, Exception e) {
        setTopologyInfo(info);
        ingestions.get(flavor).withLabel(label).failIngestion(partialStats, e);
    }

    void startHourRollup() {
        this.hourRollupStart = Instant.now();
        setDirty();
    }

    void finishHourRollup() {
        this.hourRollupDuration = Duration.between(hourRollupStart, Instant.now());
        setDirty();
    }

    void startDayMonthRollup() {
        this.dayMonthRollupStart = Instant.now();
        setDirty();
    }

    void finishDayMonthRollup() {
        this.dayMonthRollupDuration = Duration.between(dayMonthRollupStart, Instant.now());
        setDirty();
    }

    IngestionState getState(TopologyFlavor flavor) {
        final IngestionStatus status = ingestions.get(flavor);
        return status != null ? status.getState() : IngestionStatus.IngestionState.None;
    }

    /**
     * Check whether it is safe to remove this snapshot status from the overall processing status.
     *
     * <p>If any ingestion cannot be removed, then this snapshot cannot be removed.</p>
     *
     * @return true if this snapshot status can be removed
     */
    boolean canRemove() {
        return ingestions.values().stream().allMatch(IngestionStatus::canRemove);
    }

    IngestionStatus getIngestion(final TopologyFlavor flavor) {
        return ingestions.get(flavor);
    }

    Stream<Table<?>> getIngestionTables() {
        return ingestions.values().stream()
                .map(IngestionStatus::getActiveTables)
                .flatMap(Collection::stream);
    }

    String getIngestionSummary() {
        StringBuilder summary = new StringBuilder();
        Stream.of(TopologyFlavor.values())
                .map(f -> ingestions.get(f).getStateIndicator(f))
                .forEach(summary::append);
        return summary.toString();
    }

    String getRollupSummary() {
        return (hourRollupDuration != null ? 'H' : ' ') +
                (dayMonthRollupDuration != null ? "DM" : "  ");
    }

    boolean isDirty() {
        boolean anyDirtyIngestion = ingestions.values().stream()
                .anyMatch(IngestionStatus::isDirty);
        return dirty.getAndSet(false) || anyDirtyIngestion;
    }

    private void setTopologyInfo(TopologyInfo info) {
        if (this.info == null) {
            this.info = info;
            setDirty();
        }
    }

    TopologyInfo getTopologyInfo() {
        return info;
    }

    Instant getSnapshotTime() {
        return Instant.ofEpochMilli(info.getCreationTime());
    }

    TopologyFlavor getIngestionFlavor(final IngestionStatus ingestionStatus) {
        for (Entry<TopologyFlavor, IngestionStatus> entry : ingestions.entrySet()) {
            if (entry.getValue() == ingestionStatus) {
                return entry.getKey();
            }
        }
        return null;
    }

    private void setDirty() {
        dirty.set(true);
    }

    String toJson() {
        return gson().toJson(this);
    }

    static SnapshotStatus fromJson(String json) {
        return gson().fromJson(json, SnapshotStatus.class);
    }

    private static Gson gson;

    private static Gson gson() {
        if (gson == null) {
            gson = new GsonBuilder()
                    .registerTypeAdapter(SnapshotStatus.class, new SnapshotStatusTypeAdapter())
                    .create();
        }
        return gson;
    }

    /**
     * Adapter to handle GSON de/serialization of {@link SnapshotStatus} objects.
     */
    static class SnapshotStatusTypeAdapter extends TypeAdapter<SnapshotStatus> {

        private static final IngestionStatusTypeAdapter ingestionStatusTypeAdapter
                = new IngestionStatusTypeAdapter();
        static final String HOUR_ROLLUP_DURATION_NAME = "hourRollupDuration";
        static final String DAY_MONTH_ROLLUP_DURATION_NAME = "dayMonthRollupDuration";
        static final String INFO_NAME = "info";
        static final String INGESTIONS_NAME = "ingestions";

        @Override
        public void write(final JsonWriter out, final SnapshotStatus value) throws IOException {
            out.beginObject();
            if (value.hourRollupDuration != null) {
                out.name(HOUR_ROLLUP_DURATION_NAME).value(value.hourRollupDuration.toMillis());
            }
            if (value.dayMonthRollupDuration != null) {
                out.name(DAY_MONTH_ROLLUP_DURATION_NAME)
                        .value(value.dayMonthRollupDuration.toMillis());
            }
            if (value.info != null) {
                out.name(INFO_NAME);
                writeTopologyInfo(out, value.info);
            }
            out.name(INGESTIONS_NAME);
            writeIngestions(out, value.ingestions);
            out.endObject();
        }

        @Override
        public SnapshotStatus read(final JsonReader in) throws IOException {
            SnapshotStatus status = new SnapshotStatus();
            in.beginObject();
            while (in.hasNext()) {
                switch (in.nextName()) {
                    case HOUR_ROLLUP_DURATION_NAME:
                        status.hourRollupDuration = Duration.ofMillis(in.nextLong());
                        break;
                    case DAY_MONTH_ROLLUP_DURATION_NAME:
                        status.dayMonthRollupDuration = Duration.ofMillis(in.nextLong());
                        break;
                    case INFO_NAME:
                        status.info = readTopologyInfo(in);
                        break;
                    case INGESTIONS_NAME:
                        readIngestions(in, status);
                        break;
                }
            }
            in.endObject();
            return status;
        }

        private void writeTopologyInfo(final JsonWriter out, final TopologyInfo value) throws IOException {
            out.jsonValue(JsonFormat.printer().omittingInsignificantWhitespace().print(value));
        }

        private TopologyInfo readTopologyInfo(final JsonReader in) throws IOException {
            final Builder builder = TopologyInfo.newBuilder();
            JsonFormat.parser().merge(
                    new JsonParser().parse(in).toString(), builder);
            return builder.build();
        }

        private void writeIngestions(JsonWriter out, Map<TopologyFlavor, IngestionStatus> ingestions)
                throws IOException {

            out.beginObject();
            for (Entry<TopologyFlavor, IngestionStatus> entry : ingestions.entrySet()) {
                out.name(entry.getKey().name());
                ingestionStatusTypeAdapter.write(out, entry.getValue());
            }
            out.endObject();
        }

        private void readIngestions(JsonReader in, SnapshotStatus status) throws IOException {
            in.beginObject();
            while (in.hasNext()) {
                TopologyFlavor flavor = TopologyFlavor.valueOf(in.nextName());
                IngestionStatus ingestionStatus =
                        ingestionStatusTypeAdapter.read(in, status);
                status.ingestions.put(flavor, ingestionStatus);
            }
            in.endObject();

        }

    }
}
