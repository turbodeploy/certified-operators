package com.vmturbo.history.listeners;

import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Failed;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Missed;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.None;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Processed;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Processing;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Received;
import static com.vmturbo.history.listeners.IngestionStatus.IngestionState.Skipped;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Table;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.history.db.bulk.BulkInserterFactoryStats;
import com.vmturbo.history.listeners.TopologyCoordinator.TopologyFlavor;
import com.vmturbo.history.schema.abstraction.Vmtdb;

/**
 * This class represents the state of a topology ingestion, for a topology that may or may not yet
 * have been broadcast.
 *
 * <p>Instances of this class are incorproated into the {@link ProcessingStatus} class that
 * drives processing decisions by the {@link ProcessingLoop} class.</p>
 */
class IngestionStatus {
    private static Logger logger = LogManager.getLogger(IngestionStatus.class);

    /**
     * Nonsensical duration value used when an ingestion finishes after its start time has been
     * pruned from the processing status.
     */
    public static final Duration UNKNOWN_PROCESSING_DURATION = Duration.ofSeconds(Long.MAX_VALUE);

    // SnapshotStatus that contains us
    private final SnapshotStatus parent;
    // state of this ingestion
    private IngestionState state;
    // time at which ingestion parocessing starts, or null if it has not yet started
    private Instant ingestionStart;
    // duration of ingestion processing, if it has occured
    private Duration processedDuration;
    // exception representing the cause of a failed exception, if any
    private Exception failureCause;
    // tables that participated in a completed ingestion
    private List<String> activeTables = new ArrayList<>();
    // a readable label for our topology
    private String topologyLabel;
    // whether we've changed since last dirty query
    private AtomicBoolean dirty = new AtomicBoolean(false);
    // a lock for to go with activationCondition (which see)
    private Lock activationLock = new ReentrantLock();
    // a condition that a listener will use to wait for a decision by ProcessingLoop
    // as to whether the associated topology should be processed or skipped
    private Condition activationCondition = activationLock.newCondition();

    /**
     * Create a new instance.
     *
     * <p>The new instance will begin in None state</p>
     *
     * @param parent the {@link SnapshotStatus} for our topology
     */
    IngestionStatus(SnapshotStatus parent) {
        this.parent = parent;
        this.state = None;
    }

    /**
     * Wait to be signaled by {@link ProcessingLoop} thread, or hit a time limit.
     *
     * <p>Normally we should be signaled when the processing loop has decide it's time this
     * topology to be either processed or skipped. As a safety valve, we time out and skip the
     * topology if no decision is signaled.</p>
     *
     * @param time      max amount of time to wait
     * @param unit      temporal time unit for wait time
     * @param waitState ingestion state we're waiting to move from; any state other than this will
     *                  cause true return
     * @return true if our state changed, else we timed out
     * @throws InterruptedException if interrupted
     */
    boolean await(long time, ChronoUnit unit, final IngestionState waitState)
            throws InterruptedException {
        activationLock.lock();
        try {
            Instant now = Instant.now();
            Instant wakeupTime = now.plus(time, unit);
            // keep trying until timeout is expired or our state changes
            while (now.isBefore(wakeupTime) && state == waitState) {
                logger.debug("Waiting for ingestion {} state to change from {}", this, waitState);
                final long millis = Duration.between(now, wakeupTime).toMillis();
                activationCondition.await(millis, TimeUnit.MILLISECONDS);
                now = Instant.now();
            }
            final boolean stateChanged = state != waitState;
            logger.debug("Ingestion {} {}", () -> this,
                    () -> stateChanged ? "ready to go" : "timed out waiting for state change");
            return stateChanged;
        } finally {
            activationLock.unlock();
        }
    }

    /**
     * Wake up the listener thread so that it can process its topology or skip it, depending on the
     * ingestion state - which generally should be updated prior to signaling.
     */
    void signal() {
        logger.debug("Waking up waiting ingestion {}, current state {}", this, this.state);
        activationLock.lock();
        try {
            activationCondition.signalAll();
        } finally {
            activationLock.unlock();
        }
    }

    /**
     * Specify the label for the ingested topology.
     *
     * @param topologyLabel the label
     * @return this ingestion instance, for convenience when creating an instance with a label
     */
    IngestionStatus withLabel(final String topologyLabel) {
        this.topologyLabel = topologyLabel;
        setDirty();
        return this;
    }

    private void setDirty() {
        dirty.set(true);
    }

    IngestionState getState() {
        return state;
    }

    Duration getProcessedDuration() {
        return processedDuration;
    }

    List<Table<?>> getActiveTables() {
        return activeTables.stream()
                .map(Vmtdb.VMTDB::getTable)
                .collect(Collectors.toList());
    }

    Exception getFailureCause() {
        return failureCause;
    }

    /**
     * Put this ingestion into Expected state if its current state is None.
     *
     * <p>Current state other than None means we received the topology before we received the
     * announcement that it's on the way.</p>
     */
    void expect() {
        if (state == None) {
            this.state = IngestionState.Expected;
            setDirty();
        }
    }

    /**
     * Put this ingestion into Receive state.
     */
    void receive() {
        this.state = IngestionState.Received;
        setDirty();
    }

    /**
     * Put this ingestion into Processing state, and record its start time.
     */
    void startIngestion() {
        this.ingestionStart = Instant.now();
        this.state = Processing;
        setDirty();
    }

    /**
     * Put this ingestion into Processed state, resolve its procesing duration, and capture
     * its inserter stats.
     *
     * @param stats inserter stats from the ingestion
     */
    void finishIngestion(BulkInserterFactoryStats stats) {
        if (ingestionStart == null) {
            logger.warn("Ingestion finished but start time not available: {}", this);
            processedDuration = UNKNOWN_PROCESSING_DURATION;
        } else {
            this.processedDuration = Duration.between(ingestionStart, Instant.now());
        }
        this.state = Processed;
        this.activeTables = getStatsActiveTables(stats);
        setDirty();
    }

    /**
     * Put this ingestion into Failed state and record its failure cause.
     *
     * @param partialStats inserter stats, if available, reflecting records inserted before
     *                     this ingestion failed
     * @param e exception describing the cause of the ingestion failure
     */
    void failIngestion(Optional<BulkInserterFactoryStats> partialStats, Exception e) {
        this.failureCause = e;
        partialStats.ifPresent(s -> {
            this.activeTables = getStatsActiveTables(s);
        });
        setDirty();
    }

    private List<String> getStatsActiveTables(BulkInserterFactoryStats stats) {
        return stats.getOutTables().stream()
                .map(Table::getName)
                .collect(Collectors.toList());
    }

    /**
     * Put this ingestion into Skipped state.
     */
    void skip() {
        this.state = Skipped;
        setDirty();
    }

    /**
     * Check whether it's OK to remove this ingestion status from the overall processing status.
     *
     *<p>Current cases where we pin the entry are:</p>
     * <ul>
     *     <li>Received state: dropping the entry will mean we can never go back and process the
     *     topology. (It will eventually be skipped when teh listener times out waiting for
     *     {@link ProcessingLoop} to decide what to do.)</li>
     *     <li>Processing state: In this case we run the risk of </li>
     * </ul>
     * @return true if it's OK to remove this ingestion status object
     */
    boolean canRemove() {
        return state != Received && state != Processing;
    }

    /**
     * Put this ingestion into Missed state.
     */
    void miss() {
        this.state = Missed;
        setDirty();
    }

    /**
     * Check if this ingestion is in a resolved state, meaning a state from which it should
     * not change.
     *
     * <p>Resolved states include Processed, Skipped, Missed, and Failed</p>
     *
     * @return true if the ingestion is resolved
     */
    boolean isResolved() {
        return state == Processed || state == Skipped || state == Missed || state == Failed;
    }

    /**
     * Move this ingestion status into a resolved state, if it's not already resolved.
     *
     * <p>This happens when a snapshot has exceeded the resolution timeout for hourly rollups, so
     * we choose a resolved state based on current state and proceed accordingly.</p>
     *
     * @return this ingestion status object, if its state was changed, else null
     */
    IngestionStatus forceResolved() {
        IngestionState newState;
        switch (state != null ? state : None) {
            case None:
            case Expected:
                newState = Missed;
                break;
            case Received:
                newState = Skipped;
                break;
            case Processing:
                logger.error("Cannot force processing ingestion to resolved state: "
                        + this.toString());
                return null;
            default:
                // all other states are already resolved, so nothing to do
                return null;
        }
        if (newState == state) {
            return null;
        }
        state = newState;
        return this;
    }

    /**
     * Return a single character to inciate this ingestion state in a logged summary.
     *
     * <p>All but processed are lower-case letters. Processed states are upper-case letters based
     * on the topology flavor, and Processing use the same lower-cased letters.</p>
     *
     * @param flavor topology flavor
     * @return state indicator
     */
    Character getStateIndicator(TopologyFlavor flavor) {
        switch (state) {
            // Processing and Processed state use flavor-specific indicators
            case Processing:
                return flavor.getProcessingStatusChar(false);
            case Processed:
                return flavor.getProcessingStatusChar(true);
            default:
                // all others are independent of flavor
                return state.getStatusChar();
        }
    }

    String getTopologyLabel() {
        return topologyLabel;
    }

    Boolean isDirty() {
        return dirty.getAndSet(false);
    }

    public TopologyInfo getInfo() {
        return parent.getTopologyInfo();
    }

    @Override
    public String toString() {
        return String.format("IngestionStatus[snapshot=%s, flavor=%s, state=%s]",
                parent.getSnapshotTime(), parent.getIngestionFlavor(this), state);
    }

    /**
     * Possible states of an ingestion.
     */
    enum IngestionState {
        /**
         * A topology has been announced, but has not been received.
         */
        Expected('e'),
        /**
         * A topology has been received, but yet processed or skipped.
         */
        Received('r'),
        /**
         * A topology has begun ingestion processing.
         */
        Processing('x'),
        /**
         * A topology has completed ingestion processing successfully.
         */
        Processed('X'),
        /**
         * A topology that was received and discarded, after deciding not to ingest it.
         */
        Skipped('s'),
        /**
         * A topology was expected, but it never materialized and a later topology was
         * received. The expected topology is presumed lost.
         */
        Missed('m'),
        /**
         * A topology failed ingestion procesing.
         */
        Failed('f'),
        /**
         * A newly created ingestion.
         */
        None('-');

        private final char statusChar;

        IngestionState(char statusChar) {
            this.statusChar = statusChar;
        }

        public char getStatusChar() {
            return statusChar;
        }
    }

    /**
     * Adapter to handle GSON de/serializing of {@link IngestionStatus} objects.
     */
    static class IngestionStatusTypeAdapter extends TypeAdapter<IngestionStatus> {

        static final String STATE_NAME = "state";
        static final String ACTIVE_TABLES_NAME = "activeTables";
        static final String TOPOLOGY_LABEL_NAME = "topologyLabel";
        static final String FAILURE_CAUSE_NAME = "failureCause";
        static final String FAILURE_CAUSE_CLASS_NAME = "class";
        static final String FAILURE_CAUSE_MESSAGE_NAME = "message";
        static final String PROCESSING_DURATION_NAME = "processingDuration";

        @Override
        public void write(final JsonWriter out, final IngestionStatus value) throws IOException {
            out.beginObject();
            out.name(STATE_NAME).value(value.state.name());
            if (value.processedDuration != null) {
                out.name(PROCESSING_DURATION_NAME).value(value.processedDuration.toMillis());
            }
            if (value.activeTables != null) {
                out.name(ACTIVE_TABLES_NAME);
                out.beginArray();
                for (String activeTable : value.activeTables) {
                    out.value(activeTable);
                }
                out.endArray();
            }
            if (value.topologyLabel != null) {
                out.name(TOPOLOGY_LABEL_NAME).value(value.topologyLabel);
            }
            if (value.failureCause != null) {
                final Exception cause = value.failureCause;
                out.name(FAILURE_CAUSE_NAME);
                out.beginObject();
                out.name(FAILURE_CAUSE_CLASS_NAME).value(cause.getClass().getName());
                out.name(FAILURE_CAUSE_MESSAGE_NAME).value(cause.getMessage());
                out.endObject();
            }
            out.endObject();
        }

        @Override
        public IngestionStatus read(final JsonReader in) throws IOException {
            return read(in, null);
        }

        public IngestionStatus read(final JsonReader in, final SnapshotStatus parent)
                throws IOException {

            IngestionStatus status = new IngestionStatus(parent);
            in.beginObject();
            while (in.hasNext()) {
                switch (in.nextName()) {
                    case STATE_NAME:
                        status.state = IngestionState.valueOf(in.nextString());
                        if (status.state == Processing) {
                            status.state = IngestionState.Received;
                        }
                        break;
                    case PROCESSING_DURATION_NAME:
                        status.processedDuration = Duration.ofMillis(in.nextLong());
                        break;
                    case ACTIVE_TABLES_NAME:
                        in.beginArray();
                        while (in.hasNext()) {
                            status.activeTables.add(in.nextString());
                        }
                        in.endArray();
                        break;
                    case TOPOLOGY_LABEL_NAME:
                        status.topologyLabel = in.nextString();
                        break;
                    case FAILURE_CAUSE_NAME:
                        status.failureCause = readFailureCause(in);
                        break;
                }
            }
            in.endObject();
            return status;
        }

        private Exception readFailureCause(final JsonReader in) throws IOException {
            in.beginObject();
            String className = null;
            Class<? extends Exception> cls = null;
            String msg = null;
            while (in.hasNext()) {
                try {
                    switch (in.nextName()) {
                        case FAILURE_CAUSE_CLASS_NAME:
                            className = in.nextString();
                            cls = (Class<? extends Exception>)Class.forName(className);
                            break;
                        case FAILURE_CAUSE_MESSAGE_NAME:
                            msg = in.nextString();
                            break;
                    }
                } catch (ClassNotFoundException e) {
                    logger.error("Failed to find constructor for class {}", className, e);
                }
            }
            in.endObject();

            if (cls != null) {
                try {
                    final Constructor<? extends Exception> cons = cls.getConstructor(String.class);
                    return cons.newInstance(msg);
                } catch (IllegalAccessException | NoSuchMethodException
                        | InstantiationException | InvocationTargetException e) {

                    logger.error("Failed to instantiate instance of class {}", className, e);
                }
            }
            return null;
        }
    }
}
