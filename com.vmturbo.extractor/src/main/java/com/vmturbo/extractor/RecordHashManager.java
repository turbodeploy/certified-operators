package com.vmturbo.extractor;

import java.sql.Timestamp;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.Nonnull;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.WriterConfig;

/**
 * Class to manage evolving record state (where the record can be any database {@link Record} with the
 * appropriate columns).
 *
 * <p>This class houses maps related to object hashes that are updated during a processing cycle,
 * and it provides methods to set associated values in {@link Record}s being prepared for
 * the database.</p>
 *
 * <p>To use this class, open a {@link SnapshotManager}, then direct operations
 * required for the processing cycle to that manager. Close the manager once all processing is
 * complete.</p>
 *
 * <p>It is an error to attempt to open a new {@link SnapshotManager} while one
 * is already open, or to open a sequence of snapshot managers with times that are not strictly
 * increasing.</p>
 */
public class RecordHashManager {

    private final Set<Column<?>> columnsForHash;
    private final HashState state;
    // currently open snapshot manager
    private final AtomicReference<SnapshotManager> openSnapshot = new AtomicReference<>();
    private final WriterConfig config;
    private Long priorSnapshotTime = null;
    private final Column<Long> oidColumn;
    private final Column<Long> hashColumn;
    private final Column<Timestamp> firstSeenColumn;
    private final Column<Timestamp> lastSeenColumn;

    protected RecordHashManager(Set<Column<?>> columnsForHash,
            Column<Long> oidColumn,
            Column<Long> hashColumn,
            Column<Timestamp> firstSeenColumn,
            Column<Timestamp> lastSeenColumn,
            WriterConfig config) {
        this.columnsForHash = columnsForHash;
        this.config = config;
        this.oidColumn = oidColumn;
        this.hashColumn = hashColumn;
        this.firstSeenColumn = firstSeenColumn;
        this.lastSeenColumn = lastSeenColumn;
        this.state = new HashState(this, new Long2LongOpenHashMap(), new Long2LongOpenHashMap(), null);
    }

    /**
     * Create a new snapshot manager.
     *
     * @param time Processing time (this will be used to check and set the first seen and
     *        last seen columns).
     * @return new {@link SnapshotManager}.
     */
    @Nonnull
    public SnapshotManager open(final long time) {
        synchronized (openSnapshot) {
            if (!openSnapshot.compareAndSet(null, new SnapshotManager(time, state, config))) {
                throw new IllegalStateException("Cannot open a snapshot time when another is open");
            }
            if (priorSnapshotTime != null && priorSnapshotTime >= time) {
                openSnapshot.set(null);
                throw new IllegalArgumentException("Snapshot times must be strictly increasing");
            }
            this.priorSnapshotTime = time;
            return openSnapshot.get();
        }
    }

    /**
     * Get the currently recorded hash value for a given record.
     *
     * @param oid record oid
     * @return current hash, or null if there is no current hash.
     */
    public Long getEntityHash(long oid) {
        return state.hashesById.containsKey(oid) ? state.hashesById.get(oid) : null;
    }

    /**
     * Get the current last-seen value for the given record hash.
     *
     * @param recordHash hash value
     * @return last seen time, or null if this hash was never seen before.
     */
    public Long getHashLastSeen(long recordHash) {
        return state.hashLastSeen.containsKey(recordHash) ? state.hashLastSeen.get(recordHash) : null;
    }

    /**
     * Close the given snapshot manager, which must be the currently open manager.
     *
     * @param snapshotManager snapshot manager to close
     */
    public void close(SnapshotManager snapshotManager) {
        synchronized (openSnapshot) {
            if (!openSnapshot.compareAndSet(snapshotManager, null)) {
                throw new IllegalStateException(
                        "Closing a SnapshotManager should not happen when it's not the currently open manager");
            }
        }
    }

    /**
     * Class to operate on the {@link RecordHashManager} data in the context of a specific
     * processing cycle (e.g. a specific topology ingestion).
     */
    public static class SnapshotManager implements AutoCloseable {

        private final long time;
        private final HashState state;
        private final Timestamp firstSeenTimestamp;
        private final Timestamp lastSeenTimestamp;
        private boolean doLastSeenUpdateForThisSnapshot = false;
        private final LongSet addedHashes = new LongOpenHashSet();

        /**
         * Create a new instance.
         *
         * @param time   creation time fo the topology being processed.
         * @param state  manager state to be updated by this snapshot manager.
         * @param config writer config, for access to relevant config properties.
         */
        SnapshotManager(final long time,
                @Nonnull final HashState state,
                @Nonnull final  WriterConfig config) {
            this.time = time;
            this.state = state;
            final long updateInterval = TimeUnit.MINUTES.toMillis(config.lastSeenUpdateIntervalMinutes());
            final long updateFuzz = TimeUnit.MINUTES.toMillis(config.lastSeenAdditionalFuzzMinutes());
            // compute timestamps to use as first- and last-seen times in current cycle
            // also, coming out of this, timeOfLastSeenUpdate be updated to reflect this
            // snapshot time, if we'll be doing an update as part of processing this snapshot
            this.firstSeenTimestamp = new Timestamp(time);
            long priorUpdate = state.lastLastSeenUpdateTime != null ? state.lastLastSeenUpdateTime
                    // for first snapshot, pretend that we did one exactly one interval before time
                    // zero, so that we'll treat it like landing precisely on our next expected
                    // update time, and proceed accordingly
                    : -updateInterval;
            long timeOfNextUpdate = priorUpdate + updateInterval;
            if (timeOfNextUpdate <= time) {
                // we'll be doing a new last-seen update for this cycle, so use its time
                // as a basis for this and following cycles
                this.doLastSeenUpdateForThisSnapshot = true;
                timeOfNextUpdate = time + updateInterval;
                state.lastLastSeenUpdateTime = time;
            }
            this.lastSeenTimestamp = new Timestamp(timeOfNextUpdate
                    + updateFuzz);
        }

        /**
         * Update the hash-related data for the given record and set its value.
         *
         * <p>As a side-effect, if the computed hash was not present in the prior snapshot, it's
         * added into the {@link #addedHashes} set.</p>
         *
         * @param record record that is complete wrt hashed fields
         * @return If the record should be written to the database, returns the record hash.
         *         Returns null if the record should NOT be written to the database (because the
         *         hash already exists).
         */
        public Long updateRecordHash(final Record record) {
            final long oid = record.get(state.recordHashManager.oidColumn);
            long hash = record.getXxHash(state.recordHashManager.columnsForHash);
            state.hashLastSeen.put(hash, time);
            if (state.hashesById.containsKey(oid) && hash == state.hashesById.get(oid)) {
                return null;
            } else {
                state.hashesById.put(oid, hash);
                addedHashes.add(hash);
                return hash;
            }
        }

        /**
         * Set first-seen and last-seen values in the given record for a record that appears
         * in the current processing cycle.
         *
         * <p>The first-seen value is set to the snapshot time, but will only be written
         * to the database if no DB record for this oid previously exists.</p>
         *
         * <p>The last-seen value is set to a future time computed from the time of the last
         * last-seen update, and based on configuration parameters. This will always result in a
         * value that is greater or equal to the current topology time. The the potential for
         * "greater than" reflects an optimization for the common case that an entity is present in
         * the topology over many consecutive cycles. We only update the last-seen value stored in
         * the DB for such an entity occasionally, rather than on every cycle. To avoid missing this
         * entity in queries constraining last-seen time, we choose a time that will exceed that of
         * the next periodic last-seen update.</p>
         *
         * @param record record in which to set time fields
         */
        public void setRecordTimes(final Record record) {
            record.set(state.recordHashManager.firstSeenColumn, firstSeenTimestamp);
            record.set(state.recordHashManager.lastSeenColumn, lastSeenTimestamp);
        }

        /**
         * Should be called after all records in the current processing cycle have been assigned
         * their hashes and written to the database.
         *
         * <p>This will update any necessary last_seen values in the database.</p>
         *
         * @param updater The table writer used to update last_seen values.
         */
        public void processChanges(TableWriter updater) {
            LongList drops = computeDroppedHashes();
            updateDroppedHashLastSeen(drops, updater);
            state.hashLastSeen.keySet().removeAll(drops);
            state.hashesById.keySet().removeAll(computeOrphanedOids(new LongOpenHashSet(drops)));
            maybeUpdateLastSeenForCurrentEntities(updater);
        }

        /**
         * Figure out which of the hashes we're tracking were not seen in the current processing cycle.
         *
         * <p>Those hashes need to be dropped, but not until their last-seen times have been
         * updated in the DB.</p>
         *
         * @return hash values to be dropped
         */
        private LongList computeDroppedHashes() {
            // figure out what record hashes dropped out of the topology in this cycle
            long[] drops = state.hashLastSeen.long2LongEntrySet().stream()
                    .filter(e -> e.getLongValue() != time)
                    .mapToLong(Long2LongMap.Entry::getLongKey)
                    .toArray();
            return LongArrayList.wrap(drops);
        }

        /**
         * Add dropped hash values - with corresponding last-seen times according to our current
         * data - to the updates to be applied to the entities table.
         *
         * @param drops           hash values that are being dropped
         * @param updater         table writer that will update last-seen values in table
         */
        private void updateDroppedHashLastSeen(
                LongList drops, TableWriter updater) {
            drops.forEach((long oid) -> {
                try (Record r = updater.open()) {
                    r.set(state.recordHashManager.hashColumn, oid);
                    r.set(state.recordHashManager.lastSeenColumn, new Timestamp(state.hashLastSeen.get(oid)));
                }
            });
        }

        /**
         * Compute a list of oids that were not present in this processing cycle.
         *
         * <p>We get this by finding oids for which we are no longer tracking last-seen times for
         * their associated hash.</p>
         *
         * @param drops hash values being dropped
         * @return oids of orphaned entities
         */
        private LongList computeOrphanedOids(LongSet drops) {
            long[] orphans = state.hashesById.long2LongEntrySet().stream()
                    .filter(e -> drops.contains(e.getLongValue()))
                    .mapToLong(Long2LongMap.Entry::getLongKey)
                    .toArray();
            return LongArrayList.wrap(orphans);
        }

        /**
         * Perform periodic update of all last-seen values, if it's time.
         *
         * <p>The new last-seen times will be based on the time of the next expected update (after
         * this one), plus the configured fuzz factor.</p>
         *
         * <p>By this time, our record state data must be fully updated per the current
         * topology.</p>
         *
         * @param entitiesUpdater table-writer that will perform last-seen updates of the entities
         *                        table
         */
        private void maybeUpdateLastSeenForCurrentEntities(TableWriter entitiesUpdater) {
            if (doLastSeenUpdateForThisSnapshot) {
                state.hashLastSeen.long2LongEntrySet().forEach(e -> {
                    // no need to send updates for current-topology additions, since their upserted
                    // records will already set the last-seen time correctly
                    if (!addedHashes.contains(e.getLongKey())) {
                        try (Record r = entitiesUpdater.open()) {
                            r.set(state.recordHashManager.hashColumn, e.getLongKey());
                            r.set(state.recordHashManager.lastSeenColumn, lastSeenTimestamp);
                        }
                    }
                });
            }
        }

        @Override
        public void close() {
            state.recordHashManager.close(this);
        }
    }

    /**
     * State maintained from one processing cycle to the next by {@link RecordHashManager}, packaged for
     * easy access by {@link RecordHashManager.SnapshotManager} instances.
     */
    private static class HashState {

        private final RecordHashManager recordHashManager;
        private final Long2LongMap hashesById;
        private final Long2LongMap hashLastSeen;
        private Long lastLastSeenUpdateTime;

        private HashState(RecordHashManager recordHashManager,
                Long2LongMap recordHashesById, Long2LongMap hashLastSeen, Long lastLastSeenUpdateTime) {
            this.recordHashManager = recordHashManager;
            this.hashesById = recordHashesById;
            this.hashLastSeen = hashLastSeen;
            this.lastLastSeenUpdateTime = lastLastSeenUpdateTime;
        }
    }

}
