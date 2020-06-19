package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_HASH_AS_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.FIRST_SEEN;
import static com.vmturbo.extractor.models.ModelDefinitions.LAST_SEEN;

import java.sql.Timestamp;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;

/**
 * Class to manage evolving entity state.
 *
 * <p>This class houses maps related to entity hash that are updated during the processing of a
 * topology, and it provides methods to set associated values in entity records being prepared for
 * the database.</p>
 *
 * <p>To use this class, open a {@link SnapshotManager} for a topology, then direct operations
 * required while processing that topology to that manager, and close the manager once topology
 * processing is complete. It is an error to attempt to open a new {@link SnapshotManager} while one
 * is already open, or to open a sequence of snapshot managers with times that are not strictly
 * increasing.</p>
 */
public class EntityHashManager {
    private static final Set<String> INCLUDE_COLUMNS_FOR_ENTITY_HASH = ImmutableSet.of(
            ENTITY_OID_AS_OID.getName(),
            ModelDefinitions.ENTITY_NAME.getName(),
            ModelDefinitions.ENTITY_TYPE_AS_TYPE.getName(),
            ModelDefinitions.ENTITY_STATE.getName(),
            ModelDefinitions.ENVIRONMENT_TYPE.getName(),
            ModelDefinitions.ATTRS.getName(),
            ModelDefinitions.SCOPED_OIDS.getName());

    private final EntityHashState state;
    // currently open snapshot manager
    private final AtomicReference<SnapshotManager> openSnapshot = new AtomicReference<>();
    private final WriterConfig config;
    private Long priorSnapshotTime = null;

    EntityHashManager(WriterConfig config) {
        this.config = config;
        this.state = new EntityHashState(this, new Long2LongOpenHashMap(), new Long2LongOpenHashMap(), null);
    }

    /**
     * Create a new snapshot manager that can be used while processing a new topology.
     *
     * @param time creation time of the topology
     * @return new snapshot manager
     */
    public SnapshotManager open(long time) {
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
     * Get the currently recorded hash value for a given entity.
     *
     * @param entityOid entity oid
     * @return current hash
     */
    public Long getEntityHash(long entityOid) {
        return state.entityHash.containsKey(entityOid) ? state.entityHash.get(entityOid) : null;
    }

    /**
     * Get the current last-seen value for the given entity hash.
     *
     * @param entityHash hash value
     * @return last seen time
     */
    public Long getHashLastSeen(long entityHash) {
        return state.hashLastSeen.containsKey(entityHash) ? state.hashLastSeen.get(entityHash) : null;
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
     * Class to operate on the {@link EntityHashManager} data in the context of a specific
     * topology.
     */
    public static class SnapshotManager implements AutoCloseable {

        private final long time;
        private final EntityHashState state;
        private final Timestamp firstSeenTimestamp;
        private final Timestamp lastSeenTimestamp;
        private boolean doLastSeenUpdateForThisSnapshot = false;
        private final LongSet addedHashes = new LongOpenHashSet();

        /**
         * Create a new instance.
         *
         * @param time   creation time fo the topology being processed
         * @param state  entity manager state to be updated by this snapshot manager
         * @param config writer config, for access to relevant config properties
         */
        SnapshotManager(long time, EntityHashState state, WriterConfig config) {
            this.time = time;
            this.state = state;
            final long updateInterval = TimeUnit.MINUTES.toMillis(config.lastSeenUpdateIntervalMinutes());
            final long updateFuzz = TimeUnit.MINUTES.toMillis(config.lastSeenAdditionalFuzzMinutes());
            // compute timestamps to use as first- and last-seen times in current topology
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
                // we'll be doing a new last-seen update for this topology, so use its time
                // as a basis for this and following topologies
                this.doLastSeenUpdateForThisSnapshot = true;
                timeOfNextUpdate = time + updateInterval;
                state.lastLastSeenUpdateTime = time;
            }
            this.lastSeenTimestamp = new Timestamp(timeOfNextUpdate
                    + updateFuzz);
        }

        /**
         * Update the hash-related data for the given entity and set its value in the record.
         *
         * <p>As a side-effect, if the computed hash was not present in the prior snapshot, it's
         * added into the {@link #addedHashes} set.</p>
         *
         * @param entityRecord entity record that is complete wrt hashed fields
         * @return true if the record should be written to the database (either the entity did not
         * appear in the prior topology or its hash value has changed
         */
        public Long updateEntityHash(final Record entityRecord) {
            final long oid = entityRecord.get(ENTITY_OID_AS_OID);
            long hash = entityRecord.getXxHash(INCLUDE_COLUMNS_FOR_ENTITY_HASH);
            state.hashLastSeen.put(hash, time);
            if (state.entityHash.containsKey(oid) && hash == state.entityHash.get(oid)) {
                return null;
            } else {
                state.entityHash.put(oid, hash);
                addedHashes.add(hash);
                return hash;
            }
        }

        /**
         * Set first-seen and last-seen values in the given entity record for an entity that appears
         * in the current topology.
         *
         * <p>The first-seen value is set to the current topology time, but will only be written
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
         * @param entityRecord entity record in which to set time fields
         */
        public void setEntityTimes(final Record entityRecord) {
            entityRecord.set(FIRST_SEEN, firstSeenTimestamp);
            entityRecord.set(LAST_SEEN, lastSeenTimestamp);
        }

        void processChanges(TableWriter entitiesUpdater) {
            LongList drops = computeDroppedHashes();
            updateDroppedHashLastSeen(drops, entitiesUpdater);
            state.hashLastSeen.keySet().removeAll(drops);
            state.entityHash.keySet().removeAll(computeOrphanedOids(new LongOpenHashSet(drops)));
            maybeUpdateLastSeenForCurrentEntities(entitiesUpdater);
        }

        /**
         * Figure out which of the hashes we're tracking were not seen in the current topology.
         *
         * <p>Those hashes need to be dropped, but not until their last-seen times have been
         * updated in the DB.</p>
         *
         * @return hash values to be dropped
         */
        private LongList computeDroppedHashes() {
            // figure out what entity hashes dropped out of the topology in this cycle
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
         * @param entitiesUpdater table writer that will update last-seen values in entities table
         */
        private void updateDroppedHashLastSeen(
                LongList drops, TableWriter entitiesUpdater) {
            drops.forEach((long oid) -> {
                try (Record r = entitiesUpdater.open()) {
                    r.set(ModelDefinitions.ENTITY_HASH_AS_HASH, oid);
                    r.set(ModelDefinitions.LAST_SEEN, new Timestamp(state.hashLastSeen.get(oid)));
                }
            });
        }

        /**
         * Compute a list of oids that were not present in this topology.
         *
         * <p>We get this by finding oids for which we are no longer tracking last-seen times for
         * their associated hash.</p>
         *
         * @param drops hash values being dropped
         * @return oids of orphaned entities
         */
        private LongList computeOrphanedOids(LongSet drops) {
            long[] orphans = state.entityHash.long2LongEntrySet().stream()
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
         * <p>By this time, our entity state data must be fully updated per the current
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
                            r.set(ENTITY_HASH_AS_HASH, e.getLongKey());
                            r.set(LAST_SEEN, lastSeenTimestamp);
                        }
                    }
                });
            }
        }

        @Override
        public void close() {
            state.entityHashManager.close(this);
        }
    }

    /**
     * State maintained from one topology to the next by {@link EntityHashManager}, packaged for
     * easy access by {@link SnapshotManager} instances.
     */
    private static class EntityHashState {

        private final EntityHashManager entityHashManager;
        private final Long2LongMap entityHash;
        private final Long2LongMap hashLastSeen;
        private Long lastLastSeenUpdateTime;

        private EntityHashState(EntityHashManager entityHashManager,
                Long2LongMap entityHash, Long2LongMap hashLastSeen, Long lastLastSeenUpdateTime) {
            this.entityHashManager = entityHashManager;
            this.entityHash = entityHash;
            this.hashLastSeen = hashLastSeen;
            this.lastLastSeenUpdateTime = lastLastSeenUpdateTime;
        }
    }
}
