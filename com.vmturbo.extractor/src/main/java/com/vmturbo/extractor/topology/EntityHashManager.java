package com.vmturbo.extractor.topology;

import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.getSourceTopologyLabel;
import static com.vmturbo.extractor.models.Constants.MAX_TIMESTAMP;
import static com.vmturbo.extractor.models.ModelDefinitions.ATTRS;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.ENVIRONMENT_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.FIRST_SEEN;
import static com.vmturbo.extractor.models.ModelDefinitions.LAST_SEEN;
import static com.vmturbo.extractor.schema.Tables.ENTITY;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntArraySet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Constants;
import com.vmturbo.extractor.models.Table.Record;

/**
 * Class to manage writing of entity records to the database.
 *
 * <p>In each topology cycle, our goals are:</p>
 * <ol>
 *     <li>Persist entities that are appearing for the first time to the database.</li>
 *     <li>Update entities whose persisted state needs to change.</li>
 *     <li>Set <code>first_seen</code> and <code>last_seen</code> timestamps for all entities</li>
 * </ol>
 *
 * <p>Entity changes are detected by computing, for each entity, a hash value computed from values
 * that are part of the persisted entity state. Those hash values are retained from one topology
 * cycle to the next, so we detect entity state changes by comparing the retained hash value for
 * a given entity with a newly computed hash value based on information from the current topology.</p>
 *
 * <p>At the end of the cycle, new and changed records are persisted via an upsert operation. In
 * addition, records for entities that were previously present (as evidenced by the presence of a
 * retained hash value) but did not appear in the current cycle are updated to adjust their
 * <code>last_seen</code> values.</p>
 *
 * <p>Rules for <code>first_seen</code> and <code>last_seen</code> are as follows:</p>
 * <ul>
 *     <li><code>first_seen</code> should always be the topology timestamp from the first topology
 *     cycle in which the entity is detected. This column does not participate in the "update" part
 *     of the upsert operation used to persist current-cycle entities, so once a record for the
 *     entity has appeared in the database, <code>first_seen</code> will never change.</li>
 *     <li><code>last_seen</code> for an entity appearing in the current topology will always be
 *     {@link Constants#MAX_TIMESTAMP}.</li>
 *     <li><code>last_seen</code> for an entity that appeared in the prior topology but not in the
 *     current one will be set to one millisecond prior to the current topology timestamp. It will
 *     not be changed in subsequent cycles unless the entity reappears, at which time it will be
 *     reset to MAX_TIMESTAMP.</li>
 * </ul>
 *
 * <p>Upon restart, prior hash data will be unavailable. To handle the first cycle processed after
 * a restart, all existing entities with <code>last_seen</code> set to MAX_TIMESTAMP are retrieved
 * from the DB and used to seed a fake set of retained hashes, with each hash value set to zero
 * (a value that will never appear as an actual hash value). Other than that, the cycle will proceed
 * as detailed above.</p>
 */
public class EntityHashManager {
    private static final Logger logger = LogManager.getLogger();

    /** Columns that are used to compute entity hash. */
    private static final Set<Column<?>> HASHED_ENTITY_COLUMNS = ImmutableSet.of(
            ENTITY_NAME, ENTITY_TYPE_ENUM, ENVIRONMENT_TYPE_ENUM, ATTRS
    );
    private DSLContext dsl;

    final Int2LongMap priorHashes = new Int2LongOpenHashMap();
    final Int2LongMap newHashes = new Int2LongOpenHashMap();

    private final AtomicReference<TopologyInfo> openTopology = new AtomicReference<>();
    private final DataPack<Long> oidPack;
    private TopologyInfo priorTopology = null;
    private OffsetDateTime topologyTimestamp;
    private final IntList unchangedEntityIids = new IntArrayList();

    EntityHashManager(DataPack<Long> oidPack, WriterConfig config) {
        this.oidPack = oidPack;
    }

    /**
     * Start processing a topology.
     *
     * <p>It is an error to do this when another topology is being processed, or for topologies to
     * be processed out of chronological order.</p>
     *
     * @param topologyInfo info for the topology to be processed
     * @param dsl          for database access
     */
    public void open(final TopologyInfo topologyInfo, DSLContext dsl) {
        synchronized (openTopology) {
            if (priorTopology != null
                    && priorTopology.getCreationTime() >= topologyInfo.getCreationTime()) {
                throw new IllegalArgumentException(String.format(
                        "Cannot process topology %s because it precedes previously processed topology %s",
                        getSourceTopologyLabel(topologyInfo), getSourceTopologyLabel(priorTopology)));
            }
            if (!openTopology.compareAndSet(null, topologyInfo)) {
                throw new IllegalStateException(String.format(
                        "Cannot process topology %s because topology %s is being processed",
                        getSourceTopologyLabel(topologyInfo), getSourceTopologyLabel(openTopology.get())));
            }
        }
        logger.info("Processing entity hashes for topology {}",
                TopologyDTOUtil.getSourceTopologyLabel(topologyInfo));
        this.dsl = dsl;
        this.topologyTimestamp = OffsetDateTime.ofInstant(
                Instant.ofEpochMilli(openTopology.get().getCreationTime()), ZoneOffset.UTC);
        if (priorTopology == null) {
            restoreEntityState();
        }
    }

    /**
     * Compute the hash for an entity in the current topology to determine whether the record should
     * participate in the entity upserts performed in this cycle.
     *
     * @param entityRecord record to consider
     * @return true of the records should be part of the upsert
     */
    public boolean processEntity(Record entityRecord) {
        long oid = entityRecord.get(ENTITY_OID_AS_OID);
        int iid = oidPack.toIndex(oid);
        long hash = entityRecord.getXxHash(HASHED_ENTITY_COLUMNS);
        if (!priorHashes.containsKey(iid) || hash != priorHashes.get(iid)) {
            if (logger.isDebugEnabled()) {
                if (!priorHashes.containsKey(iid)) {
                    logger.info("Entity {} was not present in prior topology, hash is {}",
                            oid, hash);
                } else {
                    logger.info("Entity {} hash changed from {} to {}",
                            oid, priorHashes.get(iid), hash);
                }
            }
            // new or newly reappearing or changed entity record - prepare it for upsert
            // this will only be persisted if this entity has never appeared before
            entityRecord.set(FIRST_SEEN, topologyTimestamp);
            // this will be persisted regardless
            entityRecord.set(LAST_SEEN, MAX_TIMESTAMP);
            // this will replace the priorHashes entry for next cycle
            newHashes.put(iid, hash);
            return true;
        } else {
            // between this and newHashes keys, we'll know the ids of all entities that appeared
            // in the current topology, which will allow us to compute the list of entities that
            // dropped out since the prior topology
            unchangedEntityIids.add(iid);
            return false;
        }
    }

    /**
     * Stop processing the current topology and perform any require DB operations.
     */
    public void close() {
        TopologyInfo currentTopology;
        synchronized (openTopology) {
            currentTopology = openTopology.getAndSet(null);
            if (currentTopology == null) {
                throw new IllegalStateException(
                        "Cannot close current topology when no topology is being processed");

            }
        }

        IntSet droppedIids = getDroppedIids();
        // update the last_seen values for all the dropped entities, if any
        if (!droppedIids.isEmpty()) {
            List<Long> droppedOids = IntStream.of(droppedIids.toIntArray())
                    .mapToLong(oidPack::fromIndex)
                    .boxed()
                    .collect(Collectors.toList());
            dsl.update(ENTITY)
                    .set(ENTITY.LAST_SEEN, topologyTimestamp.minus(1, ChronoUnit.MILLIS))
                    .where(ENTITY.OID.in(droppedOids))
                    .execute();
        }
        logger.info("Finished processing topology {}: {} entities unchanged, {} changed, {} dropped",
                unchangedEntityIids.size(), newHashes.size(), droppedIids.size());

        // update prior hashes for next time
        priorHashes.keySet().removeAll(droppedIids);
        priorHashes.putAll(newHashes);

        // reset internal state to prepare for next cycle
        unchangedEntityIids.clear();
        newHashes.clear();
        priorTopology = currentTopology;
    }

    /**
     * This method is used after restart to create a priorHash map that represents the entities
     * present in the last topology processed prior to restart. Those entities are identified by
     * selecting those for whom `last_seen` is our special MAX_TIMESTAMP value. All reconstructed
     * hash values are zero, which is an impossible actual hash value, so all entities in the
     * current topology will participate in the entity upsert, most unnecessarily.
     */
    private void restoreEntityState() {
        priorHashes.clear();
        dsl.select(ENTITY.OID)
                .from(ENTITY)
                .where(ENTITY.LAST_SEEN.eq(MAX_TIMESTAMP))
                .stream()
                .mapToLong(r -> r.getValue(0, Long.class))
                .forEach(oid -> priorHashes.put(oidPack.toIndex(oid), 0L));
        logger.info("Retrived oids of {} entities present in the last topology prior to restart",
                priorHashes.size());
    }

    /**
     * Use this method from a test to prevent an {@link EntityHashManager} from accessing the
     * database to restore its knowledge of prior oids.
     *
     * @param entityOids oids of entities to assert as present in prior topology
     */
    @VisibleForTesting
    void injectPriorTopology(long... entityOids) {
        priorHashes.clear();
        LongStream.of(entityOids)
                .forEach(oid -> priorHashes.put(oidPack.toIndex(oid), 0L));
        priorTopology = TopologyInfo.newBuilder().setCreationTime(0L).build();
    }

    /**
     * Get the hash value of a given entity.
     *
     * <p>The value will be from the current topology if it's been computed, else it will be the
     * retained value from the prior topology.</p>
     *
     * @param oid entity whose hash is requested
     * @return hash value
     */
    @VisibleForTesting
    Long getEntityHash(final Long oid) {
        final int iid = oidPack.toIndex(oid);
        return newHashes.getOrDefault(iid, priorHashes.get(iid));
    }

    /**
     * Compute the set of entities that appeared in the prior topology but not the current one.
     *
     * <p>This is used by the {@link #close()} method, and it is also used in some tests. In the
     * latter case, it must be called prior to closing the topology, since that clears out data
     * needed for the calculation.</p>
     *
     * @return list of iids of entities that have dropped out of the topology
     */
    @VisibleForTesting
    IntSet getDroppedIids() {
        // compute entities that dropped out of topology in this cycle
        IntSet droppedIids = new IntArraySet(priorHashes.keySet());
        droppedIids.removeAll(unchangedEntityIids);
        droppedIids.removeAll(newHashes.keySet());
        return droppedIids;
    }

    @VisibleForTesting
    Int2LongMap getPriorHashes() {
        return priorHashes;
    }
}
