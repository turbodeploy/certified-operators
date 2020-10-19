package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.SCOPED_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPE_START;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPE_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.SEED_OID;
import static com.vmturbo.extractor.schema.Tables.ENTITY;
import static com.vmturbo.extractor.schema.Tables.SCOPE;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.function.IntConsumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.jooq.DSLContext;
import org.jooq.Record2;
import org.jooq.Record5;
import org.jooq.Select;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.sql.utils.jooq.JooqUtil;
import com.vmturbo.sql.utils.jooq.JooqUtil.TempTable;

/**
 * Class to manage and update information in the `scope` table to track evolving entity scopes.
 *
 * <p>Basic approach is as follows:</p>
 * <ul>
 *     <li>
 *         Scope structure is essentially a map from entity/group ids to sets of scoped entity ids.
 *     </li>
 *     <li>
 *         Scope is retained from one topology to the next, so each topology's computed scopes can be
 *         compared to the prior topology's scopes.</li>
 *     <li>
 *         If a scoped entity newly appears (or reappears after an absence) in a cycle, a new
 *         `scope` table record is created for that scoping relationship, even if that relation was
 *         present in the past. That record's `begin` timestamp is the current topology timestamp.
 *         Its `finish` value is set to a fixed timestamp far in the future and will be reset to its
 *         last appearance if it later drops out of the topology.</li>
 *     <li>
 *         If a scoping relationship drops out in a cycle, the corresponding `scope` table record is
 *         updated to set its `finish` value to the prior topology timestamp.</li>
 *     <li>
 *         If a scoping relationship is unchanged from the prior topology, the corresponding `scope`
 *         record is unchanged.</li>
 *     <li>
 *         Upon component restart, the retained scope data will be lost, so it will be restored by
 *         querying the `scope` table for records with `finish` values set to the fixed future
 *         timestamp used for new records.
 *     </li>
 *     <li>
 *         Entity ids are represented by `int` iids obtained by an {@link EntityIdManager} instance
 *         that spans cycles.
 *     </li>
 * </ul>
 *
 * <p>Probably counterintuitively, we keep the scope relationship in "reverse", keeping track, for each
 * entity, the seeds in whose scope that entity appears. This allows us to directly populate the
 * `scope` arrays that appear in the `entity` table in the original schema, while we transition
 * to the new schema.</p>
 */
public class ScopeManager {

    /** valid DB timestamp value that's far in the future.
     *
     * <p>We're specifying a day before end of 9999, since Postgres doesn't deal with larger years.
     * The one-day gap ensures if jOOQ uses this value in a literal and expresses it in local
     * time zone it won't get bumped into year-10000 in that literal.</p>
     */
    public static final OffsetDateTime MAX_TIMESTAMP = OffsetDateTime.parse("9999-12-31T00:00:00Z");
    static final OffsetDateTime EPOCH_TIMESTAMP = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

    private final DbEndpoint db;
    private final EntityIdManager entityIdManager;
    private final ExecutorService pool;
    private final WriterConfig config;

    private DSLContext dsl;
    // for a (seedIid, scopedIid) pair in the scoping relationship, we will have
    // scope.get(scopedIid).contains(seedIid) in both the prior and current scope tables
    Int2ObjectMap<IntSet> priorScope = new Int2ObjectOpenHashMap<>();
    private OffsetDateTime priorTimestamp;
    Int2ObjectMap<IntSet> currentScope = new Int2ObjectOpenHashMap<>();
    private OffsetDateTime currentTimestamp;

    /**
     * Create a new instance.
     *
     * @param entityIdManager entity id manager
     * @param db              DB endpoint (may not yet be initialized)a
     * @param config          ingester config
     * @param pool            thread pool
     */
    public ScopeManager(
            EntityIdManager entityIdManager, DbEndpoint db, WriterConfig config, ExecutorService pool) {
        this.entityIdManager = entityIdManager;
        this.db = db;
        this.pool = pool;
        this.config = config;
    }

    /**
     * Prepare to update scope for a new topology.
     *
     * <p>If prior scope is empty, we reload it from the database. This should happen only on
     * initial execution (in which case the prior scope will still be empty after reload), or after
     * a component restart, which will have discarded the retained scope.
     *
     * @param info {@link TopologyInfo} for topology to be processed
     * @throws UnsupportedDialectException if db endpoint is wack
     * @throws InterruptedException        if we're interrupted
     * @throws SQLException                if a DB operation fails
     */
    public void startTopology(TopologyInfo info)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        this.currentTimestamp = OffsetDateTime.ofInstant(
                Instant.ofEpochMilli(info.getCreationTime()), ZoneOffset.UTC);
        if (dsl == null) {
            this.dsl = db.dslContext();
        }
        if (priorScope.isEmpty()) {
            reloadPriorScope();
        }
    }

    /**
     * Add one or more oids to the current scope of the given seed entity.
     *
     * @param seedOid    iid of entity whose scope is updated
     * @param scopedOids iids of entities/groups/... to add to the seed entity's scope
     */
    public void addInCurrentScope(long seedOid, long... scopedOids) {
        final int seedIid = entityIdManager.toIid(seedOid);
        for (final long scopedOid : scopedOids) {
            addScope(currentScope, seedIid, entityIdManager.toIid(scopedOid));
        }
    }

    private void addScope(Int2ObjectMap<IntSet> scopeMap, int seedIid, int scopedIid) {
        scopeMap.computeIfAbsent(scopedIid, _iid -> new IntOpenHashSet()).add(seedIid);
    }

    /**
     * Call when all scope info for current topology, and it's time to update the persisted scope
     * data.
     */
    public void finishTopology() {
        trim(currentScope); // scope map is complete, so this is a good time to optimize
        try (TableWriter scopeInserter = getScopeInsertWriter();
             TableWriter scopeUpdater = getScopeUpdateWriter()) {

            final IntStream priorIids = priorScope.keySet().stream().mapToInt(Integer::intValue);
            final IntStream currentIids = currentScope.keySet().stream().mapToInt(Integer::intValue);
            IntStream.concat(priorIids, currentIids).distinct()
                    .forEach(scopedIid ->
                            // handle every entity that appeared in this or prior topology
                            finishScopingEntity(scopedIid, scopeInserter, scopeUpdater));
            // update the "0/0" record's finish date to be the current timestamp, for restoration
            // following  a restart. We use an upsert because on very first cycle there will be no
            // record.
            dsl.insertInto(SCOPE,
                    SCOPE.SEED_OID, SCOPE.SCOPED_OID, SCOPE.SCOPED_TYPE, SCOPE.START, SCOPE.FINISH)
                    .values(0L, 0L, EntityType._NONE_, EPOCH_TIMESTAMP, currentTimestamp)
                    .onConflictOnConstraint(SCOPE.getPrimaryKey()).doUpdate()
                    .set(SCOPE.FINISH, currentTimestamp)
                    .execute();
        } finally {
            priorScope = currentScope;
            currentScope = new Int2ObjectOpenHashMap<>();
            priorTimestamp = currentTimestamp;
            currentTimestamp = null;
        }
    }

    /**
     * Get the seed oids currently associated with given scoped oid in the current scope data.
     *
     * @param oid oid whose scoping seeds are needed
     * @return the scoping seeds
     */
    public LongSet getCurrentScopingSeeds(long oid) {
        return getScopingSeeds(currentScope, oid);
    }

    /**
     * Get the seed oids currently associated with given scoped oid in the prior scope data.
     *
     * @param oid oid whose scoping seeds are needed
     * @return the scoping seeds
     */
    public LongSet getPriorScopingSeeds(long oid) {
        return getScopingSeeds(priorScope, oid);
    }

    private LongSet getScopingSeeds(Int2ObjectMap<IntSet> scope, long oid) {
        IntSet iids = scope != null
                ? scope.getOrDefault(entityIdManager.toIid(oid), IntSets.EMPTY_SET)
                : IntSets.EMPTY_SET;
        return iids.stream().mapToLong(entityIdManager::toOid)
                .collect(LongOpenHashSet::new, LongOpenHashSet::add, LongOpenHashSet::addAll);
    }

    private TableWriter getScopeInsertWriter() {
        return SCOPE_TABLE.open(new ScopeInserterSink());
    }

    private TableWriter getScopeUpdateWriter() {
        return SCOPE_TABLE.open(new ScopeUpdaterSink());
    }

    /**
     * Update persisted scope info for this scoped entity, which appeared in the current topology,
     * the prior topology, or both.
     *
     * @param scopedIid     iid of scoped entity
     * @param scopeInserter where to send newly appearing entities
     * @param scopeUpdater  where to send updates for dropped entities
     */
    private void finishScopingEntity(
            int scopedIid, TableWriter scopeInserter, TableWriter scopeUpdater) {
        if (currentScope.containsKey(scopedIid)) {
            if (!priorScope.containsKey(scopedIid)) {
                finishNewEntity(scopedIid, scopeInserter);
            } else {
                finishOverlappingEntity(scopedIid, scopeInserter, scopeUpdater);
            }
        } else if (priorScope.containsKey(scopedIid)) {
            finishDroppedEntity(scopedIid, scopeUpdater);
        }
    }

    /**
     * Persist scope info for a scoped entity that appeared in current topology but not prior.
     *
     * @param scopedIid     iid of new scoped entity
     * @param scopeInserter where to send new records for insertion
     */
    private void finishNewEntity(final int scopedIid, TableWriter scopeInserter) {
        long scopedOid = entityIdManager.toOid(scopedIid);
        currentScope.get(scopedIid).forEach((IntConsumer)seedIid -> {
            try (Record r = scopeInserter.open()) {
                r.set(SEED_OID, entityIdManager.toOid(seedIid));
                r.set(SCOPED_OID, scopedOid);
                r.set(SCOPE_START, currentTimestamp);
            }
        });
    }

    /**
     * Update persisted scope for a scoped entity that appeared in current and prior topology,
     * possibly with different scoping seeds.
     *
     * <p>Scoping seeds that are new in the current scope are inserted into the `scopes` table,
     * while seeds that are no longer present have their `finish` times updated to the prior
     * snapshot time.</p>
     *
     * @param scopedIid     iid of scoped entity
     * @param scopeInserter where to send new `scope` records
     * @param scopeUpdater  where to send existing `scope` record updates
     */
    private void finishOverlappingEntity(
            final int scopedIid, TableWriter scopeInserter, TableWriter scopeUpdater) {
        long scopedOid = entityIdManager.toOid(scopedIid);
        final IntSet priorSet = priorScope.getOrDefault(scopedIid, IntSets.EMPTY_SET);
        final IntSet currentSet = currentScope.getOrDefault(scopedIid, IntSets.EMPTY_SET);
        IntStream.concat(
                Arrays.stream(priorSet.toIntArray()), Arrays.stream(currentSet.toIntArray()))
                .distinct().forEach(seedIid -> {
            if (priorSet.contains(seedIid)) {
                if (!currentSet.contains(seedIid)) {
                    // entity dropped out of the topology... tie off its record in DB
                    try (Record r = scopeUpdater.open()) {
                        r.set(SEED_OID, entityIdManager.toOid(seedIid));
                        r.set(SCOPED_OID, scopedOid);
                    }
                }
            } else if (currentSet.contains(seedIid)) {
                // new entity (or reappearance of an old entity) requires a new record
                try (Record r = scopeInserter.open()) {
                    r.set(SEED_OID, entityIdManager.toOid(seedIid));
                    r.set(SCOPED_OID, scopedOid);
                    r.set(SCOPE_START, currentTimestamp);
                }
            }
        });
    }

    /**
     * Update scope info for a scoped entity that has dropped out of the topology, by setting the
     * `finish` values in the corresponding `scope` records to the prior timestamp.
     *
     * @param scopedIid    scoped entity iid
     * @param scopeUpdater where to send updates to existing records
     */
    private void finishDroppedEntity(final int scopedIid, TableWriter scopeUpdater) {
        long scopedOid = entityIdManager.toOid(scopedIid);
        priorScope.get(scopedIid).forEach((IntConsumer)seedIid -> {
            try (Record r = scopeUpdater.open()) {
                r.set(SEED_OID, entityIdManager.toOid(seedIid));
                r.set(SCOPED_OID, scopedOid);
            }
        });
    }

    /**
     * Trim the given scope map to reduce heap impact.
     *
     * @param scope scope map
     */
    private void trim(Int2ObjectMap<IntSet> scope) {
        // unfortunately, fastutil does not expose its `trim` methods in its collection interfaces,
        // so we need to cast to our known implementation classes.
        scope.values().forEach(scopeSet -> ((IntOpenHashSet)scopeSet).trim());
        ((Int2ObjectOpenHashMap<IntSet>)scope).trim();
    }

    /**
     * Attempt to reconstruct latest scope info from the `scope` table by scanning the table for
     * entries with `finish` values set to MAX_TIMESTAMP.
     *
     * <p>In addition, the timestamp of the prior topology is retrieved from the `final` value of
     * a record with `oid` set to zero, set aside for this purpose.</p>
     */
    private void reloadPriorScope() {
        try (Stream<Record2<Long, Long>> stream =
                     dsl.select(SCOPE.SEED_OID, SCOPE.SCOPED_OID).from(SCOPE)
                             // greater-than comparison because MAX_TIMESTAMP used to be the
                             // end not the start of the day it falls in.
                             .where(SCOPE.FINISH.ge(MAX_TIMESTAMP))
                             .stream()) {
            stream.forEach(r -> {
                int seedIid = entityIdManager.toIid(r.value1());
                int scopedIid = entityIdManager.toIid(r.value2());
                addScope(priorScope, seedIid, scopedIid);
            });
        }
        priorTimestamp = dsl.select(SCOPE.FINISH).from(SCOPE)
                .where(SCOPE.SEED_OID.eq(0L).and(SCOPE.SCOPED_OID.eq(0L))
                        .and(SCOPE.START.eq(EPOCH_TIMESTAMP)))
                .fetchOne(SCOPE.FINISH);
    }

    /**
     * Record sink to insert new records into the `scope` table.
     *
     * <p>Posted records will be missing their scoped entity types, we write records to a temp
     * table and then copy everything to `scopes`, joining the temp table against `entity` to obtain
     * scoped entity types.</p>
     *
     * <p>Records also have their `start` and `finish` timestamps set to the current topology
     * timestamp and `MAX_TIMESTAMP` respectively.</p>
     */
    private class ScopeInserterSink extends DslRecordSink {
        private TempTable<?> tempTable;

        ScopeInserterSink() {
            super(dsl, SCOPE_TABLE, config, pool);
        }

        /**
         * Create the temp table with the fields we need.
         *
         * @param conn database connection on which COPY will execute
         */
        @Override
        protected void preCopyHook(final Connection conn) {
            this.tempTable = JooqUtil.createTemporaryTable(conn, null, getWriteTableName(),
                    SCOPE.SEED_OID, SCOPE.SCOPED_OID, SCOPE.START);
        }

        /**
         * After all records have been posted to the temp table, we enrich them with scoped entity
         * type as well as start and finish timestamps while copying them into the real `scopes`
         * table.
         *
         * @param conn database connection on which COPY operation executed (still open)
         */
        @Override
        protected void postCopyHook(final Connection conn) {
            final Select<Record5<Long, Long, EntityType, OffsetDateTime, OffsetDateTime>> selectStmt;
            // TODO Remove "distinct" during transition away from old schema; it's there because
            // there will often be multiple matching `entity` records for a given entity oid
            selectStmt = DSL.selectDistinct(
                    tempTable.field(SCOPE.SEED_OID),
                    tempTable.field(SCOPE.SCOPED_OID),
                    ENTITY.TYPE.as(SCOPE.SCOPED_TYPE),
                    tempTable.field(SCOPE.START),
                    DSL.inline(MAX_TIMESTAMP).as(SCOPE.FINISH)
            ).from(tempTable.table()).innerJoin(ENTITY)
                    .on(ENTITY.OID.eq(tempTable.field(SCOPE.SCOPED_OID)));
            DSL.using(conn, dsl.configuration().settings())
                    .insertInto(SCOPE, SCOPE.SEED_OID, SCOPE.SCOPED_OID, SCOPE.SCOPED_TYPE, SCOPE.START, SCOPE.FINISH)
                    .select(selectStmt)
                    .execute();
        }

        @Override
        protected Collection<Column<?>> getRecordColumns() {
            return Arrays.asList(SEED_OID, SCOPED_OID, SCOPE_START);
        }

        @Override
        protected String getWriteTableName() {
            return super.getWriteTableName() + "_inserts";
        }
    }

    /**
     * Record sink to close off previous existing scope records when the scoping pair is no longer
     * active in the current topology.
     *
     * <p>Records with matching oids and with their current `finish` timestamp set to our special
     * {@link #MAX_TIMESTAMP} value have their `finish` timestamps set to the prior topology's
     * timestamp.
     */
    private class ScopeUpdaterSink extends DslRecordSink {
        private TempTable<?> tempTable;

        ScopeUpdaterSink() {
            super(dsl, SCOPE_TABLE, config, pool);
        }

        @Override
        protected void preCopyHook(final Connection conn) {
            this.tempTable = JooqUtil.createTemporaryTable(conn, null, getWriteTableName(),
                    SCOPE.SEED_OID, SCOPE.SCOPED_OID);
        }

        @Override
        protected void postCopyHook(final Connection conn) {
            DSL.using(conn, dsl.configuration().settings())
                    .update(SCOPE)
                    .set(SCOPE.FINISH, priorTimestamp)
                    .from(tempTable.table())
                    .where(SCOPE.SEED_OID.eq(tempTable.field(SCOPE.SEED_OID))
                            .and(SCOPE.SCOPED_OID.eq(tempTable.field(SCOPE.SCOPED_OID)))
                            .and(SCOPE.FINISH.ge(MAX_TIMESTAMP)))
                    .execute();
        }

        @Override
        protected Collection<Column<?>> getRecordColumns() {
            return Arrays.asList(SEED_OID, SCOPED_OID);
        }

        @Override
        protected String getWriteTableName() {
            return super.getWriteTableName() + "_updates";
        }
    }
}
