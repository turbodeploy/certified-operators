package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPED_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPE_START;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPE_TABLE;
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
 */
public class ScopeManager {

    /** valid DB timestamp value that's far in the future. */
    public static final OffsetDateTime MAX_TIMESTAMP = OffsetDateTime.parse("9999-12-31T23:59:59Z");
    static final OffsetDateTime EPOCH_TIMESTAMP = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

    private final DbEndpoint db;
    private final EntityIdManager entityIdManager;
    private final ExecutorService pool;
    private final WriterConfig config;

    private DSLContext dsl;
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
     * Add one or more oids to the current scope of the given entity.
     *
     * @param entityOid  iid of entity whose scope is updated
     * @param scopedOids iids of entities/groups/... to add to this entity's scope
     */
    public void addScope(long entityOid, long... scopedOids) {
        final int entityIid = entityIdManager.toIid(entityOid);
        IntSet scopeSet = currentScope.computeIfAbsent(entityIid, IntOpenHashSet::new);
        for (final long scopedIid : scopedOids) {
            scopeSet.add(entityIdManager.toIid(scopedIid));
        }
    }

    private void addScope(Int2ObjectMap<IntSet> scopeMap, int entityIid, int scopedIid) {
        scopeMap.computeIfAbsent(entityIid, IntOpenHashSet::new).add(scopedIid);
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
                    .forEach(entityIid ->
                            // handle every entity that appeared in this or prior topology
                            finishScopingEntity(entityIid, scopeInserter, scopeUpdater));
            // update the "0/0" record's finish date to be the current timestamp, for restoration
            // following  a restart. We use an upsert because on very first cycle there will be no
            // record.
            dsl.insertInto(SCOPE,
                    SCOPE.ENTITY_OID, SCOPE.SCOPED_OID, SCOPE.SCOPED_TYPE, SCOPE.START, SCOPE.FINISH)
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
     * Get the scoped oids currently associated with given oid in the current scope data.
     *
     * @param oid oid whose scope is needed
     * @return the scope
     */
    public LongSet getCurrentScope(long oid) {
        return getScope(currentScope, oid);
    }

    /**
     * Get the scoped oids currently associated with given oid in the prior scope data.
     *
     * @param oid oid whose scope is needed
     * @return the scope
     */
    public LongSet getPriorScope(long oid) {
        return getScope(priorScope, oid);
    }

    private LongSet getScope(Int2ObjectMap<IntSet> scope, long oid) {
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
     * Update persisted scope info for this entity, which appeared in the current topology, the
     * prior topology, or both.
     *
     * @param entityIid     iid of entity
     * @param scopeInserter where to send newly appearing entities
     * @param scopeUpdater  where to send updates for dropped entities
     */
    private void finishScopingEntity(
            int entityIid, TableWriter scopeInserter, TableWriter scopeUpdater) {
        if (currentScope.containsKey(entityIid)) {
            if (!priorScope.containsKey(entityIid)) {
                finishNewEntity(entityIid, scopeInserter);
            } else {
                finishOverlappingEntity(entityIid, scopeInserter, scopeUpdater);
            }
        } else if (priorScope.containsKey(entityIid)) {
            finishDroppedEntity(entityIid, scopeUpdater);
        }
    }

    /**
     * Persist scope info for an entity that appeared in current topology but not prior.
     *
     * @param entityIid     iid of entity
     * @param scopeInserter where to send new records for insertion
     */
    private void finishNewEntity(final int entityIid, TableWriter scopeInserter) {
        long entityOid = entityIdManager.toOid(entityIid);
        currentScope.get(entityIid).forEach((IntConsumer)scopedIid -> {
            try (Record r = scopeInserter.open()) {
                r.set(ENTITY_OID, entityOid);
                r.set(SCOPED_OID, entityIdManager.toOid(scopedIid));
                r.set(SCOPE_START, currentTimestamp);
            }
        });
    }

    /**
     * Update persisted scope for an entity that appeared in current and prior topology, possibly
     * with different scopes.
     *
     * <p>Scoped entities that are new in the current scope are inserted into the `scopes` table,
     * while scoped entities that are no longer present have their `finish` times updated to the
     * prior snapshot time.</p>
     *
     * @param entityIid     of entity
     * @param scopeInserter where to send new `scope` records
     * @param scopeUpdater  where to send existing `scope` record updates
     */
    private void finishOverlappingEntity(
            final int entityIid, TableWriter scopeInserter, TableWriter scopeUpdater) {
        long entityOid = entityIdManager.toOid(entityIid);
        final IntSet priorSet = priorScope.getOrDefault(entityIid, IntSets.EMPTY_SET);
        final IntSet currentSet = currentScope.getOrDefault(entityIid, IntSets.EMPTY_SET);
        Stream.concat(priorSet.stream(), currentSet.stream()).distinct().forEach(scopedIid -> {
            if (priorSet.contains(scopedIid.intValue())) {
                if (!currentSet.contains(scopedIid.intValue())) {
                    // entity dropped out of the topology... tie off its record in DB
                    try (Record r = scopeUpdater.open()) {
                        r.set(ENTITY_OID, entityOid);
                        r.set(SCOPED_OID, entityIdManager.toOid(scopedIid));
                    }
                }
            } else if (currentSet.contains(scopedIid.intValue())) {
                // new entity (or reappearance of an old entity) requires a new record
                try (Record r = scopeInserter.open()) {
                    r.set(ENTITY_OID, entityOid);
                    r.set(SCOPED_OID, entityIdManager.toOid(scopedIid));
                    r.set(SCOPE_START, currentTimestamp);
                }
            }
        });
    }

    /**
     * Update scope info for an entity that has dropped out of the topology, by setting the `finish`
     * values in the corresponding `scope` records to the prior timestamp.
     *
     * @param entityIid    entity iid
     * @param scopeUpdater where to send updates to existing records
     */
    private void finishDroppedEntity(final int entityIid, TableWriter scopeUpdater) {
        long entityOid = entityIdManager.toOid(entityIid);
        priorScope.get(entityIid).forEach((IntConsumer)scopedIid -> {
            try (Record r = scopeUpdater.open()) {
                r.set(ENTITY_OID, entityOid);
                r.set(SCOPED_OID, entityIdManager.toOid(scopedIid));
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
                     dsl.select(SCOPE.ENTITY_OID, SCOPE.SCOPED_OID).from(SCOPE)
                             .where(SCOPE.FINISH.eq(MAX_TIMESTAMP))
                             .stream()) {
            stream.forEach(r -> {
                int eiid = entityIdManager.toIid(r.value1());
                int siid = entityIdManager.toIid(r.value2());
                addScope(priorScope, eiid, siid);
            });
        }
        priorTimestamp = dsl.select(SCOPE.FINISH).from(SCOPE)
                .where(SCOPE.ENTITY_OID.eq(0L).and(SCOPE.SCOPED_OID.eq(0L))
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
                    SCOPE.ENTITY_OID, SCOPE.SCOPED_OID, SCOPE.START);
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
                    tempTable.field(SCOPE.ENTITY_OID),
                    tempTable.field(SCOPE.SCOPED_OID),
                    ENTITY.TYPE.as(SCOPE.SCOPED_TYPE),
                    tempTable.field(SCOPE.START),
                    DSL.inline(MAX_TIMESTAMP).as(SCOPE.FINISH)
            ).from(tempTable.table()).innerJoin(ENTITY)
                    .on(ENTITY.OID.eq(tempTable.field(SCOPE.ENTITY_OID)));
            DSL.using(conn, dsl.configuration().settings())
                    .insertInto(SCOPE, SCOPE.ENTITY_OID, SCOPE.SCOPED_OID, SCOPE.SCOPED_TYPE, SCOPE.START, SCOPE.FINISH)
                    .select(selectStmt)
                    .execute();
        }

        @Override
        protected Collection<Column<?>> getRecordColumns() {
            return Arrays.asList(ENTITY_OID, SCOPED_OID, SCOPE_START);
        }

        @Override
        protected String getWriteTableName() {
            return super.getWriteTableName() + "_inserts";
        }
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
    private class ScopeUpdaterSink extends DslRecordSink {
        private TempTable<?> tempTable;

        ScopeUpdaterSink() {
            super(dsl, SCOPE_TABLE, config, pool);
        }

        @Override
        protected void preCopyHook(final Connection conn) {
            this.tempTable = JooqUtil.createTemporaryTable(conn, null, getWriteTableName(),
                    SCOPE.ENTITY_OID, SCOPE.SCOPED_OID);
        }

        @Override
        protected void postCopyHook(final Connection conn) {
            DSL.using(conn, dsl.configuration().settings())
                    .update(SCOPE)
                    .set(SCOPE.FINISH, priorTimestamp)
                    .from(tempTable.table())
                    .where(SCOPE.ENTITY_OID.eq(tempTable.field(SCOPE.ENTITY_OID))
                            .and(SCOPE.SCOPED_OID.eq(tempTable.field(SCOPE.SCOPED_OID))))
                    .execute();
        }

        @Override
        protected Collection<Column<?>> getRecordColumns() {
            return Arrays.asList(ENTITY_OID, SCOPED_OID);
        }

        @Override
        protected String getWriteTableName() {
            return super.getWriteTableName() + "_updates";
        }
    }
}
