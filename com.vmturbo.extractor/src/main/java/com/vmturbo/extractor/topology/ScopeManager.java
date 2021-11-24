package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.Constants.MAX_TIMESTAMP;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPED_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPED_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPE_FINISH;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPE_START;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPE_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.SEED_OID;
import static com.vmturbo.extractor.schema.Tables.SCOPE;
import static com.vmturbo.extractor.schema.Tables.TOPOLOGY_STATS;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntCollection;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.CreateTableColumnStep;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record2;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.impl.DSL;

import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Constants;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
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
    private static final Logger logger = LogManager.getLogger();

    static final OffsetDateTime EPOCH_TIMESTAMP = OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);

    private final DbEndpoint db;
    private final DataPack<Long> oidPack;
    private final ExecutorService pool;
    private final WriterConfig config;
    private final int dbFetchSize;

    private DSLContext dsl;
    // for a (seedIid, scopedIid) pair in the scoping relationship, we will have
    // scope.get(scopedIid).contains(seedIid) in both the prior and current scope tables
    Int2ObjectMap<IntSet> priorScope = new Int2ObjectOpenHashMap<>();
    private OffsetDateTime priorTimestamp;
    Int2ObjectMap<IntSet> currentScope = new Int2ObjectOpenHashMap<>();
    private OffsetDateTime currentTimestamp;

    // we'll need this frequently, and the enum method creates a new clone each time, so we'll
    // get one up-front
    private static EntityType[] entityTypeValues = EntityType.values();

    /**
     * Create a new instance.
     *  @param oidPack entity id manager
     * @param db              DB endpoint (may not yet be initialized)a
     * @param config          ingester config
     * @param pool            thread pool
     * @param fetchSize       DB fetch size.
     */
    public ScopeManager(
            DataPack<Long> oidPack, DbEndpoint db, WriterConfig config, ExecutorService pool,
            int fetchSize) {
        this.oidPack = oidPack;
        this.db = db;
        this.pool = pool;
        this.config = config;
        this.dbFetchSize = fetchSize;
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
        logger.info("Scope initialized for timestamp {}; prior scope timestamp {}",
                currentTimestamp, priorTimestamp);
    }

    /**
     * Add one or more oids to the current scope of the given seed entity.
     *
     * @param seedOid    oid of entity whose scope is updated
     * @param scopedOids oids of entities/groups/... to add to the seed entity's scope
     */
    public void addInCurrentScope(long seedOid, long... scopedOids) {
        addInCurrentScope(seedOid, false, scopedOids);
    }

    /**
     * Add one or more oids to the current scope of the given seed entity. If symmetric is true, the
     * same scope is added to the scopedOids.
     *
     * @param seedOid    oid of entity whose scope is updated
     * @param symmetric  whether the scope need to be added to the scopedOids too
     * @param scopedOids oids of entities/groups/... to add to the seed entity's scope
     */
    public void addInCurrentScope(long seedOid, boolean symmetric, long... scopedOids) {
        final int seedIid = oidPack.toIndex(seedOid);
        for (final long scopedOid : scopedOids) {
            final int scopedIid = oidPack.toIndex(scopedOid);
            addScope(currentScope, seedIid, scopedIid);
            if (symmetric) {
                addScope(currentScope, scopedIid, seedIid);
            }
        }
    }

    private void addScope(Int2ObjectMap<IntSet> scopeMap, int seedIid, int scopedIid) {
        scopeMap.computeIfAbsent(scopedIid, _iid -> new IntOpenHashSet()).add(seedIid);
    }

    /**
     * Call when all scope info for current topology, and it's time to update the persisted scope
     * data.
     *
     * <p>This operation requires knowledge of the {@link EntityType} of each entity or pseudo-
     * entity (e.g. group) appearing in the topology. These are supplied in the form of integers
     * which are the ordinals of the {@link EntityType} enum members.</p>
     *
     * @param entityTypes map of iids to {@link EntityType} ordinals
     */
    public void finishTopology(Int2IntMap entityTypes) {
        logger.info("Scope is complete for {}", currentTimestamp);
        trim(currentScope); // scope map is complete, so this is a good time to optimize
        boolean errorOccurred = false;
        try (TableWriter scopeInserter = getScopeInsertWriter();
             TableWriter scopeUpdater = getScopeUpdateWriter()) {

            // we need to process every iid that appears in either prior scope or current scope
            final IntStream priorIids = priorScope.keySet().stream().mapToInt(Integer::intValue);
            final IntStream currentIids = currentScope.keySet().stream().mapToInt(Integer::intValue);
            IntStream.concat(priorIids, currentIids).distinct()
                    .forEach(scopedIid ->
                            // handle every entity that appeared in this or prior topology
                            finishScopingEntity(scopedIid, scopeInserter, scopeUpdater, entityTypes));

        } catch (SQLException e) {
            logger.error("Error occurred while storing scope table updates: " + e.getMessage(), e);
            // Since an error occurred while writing records, it's possible that our in-memory cache
            // is out-of-sync with the actual state of the database table.
            // Therefore, we invalidate our cache, forcing it to be reloaded.
            logger.warn("Invalidating the scope cache based on previous error. The cache will be "
                    + "rebuilt by reading the database while processing the subsequent topology.");
            priorScope.clear();
            currentScope.clear();
            priorTimestamp = null;
            currentTimestamp = null;
            errorOccurred = true;
        }

        // If the operation was successful, advance our in memory cache to point to the new model
        if (!errorOccurred) {
            logger.info("Successfully processed scope updates for {}", currentTimestamp);
            priorScope = currentScope;
            currentScope = new Int2ObjectOpenHashMap<>();
            priorTimestamp = currentTimestamp;
            currentTimestamp = null;
        }
        logger.info("Finished scope updates for {}", currentTimestamp);
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
                ? scope.getOrDefault(oidPack.toIndex(oid), IntSets.EMPTY_SET)
                : IntSets.EMPTY_SET;
        return iids.stream().mapToLong(oidPack::fromIndex)
                .collect(LongOpenHashSet::new, LongOpenHashSet::add, LongOpenHashSet::addAll);
    }

    private TableWriter getScopeInsertWriter() {
        return SCOPE_TABLE.open(new DslRecordSink(dsl, SCOPE_TABLE, config, pool),
                "Scope Inserter", logger);
    }

    private TableWriter getScopeUpdateWriter() {
        return SCOPE_TABLE.open(new ScopeUpdaterSink(), "Scope Updater", logger);
    }

    /**
     * Update persisted scope info for this scoped entity, which appeared in the current topology,
     * the prior topology, or both.
     *
     * @param scopedIid     iid of scoped entity
     * @param scopeInserter where to send newly appearing entities
     * @param scopeUpdater  where to send updates for dropped entities
     * @param entityTypes   entity type map
     */
    private void finishScopingEntity(
            int scopedIid, TableWriter scopeInserter, TableWriter scopeUpdater,
            Int2IntMap entityTypes) {
        if (currentScope.containsKey(scopedIid)) {
            if (!priorScope.containsKey(scopedIid)) {
                finishNewEntity(scopedIid, scopeInserter, entityTypes);
            } else {
                finishOverlappingEntity(scopedIid, scopeInserter, scopeUpdater, entityTypes);
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
     * @param entityTypes   entity types map
     */
    private void finishNewEntity(final int scopedIid, TableWriter scopeInserter,
            Int2IntMap entityTypes) {
        long scopedOid = oidPack.fromIndex(scopedIid);
        currentScope.get(scopedIid).forEach((IntConsumer)seedIid -> {
            writeScopeRecord(seedIid, scopedIid, scopedOid, entityTypes, scopeInserter);
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
     * @param entityTypes   entity types map
     */
    private void finishOverlappingEntity(
            final int scopedIid, TableWriter scopeInserter, TableWriter scopeUpdater,
            Int2IntMap entityTypes) {
        long scopedOid = oidPack.fromIndex(scopedIid);
        final IntSet priorSet = priorScope.getOrDefault(scopedIid, IntSets.EMPTY_SET);
        final IntSet currentSet = currentScope.getOrDefault(scopedIid, IntSets.EMPTY_SET);
        boolean debugEnabled = logger.isDebugEnabled();
        final IntSet adds = debugEnabled ? new IntOpenHashSet() : null;
        final IntSet keeps = debugEnabled ? new IntOpenHashSet() : null;
        final IntSet drops = debugEnabled ? new IntOpenHashSet() : null;
        IntStream.concat(
                Arrays.stream(priorSet.toIntArray()), Arrays.stream(currentSet.toIntArray()))
                .distinct().forEach(seedIid -> {
            if (priorSet.contains(seedIid)) {
                if (!currentSet.contains(seedIid)) {
                    // entity dropped out of the topology... tie off its record in DB
                    try (Record r = scopeUpdater.open()) {
                        r.set(SEED_OID, oidPack.fromIndex(seedIid));
                        r.set(SCOPED_OID, scopedOid);
                    }
                    if (debugEnabled) {
                        drops.add(seedIid);
                    }
                }
            } else if (currentSet.contains(seedIid)) {
                // new entity (or reappearance of an old entity) requires a new record
                writeScopeRecord(seedIid, scopedIid, scopedOid, entityTypes, scopeInserter);
                if (debugEnabled) {
                    adds.add(seedIid);
                }
            } else if (debugEnabled) {
                keeps.add(seedIid);
            }
        });
        if (debugEnabled) {
            logger.debug("Added {} seed ids for scoped entity {}: [{}]",
                    adds.size(), toOidList(adds), scopedOid);
            logger.debug("Kept {} seed ids for scoped entity {}: [{}]",
                    keeps.size(), toOidList(keeps), scopedIid);
            logger.debug("Dropped {} seed ids for scoped entity {}: [{}]",
                    drops.size(), toOidList(drops), scopedOid);
        }
    }

    /**
     * Update scope info for a scoped entity that has dropped out of the topology, by setting the
     * `finish` values in the corresponding `scope` records to the prior timestamp.
     *
     * @param scopedIid    scoped entity iid
     * @param scopeUpdater where to send updates to existing records
     */
    private void finishDroppedEntity(final int scopedIid, TableWriter scopeUpdater) {
        long scopedOid = oidPack.fromIndex(scopedIid);
        priorScope.get(scopedIid).forEach((IntConsumer)seedIid -> {
            try (Record r = scopeUpdater.open()) {
                r.set(SEED_OID, oidPack.fromIndex(seedIid));
                r.set(SCOPED_OID, scopedOid);
            }
        });
    }

    private void writeScopeRecord(final int seedIid, final int scopedIid, final long scopedOid, final Int2IntMap entityTypes, final TableWriter scopeInserter) {
        EntityType scopeType = getEntityType(scopedIid, entityTypes);
        if (scopeType != null) {
            try (Record r = scopeInserter.open()) {
                r.set(SEED_OID, oidPack.fromIndex(seedIid));
                r.set(SCOPED_OID, scopedOid);
                r.set(SCOPED_TYPE, scopeType);
                r.set(SCOPE_START, currentTimestamp);
                r.set(SCOPE_FINISH, MAX_TIMESTAMP);
            }
        } else {
            logger.error("No entity type recorded for entity {}; scope record omitted",
                    oidPack.fromIndex(scopedIid));
        }
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
        logger.info("Loading prior scope from database after restart, streaming records at {} chunk size",
                dbFetchSize);
        dsl.connection(conn -> {
            conn.setAutoCommit(false);
            try (Stream<Record2<Long, Long>> stream =
            // renderSchema=false is required for unit tests to pass when because of DSL.using(conn).
            // These prefixes (like 'extractor.') before table names, which jOOQ calls 'schema rendering'
            // are disabled at run time via jOOQ settings anyway, but only for unit-tests to run, it
            // needed to be set to false here.
                         DSL.using(conn, new Settings().withRenderSchema(false))
                                 .select(SCOPE.SEED_OID, SCOPE.SCOPED_OID).from(SCOPE)
                                 // greater-than comparison because MAX_TIMESTAMP used to be the
                                 // end not the start of the day it falls in.
                                 .where(SCOPE.FINISH.ge(MAX_TIMESTAMP))
                                 .fetchSize(dbFetchSize)
                                 .fetchStream()) {
                stream.forEach(r -> {
                    int seedIid = oidPack.toIndex(r.value1());
                    int scopedIid = oidPack.toIndex(r.value2());
                    addScope(priorScope, seedIid, scopedIid);
                });
            }
        });
        priorTimestamp = dsl.select(DSL.max(TOPOLOGY_STATS.TIME)).from(TOPOLOGY_STATS)
                .fetchOne().value1();
        if (priorTimestamp != null) {
            logger.info("Prior scopes for {} entities loaded for timestamp {}",
                    priorScope.size(), priorTimestamp);
        } else {
            logger.warn("No prior scope available from database; proceeding with empty prior scope; "
                    + "this is expected during initial installation");
        }
    }

    /**
     * Return the entity type for a given entity iid, as specified in the map.
     *
     * <p>If the iid is not present in the map, null is returned.</p>EntityHashManagerTest
     *
     * @param iid         iid of entity whose type is needed
     * @param entityTypes map of iids to {@link EntityDTO.EntityType} ordinals
     * @return the entity's type, or null if not found in map
     */
    private EntityType getEntityType(int iid, Int2IntMap entityTypes) {
        int type = entityTypes.getOrDefault(iid, -1);
        return type != -1 ? entityTypeValues[type]
                : null;
    }

    // utility used in debug logging
    private List<Long> toOidList(IntCollection iids) {
        return iids.stream()
                .map(oidPack::fromIndex)
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Record sink to close off previous existing scope records when the scoping pair is no longer
     * active in the current topology.
     *
     * <p>Records with matching oids and with their current `finish` timestamp set to our special
     * {@link Constants#MAX_TIMESTAMP} value have their `finish` timestamps set to the prior topology's
     * timestamp.
     */
    private class ScopeUpdaterSink extends DslRecordSink {
        private TempTable<?> tempTable;

        ScopeUpdaterSink() {
            super(ScopeManager.this.dsl, SCOPE_TABLE, config, pool);
        }

        @Override
        protected List<String> getPreCopyHookSql(final Connection transConn) {
            Field<?>[] fields = new Field[]{SCOPE.SEED_OID, SCOPE.SCOPED_OID};
            this.tempTable = new TempTable<>((Table<?>)null, getWriteTableName(), fields);
            try (DSLContext transDsl = DSL.using(transConn);
                    CreateTableColumnStep anotherTempTable = transDsl.createTemporaryTable(
                            tempTable.table())) {
                final String sql = anotherTempTable.columns(fields)
                        .getSQL(ParamType.INLINED);
                return Collections.singletonList(sql);
            }
        }

        @Override
        protected List<String> getPostCopyHookSql(final Connection transConn) {
            final String sql = DSL.using(transConn, dsl.configuration().settings())
                    .update(SCOPE)
                    .set(SCOPE.FINISH, priorTimestamp)
                    .from(tempTable.table())
                    .where(SCOPE.SEED_OID.eq(tempTable.field(SCOPE.SEED_OID))
                            .and(SCOPE.SCOPED_OID.eq(tempTable.field(SCOPE.SCOPED_OID)))
                            .and(SCOPE.FINISH.ge(MAX_TIMESTAMP)))
                    .getSQL(ParamType.INLINED);
            return Collections.singletonList(sql);
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
