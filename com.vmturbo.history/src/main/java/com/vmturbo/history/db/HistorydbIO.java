package com.vmturbo.history.db;

import static com.vmturbo.history.db.jooq.JooqUtils.dField;
import static com.vmturbo.history.db.jooq.JooqUtils.dateOrTimestamp;
import static com.vmturbo.history.db.jooq.JooqUtils.doubl;
import static com.vmturbo.history.db.jooq.JooqUtils.relType;
import static com.vmturbo.history.db.jooq.JooqUtils.str;
import static com.vmturbo.history.db.jooq.JooqUtils.timestamp;
import static com.vmturbo.history.schema.StringConstants.AVG_VALUE;
import static com.vmturbo.history.schema.StringConstants.CAPACITY;
import static com.vmturbo.history.schema.StringConstants.COMMODITY_KEY;
import static com.vmturbo.history.schema.StringConstants.MAX_VALUE;
import static com.vmturbo.history.schema.StringConstants.MIN_VALUE;
import static com.vmturbo.history.schema.StringConstants.PRODUCER_UUID;
import static com.vmturbo.history.schema.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.history.schema.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.history.schema.StringConstants.RELATION;
import static com.vmturbo.history.schema.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.history.schema.StringConstants.UUID;
import static com.vmturbo.history.schema.abstraction.Tables.AUDIT_LOG_RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.MKT_SNAPSHOTS;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.SCENARIOS;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.InsertSetMoreStep;
import org.jooq.InsertSetStep;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Result;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Value;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;

import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.Period;
import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.Stats.CommodityMaxValue;
import com.vmturbo.common.protobuf.stats.Stats.EntityCommoditiesMaxValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.Entities;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.PmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.Scenarios;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.schema.abstraction.tables.records.MktSnapshotsStatsRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;
import com.vmturbo.history.stats.MarketStatsAccumulator;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.history.utils.TopologyOrganizer;
import com.vmturbo.platform.common.dto.CommonDTO;

/**
 * Dbio Component for use within the History Component.
 **/
public class HistorydbIO extends BasedbIO {

    private static final Logger logger = LogManager.getLogger();

    // length restriction on the  COMMODITY_KEY column; all stats tables have same column length
    private static final int COMMODITY_KEY_MAX_LENGTH = VmStatsLatest.VM_STATS_LATEST.COMMODITY_KEY
            .getDataType().length();

    // min and max for numerical values for the statstables; must fit in DECIMAL(15,3), 12 digits
    private static final double MAX_STATS_VALUE = 1e12D - 1;
    private static final double MIN_STATS_VALUE = -MAX_STATS_VALUE;

    // MySQL Connection parameters
    @Value("${userName:vmtplatform}")
    private String userName;

    @Value("${requestHost:%}")
    private String requestHost;

    @Value("${adapter:mysql}")
    private String adapter;

    @Value("${hostName:db}")
    private String hostName;

    @Value("${portNumber:3306}")
    private String portNumber;

    @Value("${databaseName:vmtdb}")
    private String databaseName;

    @Value("${readonlyUserName:vmtreader}")
    private String readonlyUserName;

    @Value("${queryTimeoutSeconds:120}")
    private int queryTimeout_sec;

    @Value("${migrationTimeoutSeconds:600}")
    private int migrationTimeout_sec;

    // Mapping from the retention settings DB column name -> Setting name
    private final ImmutableBiMap<String, String> retentionDbColumnNameToSettingName =
            ImmutableBiMap.of(
                    //"retention_latest_hours", , # skipping as there is no equivalent in the UI
                    "retention_hours", GlobalSettingSpecs.StatsRetentionHours.getSettingName(),
                    "retention_days", GlobalSettingSpecs.StatsRetentionDays.getSettingName(),
                    "retention_months", GlobalSettingSpecs.StatsRetentionMonths.getSettingName()
            );

    private ImmutableBiMap<String, String> retentionSettingNameToDbColumnName =
            retentionDbColumnNameToSettingName.inverse();

    private static final String AUDIT_LOG_RETENTION_POLICY_NAME = "retention_days";

    private static final String STATS_TABLE_PROPERTY_SUBTYPE_FILTER = "used";

    private static final String MAX_COLUMN_NAME = "max";

    /**
     * Maximum number of entities allowed in the getEntities method.
     * @see HistorydbIO#getEntities(List)
     */
    @Value("${getEntitiesChunkSize:4000}")
    private int entitiesChunkSize;

    /**
     * The database password utility.
     */
    private DBPasswordUtil dbPasswordUtil;

    /**
     * Constructs the HistorydbIO.
     */
    public HistorydbIO(DBPasswordUtil dbPasswordUtil) {
        this.dbPasswordUtil = dbPasswordUtil;
    }

    @Override
    protected void internalNotifyUser(String eventName, String error) {
        logger.info("notifying user {} error: {}", eventName, error);
        // TODO: implement
    }

    @Override
    protected void internalClearNotification(String eventName) {
        logger.info("clear notification user {}", eventName);
    }

    @Override
    public String getUserName() {
        return userName;
    }

    /**
     * We will have to retrieve the password here.
     * We do that in the lazy fashion instead of the constructor, as constructor is being called
     * too early, and the RestTemplate is unable to function at that stage yet.
     *
     * @return The password.
     */
    @Override
    public String getPassword() {
        return dbPasswordUtil.getSqlDbRootPassword();
    }

    @Override
    public String getRequestHost() {
        return requestHost;
    }

    @Override
    public String getAdapter() {
        return adapter;
    }

    @Override
    public String getHostName() {
        return hostName;
    }

    @Override
    public String getPortNumber() {
        return portNumber;
    }

    @Override
    public String getDatabaseName() {
        return databaseName;
    }

    @Override
    public String getReadOnlyUserName() {
        return readonlyUserName;
    }

    @Override
    public String getReadOnlyPassword() {
        return DBPasswordUtil.obtainDefaultPW();
    }

    @Override
    public int getQueryTimeoutSeconds() {
        return queryTimeout_sec;
    }

    public void setQueryTimeoutSeconds(int newTimeoutSec) {
        queryTimeout_sec = newTimeoutSec;
    }

    @Override
    public boolean isReportEnabled(int rptId) {
        return false;
    }

    @Override
    public int addSubscription(String email, Period period, DayOfWeek dayOfWeek, String scope,
                               ReportType reportType, int reportId, ReportOutputFormat format,
                               Map<Integer, String> attrs, String userName) {
        return 0;
    }

    @Override
    public boolean isDeveloper() {
        return false;
    }

    @Override
    public String httpExecuteReport(String reportUrl) {
        return null;
    }

    @Override
    public void init(boolean clearOldDb, Double version, String dbName,
                     String... additionalLocations) throws VmtDbException {
        // increase the DB timeout for the DB migration process
        int prevTimeoutSecs = getQueryTimeoutSeconds();
        setQueryTimeoutSeconds(migrationTimeout_sec);
        super.init(clearOldDb, version, dbName, additionalLocations);
        // reset the DB timeout to the typical value
        setQueryTimeoutSeconds(prevTimeoutSecs);
    }

    @Override
    public String getRootPassword() {
        return getPassword();
    }

    /**
     * Return the "_stats_latest" table for the given Entity Type based on the ID.
     *
     * The table prefix depends on the entity type.
     *
     * Returns null if the table prefix cannot be determined. This may represent an entity
     * that is not to be persisted, or an internal system configuration error.
     *
     * @param entityTypeId the type of the Service Entity for which stats are to be persisted
     * @return a DB Table object for the _stats_latest table for the entity type of this entity,
     * or null if the entity table cannot be determined.
     */
    public Table<?> getLatestDbTableForEntityType(int entityTypeId) {

        Optional<EntityType> entityDBInfo = getEntityType(entityTypeId);
        if (!entityDBInfo.isPresent()) {
            logger.debug("Cannot convert {} to EntityType ", entityTypeId);
            return null;
        }
        return entityDBInfo.get().getLatestTable();
    }

    /**
     * Return the "_stats_by_day" table for the given Entity Type based on the ID.
     *
     * The table prefix depends on the entity type.
     *
     * Returns null if the table prefix cannot be determined. This may represent an entity
     * that is not to be persisted, or an internal system configuration error.
     *
     * @param entityTypeId the type of the Service Entity for which stats are to be persisted
     * @return a DB Table object for the _stats_by_day table for the entity type of this entity,
     * or null if the entity table cannot be determined.
     */
    private Table<?> getDayStatsDbTableForEntityType(int entityTypeId) {

        Optional<EntityType> entityDBInfo = getEntityType(entityTypeId);
        if (!entityDBInfo.isPresent()) {
            return null;
        }
        return entityDBInfo.get().getDayTable();
    }

    /**
     * Convert the {@link CommonDTO.EntityDTO.EntityType} numeric id
     * to an SDK EntityType to an {@link EntityType}.
     *
     * @param sdkEntityTypeId the CommonDTO.EntityDTO.EntityType id to convert to a
     *                     database {@link EntityType}
     * @return an {@link EntityType} {@link EntityType} for the corresponding SDK Entity Type ID
     */
    public Optional<EntityType> getEntityType(int sdkEntityTypeId) {
        CommonDTO.EntityDTO.EntityType sdkEntityType = CommonDTO.EntityDTO.EntityType
                .forNumber(sdkEntityTypeId);
        if (sdkEntityType == null) {
            logger.debug("unknown entity type for entity type id {}", sdkEntityTypeId);
            return Optional.empty();
        }
        return Optional.ofNullable(HistoryStatsUtils.SDK_ENTITY_TYPE_TO_ENTITY_TYPE.get(sdkEntityType));
    }

    public Optional<String> getBaseEntityType(int sdkEntityTypeId) {
        CommonDTO.EntityDTO.EntityType sdkEntityType = CommonDTO.EntityDTO.EntityType
                .forNumber(sdkEntityTypeId);
        if (sdkEntityType == null || HistoryStatsUtils.SDK_ENTITY_TYPE_TO_ENTITY_TYPE_NO_ALIAS
                        .get(sdkEntityType) == null) {
            logger.debug("unknown entity type for entity type id {}", sdkEntityTypeId);
            return Optional.empty();
        }
        return Optional.ofNullable(HistoryStatsUtils.SDK_ENTITY_TYPE_TO_ENTITY_TYPE_NO_ALIAS
                .get(sdkEntityType)
                .getClsName());
    }



    /**
     * Create an "insert" SQL statement for the given table  to be populated with values
     * for some number of rows to be inserted.
     *
     * @param dbTable the xxx_stats_latest table into which the rows will be inserted
     * @return a jooq insert statement to which values may be added.
     */
    public @Nonnull InsertSetMoreStep<?> getCommodityInsertStatement(@Nonnull Table<?> dbTable) {
        return (InsertSetMoreStep<?>)getJooqBuilder()
                .insertInto(dbTable);
    }


    /**
     * Create an "insert" SQL statement specifically for adding a row to the 'market_stats_latest'
     * table.
     *
     * @param marketStatsData the data for one stats item; will be written to one row
     * @param snapshotTime the snapshot time being summarized
     * @param topologyContextId the fixed topology context id, constant for live market and plan/over/plan
     * @param topologyId the unique id for this snapshot
     * @return an SQL statement to insert one row in the stats table
     */
    public InsertSetMoreStep<?> getMarketStatsInsertStmt(@Nonnull MarketStatsAccumulator.MarketStatsData marketStatsData,
                                                         long snapshotTime,
                                                         long topologyContextId,
                                                         long topologyId) {

        return getCommodityInsertStatement(MarketStatsLatest.MARKET_STATS_LATEST)
                .set(MarketStatsLatest.MARKET_STATS_LATEST.SNAPSHOT_TIME,
                        new Timestamp(snapshotTime))
                .set(MarketStatsLatest.MARKET_STATS_LATEST.TOPOLOGY_CONTEXT_ID,
                        topologyContextId)
                .set(MarketStatsLatest.MARKET_STATS_LATEST.TOPOLOGY_ID,
                        topologyId)
                .set(MarketStatsLatest.MARKET_STATS_LATEST.ENTITY_TYPE,
                        marketStatsData.getEntityType())
                .set(MarketStatsLatest.MARKET_STATS_LATEST.PROPERTY_TYPE,
                        marketStatsData.getPropertyType())
                .set(MarketStatsLatest.MARKET_STATS_LATEST.PROPERTY_SUBTYPE,
                        marketStatsData.getPropertySubtype())
                .set(MarketStatsLatest.MARKET_STATS_LATEST.CAPACITY,
                        clipValue(marketStatsData.getCapacity()))
                .set(MarketStatsLatest.MARKET_STATS_LATEST.AVG_VALUE,
                        clipValue(marketStatsData.getUsed()))
                .set(MarketStatsLatest.MARKET_STATS_LATEST.MIN_VALUE,
                        clipValue(marketStatsData.getMin()))
                .set(MarketStatsLatest.MARKET_STATS_LATEST.MAX_VALUE,
                        clipValue(marketStatsData.getMax()))
                .set(MarketStatsLatest.MARKET_STATS_LATEST.RELATION,
                        marketStatsData.getRelationType());
    }

    /**
     * Populate an "insert" SQL statement with the values for this commodity.
     *  @param propertyType the string name of the property (mixed case)
     * @param snapshotTime the time of the snapshot
     * @param entityId the id of the entity being processed
     * @param relationType BOUGHT or SOLD
     * @param providerId OID of the provider, if BOUGHT
     * @param capacity the capacity of the commodity
     * @param commodityKey the external association key for this commodity
     * @param insertStmt the insert SQL statement to insert the commodity row
     * @param table stats db table for this entity type
     */
    public void initializeCommodityInsert(@Nonnull String propertyType,
                                          long snapshotTime,
                                          long entityId,
                                          @Nonnull RelationType relationType,
                                          @Nullable Long providerId,
                                          @Nullable Double capacity,
                                          @Nullable String commodityKey,
                                          @Nonnull InsertSetStep<?> insertStmt,
                                          @Nonnull Table<?> table) {
        // providerId might be null
        String providerIdString = providerId != null ? Long.toString(providerId) : null;
        insertStmt.set(str(dField(table, PRODUCER_UUID)), providerIdString);
        // commodity_key is limited in length
        if (commodityKey != null && commodityKey.length() > COMMODITY_KEY_MAX_LENGTH) {
            String longCommditiyKey = commodityKey;
            commodityKey = commodityKey.substring(0, COMMODITY_KEY_MAX_LENGTH-1);
            logger.trace("shortening commodity key {} ({}) to {}", longCommditiyKey,
                    longCommditiyKey.length(), commodityKey);
        }
        // populate the other fields; all fields are nullable based on DB design; Is that best?
        insertStmt
                .set(dateOrTimestamp(dField(table, SNAPSHOT_TIME)),
                        new java.sql.Timestamp(snapshotTime))
                .set(str(dField(table, UUID)), Long.toString(entityId))
                .set(str(dField(table, PROPERTY_TYPE)), propertyType)
                // 5 = propertySubtype - to be filled in later
                .set(doubl(dField(table, CAPACITY)), clipValue(capacity))
                // 7, 8, 9 = avg, min, max - to be filled in later
                .set(relType(dField(table, RELATION)), relationType)
                .set(str(dField(table, COMMODITY_KEY)), commodityKey);
    }

    /**
     * Set the values for the subtype of this property.
     *
     * Since this is "latest", a single point in time, then all of avg, min, max are the same.
     *
     * @param propertySubtype the subtype of the property, e.g. "used" or "utilization"

     * @param value the value of the commodity
     * @param insertStmt the SQL statement to insert the commodity row
     * @param table the xxx_stats_latest table where this data will be written
     */
    public void setCommodityValues(@Nonnull String propertySubtype, double value,
                                   @Nonnull InsertSetStep insertStmt, @Nonnull Table<?> table) {

        value = clipValue(value);
        insertStmt.set(str(dField(table, PROPERTY_SUBTYPE)), propertySubtype);
        insertStmt.set(doubl(dField(table, AVG_VALUE)), value);
        insertStmt.set(doubl(dField(table, MIN_VALUE)), value);
        insertStmt.set(doubl(dField(table, MAX_VALUE)), value);
    }

    /**
     * Return a long epoch date representing the most recent timestamp of snapshot data.
     *
     * Currently only looks for the most recent item in the PM_STATS_LATEST table; assuming any
     * topology will include a PM.
     *
     * @return a {@link Timestamp} for the most recent snapshot recorded in the xxx_stats_latest
     * tables.
     */
    public Optional<Timestamp> getMostRecentTimestamp() {
        PmStatsLatest statsLatestTable = PM_STATS_LATEST;
        try (Connection conn = getAutoCommitConnection()) {
            final Field<?> snapshotTimeField = dField(statsLatestTable, SNAPSHOT_TIME);
            Record1<Timestamp> snapshotTimeRecord = using(conn)
                    .select(timestamp(snapshotTimeField))
                    .from(statsLatestTable)
                    .orderBy(snapshotTimeField.desc())
                    .limit(1)   // TODO: remove this - rows should be unique
                    .fetchOne();

            // The snapshotTimeField could be null when the database is empty.
            if (snapshotTimeRecord != null) {
                Timestamp t = snapshotTimeRecord.value1();
                return Optional.of(t);
            }
        } catch (SQLException e) {
            logger.warn("Error fetching most recent timestamp: ", e);
        } catch (VmtDbException e) {
            logger.error("Failed to get database connection.", e);
        }
        return Optional.empty();
    }

    /**
     * Get id, display name and creation class name of entities which IDs are given.
     * Data is retrieved from the DB in chunks of 4000 (settable).
     *
     * @param entityIds the list of entity IDs to get from the DB
     * @return a map from ID to entity record
     * @throws VmtDbException if there was an error accessing the database
     */
    public Map<Long, EntitiesRecord> getEntities(List<String> entityIds) throws VmtDbException {
        logger.debug("get {} entities", entityIds.size());
        final Map<Long, EntitiesRecord> map = new HashMap<>();
        for (List<String> idsChunk : Lists.partition(entityIds, entitiesChunkSize)) {
            try (Connection conn = getAutoCommitConnection()) {
                final List<EntitiesRecord> listRecords = using(conn)
                        .fetch(Entities.ENTITIES, Entities.ENTITIES.UUID.in(idsChunk));
                for (EntitiesRecord record : listRecords) {
                    map.put(record.getId(), record);
                }
            } catch (SQLException e) {
                throw new VmtDbException(VmtDbException.READ_ERR, e);
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("getEntities returning {} entities", map::size);
            // calculate the keys in the query that are not part of the result map
            Set<Long> missingKeys = entityIds.stream()
                    .map(Long::valueOf)
                    .filter(oid -> !map.keySet().contains(oid))
                    .collect(Collectors.toSet());
            logger.debug("didn't find: {}", missingKeys);
        }
        return map;
    }

    /**
     * Get the creation_class for each of a list of entity OIDs stored as UUID.
     *
     * @param entityIds a list of service entity OIDs for which we want the Entity Types
     * @return a map from OID to Entity Type, as a String
     * @throws VmtDbException if there's an error querying the DB
     */
    public Map<String, String> getTypesForEntities(List<String> entityIds) throws VmtDbException {

        Result<? extends Record> result = execute(Style.FORCED, JooqBuilder()
                .selectDistinct(Entities.ENTITIES.UUID, Entities.ENTITIES.CREATION_CLASS)
                .from(Entities.ENTITIES)
                .where(Entities.ENTITIES.UUID.in(entityIds)));

        return result.stream().collect(
                Collectors.toMap(x -> x.getValue(Entities.ENTITIES.UUID),
                        x -> x.getValue(Entities.ENTITIES.CREATION_CLASS))
        );
    }

    /**
     * Return a select statement from the given table given selection fields, where conditions,
     * and an order-by clause.
     *
     * @param table the table to query from
     * @param selectFields the columns to return from the query
     * @param whereConditions the conditions to filter on
     * @param orderFields the ordering for the results
     * @return a Select statement to perform the given query
     */
    public Select<?> getStatsSelect(Table<?> table,
                                    List<Field<?>> selectFields,
                                    List<Condition> whereConditions,
                                    Field<?>[] orderFields) {
        return JooqBuilder()
                .select(selectFields)
                .from(table)
                .where(whereConditions)
                .orderBy(orderFields);
    }

    /**
     * Return a select statement from the given table given selection fields, where conditions,
     * and an order-by clause which is also used to group by.
     *
     * @param table the table to query from
     * @param selectFields the columns to return from the query
     * @param whereConditions the conditions to filter on
     * @param orderGroupFields the list of fields to group by; also
     * @return a Select statement to perform the given query
     */
    public Select<?> getStatsSelectWithGrouping(Table<?> table,
                                                List<Field<?>> selectFields,
                                                List<Condition> whereConditions,
                                                Field<?>[] orderGroupFields) {
        return JooqBuilder()
                .select(selectFields)
                .from(table)
                .where(whereConditions)
                .groupBy(orderGroupFields)
                .orderBy(orderGroupFields);
    }

    /**
     * Return the display name for the Entity for the given entity ID (OID).
     *
     * <p>If the entity ID is not known, then return null.
     * <p>TODO: should be changed to use Optional
     *
     * @param entityId the entity OID (as a string)
     * @return the display name for the Entity, as stored in the "display_name" for the given
     * entity OID; return null if not found.
     */
    public String getEntityDisplayNameForId(long entityId) {

        try (Connection conn = getAutoCommitConnection()) {

            Record1<String> entityTypeRecord = using(conn)
                    .select(Tables.ENTITIES.DISPLAY_NAME)
                    .from(Tables.ENTITIES)
                    .where(Tables.ENTITIES.UUID.eq(Long.toString(entityId)))
                    .fetchOne();
            if (entityTypeRecord == null) {
                logger.debug("Display name not found for: {}", entityId);
                return null;
            }
            return entityTypeRecord.value1();
        } catch (SQLException e) {
            logger.warn("Error fetching entity display_name: ", e);
        } catch (VmtDbException e) {
            logger.error("Failed to get database connection.", e);
        }
        return null;
    }

    /**
     * Test whether an entityId represents a plan. We do this by looking up the entityId in
     * the SCENARIOS table.
     *
     * @param entityId the numeric ID which may or may not represent a plan
     * @return true iff this ID is found in the SCENARIOS table
     */
    public boolean entityIdIsPlan(final long entityId) {
        return getScenariosRecord(entityId).isPresent();
    }

    /**
     * Request the Scenario information from the SCENARIOS table for the given topologyContextId.
     *
     * @param topologyContextId the ID of the scenario to search for
     * @return An optional with the scenario information record or an empty optional if not found.
     */
    public Optional<ScenariosRecord> getScenariosRecord(long topologyContextId) {
        try (Connection conn = getAutoCommitConnection()) {
            return getScenariosRecord(topologyContextId, conn);
        } catch (SQLException e) {
            return Optional.empty();
        } catch (VmtDbException e) {
            logger.error("Failed to get connection to database.", e);
            return Optional.empty();
        }
    }

    private Optional<ScenariosRecord> getScenariosRecord(long topologyContextId, Connection conn) {
        try {
            Result<? extends Record> answer = execute(JooqBuilder()
                    .selectFrom(SCENARIOS)
                    .where(SCENARIOS.ID.equal(topologyContextId)), conn);
            if (answer == null) {
                // we expect a Result; double-check
                logger.warn("Select from SCENARIOS returned null unexpectedly in getSenariosRecord."
                        + " topologyContextId " + topologyContextId + "...continuing");
                return Optional.empty();
            }
            if (answer.size() < 1) {
                // no rows found
                return Optional.empty();
            }
            if (answer.size() > 1) {
                // more than one row - not supposed to happen; return the first one, for now
                logger.warn("Select ScenariosRecord from SCENARIOS returned more than one row " +
                        "unexpectedly; topologyContextId " +
                        topologyContextId + "...using first");
            }
            return Optional.of((ScenariosRecord)answer.iterator().next());
        } catch (VmtDbException e) {
            return Optional.empty();
        }
    }

    /**
     * Retrieve the SCENARIOS record for the given topologyContextId, if already recorded;
     * otherwise, add a new record.
     *
     * @param topologyContextId the id for the context in which this topology is defined, either
     *                          the single live context, or one of the several plan contexts
     * @param topologyId the unique id for the topology within the given context
     * @return the SCENARIOS record for the given topologyContextId
     * @throws VmtDbException if there is a database error in the
     */
    public ScenariosRecord getOrAddScenariosRecord(long topologyContextId, long topologyId, long snapshotTime)
            throws VmtDbException {
        Optional<ScenariosRecord> scenarioInfo = getScenariosRecord(topologyContextId);
        if (!scenarioInfo.isPresent()) {
            logger.debug("Persisting scenario with topologyContextId {}", topologyContextId);
            TopologyOrganizer tempTopologyOrganizer = new TopologyOrganizer(topologyContextId,
                    topologyId, snapshotTime);
            addMktSnapshotRecord(tempTopologyOrganizer);
            scenarioInfo = getScenariosRecord(topologyContextId);
        }

        return scenarioInfo.orElseThrow(() -> new VmtDbException(VmtDbException.INSERT_ERR,
                "Error writing scenarios record for plan priceIndexInfo for " + topologyContextId));
    }


    /**
     * Create entries in the SCENARIOS and MKT_SNAPSHOTS table for this topology, if required.
     * Each of the insert statments specifies "onDuplicateKeyIgnore", so there's no error
     * signalled if either row already exists.
     * <p>Note that the topologyContextId is used for both the ID and the scenario_id in the
     * SCENARIOS table.
     * <p>Note that there is insufficient information to populate the displayName, the state,
     * and the times. This may be provided in the future by adding a listener to the
     * Plan Orchestrator.
     *
     * @param topologyOrganizer the information about this topology, including the
     *                          context id and snapshot time.
     * @throws VmtDbException if there's an error writing to the RDB
     */
    public void addMktSnapshotRecord(TopologyOrganizer topologyOrganizer) throws VmtDbException {

        long topologyContextId = topologyOrganizer.getTopologyContextId();
        // add this topology to the scenarios table
        final Timestamp snapshotTimestamp = new Timestamp(topologyOrganizer.getSnapshotTime());
        execute(Style.FORCED, getJooqBuilder()
                .insertInto(SCENARIOS)
                .set(SCENARIOS.ID, topologyContextId)
                .set(SCENARIOS.DISPLAY_NAME, "scenario")
                .set(SCENARIOS.CREATE_TIME, snapshotTimestamp)
                .onDuplicateKeyIgnore());

        // add this topology to the mkt_snapshots table
        execute(Style.FORCED, getJooqBuilder()
                .insertInto(MKT_SNAPSHOTS)
                .set(MKT_SNAPSHOTS.ID, topologyContextId)
                .set(MKT_SNAPSHOTS.SCENARIO_ID, topologyContextId)
                .set(MKT_SNAPSHOTS.DISPLAY_NAME, "scenario: "
                        + topologyOrganizer.getSnapshotTime())
                .set(MKT_SNAPSHOTS.RUN_COMPLETE_TIME, snapshotTimestamp)
                .onDuplicateKeyIgnore());
    }

    /**
     * Get a connection to the database.
     *
     * The connection is guaranteed to have auto-commit enabled
     * (see: {@link Connection#setAutoCommit}), so the user of the
     * connection doesn't need to worry about commiting or rolling back manually.
     *
     * @return A {@link Connection} where {@link Connection#getAutoCommit()} will return true.
     * @throws VmtDbException If unable to obtain a connection or set it's auto-commit status.
     */
    private Connection getAutoCommitConnection() throws VmtDbException {
        try {
            Connection connection = connection();
            // Due to an existing bug, BaseDbIO::connection() may or may not return
            // a connection with autoCommit set to true. Enforce autoCommit = true.
            // TODO (roman, Jan 27 2017): This is a workaround. In the long term it makes
            // more sense to fix the original bug. Also, it may make more sense to have autoCommit
            // set to false in all cases, and for us to be thorough about commit() and
            // rollback() calls.
            if (!connection.getAutoCommit()) {
                connection.setAutoCommit(true);
            }
            return connection;
        } catch (SQLException  e) {
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
    }

    /**
     * Persist multiple entitites (insert or update, depending on the record source). If a record
     * has been initially returned from the DB, UPDATE will be executed. If a record is a newly
     * created INSERT will be executed.
     *
     * Queries are splitted into smaller batches of 4k (settable by {@link #entitiesChunkSize}
     * property.
     *
     * @param entitiesRecords records, containing entities, required to update
     * @throws VmtDbException if DB error occurred
     */
    public void persistEntities(@Nonnull List<EntitiesRecord> entitiesRecords)
            throws VmtDbException {
        logger.debug("Persisting {} entities", entitiesRecords.size());
        final Connection connection = transConnection();
        try {
            for (Collection<EntitiesRecord> chunk : Lists.partition(entitiesRecords,
                    entitiesChunkSize)) {
                logger.debug("Persisting next chunk of {} entities to the DB", chunk::size);
                using(connection).batchStore(chunk).execute();
            }
            connection.commit();
        } catch (SQLException | DataAccessException e) {
            rollback(connection);
            throw new VmtDbException(VmtDbException.INSERT_ERR,
                    "Failed to insert/update entities table records", e);
        } finally {
            close(connection);
        }
        logger.debug("Successfully persisted entities");
    }

    /**
     * Persist a list of records in the mkt_snapshots_stats table.
     *
     * @param snapshotStatRecords a list of records to be inserted in the mkt_snapshots_stats
     * @throws VmtDbException database errors
     */
    public void persistMarketSnapshotsStats(@Nonnull List<MktSnapshotsStatsRecord> snapshotStatRecords)
            throws VmtDbException {
        logger.trace("Persisting {} MktSnapshotsStatsRecord", snapshotStatRecords.size());
        final Connection connection = transConnection();
        try {
            for (Collection<MktSnapshotsStatsRecord> chunk : Lists.partition(snapshotStatRecords,
                    entitiesChunkSize)) {
                logger.trace("Persisting next chunk of {} MktSnapshotsStatsRecord to the DB",
                        chunk::size);
                using(connection).batchStore(chunk).execute();
            }
            connection.commit();
        } catch (SQLException | DataAccessException e) {
            rollback(connection);
            throw new VmtDbException(VmtDbException.INSERT_ERR,
                    "Failed to insert mkt_snapshots_stats table records.", e);
        } finally {
            close(connection);
        }
        logger.trace("Successfully persisted market snapshot stats.");
    }

    /**
     * Clip a value so it fits within the decimal precision for the Stats tables.
     * This precision is currently "DECIMAL(15,3)". If the value to be clipped is null,
     * return null;
     *
     * @param rawStatsValue a numeric stats value from either discovery or priceIndex
     * @return the value clipped between MIN_STATS_VALUE (large negative number) and MAX_STATS_VALUE
     */
    public @Nullable Double clipValue(Double rawStatsValue) {
        return rawStatsValue == null
                ? null
                : Math.min(Math.max(rawStatsValue, MIN_STATS_VALUE), MAX_STATS_VALUE);
    }

    /**
     * Delete stats associated with a plan.
     *
     * @param topologyContextId Plan ID
     * @throws VmtDbException if there is a database error
    */
    public void deletePlanStats(long topologyContextId) throws VmtDbException {
        // delete from scenarios table. the Foregin Key constraint will take
        // care of removing the data in remaining tables
        // don't care if the entry exists or not as long as query succeeds
        execute(Style.FORCED, JooqBuilder()
                .delete(Scenarios.SCENARIOS)
                .where(Scenarios.SCENARIOS.ID.eq(topologyContextId)));

    }

    /**
     * Get all the stats data retention settings.
     *
     * @return List of all the data retention settings.
     * @throws VmtDbException if there is a database error.
     * @throws DataAccessException if there is a database error.
     *
     */
    public List<Setting> getStatsRetentionSettings() throws VmtDbException {

        try (Connection conn = connection()) {
            Map<String, Integer> retentionSettingsMap =
                using(conn).selectFrom(RETENTION_POLICIES)
                .fetchMap(RETENTION_POLICIES.POLICY_NAME, RETENTION_POLICIES.RETENTION_PERIOD);

            List<Setting> settings = new ArrayList<>();

            for (Entry<String, String> entry : retentionDbColumnNameToSettingName.entrySet()) {
                // should be ok to skip if the entries don't exist in the DB.
                Integer retentionPeriod = retentionSettingsMap.get(entry.getKey());
                if (retentionPeriod != null) {
                    settings.add(createSetting(entry.getValue(), retentionPeriod));
                }
            }
            return settings;
        }
        catch (SQLException e) {
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
    }

    private Setting createSetting(String name, int value) {
        return Setting.newBuilder()
                    .setSettingSpecName(name)
                    .setNumericSettingValue(SettingDTOUtil.createNumericSettingValue(value))
                    .build();
    }

    /**
     * Update the value of stats data retention setting.
     *
     * @param settingName The name of the setting to update.
     * @param retentionPeriod The new retention period.
     * @return The updated Setting.
     * @throws VmtDbException if there is a database error.
     *
     */
    public Optional<Setting> setStatsDataRetentionSetting(String settingName, int retentionPeriod)
        throws VmtDbException {

        if (!retentionSettingNameToDbColumnName.containsKey(settingName)) {
            return Optional.empty();
        }

        execute(Style.PATIENT, getJooqBuilder()
                .update(RETENTION_POLICIES)
                .set(RETENTION_POLICIES.RETENTION_PERIOD, retentionPeriod)
                .where(RETENTION_POLICIES.POLICY_NAME.eq(
                    retentionSettingNameToDbColumnName.get(settingName))));

        return Optional.of(createSetting(settingName, retentionPeriod));
    }

    /**
     * Get the audit log entries data retention settings.
     *
     * @return The data retention setting.
     * @throws VmtDbException if there is a database error.
     * @throws DataAccessException if there is a database error.
     *
     */
    public Setting getAuditLogRetentionSetting() throws VmtDbException {

        try (Connection conn = connection()) {
            int retentionPeriodDays =
                using(conn)
                    .selectFrom(AUDIT_LOG_RETENTION_POLICIES)
                    .where(AUDIT_LOG_RETENTION_POLICIES.POLICY_NAME
                            .eq(AUDIT_LOG_RETENTION_POLICY_NAME))
                    .fetchOne(AUDIT_LOG_RETENTION_POLICIES.RETENTION_PERIOD);

            return createSetting(GlobalSettingSpecs.AuditLogRetentionDays.getSettingName(),
                        retentionPeriodDays);
        }
        catch (SQLException e) {
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
    }

    /**
     * Update the value of audit log data retention setting.
     *
     * @param retentionPeriod The new retention period.
     * @return The updated Setting.
     * @throws VmtDbException if there is a database error.
     *
     */
    public Optional<Setting> setAuditLogRetentionSetting(int retentionPeriod)
        throws VmtDbException {

        execute(Style.PATIENT, getJooqBuilder()
                .update(AUDIT_LOG_RETENTION_POLICIES)
                .set(AUDIT_LOG_RETENTION_POLICIES.RETENTION_PERIOD, retentionPeriod)
                .where(AUDIT_LOG_RETENTION_POLICIES.POLICY_NAME.eq(AUDIT_LOG_RETENTION_POLICY_NAME)));

        return Optional.of(createSetting(GlobalSettingSpecs.AuditLogRetentionDays.getSettingName(),
                    retentionPeriod));
    }

    /**
     * The stats in the db get rolled-up every 10 minutes from latest->hourly, hourly->daily.
     * So the daily table will have all the max values. Querying the daily table should suffice.
     * As the daily table stores all historic stats(until the retention period), we may return
     * entries which may not be relevant to the current enviornment(because targets could be removed).
     * We leave the filtering of the entities to the clients.
     * The access commodities are already filtered as we store stats only for non-access commmodities.
     * TODO:karthikt - Do batch selects(paginate) from the DB.
     */
    public List<EntityCommoditiesMaxValues> getEntityCommoditiesMaxValues(int entityType)
        throws VmtDbException, SQLException {

        // Get the name of the table in the db associated with the entityType.
        Table<?> tbl = getDayStatsDbTableForEntityType(entityType);
        if (tbl == null) {
            logger.warn("No table for entityType: {}", entityType);
            return Collections.emptyList();
        }
        // Query for the max of the max values from all the days in the DB for
        // each commodity in each entity.
        try (Connection conn = connection()) {
            Result<? extends Record> statsRecords =
                using(conn)
                    .select(dField(tbl, UUID), dField(tbl, PROPERTY_TYPE), dField(tbl, COMMODITY_KEY),
                                DSL.max(dField(tbl, MAX_VALUE)))
                    .from(tbl)
                    // only interested in used and sold commodities
                    .where(str(dField(tbl, PROPERTY_SUBTYPE)).eq(STATS_TABLE_PROPERTY_SUBTYPE_FILTER).and(
                        (relType(dField(tbl, RELATION))).eq(RelationType.COMMODITIES)))
                    .groupBy(dField(tbl, UUID), dField(tbl, PROPERTY_TYPE))
                    .fetch(); //TODO:karthikt - check if fetchLazy would help here.
            logger.debug("Number of records fetched for table {} = {}", tbl, statsRecords.size());
            return convertToEntityCommoditiesMaxValues(tbl, statsRecords);
        }
    }

    /**
     * Convert the max value db records into EntityCommoditiesMaxValues.
     *
     * @param tbl DB table from which the records were fetched.
     * @param maxStatsRecords Jooq Result containing the lisf of max values DB records.
     * @return List of converted records.
     */
    private List<EntityCommoditiesMaxValues> convertToEntityCommoditiesMaxValues(
                                                Table<?> tbl, Result<? extends Record> maxStatsRecords) {
        List<EntityCommoditiesMaxValues> maxValues = new ArrayList<>();
        // Group the records by entityId
        // TODO: karthikt - check the memory profile for large number of entities
        Map<String, List<Record>> entityIdToRecordGrouping =
            maxStatsRecords
            .stream()
            .collect(Collectors.groupingBy(
                rec -> rec.getValue(str(dField(tbl, UUID)))));
        // Create the protobuf messages
        entityIdToRecordGrouping.forEach((key, records) -> {
            EntityCommoditiesMaxValues.Builder entityMaxValuesBuilder =
                EntityCommoditiesMaxValues.newBuilder()
                    .setOid(Long.parseLong(key));

            records.forEach(record -> {
                CommodityMaxValue commodityMaxValue =
                    CommodityMaxValue.newBuilder()
                        .setMaxValue(record.getValue(DSL.field(MAX_COLUMN_NAME, Double.class)))
                        .setCommodityType(
                    CommodityType.newBuilder()
                        .setType(ClassicEnumMapper.commodityType(
                            record.getValue(str(dField(tbl, PROPERTY_TYPE)))).getNumber())
                        // WARN : CommodityKey gets truncated in length when
                        // being stored in the DB. It's a one-way function. This will lead to
                        // correctness problems if keys share common prefix and they get truncated
                        // at a common prefix boundary.
                        .setKey(record.getValue(str(dField(tbl, COMMODITY_KEY))))
                        .build())
                    .build();

                entityMaxValuesBuilder.addCommodityMaxValues(commodityMaxValue);
            });
            maxValues.add(entityMaxValuesBuilder.build());
        });

        return maxValues;
    }
}
