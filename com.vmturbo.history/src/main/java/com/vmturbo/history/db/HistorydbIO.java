package com.vmturbo.history.db;

import static com.vmturbo.common.protobuf.utils.StringConstants.AVG_VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.COMMODITY_KEY;
import static com.vmturbo.common.protobuf.utils.StringConstants.CURRENT_PRICE_INDEX;
import static com.vmturbo.common.protobuf.utils.StringConstants.EFFECTIVE_CAPACITY;
import static com.vmturbo.common.protobuf.utils.StringConstants.MAX_VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.MIN_VALUE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PRICE_INDEX;
import static com.vmturbo.common.protobuf.utils.StringConstants.PRODUCER_UUID;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE_USED;
import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_TYPE;
import static com.vmturbo.common.protobuf.utils.StringConstants.RELATION;
import static com.vmturbo.common.protobuf.utils.StringConstants.SNAPSHOT_TIME;
import static com.vmturbo.common.protobuf.utils.StringConstants.UUID;
import static com.vmturbo.history.db.jooq.JooqUtils.getDateOrTimestampField;
import static com.vmturbo.history.db.jooq.JooqUtils.getDoubleField;
import static com.vmturbo.history.db.jooq.JooqUtils.getRelationTypeField;
import static com.vmturbo.history.db.jooq.JooqUtils.getStringField;
import static com.vmturbo.history.db.jooq.JooqUtils.getTimestampField;
import static com.vmturbo.history.schema.abstraction.Tables.AUDIT_LOG_RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.MKT_SNAPSHOTS;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_DAY;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_BY_HOUR;
import static com.vmturbo.history.schema.abstraction.Tables.PM_STATS_LATEST;
import static com.vmturbo.history.schema.abstraction.Tables.RETENTION_POLICIES;
import static com.vmturbo.history.schema.abstraction.Tables.SCENARIOS;
import static com.vmturbo.history.schema.abstraction.Tables.VM_STATS_BY_HOUR;
import static org.jooq.impl.DSL.avg;
import static org.jooq.impl.DSL.row;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.tomcat.jdbc.pool.PoolProperties;
import org.jooq.Condition;
import org.jooq.Field;
import org.jooq.InsertSetStep;
import org.jooq.Query;
import org.jooq.Record;
import org.jooq.Record1;
import org.jooq.Record2;
import org.jooq.Record3;
import org.jooq.Result;
import org.jooq.ResultQuery;
import org.jooq.SQLDialect;
import org.jooq.Select;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.beans.factory.annotation.Value;

import com.vmturbo.api.enums.DayOfWeek;
import com.vmturbo.api.enums.Period;
import com.vmturbo.api.enums.ReportOutputFormat;
import com.vmturbo.api.enums.ReportType;
import com.vmturbo.auth.api.db.DBPasswordUtil;
import com.vmturbo.common.protobuf.setting.SettingProto.Setting;
import com.vmturbo.common.protobuf.stats.Stats.CommodityMaxValue;
import com.vmturbo.common.protobuf.stats.Stats.EntityCommoditiesMaxValues;
import com.vmturbo.common.protobuf.stats.Stats.EntityStatsScope;
import com.vmturbo.common.protobuf.stats.Stats.EntityToCommodityTypeCapacity;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesCapacityValuesResponse;
import com.vmturbo.common.protobuf.stats.Stats.GetEntityCommoditiesCapacityValuesResponse.Builder;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.CommodityRequest;
import com.vmturbo.common.protobuf.stats.Stats.StatsFilter.PropertyValueFilter;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.UICommodityType;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.TimeFrame;
import com.vmturbo.components.common.ClassicEnumMapper;
import com.vmturbo.components.common.pagination.EntityStatsPaginationParams;
import com.vmturbo.components.common.setting.GlobalSettingSpecs;
import com.vmturbo.components.common.setting.SettingDTOUtil;
import com.vmturbo.history.db.jooq.JooqUtils;
import com.vmturbo.history.db.queries.AvailableEntityTimestampsQuery;
import com.vmturbo.history.db.queries.AvailableTimestampsQuery;
import com.vmturbo.history.db.queries.EntityCommoditiesCapacityValuesQuery;
import com.vmturbo.history.db.queries.EntityCommoditiesMaxValuesQuery;
import com.vmturbo.history.schema.HistoryVariety;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.Tables;
import com.vmturbo.history.schema.abstraction.tables.Entities;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.Scenarios;
import com.vmturbo.history.schema.abstraction.tables.VmStatsByHour;
import com.vmturbo.history.schema.abstraction.tables.VmStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.EntitiesRecord;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.schema.abstraction.tables.records.ScenariosRecord;
import com.vmturbo.history.stats.MarketStatsAccumulatorImpl.MarketStatsData;
import com.vmturbo.history.stats.PropertySubType;
import com.vmturbo.platform.common.dto.CommonDTO;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.sql.utils.SQLDatabaseConfig.SQLConfigObject;

/**
 * Dbio Component for use within the History Component.
 **/
public class HistorydbIO extends BasedbIO {

    private static final Logger logger = LogManager.getLogger();

    /**
     * Use this to get stats table fields by name when working with a dynamically typed table.
     *
     * <p>For example, suppose you have a variable of type <code>Table&lt;?&gt;</code>, and you know
     * it's one of the various entity stats tables, and you want to access its fields. You can
     * use something like <code>table.get(GENERIC_STATS.AVG_VALUE, Double.class)</code>.</p>
     *
     * <p>An hourly table is used because it has all the fields that appear in any table (all the
     * *_key fields, samples, etc.)</p>
     */
    public static final VmStatsByHour GENERIC_STATS_TABLE = VM_STATS_BY_HOUR.VM_STATS_BY_HOUR;

    // length restriction on the  COMMODITY_KEY column; all stats tables have same column length
    private static final int COMMODITY_KEY_MAX_LENGTH = VmStatsLatest.VM_STATS_LATEST.COMMODITY_KEY
        .getDataType().length();

    // min and max for numerical values for the statstables; must fit in DECIMAL(15,3), 12 digits
    private static final double MAX_STATS_VALUE = 1e12D - 1;
    private static final double MIN_STATS_VALUE = -MAX_STATS_VALUE;
    private static final String SPACE = UICommodityType.SPACE.apiStr();
    private final SQLConfigObject sqlConfigObject;
    private static final String SECURE_DB_QUERY_PARMS = "useSSL=true&trustServerCertificate=true";
    private final PoolProperties poolPropertiesBase;

    /**
     * DB user name accessible to given schema.
     */
    @Value("${historyDbUsername:history}")
    private String historyDbUsername;

    /**
     * DB user password accessible to given schema.
     */
    @Value("${historyDbPassword:}")
    private String historyDbPassword;

    @Value("${requestHost:%}")
    private String requestHost;

    @Value("${dbSchemaName:vmtdb}")
    private String dbSchemaName;

    @Value("${readonlyUserName:vmtreader}")
    private String readonlyUserName;

    @Value("${queryTimeoutSeconds:120}")
    private int queryTimeout_sec;

    @Value("${migrationTimeoutSeconds:600}")
    private int migrationTimeout_sec;

    @Value("${maxUsedLookbackDays:90}")
    private int maxUsedLookbackDays;

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

    private static final String MAX_COLUMN_NAME = "max";

    /**
     * Maximum number of entities allowed in the getEntities method.
     *
     * @see HistorydbIO#getEntities(List)
     */
    @Value("${getEntitiesChunkSize:4000}")
    private int entitiesChunkSize;

    /**
     * The database password utility.
     */
    private DBPasswordUtil dbPasswordUtil;

    /**
     * Constructs the HistorydbIO instance.
     *
     * @param dbPasswordUtil     for access to DB credentials
     * @param sqlConfigObject    DB configuration information
     * @param poolPropertiesBase configurable connection pool properties
     */
    public HistorydbIO(@Nonnull DBPasswordUtil dbPasswordUtil,
            @Nonnull final SQLConfigObject sqlConfigObject,
            @Nonnull final PoolProperties poolPropertiesBase) {
        this.dbPasswordUtil = dbPasswordUtil;
        this.sqlConfigObject = sqlConfigObject;
        this.poolPropertiesBase = poolPropertiesBase;
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
    protected String getRootConnectionUrl() {
        return sqlConfigObject.getDbRootUrl();
    }

    @Override
    public String getUserName() {
        return historyDbUsername;
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
        return !Strings.isEmpty(historyDbPassword) ? historyDbPassword : dbPasswordUtil.getSqlDbRootPassword();
    }

    @Override
    public String getRequestHost() {
        return requestHost;
    }

    @Override
    public String getAdapter() {
        return sqlConfigObject.getSqlDialect().name().toLowerCase();
    }

    @Override
    public String getHostName() {
        return sqlConfigObject.getDbHost();
    }

    @Override
    public String getPortNumber() {
        return String.valueOf(sqlConfigObject.getDbPort());
    }

    @Override
    public String getDbSchemaName() {
        return dbSchemaName;
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
        // Get the query timeout from the internal connection pool if it has been initialized.
        // If not, return the value used to initialize the internal connection pool.
        if (isInternalConnectionPoolInitialized()) {
            return getInternalConnectionPoolTimeoutSeconds();
        } else {
            return queryTimeout_sec;
        }
    }

    public void setQueryTimeoutSeconds(int newTimeoutSec) {
        // The query timeout must be set on the internal connection pool to have any effect.
        // If it has been created, set it there, otherwise set it on the internal state used
        // to initialize the internal connection pool query timeout.
        if (isInternalConnectionPoolInitialized()) {
            setInternalConnectionPoolTimeoutSeconds(newTimeoutSec);
        }

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
    protected PoolProperties getPoolPropertiesBase() {
        return poolPropertiesBase;
    }

    @Override
    public void init(boolean clearOldDb, Double version, String dbName, Optional<String> migrationLocation) throws VmtDbException {
        // increase the DB timeout for the DB migration process
        int prevTimeoutSecs = getQueryTimeoutSeconds();
        setQueryTimeoutSeconds(migrationTimeout_sec);
        super.init(clearOldDb, version, dbName, migrationLocation);
        // reset the DB timeout to the typical value
        setQueryTimeoutSeconds(prevTimeoutSecs);
    }

    @Override
    protected String getMySQLConnectionUrl() {
        String baseUrl = "jdbc:" + getAdapter()
            + "://"
            + getHostName()
            + ":"
            + getPortNumber()
            + "/"
            + getDbSchemaName();
        final String settings = sqlConfigObject.getDriverProperties();
        String sep = "?";
        if (StringUtils.isNotEmpty(settings)) {
            baseUrl += sep + settings;
            sep = "&";
        }
        if (sqlConfigObject.isSecureDBConnectionRequested()) {
            // E.g.: "jdbc:mysql://host:3306/vmtdt?useSSL=true&trustServerCertificate=true
            return baseUrl + sep + SECURE_DB_QUERY_PARMS;
        }
        // E.g.: "jdbc:mysql://host:3306/vmtdt
        return baseUrl;
    }

    @Override
    public String getRootUsername() {
        if (sqlConfigObject.getRootCredentials().isPresent()) {
            return sqlConfigObject.getRootCredentials().get().getUserName();
        }
        return dbPasswordUtil.getSqlDbRootUsername(SQLDialect.MYSQL.toString());
    }

    @Override
    public String getRootPassword() {
        if (sqlConfigObject.getRootCredentials().isPresent()) {
            return sqlConfigObject.getRootCredentials().get().getPassword();
        }
        return getPassword();
    }

    /**
     * Convert the {@link CommonDTO.EntityDTO.EntityType} numeric id
     * to an SDK EntityType to an {@link EntityType}.
     *
     * @param sdkEntityTypeId the CommonDTO.EntityDTO.EntityType id to convert to a
     *                        database {@link EntityType}
     * @return an {@link EntityType} {@link EntityType} for the corresponding SDK Entity Type ID
     */
    public Optional<EntityType> getEntityType(int sdkEntityTypeId) {
        CommonDTO.EntityDTO.EntityType sdkEntityType = CommonDTO.EntityDTO.EntityType
            .forNumber(sdkEntityTypeId);
        if (sdkEntityType == null) {
            logger.debug("unknown entity type for entity type id {}", sdkEntityTypeId);
            return Optional.empty();
        }
        return EntityType.fromSdkEntityType(sdkEntityType).map(EntityType::resolve);
    }

    public Optional<String> getBaseEntityType(int sdkEntityTypeId) {
        CommonDTO.EntityDTO.EntityType sdkEntityType = CommonDTO.EntityDTO.EntityType
            .forNumber(sdkEntityTypeId);
        if (sdkEntityType == null || !EntityType.fromSdkEntityType(sdkEntityType).isPresent()) {
            logger.debug("unknown entity type for entity type id {}", sdkEntityTypeId);
            return Optional.empty();
        }
        return EntityType.fromSdkEntityType(sdkEntityType).map(EntityType::getName);
    }

    /**
     * Create an "insert" SQL statement for the given table  to be populated with values
     * for some number of rows to be inserted.
     *
     * @param dbTable the xxx_stats_latest table into which the record will be inserted
     * @param <R> record type
     * @return a jooq {@link Record} object appropriate for that table
     */
    public @Nonnull <R extends Record> InsertSetStep<R> getCommodityInsertStatement(
        @Nonnull Table<R> dbTable) {
        return getJooqBuilder().insertInto(dbTable);
    }


    /**
     * Create and populate a {@link MarketStatsLatestRecord} for insertion into the
     * associated table.
     *
     * @param marketStatsData the data for one stats item; will be written to one row
     * @param topologyInfo    Information about the topology causing the market stats insert.
     * @return a record to be inserted into the MarketStatsLatest table
     */
    public MarketStatsLatestRecord getMarketStatsRecord(
        @Nonnull final MarketStatsData marketStatsData,
        @Nonnull final TopologyInfo topologyInfo) {

        final MarketStatsLatestRecord record = MarketStatsLatest.MARKET_STATS_LATEST.newRecord();
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.SNAPSHOT_TIME,
                new Timestamp(topologyInfo.getCreationTime()));
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.TOPOLOGY_CONTEXT_ID,
                topologyInfo.getTopologyContextId());
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.TOPOLOGY_ID,
                topologyInfo.getTopologyId());
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.ENTITY_TYPE,
            marketStatsData.getEntityType());
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.ENVIRONMENT_TYPE,
            marketStatsData.getEnvironmentType());
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.PROPERTY_TYPE,
            marketStatsData.getPropertyType());
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.PROPERTY_SUBTYPE,
            marketStatsData.getPropertySubtype());
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.CAPACITY,
            clipValue(marketStatsData.getCapacity()));
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.AVG_VALUE,
            clipValue(marketStatsData.getUsed()));
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.MIN_VALUE,
            clipValue(marketStatsData.getMin()));
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.MAX_VALUE,
            clipValue(marketStatsData.getMax()));
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.EFFECTIVE_CAPACITY,
            clipValue(marketStatsData.getEffectiveCapacity()));
        record.set(MarketStatsLatest.MARKET_STATS_LATEST.RELATION,
            marketStatsData.getRelationType());
        return record;
    }

    /**
     * Populate a {@link Record} instance with commodity values.
     *
     * @param propertyType      the string name of the property (mixed case)
     * @param snapshotTime      the time of the snapshot
     * @param entityId          the id of the entity being processed
     * @param relationType      BOUGHT or SOLD
     * @param providerId        OID of the provider, if BOUGHT
     * @param capacity          the capacity of the commodity
     * @param effectiveCapacity the effective capacity of the commodity
     * @param commodityKey      the external association key for this commodity
     * @param record            the record being populated
     * @param table             stats db table for this entity type
     * @param longCommodityKeys for consolidated logging of shortened commodity keys
     */
    public void initializeCommodityRecord(
            @Nonnull String propertyType,
            long snapshotTime,
            long entityId,
            @Nonnull RelationType relationType,
            @Nullable Long providerId,
            @Nullable Double capacity,
            @Nullable Double effectiveCapacity,
            @Nullable String commodityKey,
            @Nonnull Record record,
            @Nonnull Table<?> table,
            @Nonnull Set<String> longCommodityKeys) {
        // providerId might be null
        String providerIdString = providerId != null ? Long.toString(providerId) : null;
        record.set(getStringField(table, PRODUCER_UUID), providerIdString);
        // commodity_key is limited in length
        // populate the other fields; all fields are nullable based on DB design; Is that best?
        // property_subtype, avg_value, min_value, max_value are all set elsewhere
        record.set(getDateOrTimestampField(table, SNAPSHOT_TIME),
                new java.sql.Timestamp(snapshotTime));
        record.set(getStringField(table, UUID), Long.toString(entityId));
        record.set(getStringField(table, PROPERTY_TYPE), propertyType);
        record.set(getDoubleField(table, CAPACITY), clipValue(capacity));
        record.set(getDoubleField(table, EFFECTIVE_CAPACITY), clipValue(effectiveCapacity));
        record.set(getRelationTypeField(table, RELATION), relationType);
        record.set(getStringField(table, COMMODITY_KEY),
                limitCommodityKey(commodityKey, longCommodityKeys));
    }

    /**
     * Check that a commodity key does not exceed its maximum allowed length, and truncate it
     * if necessary.
     *
     * @param commodityKey      commodity key value
     * @param longCommodityKeys where caller keeps track of shortened keys for consolidated logging
     * @return possibly truncated value
     */
    public static String limitCommodityKey(String commodityKey, Set<String> longCommodityKeys) {
        if (commodityKey != null && commodityKey.length() > COMMODITY_KEY_MAX_LENGTH) {
            longCommodityKeys.add(commodityKey);
            return commodityKey.substring(0, COMMODITY_KEY_MAX_LENGTH - 1);
        } else {
            return commodityKey;
        }
    }

    /**
     * Set the used and peak values for the subtype of this property.
     *
     * <p>For commodities that have peak values set, peak will contain a
     * non-zero value while for attributes that do not have peak value,
     * (Produces/PriceIndex etc.) 0 will be sent as parameter</p>
     *`
     * @param propertySubtype the subtype of the property, e.g. "used" or "utilization"
     * @param used           the average used value of the commodity
     * @param peak           the peak value of the commodity
     * @param record         the record being populated
     * @param table          the xxx_stats_latest table where this data will be written
     */
    public void setCommodityValues(
        @Nonnull String propertySubtype,
        @Nonnull final Optional<Double> used,
        @Nonnull final Optional<Double> peak,
        @Nonnull Record record,
        @Nonnull Table<?> table) {

        record.set(getStringField(table, PROPERTY_SUBTYPE), propertySubtype);

        if (used.isPresent()) {
            Double clipped = clipValue(used.get());
            record.set(getDoubleField(table, AVG_VALUE), clipped);
            record.set(getDoubleField(table, MIN_VALUE), clipped);
        }

        if (peak.isPresent()) {
            Double clippedPeak = clipValue(peak.get());
            record.set(getDoubleField(table, MAX_VALUE), clippedPeak);
        }
    }

    /**
     * Get the list of {@link Timestamp} objects describing the stat snapshot times in a
     * requested time range.
     *
     * <p>Note: This currently assumes each topology has at least one virtual machine.
     * Todo: We assume at least one PM before, but for new cloud model there is no PM, if user only
     * adds cloud target, it will not be able to get correct timestamp. Thus we change it to VM.
     * We should considering handling this better, when serverless is down the road. One possible
     * solution is keep a separate table which just has the updated timestamps. Opened OM-40052 for
     * future improvement.</p>
     *
     * @param timeFrame          The {@link TimeFrame} to look in.
     * @param startTime          The start time, in epoch millis.
     * @param endTime            The end time, in epoch millis.
     * @param specificEntityType entity type related to the timestamp we want to look for.
     * @param specificEntityOid  entity to use for the lookup in the table.
     * @return A list of {@link Timestamp} objects, in descending order. Each timestamp represents
     * the snapshot time for a set of records in the database - i.e. you can use any of
     * the snapshot times in a query, and it should return results (assuming other filters
     * are also satisfied).
     * @throws VmtDbException If there is an exception running the query.
     */
    @Nonnull
    public List<Timestamp> getTimestampsInRange(@Nonnull final TimeFrame timeFrame,
            final long startTime,
            final long endTime,
            @Nonnull final Optional<EntityType> specificEntityType,
            @Nonnull final Optional<String> specificEntityOid) throws VmtDbException {

        final Query query;
        if (specificEntityType.isPresent() && specificEntityOid.isPresent()) {
            query = new AvailableEntityTimestampsQuery(
                    timeFrame, specificEntityType.get(), specificEntityOid.get(), 0,
                    new Timestamp(startTime), new Timestamp(endTime), false).getQuery();
        } else {
            query = new AvailableTimestampsQuery(timeFrame, HistoryVariety.ENTITY_STATS, 0,
                    new Timestamp(startTime), new Timestamp(endTime)).getQuery();
        }
        return (List<Timestamp>)execute(Style.FORCED, query).getValues(0);
    }

    /**
     * Return a long epoch date representing the closest time stamp equals or before a given time point.
     * The timestamps are selected among the already ingested topologies. If the only
     * commodity in the stasFilter is priceIndex, the timestamp will be selected among the
     * ingested priceIndex timestamps
     *
     * @param statsFilter          stats filter which contains commodity requests.
     * @param timepPointOpt        time point to specify end time. If not present, the result is
     *                             the most recent time stamp.
     * @param timeFrameOpt         required timeframe, or LATEST if not specified
     * @param paginationParams     The option to use for getting the time based on pagination sort commodity.
     * @return a {@link Timestamp} for the snapshot recorded in the xxx_stats_latest table having
     *                             closest time to a given time point.
     * @throws IllegalArgumentException
     */
    public Optional<Timestamp> getClosestTimestampBefore(@Nonnull final StatsFilter statsFilter,
                                                         @Nonnull final Optional<Long> timepPointOpt,
                                                         @Nonnull final Optional<TimeFrame> timeFrameOpt,
                                                         @Nonnull final Optional<EntityStatsPaginationParams> paginationParams)
            throws IllegalArgumentException {
        try {
            Timestamp exclusiveUpperTimeBound =
                    // timePointOpt, if provided, is inclusive; we need an exclusive upper bound
                    timepPointOpt.map(t -> new Timestamp(t + 1))
                            .orElse(null);
            // PI timestamp needs to be treated differently because it doesn't get persisted when a topology is ingested.
            // Check the commodity request list, if it only contains PI, fetch data for PI.
            // Check if query is sorted by PI, if so, fetch timestamp for PI.
            // Otherwise, get the timestamp of the most recent ingested topology.
            final boolean usePriceDataBasedVariety = isCommRequestsOnlyPI(statsFilter.getCommodityRequestsList())
                    || isPaginationParamsSortByPI(paginationParams);
            final HistoryVariety historyVariety = usePriceDataBasedVariety ? HistoryVariety.PRICE_DATA : HistoryVariety.ENTITY_STATS;

            final Query query = new AvailableTimestampsQuery(timeFrameOpt.orElse(TimeFrame.LATEST),
                    historyVariety, 1, null, exclusiveUpperTimeBound).getQuery();
            final List<Timestamp> snapshotTimeRecords
                    = (List<Timestamp>)execute(Style.FORCED, query).getValues(0);

            if (!snapshotTimeRecords.isEmpty()) {
                return Optional.of(snapshotTimeRecords.get(0));
            }

        } catch (VmtDbException e) {
            logger.error("Failed to get database connection.", e);
        }
        return Optional.empty();
    }

    /**
     * Whether a given list contains only price index or current price index request.
     *
     * <p>Note: If the request is empty, returns false.</p>
     *
     * @param commRequestsList a given commodity request list
     * @return true if the commRequestsList contains price index or current price index
     *         request, false if the list is empty or it has at least one commodity
     *         request that is not price index and not current price index.
     */
    public boolean isCommRequestsOnlyPI(List<CommodityRequest> commRequestsList) {
        // If the commodity request contains priceIndex or currentPriceIndex,
        // return true.
        // NOTE: Though XL tables do not have the current price index commodity,
        // UX is shared between classic and XL which asks for both price index
        // and current price index commodity when populating the risk widget.
        return !commRequestsList.isEmpty() && commRequestsList.stream()
                .allMatch(c -> c.getCommodityName().equals(PRICE_INDEX)
                        || c.getCommodityName().equals(CURRENT_PRICE_INDEX));
    }

    private boolean isPaginationParamsSortByPI(@Nonnull final Optional<EntityStatsPaginationParams> paginationParams) {
        return paginationParams.isPresent() && StringConstants.PRICE_INDEX.equals(paginationParams.get().getSortCommodity());
    }

    private Set<String> getRequestedScopeIds(EntityStatsScope entityScope) {
        if (entityScope.hasEntityList()) {
            return Sets.newHashSet(Collections2.transform(
                    entityScope.getEntityList().getEntitiesList(), id -> Long.toString(id)));
        }

        return Collections.emptySet();
    }

    /**
     * Gets entityType from {@link EntityStatsScope}.
     *
     * @param entityScope   The {@link EntityStatsScope} to use
     * @return single {@link EntityType} within EntityStatsScope
     * @throws VmtDbException if there's an error querying DB for types of entities
     * @throws IllegalArgumentException if entityType resolves to multiple or invalid entityType
     */
    public EntityType getEntityTypeFromEntityStatsScope(final EntityStatsScope entityScope)
            throws VmtDbException, IllegalArgumentException {
        if (!entityScope.hasEntityList()) {
            return getEntityType(entityScope.getEntityType())
                    .orElseThrow(() -> new IllegalArgumentException("Invalid entity type: " + entityScope.getEntityType()));
        }

        final Set<String> requestedIdSet = getRequestedScopeIds(entityScope);

        final Set<String> entityTypes = new HashSet<>(getTypesForEntities(requestedIdSet).values());

        if (entityTypes.isEmpty()) {
            return null;
        }

        if (entityTypes.size() > 1) {
            logger.error("Attempting to paginate across multiple entity types: {}",
                    entityTypes);
            throw new IllegalArgumentException("Pagination across multiple entity types not supported.");
        }

        String entityType = entityTypes.iterator().next();
        return EntityType.named(entityType)
                .orElseThrow(() -> new IllegalArgumentException("Entities resolve to invalid entity type: " + entityType));
    }

    /**
     * Get the next page of entity IDs given a time frame and pagination parameters.
     *
     * @param entityScope      The {@link EntityStatsScope} for the stats query.
     * @param timestamp        The timestamp to use to calculate the next page.
     * @param tFrame           The timeframe to use for the timestamp.
     * @param paginationParams The pagination parameters. For princeIndex, we sort the results by
     *                         the average value; for others, we sort the results by the utilization
     *                        (average/capacity) value of the sort commodity. And then by the UUID of the entity.
     * @param entityType       The {@link EntityType} to determine tables to query
     * @param statsFilter      The filter specifying which stats to get. If the filter time range spans
     *                         across multiple snapshots, the sort order for pagination will be
     *                         derived from the most recent snapshot. However, once we determine the
     *                         IDs of the entities in the next page, we retrieve records for those
     *                         entities from all matching snapshots.
     * @return A {@link NextPageInfo} object describing the entity IDs that should be in the next page.
     * @throws VmtDbException           If there is an error interacting with the database.
     * @throws IllegalArgumentException If the input is invalid.
     */
    @Nonnull
    public NextPageInfo getNextPage(final EntityStatsScope entityScope,
                                    final Timestamp timestamp,
                                    final TimeFrame tFrame,
                                    final EntityStatsPaginationParams paginationParams,
                                    final EntityType entityType,
                           @Nonnull final StatsFilter statsFilter) throws VmtDbException {
        // This will be an empty list if entity list is not set.
        // This should NOT be an empty list if the entity list is set (we should filter out
        // those requests earlier on).
        Preconditions.checkArgument(entityScope.hasEntityType()
            || (entityScope.hasEntityList() && entityScope.getEntityList().getEntitiesCount() > 0));

        if (entityType == null) {
            // We don't want to treat this an error, because this may occur when the user
            // scopes to an entity of a type for which we do not collect stats. We'll log
            // a warning and send back an empty result.
            logger.warn("No entity types resolved from entity IDs {}; " +
                            "this is expected if entity type is one for which we do not collect stats.",
                    entityScope.getEntityList().getEntitiesList());
            return NextPageInfo.EMPTY_INSTANCE;

        }

        final SeekPaginationCursor seekPaginationCursor = SeekPaginationCursor.parseCursor(paginationParams.getNextCursor().orElse(""));
        // Now we have the entity type, we can use it together with the time frame to
        // figure out the table to paginate in.
        final Table<?> table = entityType.getTimeFrameTable(tFrame).get();

        final List<Condition> conditions = new ArrayList<>();
        conditions.add(getTimestampField(table, SNAPSHOT_TIME).eq(timestamp));
        conditions.add(getStringField(table, PROPERTY_TYPE).eq(paginationParams.getSortCommodity()));
        final String sortCommodity = paginationParams.getSortCommodity();
        if (!sortCommodity.equals(PRICE_INDEX)) {
            // For "regular" commodities (e.g. CPU, Mem), we want to make sure to sort by the used
            conditions.add(getStringField(table, PROPERTY_SUBTYPE).eq(PROPERTY_SUBTYPE_USED));
            conditions.add(getRelationTypeField(table, RELATION).eq(
                    getRelationType(statsFilter.getCommodityRequestsList(), sortCommodity)));
        }

        final Field<BigDecimal> avgValue =
            avg(getDoubleField(table, AVG_VALUE)).as(AVG_VALUE);
        final Field<BigDecimal> avgCapacity =
            avg(getDoubleField(table, CAPACITY)).as(CAPACITY);
        final Field<String> uuid =
            getStringField(table, UUID);

        final Set<String> requestedIdSet = getRequestedScopeIds(entityScope);
        if (!requestedIdSet.isEmpty()) {
            conditions.add(uuid.in(requestedIdSet));
        }
        // Given an entity id, a commodity type and a time snapshot, we might have multiple
        // entries in the db. We want to group them first, by averaging their values and
        // capacity, this will be stored in the aggregate stats table. Then, from this table we
        // can apply our cursor conditions and order the members based on the pagination
        // parameters.
        try (Connection conn = transConnection()) {
            Table<Record3<String, BigDecimal, BigDecimal>> aggregatedStats = using(conn)
                .select(uuid, avgValue, avgCapacity)
                .from(table)
                .where(conditions)
                .groupBy(uuid).asTable();
            final Field<String> aggregateUuidField = getStringField(aggregatedStats, UUID);
            final Field<BigDecimal> aggregateValueField =
                seekPaginationCursor.getValueField(paginationParams,
                aggregatedStats);

            // This call adds the seek pagination parameters to the list of conditions.
            final List<Condition> cursorConditions = new ArrayList<>();
            seekPaginationCursor.toCondition(aggregatedStats, paginationParams)
                .ifPresent(cursorCondition -> cursorConditions.addAll(cursorCondition));

            final Result<Record2<String, BigDecimal>> results = using(conn)
                .select(aggregateUuidField, aggregateValueField)
                .from(aggregatedStats)
                // The pagination is enforced by the conditions (see above).
                .where(cursorConditions)
                .orderBy(paginationParams.isAscending() ? aggregateValueField.asc().nullsLast() :
                        aggregateValueField.desc().nullsLast(),
                    paginationParams.isAscending() ?
                        aggregateUuidField.asc() : aggregateUuidField.desc())
                // Add one to the limit so we can tests to see if there are more results.
                .limit(paginationParams.getLimit() + 1)
                .fetch();

            Integer totalRecordCount = getTotalRecordsCount(conn, aggregatedStats);
            if (results.size() > paginationParams.getLimit()) {
                // If there are more results, we trim the last result (since that goes beyond
                // the page limit).
                final List<String> nextPageIds =
                    results.getValues(aggregateUuidField).subList(0, paginationParams.getLimit());
                final int lastIdx = nextPageIds.size() - 1;
                return new NextPageInfo(nextPageIds, table,
                        SeekPaginationCursor.nextCursor(nextPageIds.get(lastIdx),
                                results.getValue(lastIdx, aggregateValueField)),
                        totalRecordCount);
            } else {
                return new NextPageInfo(results.getValues(aggregateUuidField),
                        table,
                        SeekPaginationCursor.empty(),
                        totalRecordCount);
            }
        } catch (SQLException e) {
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
    }

    @Nonnull
    private static RelationType getRelationType(
            @Nonnull Collection<CommodityRequest> commodityRequests,
            @Nonnull String sortCommodity) {
        final List<CommodityRequest> sortCommodities = commodityRequests.stream()
                .filter(c -> c.getCommodityName().equalsIgnoreCase(sortCommodity))
                .collect(Collectors.toList());
        if (sortCommodities.size() != 1) {
            logger.debug(
                    "Cannot definitely identify commodity request '{}' for specified sort commodity '{}'",
                    commodityRequests, sortCommodity);
            return RelationType.COMMODITIES;
        }
        final List<PropertyValueFilter> sortCommodityProperties =
                sortCommodities.iterator().next().getPropertyValueFilterList();
        return sortCommodityProperties
                .stream()
                .filter(c -> c.getProperty().equalsIgnoreCase(RELATION))
                .map(c -> RelationType.getApiRelationType(c.getValue()))
                .findAny()
                .orElse(RelationType.COMMODITIES);
    }

    /**
     * Will count total records in {@link Table}.
     *
     * @param conn db connection used to run query
     * @param aggregatedStats {@link Table} used to count total records
     * @return Total records in {@link Table}
     */
    @VisibleForTesting
    protected int getTotalRecordsCount(Connection conn, Table<Record3<String, BigDecimal, BigDecimal>> aggregatedStats) {
        return using(conn).fetchCount(aggregatedStats);
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
            try (Connection conn = connection()) {
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
    public Map<String, String> getTypesForEntities(Set<String> entityIds) throws VmtDbException {

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
     * @param table           the table to query from
     * @param selectFields    the columns to return from the query
     * @param whereConditions the conditions to filter on
     * @param orderFields     the ordering for the results
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
     * @param table            the table to query from
     * @param selectFields     the columns to return from the query
     * @param whereConditions  the conditions to filter on
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
     * <p>If the entity ID is not known, then return null.</p>
     *
     * <p>TODO: should be changed to use Optional</p>
     *
     * @param entityId the entity OID (as a string)
     * @return the display name for the Entity, as stored in the "display_name" for the given
     * entity OID; return null if not found.
     */
    @Nullable
    public String getEntityDisplayNameForId(long entityId) {

        try (Connection conn = connection()) {

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
        try (Connection conn = connection()) {
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
     * @param topologyInfo the information about this topology, including the
     *                     context id and snapshot time.
     * @return the SCENARIOS record for the given topologyContextId
     * @throws VmtDbException if there is a database error in the
     */
    public ScenariosRecord getOrAddScenariosRecord(@Nonnull final TopologyInfo topologyInfo)
        throws VmtDbException {
        Optional<ScenariosRecord> scenarioInfo =
            getScenariosRecord(topologyInfo.getTopologyContextId());
        if (!scenarioInfo.isPresent()) {
            logger.debug("Persisting scenario with topologyContextId {}",
                topologyInfo.getTopologyContextId());
            addMktSnapshotRecord(topologyInfo);
            scenarioInfo = getScenariosRecord(topologyInfo.getTopologyContextId());
        }

        return scenarioInfo.orElseThrow(() -> new VmtDbException(VmtDbException.INSERT_ERR,
            "Error writing scenarios record for plan priceIndexInfo for " +
                topologyInfo.getTopologyContextId()));
    }

    /**
     * Create entries in the SCENARIOS and MKT_SNAPSHOTS table for this topology, if required.
     * Each of the insert statments specifies "onDuplicateKeyIgnore", so there's no error
     * signalled if either row already exists.
     *
     * <p>Note that the topologyContextId is used for both the ID and the scenario_id in the
     * SCENARIOS table.</p>
     *
     * <p>Note that there is insufficient information to populate the displayName, the state,
     * and the times. This may be provided in the future by adding a listener to the
     * Plan Orchestrator.</p>
     *
     * @param topologyInfo the information about this topology, including the
     *                     context id and snapshot time.
     * @throws VmtDbException if there's an error writing to the RDB
     */
    public void addMktSnapshotRecord(TopologyInfo topologyInfo) throws VmtDbException {

        long topologyContextId = topologyInfo.getTopologyContextId();
        // add this topology to the scenarios table
        final Timestamp snapshotTimestamp = new Timestamp(topologyInfo.getCreationTime());
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
                + topologyInfo.getCreationTime())
            .set(MKT_SNAPSHOTS.RUN_COMPLETE_TIME, snapshotTimestamp)
            .onDuplicateKeyIgnore());
    }

    /**
     * Persist multiple entitites (insert or update, depending on the record source). If a record
     * has been initially returned from the DB, UPDATE will be executed. If a record is a newly
     * created INSERT will be executed.
     *
     * <p>Queries are splitted into smaller batches of 4k (settable by {@link #entitiesChunkSize}
     * property.</p>
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
     * Clip a value so it fits within the decimal precision for the Stats tables.
     * This precision is currently "DECIMAL(15,3)". If the value to be clipped is null,
     * return null;
     *
     * @param rawStatsValue a numeric stats value from either discovery or priceIndex
     * @return the value clipped between MIN_STATS_VALUE (large negative number) and MAX_STATS_VALUE
     */
    @Nullable
    public Double clipValue(Double rawStatsValue) {
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
     * List the ids of plans that have data in the database.
     *
     * @return The set of ids.
     * @throws VmtDbException If there is an error connecting to the database.
     */
    public Set<Long> listPlansWithStats() throws VmtDbException {
        try (Connection conn = connection()) {
            return using(conn).selectDistinct(Scenarios.SCENARIOS.ID).from(Scenarios.SCENARIOS)
                .fetch().stream()
                .map(Record1::value1)
                .collect(Collectors.toSet());
        } catch (SQLException e) {
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
    }

    /**
     * Get all the stats data retention settings.
     *
     * @return List of all the data retention settings.
     * @throws VmtDbException      if there is a database error.
     * @throws DataAccessException if there is a database error.
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
        } catch (SQLException e) {
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
     * @param settingName     The name of the setting to update.
     * @param retentionPeriod The new retention period.
     * @return The updated Setting.
     * @throws VmtDbException if there is a database error.
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
        // reload data cached in RetentionPolicy instance, and adjust expiration times in existing
        // available_timestamps records
        RetentionPolicy.onChange();
        return Optional.of(createSetting(settingName, retentionPeriod));
    }

    /**
     * Get the audit log entries data retention settings.
     *
     * @return The data retention setting.
     * @throws VmtDbException      if there is a database error.
     * @throws DataAccessException if there is a database error.
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
        } catch (SQLException e) {
            throw new VmtDbException(VmtDbException.SQL_EXEC_ERR, e);
        }
    }

    /**
     * Update the value of audit log data retention setting.
     *
     * @param retentionPeriod The new retention period.
     * @return The updated Setting.
     * @throws VmtDbException if there is a database error.
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
     * Compute max values aggregated over all monthly stats records for each entity-id/sold-commodity
     * combination.
     *
     * <p>We're really after all the max values across all entity stats records. But since the max
     * values get rolled up approximately every hour to monthly stats, querying monthly tables
     * suffices.</p>
     *
     * <p>As the stats table stores all historic stats(until the retention period), we may return
     * entries which may not be relevant to the current environment(because targets could be removed).
     * We leave the filtering of the entities to the clients.</p>
     *
     * <p>The access commodities are not of interest, but stats are not stored for them, so no need
     * to filter here.</p>
     *
     * <p>TODO:karthikt - Do batch selects(paginate) from the DB.</p>
     *
     * @param entityTypeNo entity type number
     * @param comms commodity list to obtain max value for.
     * @return query results, transformed into  {@link EntityCommoditiesMaxValues} strucures
     * @throws VmtDbException on DB exceptions
     * @throws SQLException on DB exceptions
     */
    public List<EntityCommoditiesMaxValues> getEntityCommoditiesMaxValues(int entityTypeNo, List<Integer> comms)
            throws VmtDbException, SQLException {
        final EntityType entityType = EntityType.fromSdkEntityType(entityTypeNo).orElse(null);
        Optional<Table<?>> table = entityType != null ? entityType.getMonthTable() : Optional.empty();
        if (table.isPresent()) {
            List<String> commStrings = comms.stream().map(comm -> {
                return ClassicEnumMapper.getCommodityString(
                    CommodityDTO.CommodityType.forNumber(comm));
            }).collect(Collectors.toList());
            // Query for the max of the max values from all the days in the DB for
            // each commodity in each entity.
            try (Connection conn = connection()) {
                final ResultQuery<?> query = new EntityCommoditiesMaxValuesQuery(table.get(), commStrings, maxUsedLookbackDays).getQuery();
                Result<? extends Record> statsRecords = using(conn).fetch(query);
                logger.debug("Number of records fetched for table {} = {}", table.get(), statsRecords.size());
                return convertToEntityCommoditiesMaxValues(table.get(), statsRecords);
            }
        } else {
            logger.error("No monthly stats table for entityType: {}",
                    entityType != null ? entityType.getName() : "Entity type #" + entityTypeNo);
            return Collections.emptyList();
        }
    }

    /**
     * Constructing a query for getting entities commodity capacity and get the result from the db.
     * according to entity type to know which for the stats tables to select.
     *
     * @param entityTypeNo for knowing the table to access
     * @param uuids of the entity record we want
     * @param commodityTypeName of the record we want
     * @param retentionPeriod used to know which table should we query
     * @return a converted response ready for sending back
     * @throws VmtDbException in case of a db connection issue
     * @throws SQLException in case if issues executing the sql query
     */
    public List<GetEntityCommoditiesCapacityValuesResponse> getEntityCommoditiesCapacityValues(int entityTypeNo,
           Set<String> uuids, String commodityTypeName, TimeUnit retentionPeriod)
        throws VmtDbException, SQLException {
        final EntityType entityType = EntityType.fromSdkEntityType(entityTypeNo).orElse(null);
        Optional<Table<?>> table = getTableByTimeUnit(entityType, retentionPeriod);
        if (table.isPresent()) {
            // Query for the max of the capacities from the last 7 days in the DB for
            // each commodity in each entity.
            try (Connection conn = connection()) {
                final ResultQuery<?> query = new EntityCommoditiesCapacityValuesQuery(table.get(),
                    uuids, commodityTypeName).getQuery();
                Result<? extends Record> statsRecords = using(conn).fetch(query);
                logger.debug("Number of records fetched for table {} = {}", table.get(), statsRecords.size());
                return convertToEntityCommoditiesCapacityValues(table.get(), statsRecords);
            }
        } else {
            logger.error("No monthly stats table for entityType: {}",
                entityType != null ? entityType.getName() : "Entity type #" + entityTypeNo);
            return Collections.emptyList();
        }
    }

    private Optional<Table<?>> getTableByTimeUnit(final EntityType entityType, final TimeUnit retentionPeriod) {
        return Optional.ofNullable(entityType)
            .flatMap(eType -> {
                switch (retentionPeriod) {
                    case DAYS:
                        return entityType.getDayTable();
                    case HOURS:
                        return entityType.getHourTable();
                    default:
                        return Optional.empty();
                }
            });
    }

    private List<GetEntityCommoditiesCapacityValuesResponse> convertToEntityCommoditiesCapacityValues(
        Table<?> tbl, final Result<? extends Record> statsRecords) {
        final Builder responseBuilder = GetEntityCommoditiesCapacityValuesResponse.newBuilder();
        statsRecords.forEach(record -> {
            final Long uuid = Long.parseLong(record.getValue(getStringField(tbl, UUID)));
            final String comKey = record.getValue(getStringField(tbl, COMMODITY_KEY));
            final String comTypeString = record.getValue(getStringField(tbl, PROPERTY_TYPE));
            final Integer comTypeInt = new Integer(UICommodityType.fromString(comTypeString).typeNumber());
            final Double maxCapacity = record.getValue(DSL.field(MAX_COLUMN_NAME, Double.class));
            responseBuilder.addEntitiesToCommodityTypeCapacity(
                    EntityToCommodityTypeCapacity.newBuilder()
                        .setEntityUuid(uuid)
                        .setCommodityType(CommodityType.newBuilder()
                            .setType(comTypeInt)
                            .setKey(comKey).build())
                        .setCapacity(maxCapacity).build()
            );
        });
        return Collections.singletonList(responseBuilder.build());
    }

    /**
     * Convert the max value db records into EntityCommoditiesMaxValues.
     *
     * @param tbl             DB table from which the records were fetched.
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
                    rec -> rec.getValue(getStringField(tbl, UUID))));
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
                                .setType(UICommodityType.fromString(
                                    record.getValue(getStringField(tbl, PROPERTY_TYPE))).typeNumber())
                                // WARN : CommodityKey gets truncated in length when
                                // being stored in the DB. It's a one-way function. This will lead to
                                // correctness problems if keys share common prefix and they get truncated
                                // at a common prefix boundary.
                                .setKey(record.getValue(getStringField(tbl, COMMODITY_KEY)))
                                .build())
                        .build();

                entityMaxValuesBuilder.addCommodityMaxValues(commodityMaxValue);
            });
            maxValues.add(entityMaxValuesBuilder.build());
        });

        return maxValues;
    }

    /**
     * Return the mapping from EntityIds -> EntityTypes stored in
     * the entities table.
     *
     * @return Map of EntityId -> {@link EntityDTO.EntityType}
     * @throws VmtDbException
     */
    public Map<Long, EntityDTO.EntityType> getEntityIdToEntityTypeMap()
        throws VmtDbException {

        // As PM and DC entities are stored in the same stats table, we have to
        // run additional query to distinguish between the PM and DC entities.
        Set<Long> datacenterEntities = getDatacenterEntities();
        Map<Long, EntityDTO.EntityType> entityIdToEntityTypeMap = new HashMap<>();

        try (Connection conn = connection()) {
            final Map<Long, String> entityIdToCreationClass =
                using(conn)
                    .selectFrom(Entities.ENTITIES)
                    .fetch()
                    .intoMap(Entities.ENTITIES.ID, Entities.ENTITIES.CREATION_CLASS);

            Set<String> unresolvedCreationClasses = new HashSet<>();
            entityIdToCreationClass.entrySet()
                .forEach(entry -> {
                    EntityDTO.EntityType type;
                    if (datacenterEntities.contains(entry.getKey())) {
                        type = EntityDTO.EntityType.DATACENTER;
                    } else {
                        Optional<EntityType> entityType =
                            EntityType.named(entry.getValue());
                        if (!entityType.isPresent()) {
                            unresolvedCreationClasses.add(entry.getValue());
                        }
                        type = entityType.get().getSdkEntityType().orElse(null);
                    }

                    entityIdToEntityTypeMap.put(entry.getKey(), type);
                });
            if (!unresolvedCreationClasses.isEmpty()) {
                logger.error("Failed to resolve following class names to entity types: {}",
                        unresolvedCreationClasses);
            }
        } catch (SQLException e) {
            throw new VmtDbException(VmtDbException.READ_ERR, e);
        }
        return entityIdToEntityTypeMap;
    }

    /*
      Datacenter entities are stored in PM entities table. This function
      returns all the Datacenter entities which are in the PM stats tables.
     */
    private Set<Long> getDatacenterEntities() throws VmtDbException {

        // Look at _latest, _by_hour and _by_day tables so that we don't miss any entities.
        // select distinct(uuid) from pm_stats_* where producer_uuid is NULL and property_type='Space'
        // and property_subtype='used';
        // Since PM and DC are stored in the same PM stats tables, this query helps in distinguishing
        // the Datacenter entities from the PM entities.
        final Set<Long> allDatacenterEntities = new HashSet<>();
        try (Connection conn = connection()) {
            // query from latest table.
            final String usedSubType = PropertySubType.Used.getApiParameterName();
            using(conn)
                .selectDistinct(PM_STATS_LATEST.UUID)
                .from(PM_STATS_LATEST)
                .where(PM_STATS_LATEST.PRODUCER_UUID.isNull()).and(
                PM_STATS_LATEST.PROPERTY_TYPE.eq(SPACE)).and(
                PM_STATS_LATEST.PROPERTY_SUBTYPE.eq(usedSubType))
                .fetch()
                .listIterator()
                .forEachRemaining(record -> allDatacenterEntities.add(Long.valueOf(record.value1())));

            // query from hourly table
            using(conn)
                .selectDistinct(PM_STATS_BY_HOUR.UUID)
                .from(PM_STATS_BY_HOUR)
                .where(PM_STATS_BY_HOUR.PRODUCER_UUID.isNull()).and(
                PM_STATS_BY_HOUR.PROPERTY_TYPE.eq(SPACE)).and(
                PM_STATS_BY_HOUR.PROPERTY_SUBTYPE.eq(usedSubType))
                .fetch()
                .listIterator()
                .forEachRemaining(record -> allDatacenterEntities.add(Long.valueOf(record.value1())));

            // query from daily table
            using(conn)
                .selectDistinct(PM_STATS_BY_DAY.UUID)
                .from(PM_STATS_BY_DAY)
                .where(PM_STATS_BY_DAY.PRODUCER_UUID.isNull()).and(
                PM_STATS_BY_DAY.PROPERTY_TYPE.eq(SPACE)).and(
                PM_STATS_BY_DAY.PROPERTY_SUBTYPE.eq(usedSubType))
                .fetch()
                .listIterator()
                .forEachRemaining(record -> allDatacenterEntities.add(Long.valueOf(record.value1())));

        } catch (SQLException e) {
            throw new VmtDbException(VmtDbException.READ_ERR, e);
        }

        return allDatacenterEntities;
    }

    /**
     * Information about the next page of entities, when doing a paginated traversal
     * through a table.
     */
    public static class NextPageInfo {

        // this can be used as an empty first page for a query from which a table type cannot
        // be determined. Take care that the caller does not depend on a non-null table.
        static final NextPageInfo EMPTY_INSTANCE
            = new NextPageInfo(Collections.emptyList(), null, null, null);

        private final List<String> entityOids;

        private final Table table;

        private final Optional<String> nextCursor;

        //Total Record Count available for current paginated query
        private final Optional<Integer> totalRecordCount;

        private NextPageInfo(final List<String> entityOids,
                @Nullable final Table<?> table,
                @Nullable final SeekPaginationCursor seekPaginationCursor,
                             @Nullable final Integer totalRecordCount) {
            this.entityOids = Objects.requireNonNull(entityOids);
            this.table = table;
            this.nextCursor = seekPaginationCursor != null ? seekPaginationCursor.toCursorString()
                : Optional.empty();
            this.totalRecordCount = Optional.ofNullable(totalRecordCount);
        }

        /**
         * Get the entities that comprise the next page of results, in order.
         *
         * @return The list of entities, in the requested order.
         */
        public List<String> getEntityOids() {
            return entityOids;
        }

        /**
         * Get the next (serialized) cursor, if any.
         *
         * @return An {@link Optional} containing the next cursor, or an empty optional if there are
         * no more results.
         */
        public Optional<String> getNextCursor() {
            return nextCursor;
        }

        /**
         * Get the table that the page is coming from.
         *
         * @return A {@link Table}, or null if this is the special EMPTY_INSTANCE
         */
        public Table getTable() {
            return table;
        }

        public Optional<Integer> getTotalRecordCount() {
            return totalRecordCount;
        }
    }

    /**
     * A helper class to construct a seek pagination cursor to track pagination through a large
     * number of entities. This not the same as a MySQL cursor!
     *
     * <p>We use the seek method for pagination, so the cursor is a serialized version of the
     * last value and ID:
     * (https://blog.jooq.org/2013/10/26/faster-sql-paging-with-jooq-using-the-seek-method/)</p>
     */
    @VisibleForTesting
    static class SeekPaginationCursor {
        private final Optional<String> lastId;

        private final Optional<BigDecimal> lastValue;

        /**
         * Do not use the constructor - use the helper methods!
         * @param lastId Last id in a set of results.
         * @param lastValue Last value in a set of results.
         */
        @VisibleForTesting
        SeekPaginationCursor(final Optional<String> lastId,
                                   final Optional<BigDecimal> lastValue) {
            this.lastId = lastId;
            this.lastValue = lastValue;
        }

        /**
         * Parse a string produced by {@link SeekPaginationCursor#toCursorString()} into a {@link SeekPaginationCursor} object.
         *
         * @param nextCursor The next cursor string.
         * @return The {@link SeekPaginationCursor}
         * @throws IllegalArgumentException If the cursor is invalidky
         */
        @Nonnull
        public static SeekPaginationCursor parseCursor(final String nextCursor) {
            // The cursor should be: "<lastId>:<lastValue>", where lastId is the ID of the
            // last element in the previous page, and lastValue is the stat value of the sort
            // commodity for that ID.
            String[] results = nextCursor.split(":");
            if (results.length != 2) {
                return new SeekPaginationCursor(Optional.empty(), Optional.empty());
            } else {
                try {
                    return new SeekPaginationCursor(Optional.of(results[0]),
                        Optional.of(new BigDecimal(results[1])));
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid cursor: " + nextCursor);
                }
            }
        }

        /**
         * Create an empty cursor.
         */
        public static SeekPaginationCursor empty() {
            return new SeekPaginationCursor(Optional.empty(), Optional.empty());
        }

        /**
         * Create the cursor to access the next page of results.
         *
         * @param lastId The last ID in the current page of results.
         * @param lastValue The last value in the current page of results.
         * @return The {@link SeekPaginationCursor} object.
         */
        public static SeekPaginationCursor nextCursor(@Nonnull final String lastId,
                                                      @Nonnull final BigDecimal lastValue) {
            return new SeekPaginationCursor(Optional.of(lastId), Optional.of(lastValue));
        }

        /**
         * Create the cursor string to respresent this cursor.
         *
         * @return An optional containing the cursor string,
         * or an empty optional if the cursor is empty.
         */
        public Optional<String> toCursorString() {
            if (lastId.isPresent() && lastValue.isPresent()) {
                return Optional.of(lastId.get() + ":" + lastValue.get().toString());
            } else {
                return Optional.empty();
            }
        }

        /**
         * Return the value field used in sort by.
         * For princeIndex, sort by the average value, because it's a compound metrics already.
         * For others commodity, sort by the average/capacity value, because the average value
         * is from "used", so divided by "capacity" to be utilization.
         * @param paginationParams Parameters for pagination.
         * @param table The table containing the results.
         * @return A {@link Field} containing the value field.
         */
        @VisibleForTesting
        static Field<BigDecimal> getValueField(@Nonnull final EntityStatsPaginationParams paginationParams,
                                    @Nonnull final Table<?> table) {
            final Field<BigDecimal> avgValueField =
                JooqUtils.getBigDecimalField(table, AVG_VALUE);
            // this approach of sorting by composite is questionable, definitely very slow
            // strictly speaking capacity is nullable and can contain zeros
            return paginationParams.getSortCommodity().equals(PRICE_INDEX)
                ? avgValueField :
                avgValueField.divide(JooqUtils.getBigDecimalField(table, CAPACITY));
        }

        /**
         * Create the condition that will apply this cursor to the results in the database.
         *
         * @param table            The table we're paginating through.
         * @param paginationParams The parameters for pagination.
         *                         TODO (roman, June 28 2018): We should encode the sort order into the
         *                         cursor, and return an error if the sort order changes between
         *                         calls.
         * @return An {@link Optional} containing the condition to insert into the query to get
         * the next page of results, or an empty optional if the cursor is empty (i.e.
         * we just want the first page of results).
         */
        public Optional<List<Condition>> toCondition(final Table<?> table,
                                                @Nonnull final EntityStatsPaginationParams paginationParams) {
            if (lastId.isPresent() && lastValue.isPresent()) {
                // See: https://blog.jooq.org/2013/10/26/faster-sql-paging-with-jooq-using-the-seek-method/
                // In some cases, because of approximations in the utilization (since it's a
                // ratio between two decimals), we might get the same element that is expressed
                // in the cursor, when we evaluate a < or > expression. To avoid those edge cases
                // we explicitly add a condition that states that the ids we fetch need
                // to be different than the one passed in the cursor

                List<Condition> conditions = new ArrayList<>();
                conditions.add(row(getStringField(table, UUID)).ne(lastId.get()));
                if (paginationParams.isAscending()) {
                    conditions.add(row(getValueField(paginationParams, table),
                        getStringField(table, UUID)).ge(lastValue.get(), lastId.get()));
                    return Optional.of(conditions);
                } else {
                    conditions.add(row(getValueField(paginationParams, table),
                        getStringField(table, UUID)).le(lastValue.get(), lastId.get()));
                    return Optional.of(conditions);
                }
            } else {
                return Optional.empty();
            }
        }
    }
}
