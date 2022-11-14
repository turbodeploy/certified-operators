package com.vmturbo.history.stats;

import static com.vmturbo.common.protobuf.utils.StringConstants.PROPERTY_SUBTYPE_USED;
import static com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits.NUM_CPUS;
import static com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits.NUM_SOCKETS;
import static com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits.NUM_VCPUS;
import static com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits.PRODUCES;
import static com.vmturbo.history.utils.HistoryStatsUtils.countSEsMetrics;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableTable;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table.Cell;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Table;
import org.jooq.exception.DataAccessException;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.HistoricalValues;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.components.common.HistoryUtilizationType;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.bulk.BulkInserter;
import com.vmturbo.history.db.bulk.BulkLoader;
import com.vmturbo.history.db.bulk.SimpleBulkLoaderFactory;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.schema.abstraction.tables.HistUtilization;
import com.vmturbo.history.schema.abstraction.tables.MarketStatsLatest;
import com.vmturbo.history.schema.abstraction.tables.records.HistUtilizationRecord;
import com.vmturbo.history.schema.abstraction.tables.records.MarketStatsLatestRecord;
import com.vmturbo.history.stats.live.CommodityCache;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;

/**
 * Accumulate the stats for a given Entity Type organized by the stats property_name.
 **/
public class MarketStatsAccumulatorImpl implements MarketStatsAccumulator {

    private static final Logger logger = LogManager.getLogger();
    private static final long DEFAULT_VALUE_PROVIDER_ID = 0L;
    private static final String DEFAULT_VALUE_COMMODITY_KEY = "";
    private static final String NUMBER_FORMAT_EXCEPTION_LOG_MESSAGE =
                    "Value calculation for oid {} failed (provider id is {}). Skipping it. capacity = {}, commodity type ID = {}, commodity key = {}. {}";

    private final TopologyInfo topologyInfo;

    /**
     * DB entity type to which these stats belong. Determines the table stats are written to.
     */
    private final String entityType;

    /**
     * The environment type for this accumulator.
     *
     * <p>Note that the environment type currently only affects how the aggregate market stats
     * are saved - the individual entity records are saved without environment type information.</p>
     */
    private final EnvironmentType environmentType;

    /**
     * The Jooq table for stats of this entity type.
     */
    private final Table<Record> dbTable;

    /**
     * Providers of record loaders for our table.
     */
    private final SimpleBulkLoaderFactory loaders;

    /**
     * A records loader for our table.
     */
    private final BulkLoader<Record> loader;

    private final BulkLoader<HistUtilizationRecord> historicalUtilizationLoader;
    private final Set<String> longCommodityKeys;

    private int numEntitiesCount = 0;

    /**
     * A list of commodities that are to be excluded, i.e. not written to the DB.
     */
    private final Set<CommodityType> excludedCommodityTypes;


    /**
     * The provider for all the database- and table-specific methods. Based on Jooq and
     * does all the reading from and writing to the DB. ALso used for creating DB statements,
     * both query and insert, to be populated here and then executed.
     */
    private final HistorydbIO historydbIO;

    /**
     * map from a commodity key, constructed from (property type & subtype, relation)
     * to a Market Stats Data item for the given key.
     */
    private final Map<String, MarketStatsData> statsMap = new HashMap<>();


    /**
     * We are using function to get the {@link TypeSpecificInfo} attribute value here. The input is
     * the {@link TypeSpecificInfo} and we define what is the field we need to get in the function.
     * The return value is option of double as the persist value.
     */
    private static final Function<TypeSpecificInfo, Optional<Double>> NUM_CPU_CORES_FUNC =
            typeSpecificInfo -> typeSpecificInfo.hasPhysicalMachine()
                    && typeSpecificInfo.getPhysicalMachine().hasNumCpus()
                    ? Optional.of((double)typeSpecificInfo.getPhysicalMachine().getNumCpus())
                    : Optional.empty();
    private static final Function<TypeSpecificInfo, Optional<Double>> NUM_CPU_SOCKETS_FUNC =
            typeSpecificInfo -> typeSpecificInfo.hasPhysicalMachine()
                    && typeSpecificInfo.getPhysicalMachine().hasNumCpuSockets()
                    ? Optional.of((double)typeSpecificInfo.getPhysicalMachine().getNumCpuSockets())
                    : Optional.empty();
    private static final Function<TypeSpecificInfo, Optional<Double>> NUM_VCPU_FUNC =
            typeSpecificInfo -> typeSpecificInfo.hasVirtualMachine()
                    && typeSpecificInfo.getVirtualMachine().hasNumCpus()
                    ? Optional.of((double)typeSpecificInfo.getVirtualMachine().getNumCpus())
                    : Optional.empty();


    /**
     * This map lists properties of entities which are to be persisted as stats.
     * If an entity property with the given property key is found, the value of that property
     * is persisted as the corresponding {@link CommodityTypeUnits} using the mixedCase name.
     */
    private static final Map<Function<TypeSpecificInfo, Optional<Double>>, CommodityTypeUnits>
            PERSISTED_ATTRIBUTE_MAP = new ImmutableMap.Builder<Function<TypeSpecificInfo,
            Optional<Double>>, CommodityTypeUnits>()
            .put(NUM_CPU_CORES_FUNC, NUM_CPUS)
            .put(NUM_CPU_SOCKETS_FUNC, NUM_SOCKETS)
            .put(NUM_VCPU_FUNC, NUM_VCPUS)
            .build();

    /**
     * Set of commodities which were set to inactive, but we still want to persist them since they
     * are useful and we want to show them to user.
     */
    private static final Set<Integer> INACTIVE_COMMODITIES_TO_PERSIST = ImmutableSet.of(
            CommodityType.SWAPPING_VALUE, CommodityType.BALLOONING_VALUE, CommodityType.COOLING_VALUE,
            CommodityType.POWER_VALUE, CommodityType.NET_THROUGHPUT_VALUE,
            CommodityType.TOTAL_SESSIONS_VALUE, CommodityType.RESPONSE_TIME_VALUE,
            CommodityType.TRANSACTION_VALUE, CommodityType.NUMBER_REPLICAS_VALUE,
            CommodityType.STORAGE_ACCESS_VALUE, CommodityType.STORAGE_LATENCY_VALUE,
            CommodityType.STORAGE_AMOUNT_VALUE, CommodityType.STORAGE_PROVISIONED_VALUE,
            CommodityType.STORAGE_AMOUNT_VALUE, CommodityType.ENERGY_VALUE,
            CommodityType.PROCESSING_UNITS_PROVISIONED_VALUE
    );

    /**
     * Create an object to accumulate min / max / total / capacity over the commodities for a given
     * EntityType.
     *
     * @param topologyInfo           topology info
     * @param entityType             the type of entity for which these stats are being accumulated.
     *                               A given stat may be bought and sold be different entities. We
     *                               must record those usages separately.
     * @param environmentType        environment type
     * @param historydbIO            DBIO handler for the History tables
     * @param excludedCommodityTypes a list of commodity names used by the market but not necessary
     *                               to be persisted as stats in the db
     * @param loaders                {@link SimpleBulkLoaderFactory} from which needed {@link
     *                               BulkInserter} objects
     * @param longCommodityKeys      where to store commodity keys that had to be shortened, for
     *                               consolidated logging
     */
    public MarketStatsAccumulatorImpl(@Nonnull final TopologyInfo topologyInfo,
            @Nonnull final String entityType,
            @Nonnull final EnvironmentType environmentType,
            @Nonnull final HistorydbIO historydbIO,
            @Nonnull final Set<CommodityType> excludedCommodityTypes,
            @Nonnull final SimpleBulkLoaderFactory loaders,
            @Nonnull final Set<String> longCommodityKeys) {
        this.topologyInfo = topologyInfo;
        this.entityType = entityType;
        this.environmentType = environmentType;
        this.historydbIO = historydbIO;
        this.loaders = loaders;

        // the entity type determines which xxx_stats_yyy stats table these stats should go to
        dbTable = (Table<Record>)EntityType.named(entityType).flatMap(EntityType::getLatestTable).orElse(null);
        if (dbTable == null) {
            // can't find a table to write stats to, so we'll log and create a do-nothing instance
            throw new IllegalArgumentException(
                    "No _latest table for entity type, so can't save entity stats: " + entityType);
        }

        this.excludedCommodityTypes = excludedCommodityTypes;

        loader = loaders.getLoader(dbTable);
        historicalUtilizationLoader = loaders.getLoader(HistUtilization.HIST_UTILIZATION);
        this.longCommodityKeys = longCommodityKeys;
    }

    /**
     * Accumulate an entity count stat - the number of entities of the given type.
     *
     * @param countStatsName the name of the stat, i.e. the Entity Type being counted
     * @param count          the number of Service Entities of the given type
     */
    private void addEntityCountStat(String countStatsName, double count) {
        internalAddCommodity(countStatsName, countStatsName, count, count, count, count,
                RelationType.METRICS);
    }

    /**
     * Accumulate a stat given the property type, subtype, and relation type.
     *
     * @param propertyType      specific property type to record
     * @param propertySubtype   subtype of property to record
     * @param used              current amount of the commodity being bought
     * @param capacity          amount of the commodity the seller is providing
     * @param effectiveCapacity amount of the commodity the seller is providing
     * @param peak              (recent?) peak amount of the commodity being bought
     * @param relationType      type of commodity stat this is:  sold=0, bought=1,
     *                          entity attribute based=2)
     */
    private void internalAddCommodity(String propertyType,
            String propertySubtype,
            double used,
            Double capacity,
            Double effectiveCapacity,
            double peak,
            RelationType relationType) {
        String commodityKey = MessageFormat.format("{0}::{1}::{2}",
                propertyType, propertySubtype, relationType.getValue());

        synchronized (statsMap) {
            MarketStatsData statsData = statsMap.computeIfAbsent(commodityKey, key ->
                    new MarketStatsData(entityType, environmentType, propertyType, propertySubtype,
                            relationType));
            // accumulate the values from this stat item
            statsData.accumulate(used, peak, capacity, effectiveCapacity);
        }
    }

    /**
     * Access the accumulated {@link MarketStatsData} values.
     *
     * @return an unmodifiable collection of {@link MarketStatsData} accumulated here
     */
    @Override
    public Collection<MarketStatsData> values() {
        return Collections.unmodifiableCollection(statsMap.values());
    }

    /**
     * Call this for every entity of the entity type and environment type associated with the
     * {@link MarketStatsAccumulator}.
     *
     * @param entityDTO                The entity.
     * @param commodityCache            cached seller capacities for selling entities seen so far
     * @param delayedCommoditiesBought a map of (providerId) -> ({@link DelayedCommodityBoughtWriter}).
     *                                 This method may add entries to the map if the entity being
     *                                 processed is buying commodities from a provider that does
     *                                 not exist in the capacities input map.
     * @param entityByOid              mapping from oid of the entity to the entity object
     * @throws InterruptedException if interrupted
     */
    @Override
    public void recordEntity(@Nonnull final TopologyEntityDTO entityDTO,
            @Nonnull final CommodityCache commodityCache,
            @Nonnull final Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought,
            @Nonnull final Map<Long, TopologyEntityDTO> entityByOid) throws InterruptedException {
        persistCommoditiesBought(entityDTO, commodityCache,
                delayedCommoditiesBought, entityByOid);

        persistCommoditiesSold(entityDTO.getOid(),
                entityDTO.getCommoditySoldListList());

        persistEntityAttributes(entityDTO);

        numEntitiesCount++;
    }

    /**
     * Call this after all relevant entities in the topology have been recorded via calls to.
     *
     * @throws InterruptedException if interrupted
     */
    @Override
    public void writeFinalStats() throws InterruptedException {
        persistMarketStats();
    }

    /**
     * Persist the overall stats for an entity type within Live Market once the stats have
     * been aggregated here. Append the "counts" stats for a
     * selected set of Entity Types, e.g. "numVMs" = # of VMs in the topology.
     *
     * <p>Note: not batched.</p>
     *
     * @throws InterruptedException if interrupted
     */
    @VisibleForTesting
    void persistMarketStats() throws InterruptedException {

        // first add counts for the given entity, if applicable
        String countMetric = countSEsMetrics.get(entityType);
        if (countMetric != null) {
            addEntityCountStat(countMetric, numEntitiesCount);
        }

        // insert a record for each of our stats values
        final BulkLoader<MarketStatsLatestRecord> mktStatsWriter
                = loaders.getLoader(MarketStatsLatest.MARKET_STATS_LATEST);
        for (MarketStatsData data : values()) {
            MarketStatsLatestRecord marketStatsRecord
                    = historydbIO.getMarketStatsRecord(data, topologyInfo);
            mktStatsWriter.insert(marketStatsRecord);
        }
    }

    /**
     * Persist the SOLD commodities for a TopologyEntityDTO.
     *
     * <p>Note that the insertStmt parameter is re-used, and so is not closed here.
     * It must be closed by the caller.
     *
     * @param entityId          the OID of the entity which is selling these commodities
     * @param commoditySoldList a list of CommoditySoldDTO values to be persisted
     * @throws InterruptedException if interrupted
     */
    @VisibleForTesting
    void persistCommoditiesSold(final long entityId,
            @Nonnull final List<TopologyDTO.CommoditySoldDTO> commoditySoldList)
            throws InterruptedException {
        final long snapshotTime = topologyInfo.getCreationTime();
        for (TopologyDTO.CommoditySoldDTO commoditySoldDTO : commoditySoldList) {
            final int intCommodityType = commoditySoldDTO.getCommodityType().getType();
            // do not persist commodity if it is not active, but we want to persist some special
            // inactive commodities like Swapping and Ballooning
            if (!commoditySoldDTO.getActive()
                    && !INACTIVE_COMMODITIES_TO_PERSIST.contains(intCommodityType)) {
                logger.debug("Skipping inactive sold commodity type {}", intCommodityType);
                continue;
            }

            String mixedCaseCommodityName = HistoryStatsUtils.formatCommodityName(intCommodityType);
            if (mixedCaseCommodityName == null) {
                logger.warn("Skipping commodity sold type {}", intCommodityType);
                continue;
            }
            // filter out Commodities, such as Access Commodities, that shouldn't be persisted
            if (isExcludedCommodity(CommodityType.forNumber(intCommodityType))) {
                continue;
            }
            double capacity = commoditySoldDTO.getCapacity();
            // all "used" subtype entries should have a capacity
            if (!commoditySoldDTO.hasCapacity() || capacity <= 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Skipping sold commodity with unset capacity {}:{}:{}",
                                    entityId, commoditySoldDTO.getCommodityType().getType(),
                                    commoditySoldDTO.getCommodityType().getKey());
                }
                continue;
            }

            // if we have a non-null capacity and an effective capacity %, calculate effective capacity
            // otherwise set it to capacity.
            Double effectiveCapacity
                    = (commoditySoldDTO.hasEffectiveCapacityPercentage())
                    ? (commoditySoldDTO.getEffectiveCapacityPercentage() / 100.0 * capacity)
                    : capacity;
            Record record = dbTable.newRecord();
            final String key = commoditySoldDTO.getCommodityType().getKey();
            historydbIO.initializeCommodityRecord(mixedCaseCommodityName, snapshotTime,
                    entityId, RelationType.COMMODITIES, /*providerId*/null, capacity,
                    effectiveCapacity, key, record,
                    dbTable, longCommodityKeys);
            // set the values specific to used component of commodity and write
            historydbIO.setCommodityValues(PROPERTY_SUBTYPE_USED, commoditySoldDTO.getUsed(),
                    commoditySoldDTO.getPeak(), record, dbTable);
            // mark the end of this row of values
            loader.insert(record);

            // aggregate this stats value as part of the Market-wide stats
            internalAddCommodity(mixedCaseCommodityName, PROPERTY_SUBTYPE_USED,
                    commoditySoldDTO.getUsed(), capacity, effectiveCapacity, commoditySoldDTO.getPeak(),
                    RelationType.COMMODITIES);

            if (commoditySoldDTO.hasHistoricalUsed()) {
                createPercentileAndTimeslotsQueries(intCommodityType, entityId, null,
                        key, capacity, commoditySoldDTO.getHistoricalUsed());
            }
        }
    }

    private void createPercentileAndTimeslotsQueries(int commodityTypeId, long entityId,
            Long providerId, String commodityKey, double capacity,
            HistoricalValues historicalUsed) throws InterruptedException {
        ImmutableTable.Builder<HistoryUtilizationType, Integer, Double> tableBuilder =
                ImmutableTable.builder();
        try {
            for (int i = 0; i < historicalUsed.getTimeSlotCount(); i++) {
                final double timeSlot = historicalUsed.getTimeSlot(i);
                tableBuilder.put(HistoryUtilizationType.Timeslot, i, timeSlot / capacity);
            }
        } catch (NumberFormatException e) {
            logger.warn("Value calculation for oid {} failed (provider id is {}). Skipping it. capacity = {}, commodity type ID = {}, commodity key = {}. {}",
                        entityId,
                        providerId,
                        capacity,
                        commodityTypeId,
                        commodityKey,
                        e);
            // Discard any written timeslots in case if one of them is invalid
            tableBuilder = ImmutableTable.builder();
        }

        if (historicalUsed.hasPercentile()) {
            try {
                final double percentile = historicalUsed.getPercentile();
                tableBuilder.put(HistoryUtilizationType.Percentile, 0, percentile);
            } catch (NumberFormatException e) {
                logger.warn(NUMBER_FORMAT_EXCEPTION_LOG_MESSAGE,
                            entityId,
                            providerId,
                            capacity,
                            commodityTypeId,
                            commodityKey,
                            e);
            }
        }

        // TODO: store smoothed utilization into the hist_utilization table.
        formInsertOrUpdateQueries(entityId,
                providerId,
                commodityKey,
                capacity,
                commodityTypeId,
                tableBuilder.build().cellSet());
    }

    private void formInsertOrUpdateQueries(long entityId, @Nullable Long providerId,
            @Nullable String commodityKey, double capacity, int commodityTypeId,
            @Nonnull Collection<Cell<HistoryUtilizationType, Integer, Double>> cells)
            throws InterruptedException {
        final Long providerIdValue = providerId == null ? DEFAULT_VALUE_PROVIDER_ID : providerId;
        final String commodityKeyValue =
                commodityKey == null ? DEFAULT_VALUE_COMMODITY_KEY : commodityKey;
        for (Cell<HistoryUtilizationType, Integer, Double> cell : cells) {
            historicalUtilizationLoader.insert(
                    new HistUtilizationRecord(entityId, providerIdValue, commodityTypeId,
                            PropertySubType.Utilization.ordinal(), commodityKeyValue,
                            cell.getRowKey().ordinal(), cell.getColumnKey(), cell.getValue(),
                            capacity));
        }
    }

    /**
     * Whether this commodity name is to be excluded, i.e. not written to the DB.
     * For example, Access Commodities.
     *
     * <p>The comparison is not case-sensitive. The instance property 'commoditiesToExclude'
     * is converted to lower case in the constructor for this class.</p>
     *
     * @param commodityType the commodity name to test
     * @return whether this commodity should be excluded, i.e. not written to the DB
     */
    private boolean isExcludedCommodity(CommodityType commodityType) {
        return excludedCommodityTypes.contains(commodityType);
    }


    /**
     * Persist the commodities bought by an entity, one row per commodity.
     * If the provider of a set of commodities is not available yet (because it shows up later
     * in the incoming topology message) then delay the insertion of that set of commodities.
     *
     * @param entityDTO                the buyer DTO
     * @param commodityCache            capacity data for seller entities seen so far
     * @param delayedCommoditiesBought a map of commodities-bought where seller entity is not yet known
     * @param entityByOid              mapping from oid of the entity to the entity object
     * @throws InterruptedException if interrupted
     */
    @VisibleForTesting
    void persistCommoditiesBought(
            @Nonnull final TopologyDTO.TopologyEntityDTO entityDTO,
            @Nonnull final CommodityCache commodityCache,
            @Nonnull final Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought,
            @Nonnull final Map<Long, TopologyEntityDTO> entityByOid) throws InterruptedException {
        for (CommoditiesBoughtFromProvider commodityBoughtGrouping : entityDTO.getCommoditiesBoughtFromProvidersList()) {
            Long providerId = commodityBoughtGrouping.hasProviderId()
                    ? commodityBoughtGrouping.getProviderId() : null;
            DelayedCommodityBoughtWriter queueCommoditiesBlock = new DelayedCommodityBoughtWriter(
                    topologyInfo.getCreationTime(),
                    entityDTO, providerId, commodityBoughtGrouping,
                    commodityCache, entityByOid);

            if (providerId != null && !commodityCache.hasEntityCapacities(providerId)) {
                delayedCommoditiesBought.put(providerId, queueCommoditiesBlock);
            } else {
                queueCommoditiesBlock.queCommoditiesNow();
            }
        }
    }

    /**
     * Class to hold bought commodities pending availability of the seller entity.
     *
     * <p>A class was required, instead of a block, as queueCommoditiesBought() throws
     * {@link DataAccessException}.</p>
     */
    public class DelayedCommodityBoughtWriter {
        private final long snapshotTime;
        private final TopologyEntityDTO entityDTO;
        private final Long providerId;
        private final CommoditiesBoughtFromProvider commoditiesBought;
        private final CommodityCache commodityCache;
        private final Map<Long, TopologyEntityDTO> entityByOid;

        /**
         * Create a new instance.
         *
         * @param snapshotTime      snapshot time
         * @param entityDTO         entity
         * @param providerId        providing entity id
         * @param commoditiesBought commodities bought from provider
         * @param commodityCache     cache of capacity info for selling entities seen so far
         * @param entityByOid       map of entities by oid
         */
        public DelayedCommodityBoughtWriter(final long snapshotTime,
                @Nonnull final TopologyEntityDTO entityDTO,
                @Nullable final Long providerId,
                @Nonnull final CommoditiesBoughtFromProvider commoditiesBought,
                @Nonnull final CommodityCache commodityCache,
                @Nonnull final Map<Long, TopologyEntityDTO> entityByOid) {
            this.snapshotTime = snapshotTime;
            this.entityDTO = entityDTO;
            this.providerId = providerId;
            this.commoditiesBought = commoditiesBought;
            this.commodityCache = commodityCache;
            this.entityByOid = entityByOid;
        }

        /**
         * Performed the delayed processing now that seller data is available.
         *
         * @throws InterruptedException if interrupted
         */
        public void queCommoditiesNow() throws InterruptedException {
            queueCommoditiesBought(snapshotTime,
                    entityDTO, providerId, commoditiesBought, commodityCache, entityByOid);
        }
    }


    /**
     * Persist selected attributes of the given entity as stats.
     * <ol>
     * <li>Persist the "Produces" stat, simply a count of the number of commodities sold.
     * <li>Persist the properties listed in PERSISTED_ATTRIBUTE_MAPas stats, e.g. NUM_CPUS,
     * NUM_SOCKETS
     * </ol>
     *
     * <p>Note that the insertStmt parameter is re-used, and so is not closed here.
     * It must be closed by the caller.
     *
     * @param entityDTO the entity for which the attributes should be persisted
     * @throws InterruptedException if interrupted
     */
    @VisibleForTesting
    void persistEntityAttributes(@Nonnull final TopologyDTO.TopologyEntityDTO entityDTO)
            throws InterruptedException {

        long entityId = entityDTO.getOid();

        // persist "Produces" == # of commodities sold
        persistEntityAttribute(entityId, PRODUCES.getMixedCase(),
                entityDTO.getCommoditySoldListCount(), loader, dbTable);

        // scan entity attributes for specific attributes to persist as commodities
        for (Map.Entry<Function<TypeSpecificInfo, Optional<Double>>, CommodityTypeUnits>
                persistedAttributesMapEntry : PERSISTED_ATTRIBUTE_MAP.entrySet()) {
            final Function<TypeSpecificInfo, Optional<Double>> func =
                    persistedAttributesMapEntry.getKey();
            final CommodityTypeUnits commodityTypeUnits = persistedAttributesMapEntry.getValue();
            try {
                final Optional<Double> floatValueOpt = func.apply(entityDTO.getTypeSpecificInfo());
                if (!floatValueOpt.isPresent()) {
                    continue;
                }
                final String commodityType = commodityTypeUnits.getMixedCase();
                persistEntityAttribute(entityId, commodityTypeUnits.getMixedCase(),
                        floatValueOpt.get(), loader, dbTable);
                internalAddCommodity(commodityType, commodityType, floatValueOpt.get(),
                        floatValueOpt.get(), floatValueOpt.get(), floatValueOpt.get(),
                        RelationType.METRICS);
            } catch (NumberFormatException e) {
                logger.warn("Error converting {} for {}",
                        commodityTypeUnits.getMixedCase(), entityDTO.getDisplayName());
            }
        }
    }

    /**
     * Persist a single entity attribute as a stat for the given entity and snapshotTime.
     *
     * <p>Note that the insertStmt parameter is re-used, and so is not closed here.
     * It must be closed by the caller.
     *
     * @param entityId               the OID of the entity to be persisted
     * @param mixedCaseCommodityName the name for this commodity
     * @param valueToPersist         the value of the commodity to be persisted
     * @param writer                 a {@link BulkLoader} to which records can be written
     * @param dbTable                the xxx_stats_latest table into which the values will be inserted
     * @throws InterruptedException if interrupted
     */
    private void persistEntityAttribute(long entityId,
            @Nonnull String mixedCaseCommodityName,
            double valueToPersist,
            @Nonnull BulkLoader<Record> writer,
            @Nonnull Table<?> dbTable) throws InterruptedException {
        Record record = dbTable.newRecord();
        // initialize the common values for this row
        historydbIO.initializeCommodityRecord(mixedCaseCommodityName, topologyInfo.getCreationTime(),
                entityId, RelationType.METRICS, null, null, null, null, record, dbTable, longCommodityKeys);
        // set the values specific to used component of commodity and write
        // since there is no peak value, that parameter is sent as 0
        historydbIO.setCommodityValues(mixedCaseCommodityName, valueToPersist, 0,
                record, dbTable);
        writer.insert(record);
    }

    /**
     * Append commodities bought to the current DB statement, and write to the DB when the number
     * of rows exceeds the writeTopologyChunkSize.
     *
     * @param snapshotTime      timestamp for the topology being persisted
     * @param entityDTO         the entity for which to queue commodity bought
     * @param providerId        the provider OID
     * @param commoditiesBought the commodity bought from provider
     * @param capacityCache     cached commodities for selling entities encountered so far
     * @param entityByOid       map of TopologyEntityDTO indexed by oid
     * @throws InterruptedException if interrupted
     */
    private void queueCommoditiesBought(long snapshotTime,
            @Nonnull TopologyEntityDTO entityDTO,
            @Nullable Long providerId,
            @Nonnull CommoditiesBoughtFromProvider commoditiesBought,
            @Nonnull CommodityCache capacityCache,
            @Nonnull Map<Long, TopologyEntityDTO> entityByOid)
            throws InterruptedException {
        for (CommodityBoughtDTO commodityBoughtDTO : commoditiesBought.getCommodityBoughtList()) {
            final int commType = commodityBoughtDTO.getCommodityType().getType();
            final String commKey = commodityBoughtDTO.getCommodityType().getKey();
            // do not persist commodity if it is not active, but we want to persist some special
            // inactive commodities like Swapping and Ballooning
            if (!commodityBoughtDTO.getActive()
                    && !INACTIVE_COMMODITIES_TO_PERSIST.contains(commType)) {
                logger.debug("Skipping inactive bought commodity type {}", commType);
                continue;
            }

            String mixedCaseCommodityName = HistoryStatsUtils.formatCommodityName(commType);
            if (mixedCaseCommodityName == null) {
                logger.warn("Skipping commodity bought type {} ", commType);
                continue;
            }
            // filter out Commodities, such as Access Commodities, that shouldn't be persisted
            if (isExcludedCommodity(CommodityType.forNumber(commType))) {
                continue;
            }
            Double capacity = null;
            if (providerId != null) {
                capacity = capacityCache.getCapacity(providerId, commodityBoughtDTO.getCommodityType());
            }
            // all "used" subtype entries should have a capacity
            if (capacity == null || capacity <= 0) {
                if (logger.isTraceEnabled()) {
                    logger.trace("Skipping bought commodity with unset capacity {}:{}:{}:{}",
                                    entityDTO.getOid(), providerId, commType, commKey);
                }
                continue;
            }

            // This conversion is very specific to the top most and bottom most cloud native
            // entities in the supply chain. The cloud native entities represent VCPU commodity in millicores
            // whereas entities above them (viz Application) or below them (viz VM) represent the same in Mhz.
            capacity =  TopologyDTOUtil.convertCapacity(entityDTO, commType, capacity);

            // set the values specific to each row and persist each
            double used = commodityBoughtDTO.getUsed();
            // get the peak to save it as max of used subtype
            double peak = commodityBoughtDTO.getPeak();

            final String key = commodityBoughtDTO.getCommodityType().getKey();

            Record record = dbTable.newRecord();
            historydbIO.initializeCommodityRecord(mixedCaseCommodityName, snapshotTime,
                entityDTO.getOid(), RelationType.COMMODITIESBOUGHT, providerId, capacity,
                capacity - commodityBoughtDTO.getReservedCapacity(),
                key, record, dbTable, longCommodityKeys);
            historydbIO.setCommodityValues(PROPERTY_SUBTYPE_USED,
                    used, peak, record, dbTable);
            // mark the end of this row to be inserted
            loader.insert(record);

            // aggregate this stats value as part of the Market-wide stats
            internalAddCommodity(mixedCaseCommodityName, PROPERTY_SUBTYPE_USED,
                    commodityBoughtDTO.getUsed(), capacity, null, commodityBoughtDTO.getPeak(),
                    RelationType.COMMODITIESBOUGHT);

            if (commodityBoughtDTO.hasHistoricalUsed()) {
                createPercentileAndTimeslotsQueries(commType, entityDTO.getOid(),
                        providerId, key, capacity, commodityBoughtDTO.getHistoricalUsed());
            }
        }
    }

    /**
     * This class is for calculating the min / max / avg / capacity over a sequence
     * of values for a single entity type, property type, property subtype, and relationType.
     **/
    public static class MarketStatsData {

        private final String entityType;
        private final EnvironmentType environmentType;
        private final String propertyType;
        private final String propertySubtype;
        private final RelationType relationType;
        private double capacityTotal = 0.0;
        private double effectiveCapacityTotal = 0.0;
        private double usedTotal = 0.0;
        private double min = Double.MAX_VALUE;
        private double max;
        private int count = 0;

        /**
         * Create a new instance.
         *
         * @param entityType      entity type
         * @param environmentType environment type
         * @param propertyType    property type
         * @param propertySubtype property subtype
         * @param relationType    relation type
         */
        public MarketStatsData(@Nonnull final String entityType,
                @Nonnull final EnvironmentType environmentType,
                @Nonnull final String propertyType,
                @Nonnull final String propertySubtype,
                @Nonnull final RelationType relationType) {
            this.entityType = entityType;
            this.environmentType = environmentType;
            this.propertyType = propertyType;
            this.propertySubtype = propertySubtype;
            this.relationType = relationType;
        }

        /**
         * Track the values for this stat. Calculate the total and count for "used" and "capacity"
         * so that we can return an average later. Also track min(used) and max(peak).
         *
         * @param used              'current' value of this commodity used for the entity being tabulated,
         *                          for this snapshot
         * @param peak              peak value for this commodity for the entity being tabulated
         *                          for this snapshot
         * @param capacity          capacity for this commodity for the entity being tabulated
         *                          for this snapshot
         * @param effectiveCapacity the effective capacity for this commodity for the entity being
         *                          tabulated for this snapshot
         */
        public void accumulate(double used, double peak, @Nullable Double capacity,
                @Nullable Double effectiveCapacity) {
            // track the count of values and the total used to give the avg total at the end
            count++;
            this.usedTotal += used;
            if (capacity != null) {
                this.capacityTotal += capacity;
                // effective capacity only makes sense in a context with capacity, so only updating
                // effective capacity when capacity was also provided.
                // Also, when effective capacity is null, then the effective capacity == capacity.
                effectiveCapacityTotal += (effectiveCapacity == null) ? capacity : effectiveCapacity;
            }
            this.min = Math.min(this.min, used);
            // ideally peak should be no less than used, but it's possible that peak is not set,
            // which is 0 by default, thus less than used. To be safe, we should choose the larger
            // of peak and used as max.
            this.max = Math.max(this.max, Math.max(peak, used));
        }

        /**
         * Return the average used based on the total capacity and count entities tabulated.
         *
         * @return the average used of this commodity spec.
         */
        public double getUsed() {
            return count > 0 ? usedTotal / count : 0D;
        }

        /**
         * Return the average capacity based on the total capacity and count entities tabulated.
         *
         * @return the average capacity of this commodity spec.
         */
        public Double getCapacity() {
            return count > 0 ? capacityTotal / count : 0D;
        }

        /**
         * Return the average effective capacity across the entities tabulated.
         *
         * @return the average effective capacity of this commodity spec.
         */
        public Double getEffectiveCapacity() {
            if (effectiveCapacityTotal == -1) {
                return -1D;
            }
            return count > 0 ? effectiveCapacityTotal / count : 0D;
        }

        public String getEntityType() {
            return entityType;
        }

        public EnvironmentType getEnvironmentType() {
            return environmentType;
        }

        public String getPropertyType() {
            return propertyType;
        }

        public String getPropertySubtype() {
            return propertySubtype;
        }

        public RelationType getRelationType() {
            return relationType;
        }

        public Double getMin() {
            return min;
        }

        public Double getMax() {
            return max;
        }
    }
}
