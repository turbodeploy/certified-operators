package com.vmturbo.history.stats;

import static com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits.NUM_CPUS;
import static com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits.NUM_SOCKETS;
import static com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits.NUM_VCPUS;
import static com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits.PRODUCES;
import static com.vmturbo.components.common.utils.StringConstants.PROPERTY_SUBTYPE_USED;
import static com.vmturbo.history.utils.HistoryStatsUtils.countSEsMetrics;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.InsertSetMoreStep;
import org.jooq.Query;
import org.jooq.Table;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.components.common.ClassicEnumMapper.CommodityTypeUnits;
import com.vmturbo.history.SharedMetrics;
import com.vmturbo.history.db.BasedbIO;
import com.vmturbo.history.db.EntityType;
import com.vmturbo.history.db.HistorydbIO;
import com.vmturbo.history.db.VmtDbException;
import com.vmturbo.history.schema.RelationType;
import com.vmturbo.history.utils.HistoryStatsUtils;
import com.vmturbo.proactivesupport.DataMetricCounter;

/**
 * Accumulate the stats for a given Entity Type organized by the stats property_name.
 **/
public class MarketStatsAccumulator {

    private static final Logger logger = LogManager.getLogger();

    private final TopologyInfo topologyInfo;

    /**
     *   DB entity type to which these stats belong. Determines the table stats are written to.
     */
    private final String entityType;

    /**
     * The environment type for this accumulator.
     *
     * Note that the environment type currently only affects how the aggregate market stats
     * are saved - the individual entity records are saved without environment type information.
     */
    private final EnvironmentType environmentType;

    /**
     * The Jooq table for stats of this entity type.
     */
    private final Table<?> dbTable;

    /**
     * How many stats rows to accumulate for each insert statement.
     */
    private final long writeTopologyChunkSize;

    /**
     * This is the DB insert statement that will be populated with data rows and executed.
     * It is created initialized for the correct table for this entity type.
     */
    private InsertSetMoreStep<?> insertStmt;

    private int queuedRows = 0;

    private int numEntitiesCount = 0;

    /**
     * A list of commodities that are to be excluded, i.e. not written to the DB.
     */
    private final ImmutableSet<String> commoditiesToExclude;


    /**
     * The provider for all the database- and table-specific methods. Based on Jooq and
     * does all the reading from and writing to the DB. ALso used for creating DB statments,
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
                ? Optional.of((double) typeSpecificInfo.getPhysicalMachine().getNumCpus())
                : Optional.empty();
    private static final Function<TypeSpecificInfo, Optional<Double>> NUM_CPU_SOCKETS_FUNC =
        typeSpecificInfo -> typeSpecificInfo.hasPhysicalMachine()
            && typeSpecificInfo.getPhysicalMachine().hasNumCpuSockets()
                ? Optional.of((double) typeSpecificInfo.getPhysicalMachine().getNumCpuSockets())
                : Optional.empty();
    private static final Function<TypeSpecificInfo, Optional<Double>> NUM_VCPU_FUNC =
        typeSpecificInfo -> typeSpecificInfo.hasVirtualMachine()
            && typeSpecificInfo.getVirtualMachine().hasNumCpus()
                ? Optional.of((double) typeSpecificInfo.getVirtualMachine().getNumCpus())
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
     * Create an object to accumulate min / max / total / capacity over the commodities for
     * a given EntityType.
     *  @param entityType the type of entity for which these stats are being accumulated. A given
     *                    stat may be bought and sold be different entities. We must record those
     *                    usages separately.
     * @param historydbIO DBIO handler for the History tables
     * @param writeTopologyChunkSize the number of stats rows to group together into a single
     *                               JOOQ Insert statement
     * @param commoditiesToExclude a list of commodity names used by the market but not necessary
     *                             to be persisted as stats in the db
     */
    public MarketStatsAccumulator(@Nonnull final TopologyInfo topologyInfo,
                                  @Nonnull final String entityType,
                                  @Nonnull final EnvironmentType environmentType,
                                  @Nonnull final HistorydbIO historydbIO,
                                  final long writeTopologyChunkSize,
                                  @Nonnull final ImmutableList<String> commoditiesToExclude) {
        this.topologyInfo = topologyInfo;
        this.entityType = entityType;
        this.environmentType = environmentType;
        this.historydbIO = historydbIO;
        this.writeTopologyChunkSize = writeTopologyChunkSize;

        final ImmutableSet.Builder<String> builder = ImmutableSet.builder();
        commoditiesToExclude.stream()
                .map(String::toLowerCase)
                .forEach(builder::add);
        this.commoditiesToExclude = builder.build();

        // the entity type determines which xxx_stats_yyy stats table these stats should go to
        dbTable = EntityType.get(entityType).getLatestTable();
        if (dbTable == null) {
            // should only be called if this entity type is persisted to _latest table
            throw new RuntimeException("Cannot accumulate stats for entity type: " + entityType);
        }
        createStatsInsertStatement();
    }

    /**
     * Accumulate an entity count stat - the number of entities of the given type.
     *
     * @param countStatsName the name of the stat, i.e. the Entity Type being counted
     * @param count the number of Service Entitis of the given type
     */
    private void addEntityCountStat(String countStatsName, double count) {
        internalAddCommodity(countStatsName, countStatsName, count, count, count, count,
                RelationType.METRICS);
    }

    /**
     * Accumulate a stat given the property type, subtype, and relation type.
     *
     * @param propertyType specificy property type to record
     * @param propertySubtype subtype of property to record
     * @param used current amount of the commodity being bought
     * @param capacity amount of the commodity the seller is providing
     * @param effectiveCapacity amount of the commodity the seller is providing
     * @param peak (recent?) peak amount of the commodity being bought
     * @param relationType type of commodity stat this is:  sold=0, bought=1,
     *                     entity attribute based=2)
     */
    private void internalAddCommodity(String propertyType,
                                      String propertySubtype,
                                      double used,
                                      Double capacity,
                                      Double effectiveCapacity,
                                      double peak,
                                      RelationType relationType) {
        String commodityKey = MessageFormat.format("{0}::{1}::{2}",
                propertyType, propertySubtype, RelationType.METRICS.getValue());

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
    public Collection<MarketStatsData> values() {
        return Collections.unmodifiableCollection(statsMap.values());
    }

    /**
     * Call this for every entity of the entity type and environment type associated with the
     * {@link MarketStatsAccumulator}.
     *
     * @param entityDTO The entity.
     * @param capacities (seller entityId) -> (commodity type) -> (capacity)
     * @param delayedCommoditiesBought a map of (providerId) -> ({@link DelayedCommodityBoughtWriter}).
     *                                 This method may add entries to the map if the entity being
     *                                 processed is buying commodities from a provider that does
     *                                 not exist in the capacities input map.
     * @param entityByOid mapping from oid of the entity to the entity object
     * @throws VmtDbException If there is an error interacting with the database.
     */
    public void recordEntity(@Nonnull final TopologyEntityDTO entityDTO,
                             @Nonnull final Map<Long, Map<Integer, Double>> capacities,
                             @Nonnull final Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought,
                             @Nonnull final Map<Long, TopologyEntityDTO> entityByOid) throws VmtDbException {
        persistCommoditiesBought(entityDTO, capacities,
            delayedCommoditiesBought, entityByOid);

        persistCommoditiesSold(entityDTO.getOid(),
            entityDTO.getCommoditySoldListList());

        persistEntityAttributes(entityDTO);

        numEntitiesCount++;
    }

    /**
     * Call this after all relevant entities in the topology have been recorded via calls
     * to {@link MarketStatsAccumulator#recordEntity(TopologyEntityDTO, Map, Multimap, Map)}.
     *
     * @throws VmtDbException If there is an error interacting with the database.
     */
    public void writeFinalStats() throws VmtDbException {
        writeQueuedRows();
        persistMarketStats();
    }

    /**
     * Persist the overall stats for an entity type within Live Market once the stats have
     * been aggregated here. Append the "counts" stats for a
     * selected set of Entity Types, e.g. "numVMs" = # of VMs in the topology.
     *
     * Note: not batched.
     *
     * @throws VmtDbException if there's a DB error writing the market_stats_latest table.
     */
    @VisibleForTesting
    void persistMarketStats()
            throws VmtDbException {

        // first add counts for the given entity, if applicable
        String countMetric = countSEsMetrics.get(entityType);
        if (countMetric != null) {
            addEntityCountStat(countMetric, numEntitiesCount);
        }

        // create a list of "insert" statements, one for each stat value.
        List<Query> insertStmts = values().stream()
                .map(marketStatsData -> historydbIO.getMarketStatsInsertStmt(marketStatsData,
                        topologyInfo))
                .collect(Collectors.toList());

        historydbIO.execute(BasedbIO.Style.FORCED, insertStmts);
    }

    /**
     * Persist the SOLD commodities for a TopologyEntityDTO.
     *
     * <p>Note that the insertStmt parameter is re-used, and so is not closed here.
     * It must be closed by the caller.
     * @param entityId the OID of the entity which is selling these commodities
     * @param commoditySoldList a list of CommoditySoldDTO values to be persisted
     */
    @VisibleForTesting
    void persistCommoditiesSold(final long entityId,
                                @Nonnull final List<TopologyDTO.CommoditySoldDTO> commoditySoldList)
            throws VmtDbException {
        for (TopologyDTO.CommoditySoldDTO commoditySoldDTO : commoditySoldList) {
            // do not persist commodity if it is not active
            if (!commoditySoldDTO.getActive()) {
                logger.debug("Skipping inactive sold commodity type {}",
                        () -> commoditySoldDTO.getCommodityType().getType());
                continue;
            }

            final int intCommodityType = commoditySoldDTO.getCommodityType().getType();

            Double capacity = adjustCapacity(commoditySoldDTO.getCapacity());
            String mixedCaseCommodityName = HistoryStatsUtils.formatCommodityName(intCommodityType);
            if (mixedCaseCommodityName == null) {
                logger.warn("Skipping commodity sold type {}",
                        commoditySoldDTO.getCommodityType().getType());
                continue;
            }
            // filter out Commodities, such as Access Commodities, that shouldn't be persisted
            if (isExcludedCommodity(mixedCaseCommodityName)) {
                continue;
            }

            // if we have a non-null capacity and an effective capacity %, calculate effective capacity
            // otherwise set it to capacity.
            Double effectiveCapacity
                    = (commoditySoldDTO.hasEffectiveCapacityPercentage() && (capacity != null))
                    ? (commoditySoldDTO.getEffectiveCapacityPercentage() / 100.0 * capacity)
                    : capacity;
            historydbIO.initializeCommodityInsert(mixedCaseCommodityName, topologyInfo.getCreationTime(),
                    entityId, RelationType.COMMODITIES, /*providerId*/null, capacity,
                    effectiveCapacity, commoditySoldDTO.getCommodityType().getKey(), insertStmt,
                    dbTable);
            // set the values specific to used component of commodity and write
            historydbIO.setCommodityValues(PROPERTY_SUBTYPE_USED, commoditySoldDTO.getUsed(),
                    insertStmt, dbTable);
            // mark the end of this row of values
            markRowComplete();

            // aggregate this stats value as part of the Market-wide stats
            internalAddCommodity(mixedCaseCommodityName, PROPERTY_SUBTYPE_USED,
                    commoditySoldDTO.getUsed(), capacity, effectiveCapacity, commoditySoldDTO.getPeak(),
                    RelationType.COMMODITIES);
        }
    }

    /**
     * Whether this commodity name is to be excluded, i.e. not written to the DB.
     * For example, Access Commodities.
     * <p>
     * The comparison is not case-sensitive. The instance property 'commoditiesToExclude'
     * is converted to lower case in the constructor for this class.
     *
     * @param mixedCaseCommodityName the commodity name to test
     * @return whether this commodity should be excluded, i.e. not written to the DB
     */
    private boolean isExcludedCommodity(String mixedCaseCommodityName) {
        return commoditiesToExclude.contains(mixedCaseCommodityName.toLowerCase());
    }



    /**
     * Persist the commodities bought by an entity, one row per commodity.
     * If the provider of a set of commodities is not available yet (because it shows up later
     * in the incoming topology message) then delay the insertion of that set of commodities.
     *
     * @param entityDTO the buyer DTO
     * @param capacities map from seller entityId and commodity type to capacity
     * @param delayedCommoditiesBought a map of commodities-bought where seller entity is not yet known
     * @param entityByOid mapping from oid of the entity to the entity object
     * @throws VmtDbException when cannot insert to the DB
     */
    @VisibleForTesting
    void persistCommoditiesBought(
            @Nonnull final TopologyDTO.TopologyEntityDTO entityDTO,
            @Nonnull final Map<Long, Map<Integer, Double>> capacities,
            @Nonnull final Multimap<Long, DelayedCommodityBoughtWriter> delayedCommoditiesBought,
            @Nonnull final Map<Long, TopologyEntityDTO> entityByOid) throws VmtDbException {
        for (CommoditiesBoughtFromProvider commodityBoughtGrouping : entityDTO.getCommoditiesBoughtFromProvidersList()) {
            Long providerId = commodityBoughtGrouping.hasProviderId() ?
                    commodityBoughtGrouping.getProviderId() : null;
            DelayedCommodityBoughtWriter queueCommoditiesBlock = new DelayedCommodityBoughtWriter(
                    topologyInfo.getCreationTime(),
                    entityDTO.getOid(), providerId, commodityBoughtGrouping,
                    capacities, entityByOid);

            if (providerId != null && capacities.get(providerId) == null) {
                delayedCommoditiesBought.put(providerId, queueCommoditiesBlock);
            } else {
                queueCommoditiesBlock.queCommoditiesNow();
            }
        }
    }

    /**
     * Class to hold bought commodities pending availability of the seller entity.
     *
     * A class was required, instead of a block, as queueCommoditiesBought() throws VmtDbException.
     */
    public class DelayedCommodityBoughtWriter {
        private final long snapshotTime;
        private final long entityOid;
        private final Long providerId;
        private final CommoditiesBoughtFromProvider commoditiesBought;
        private final Map<Long, Map<Integer, Double>> capacities;
        private final Map<Long, TopologyEntityDTO> entityByOid;

        public DelayedCommodityBoughtWriter(final long snapshotTime,
                                    final long entityOid,
                                    @Nullable final Long providerId,
                                    @Nonnull final CommoditiesBoughtFromProvider commoditiesBought,
                                    @Nonnull final Map<Long, Map<Integer, Double>> capacities,
                                    @Nonnull final Map<Long, TopologyEntityDTO> entityByOid) {
            this.snapshotTime = snapshotTime;
            this.entityOid = entityOid;
            this.providerId = providerId;
            this.commoditiesBought = commoditiesBought;
            this.capacities = capacities;
            this.entityByOid = entityByOid;
        }

        public void queCommoditiesNow() throws VmtDbException {
            queueCommoditiesBought(snapshotTime,
                entityOid, providerId, commoditiesBought, capacities, entityByOid);
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
     * @param entityDTO the entity for which the attributes should be persisted
     */
    @VisibleForTesting
    void persistEntityAttributes(@Nonnull final TopologyDTO.TopologyEntityDTO entityDTO)
            throws VmtDbException {

        long entityId = entityDTO.getOid();

        // persist "Produces" == # of commodities sold
        persistEntityAttribute(entityId, PRODUCES.getMixedCase(),
                entityDTO.getCommoditySoldListCount(), insertStmt, dbTable);

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
                    floatValueOpt.get(), insertStmt, dbTable);
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
     * @param entityId the OID of the entity to be persisted
     * @param mixedCaseCommodityName the name for this commodity
     * @param valueToPersist the value of the commodity to be persisted
     * @param insertStmt a {@link InsertSetMoreStep} pre-populated with common values for rows
     * @param dbTable the xxx_stats_latest table into which the values will be inserted
     */
    private void persistEntityAttribute(long entityId,
                                        @Nonnull String mixedCaseCommodityName,
                                        double valueToPersist,
                                        @Nonnull InsertSetMoreStep<?> insertStmt,
                                        @Nonnull Table<?> dbTable) throws VmtDbException {
        // initialize the common values for this row
        historydbIO.initializeCommodityInsert(mixedCaseCommodityName, topologyInfo.getCreationTime(),
            entityId, RelationType.METRICS, null, null, null, null, insertStmt, dbTable);
        // set the values specific to used component of commodity and write
        historydbIO.setCommodityValues(mixedCaseCommodityName, valueToPersist, insertStmt, dbTable);
        // mark the row complete
        markRowComplete();
    }

    /**
     * Append commodities bought to the current DB statement, and write to the DB when the number
     * of rows exceeds the writeTopologyChunkSize.
     *
     * @param snapshotTime timestamp for the topology being persisted
     * @param buyerId the buyer OID
     * @param providerId the provider OID
     * @param commoditiesBought the commodity bought from provider
     * @param capacities a map seller ID -> (map commodity type -> capacity for that commodity)
     * @param entityByOid map of TopologyEntityDTO indexed by oid
     */
    private void queueCommoditiesBought(long snapshotTime, Long buyerId, @Nullable Long providerId,
            CommoditiesBoughtFromProvider commoditiesBought, Map<Long, Map<Integer, Double>> capacities,
            Map<Long, TopologyEntityDTO> entityByOid) throws VmtDbException {
        for (CommodityBoughtDTO commodityBoughtDTO : commoditiesBought.getCommodityBoughtList()) {
            // do not persist commodity if it is not active
            if (!commodityBoughtDTO.getActive()) {
                logger.debug("Skipping inactive bought commodity type {}",
                        () -> commodityBoughtDTO.getCommodityType().getType());
                continue;
            }

            final int commType = commodityBoughtDTO.getCommodityType().getType();
            String mixedCaseCommodityName = HistoryStatsUtils.formatCommodityName(commType);
            if (mixedCaseCommodityName == null) {
                logger.warn("Skipping commodity bought type {} ",
                        commodityBoughtDTO.getCommodityType().getType());
                continue;
            }
            // filter out Commodities, such as Access Commodities, that shouldn't be persisted
            if (isExcludedCommodity(mixedCaseCommodityName)) {
                continue;
            }
            // set default value to -1, it will be adjust to null when commodity bought has no provider id.
            Double capacity = -1.0;
            if (providerId != null) {
                final Map<Integer, Double> soldCapacities;
                if (commoditiesBought.hasVolumeId()) {
                    // use capacity from volume if it is available
                    soldCapacities = capacities.get(commoditiesBought.getVolumeId());
                } else {
                    soldCapacities = capacities.get(providerId);
                }
                if (soldCapacities == null || !soldCapacities.containsKey(commType)) {
                    logger.warn("Missing commodity sold {} of entity {}, seller entity {}",
                            mixedCaseCommodityName, buyerId, providerId);
                    continue;
                }
                capacity = soldCapacities.get(commType);
            }

            capacity = adjustCapacity(capacity);

            // set the values specific to each row and persist each
            double used = commodityBoughtDTO.getUsed();

            // for commodity bought with associated volume, use the volume display name as key
            // otherwise, use normal key from this bought commodity
            final String key;
            if (commoditiesBought.hasVolumeId() && entityByOid.containsKey(commoditiesBought.getVolumeId())) {
                key = entityByOid.get(commoditiesBought.getVolumeId()).getDisplayName();
            } else {
                key = commodityBoughtDTO.getCommodityType().getKey();
            }

            historydbIO.initializeCommodityInsert(mixedCaseCommodityName, snapshotTime,
                    buyerId, RelationType.COMMODITIESBOUGHT, providerId, capacity, null,
                    key, insertStmt, dbTable);
            historydbIO.setCommodityValues(PROPERTY_SUBTYPE_USED,
                    used, insertStmt, dbTable);
            // mark the end of this row to be inserted
            markRowComplete();

            // aggregate this stats value as part of the Market-wide stats
            internalAddCommodity(mixedCaseCommodityName, PROPERTY_SUBTYPE_USED,
                    commodityBoughtDTO.getUsed(), capacity, null, commodityBoughtDTO.getPeak(),
                    RelationType.COMMODITIESBOUGHT);
        }
    }

    /**
     * Called after a stats row has been scheduled. Check to see how many rows have been
     * added to the current insertStmnt. If greater than the threshold, execute the
     * insert and create a new one.
     */
    private void markRowComplete() throws VmtDbException {
        insertStmt.newRecord();
        queuedRows = queuedRows + 1;
        if (queuedRows >= writeTopologyChunkSize) {
            writeQueuedRows();
            createStatsInsertStatement();
            queuedRows = 0;
        }
    }

    @VisibleForTesting
    void writeQueuedRows() throws VmtDbException {
        if (queuedRows > 0) {
            // todo (ml) consider adding an accumulating metric for the total time taken for db io
            historydbIO.execute(BasedbIO.Style.FORCED, insertStmt);
            // count the number of batches of insert statements executed
            Metrics.TOPOLOGY_INSERT_BATCH_COUNT
                    .labels(SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL,
                            SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                    .increment();
            // count the number of insert statements in this batch
            Metrics.TOPOLOGY_INSERT_COUNT
                    .labels(SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL,
                            SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                    .increment();
            // calculate the number of rows inserted, based on the number of values and values-per-row
            Metrics.TOPOLOGY_INSERT_ROW_COUNT
                    .labels(SharedMetrics.SOURCE_TOPOLOGY_TYPE_LABEL,
                            SharedMetrics.LIVE_CONTEXT_TYPE_LABEL)
                    .increment((double)queuedRows);

        }
    }

    private void createStatsInsertStatement() {
        // todo: create a prepared statement
        insertStmt = historydbIO.getCommodityInsertStatement(dbTable);
        queuedRows = 0;
    }

    /**
     * Apply the business rule where the capacity -1 should be replaced by null.
     *
     * See ReportingDatadbIO::addToBatch().
     *
     * @param capacity the capacity to check
     * @return null if the given capacity equals -1; else the given capacity itself
     */
    private Double adjustCapacity(double capacity) {
        return capacity == -1 ? null : capacity;
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
         * @param used 'current' value of this commodity used for the entity being tabulated,
         *             for this snapshot
         * @param peak peak value for this commodity for the entity being tabulated
         *             for this snapshot
         * @param capacity capacity for this commodity for the entity being tabulated
         *                 for this snapshot
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
            this.max = Math.max(this.max, peak);
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

    static class Metrics {
        /**
         * Counters for monitoring DB usage.
         */
        private static final DataMetricCounter TOPOLOGY_INSERT_BATCH_COUNT = DataMetricCounter.builder()
            .withName("history_topology_insert_batch_count")
            .withHelp("Number of batches of insert DB statements performed.")
            .withLabelNames("topology_type", "context_type")
            .build()
            .register();

        private static final DataMetricCounter TOPOLOGY_INSERT_COUNT = DataMetricCounter.builder()
            .withName("history_topology_insert_count")
            .withHelp("Number of insert DB statements performed.")
            .withLabelNames("topology_type", "context_type")
            .build()
            .register();

        private static final DataMetricCounter TOPOLOGY_INSERT_ROW_COUNT = DataMetricCounter.builder()
            .withName("history_topology_insert_rows")
            .withHelp("Number of stats rows inserted.")
            .withLabelNames("topology_type", "context_type")
            .build()
            .register();
    }
}
