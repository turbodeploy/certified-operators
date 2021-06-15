package com.vmturbo.extractor.topology;

import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.QX_VCPU_BASE_COEFFICIENT;
import static com.vmturbo.common.protobuf.utils.StringConstants.CPU_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.MEM_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.STORAGE_HEADROOM;
import static com.vmturbo.common.protobuf.utils.StringConstants.TOTAL_HEADROOM;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TYPE_AS_TYPE_ENUM;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_PATH;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_SIZE;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.IS_ATTACHED;
import static com.vmturbo.extractor.models.ModelDefinitions.METRIC_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.MODIFICATION_TIME;
import static com.vmturbo.extractor.models.ModelDefinitions.STORAGE_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.TIME;
import static com.vmturbo.extractor.models.ModelDefinitions.VOLUME_OID;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.function.DoubleBinaryOperator;
import java.util.function.Predicate;
import java.util.function.ToDoubleFunction;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.stats.Stats.StatSnapshot.StatRecord;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.DataPacks.DataPack;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.export.DataExtractionFactory;
import com.vmturbo.extractor.export.RelatedEntitiesExtractor;
import com.vmturbo.extractor.export.TargetsExtractor;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.HashedDataManager;
import com.vmturbo.extractor.models.HashedDataManager.CloseableConsumer;
import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.patchers.GroupPrimitiveFieldsOnGroupingPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher.PatchCase;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.FileType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.search.EnumUtils.CommodityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EntityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.GroupTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Writer that extracts entity and metric data from a topology and persists it to the database.
 */
public class EntityMetricWriter extends TopologyWriterBase {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The name to persist in db for ready queue commodity bought by VM.
     */
    public static final MetricType VM_QX_VCPU_NAME = MetricType.CPU_READY;

    /**
     * Matches ready queue commodity like: Q16_VCPU, QN_VCPU.
     */
    public static final Pattern QX_VCPU_PATTERN = Pattern.compile("Q.*_VCPU");

    private static final ImmutableMap<String, MetricType> propsToDbType = new Builder<String,
        MetricType>()
        .put(CPU_HEADROOM, MetricType.CPU_HEADROOM)
        .put(MEM_HEADROOM, MetricType.MEM_HEADROOM)
        .put(STORAGE_HEADROOM, MetricType.STORAGE_HEADROOM)
        .put(TOTAL_HEADROOM, MetricType.TOTAL_HEADROOM).build();

    // configurations for upsert and update operations for entity table
    private static final ImmutableList<Column<?>> upsertConflicts = ImmutableList.of(ENTITY_OID_AS_OID);
    private static final ImmutableList<Column<?>> upsertUpdates = ImmutableList.of(
            ModelDefinitions.ENTITY_NAME, ModelDefinitions.ENTITY_TYPE_AS_TYPE_ENUM, ModelDefinitions.ENVIRONMENT_TYPE_ENUM,
            ModelDefinitions.ATTRS, ModelDefinitions.LAST_SEEN);

    private final List<Record> entityRecords = new ArrayList<>();

    private final List<Record> metricRecords = new ArrayList<>();
    private final EntityHashManager entityHashManager;

    /**
     * List of wasted files records by storage oid.
     */
    private final Long2ObjectMap<List<Record>> filesByStorageId = new Long2ObjectOpenHashMap<>();
    private final PrimitiveFieldsOnTEDPatcher tedPatcher;
    private final GroupPrimitiveFieldsOnGroupingPatcher groupPatcher;
    private final DataPack<Long> oidPack;
    private final DataExtractionFactory dataExtractionFactory;
    private final ScopeManager scopeManager;
    private HashedDataManager fileTableManager;
    private DSLContext dsl;

    /**
     * Create a new writer instance.
     *
     * @param dbEndpoint            db endpoint for persisting data
     * @param entityHashManager     to track entity hash evolution across topology broadcasts
     * @param scopeManager          scope manager
     * @param fileDataManager       hashed table manager for file table
     * @param oidPack               entity id manager
     * @param pool                  thread pool
     * @param dataExtractionFactory the factory for providing extractors
     */
    public EntityMetricWriter(final DbEndpoint dbEndpoint, final EntityHashManager entityHashManager,
            final ScopeManager scopeManager, final HashedDataManager fileDataManager,
            final DataPack<Long> oidPack,
            final ExecutorService pool, final DataExtractionFactory dataExtractionFactory) {
        super(dbEndpoint, pool);
        this.entityHashManager = entityHashManager;
        this.scopeManager = scopeManager;
        this.fileTableManager = fileDataManager;
        this.oidPack = oidPack;
        this.dataExtractionFactory = dataExtractionFactory;
        TargetsExtractor targetsExtractor = dataExtractionFactory.newTargetsExtractor();
        this.tedPatcher = new PrimitiveFieldsOnTEDPatcher(PatchCase.REPORTING, targetsExtractor);
        this.groupPatcher = new GroupPrimitiveFieldsOnGroupingPatcher(PatchCase.REPORTING, targetsExtractor);
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(final TopologyInfo topologyInfo,
            final WriterConfig config, final MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
        super.startTopology(topologyInfo, config, timer);
        logger.info("Starting to process topology {}", topologyLabel);
        this.dsl = dbEndpoint.dslContext();
        return this::writeEntity;
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO e) {
        final EntityType entityType = EntityTypeUtils.protoIntToDb(e.getEntityType(), null);
        if (entityType == null) {
            logger.error("Cannot map entity type {} for db storage for entity oid {}; skipping",
                    e.getEntityType(), e.getOid());
            return;
        }
        final long oid = e.getOid();
        logger.debug("Capturing entity data for entity {}", oid);
        oidPack.toIndex(oid);
        Record entityRecord = new Record(ENTITY_TABLE);
        final HashMap<String, Object> attrs = new HashMap<>();
        PartialRecordInfo rec = new PartialRecordInfo(e.getOid(), e.getEntityType(), entityRecord, attrs);
        // populate record with data from entity dto per metadata rules
        tedPatcher.patch(rec, e);
        rec.finalizeAttrs();
        // supply chain will be added during finish processing
        entityRecords.add(entityRecord);
        writeMetrics(e, oid, entityType);
        // cache file records, since storage name can only be fetched later
        createFileRecords(e);
    }

    @Override
    public boolean requireFullSupplyChain() {
        return true;
    }

    /**
     * Record metric records for this entity, including records for bought and sold commodities.
     *
     * @param e   entity
     * @param oid entity oid
     * @param entityType type of the entity in db enum format
     */
    private void writeMetrics(final TopologyEntityDTO e, final long oid, final EntityType entityType) {
        logger.debug("Capturing metric data for entity {}", oid);
        // write bought commodity records
        e.getCommoditiesBoughtFromProvidersList().forEach(cbfp -> {
            final long producer = cbfp.getProviderId();
            final int producerType = cbfp.getProviderEntityType();
            // group by commodity type because we may need to aggregate some of the groups
            Map<Integer, List<CommodityBoughtDTO>> cbByType = cbfp.getCommodityBoughtList().stream()
                    .filter(cb -> config.reportingCommodityWhitelist().contains(cb.getCommodityType().getType()))
                    // we insist on a linked hashmap so we have predictable ordering of generated
                    // records. This solely to simplify some unit tests.
                    .collect(Collectors.groupingBy(cb -> cb.getCommodityType().getType(), LinkedHashMap::new, Collectors.toList()));
            cbByType.forEach((typeNo, cbs) -> {
                if (CommodityType.forNumber(typeNo) == null) {
                    logger.error("Skipping invalid bought commodity type {} for entity {}", typeNo, oid);
                } else if (isAggregateByKeys(typeNo, producerType)) {
                    recordAggregatedBoughtCommodity(oid, entityType, typeNo, cbs, producer);
                } else {
                    recordUnaggregatedBoughtCommodity(oid, entityType, typeNo, cbs, producer);
                }
            });
        });
        // write sold commodity records
        Map<Integer, List<CommoditySoldDTO>> csByType = e.getCommoditySoldListList().stream()
                .filter(cs -> config.reportingCommodityWhitelist().contains(cs.getCommodityType().getType()))
                .collect(Collectors.groupingBy(cs -> cs.getCommodityType().getType(),  LinkedHashMap::new, Collectors.toList()));
        csByType.forEach((typeNo, css) -> {
            MetricType type = CommodityTypeUtils.protoIntToDb(typeNo, null);
            if (CommodityType.forNumber(typeNo) == null) {
                logger.error("Skipping invalid sold commodity type {} for entity {}", typeNo, oid);
            } else if (isAggregateByKeys(typeNo, e.getEntityType())) {
                recordAggregatedSoldCommodity(oid, entityType, type, css);
            } else {
                recordUnaggregatedSoldCommodity(oid, entityType, type, css);
            }
        });
    }

    /**
     * Check whether we should be aggregating metrics across commodity key, for a given commodity
     * type and entity type.
     *
     * <p>N.B. When processing bought commodities, the passed entity type should be that of
     * the provider, not that of the consumer</p>
     *
     * @param commodityTypeNo commodity type
     * @param entityTypeNo    entity type
     * @return true if commodity should be aggregated across keys
     */
    private boolean isAggregateByKeys(int commodityTypeNo, int entityTypeNo) {
        final CommodityType commodityType = CommodityType.forNumber(commodityTypeNo);
        final EntityDTO.EntityType entityType = EntityDTO.EntityType.forNumber(entityTypeNo);
        return !config.unaggregatedCommodities().containsEntry(commodityType, entityType);
    }

    /**
     * Record a metric record for the given bought commodity structures, all of which are for the
     * same commodity type and bought from the same producer, with different commodity keys.
     *
     * <p>We aggregate the used metric across all the bought commodity structures.</p>
     * @param oid               consuming entity oid
     * @param entityType        type of the consuming entity
     * @param typeNo            commodity type
     * @param boughtCommodities bought commodity structures
     * @param producer          oid of producer entity
     */
    private void recordAggregatedBoughtCommodity(final long oid, final EntityType entityType,
            final Integer typeNo, final List<CommodityBoughtDTO> boughtCommodities, final long producer) {
        // sum across commodity keys in case same commodity type appears with multiple keys
        // and same provider
        final String type = CommodityType.forNumber(typeNo).name();
        final Double sumUsed = reduceCommodityCollection(boughtCommodities, CommodityBoughtDTO::hasUsed, CommodityBoughtDTO::getUsed, Double::sum);
        final Double sumUsedPeak = reduceCommodityCollection(boughtCommodities, CommodityBoughtDTO::hasPeak, CommodityBoughtDTO::getPeak, Double::sum);
        metricRecords.add(getBoughtCommodityRecord(oid, entityType, type, null, sumUsed, sumUsedPeak, producer));
    }

    /**
     * Record a metric record for each of the given bought commodity structures, all of which are
     * for the same commodity type and bought from the same producer, with different commodity
     * keys.
     *
     * @param oid               consuming entity oid
     * @param entityType        type of the consuming entity
     * @param typeNo            commodity type
     * @param boughtCommodities bought commodity structures
     * @param producer          oid of producer entity
     */
    private void recordUnaggregatedBoughtCommodity(final long oid, final EntityType entityType,
            final Integer typeNo, final List<CommodityBoughtDTO> boughtCommodities, final long producer) {
        // record individual records for this bought commodity
        final String type = CommodityType.forNumber(typeNo).name();
        boughtCommodities.stream()
                .map(cb -> getBoughtCommodityRecord(oid,
                        entityType,
                        type,
                        cb.getCommodityType().getKey(),
                        cb.hasUsed() ? cb.getUsed() : null,
                        cb.hasPeak() ? cb.getPeak() : null,
                        producer))
                .forEach(metricRecords::add);
    }

    /**
     * Create a new metric record for a bought commodity.
     *
     * @param oid      consuming entity oid
     * @param entityType type of the consuming entity
     * @param type     commodity type
     * @param key      commodity key
     * @param used     used metric
     * @param usedPeak peak of used metric
     * @param producer producer oid
     * @return new metric record
     */
    private Record getBoughtCommodityRecord(final long oid, final EntityType entityType,
                                            final String type, String key,
                                            @Nullable final Double used,
                                            @Nullable final Double usedPeak, final long producer) {
        Record r = new Record(ModelDefinitions.METRIC_TABLE);
        r.set(ModelDefinitions.ENTITY_OID, oid);
        if (QX_VCPU_PATTERN.matcher(type).matches()) {
            // special handling for Qx_VCPU: VM should only buy one of them, to help with
            // reports, we change the name to a single name "CPU_READY", and convert it
            // from bought to sold commodity with used and utilization, the capacity
            // is hardcoded to 20000 as defined in VC standard
            r.set(ModelDefinitions.COMMODITY_TYPE, VM_QX_VCPU_NAME);
            r.set(ModelDefinitions.COMMODITY_KEY, key);
            r.set(ModelDefinitions.COMMODITY_CAPACITY, QX_VCPU_BASE_COEFFICIENT);
            r.set(ModelDefinitions.COMMODITY_CURRENT, used);
            r.set(ModelDefinitions.COMMODITY_PEAK_CURRENT, usedPeak);
            r.set(ModelDefinitions.COMMODITY_UTILIZATION,
                  used == null ? null : used / QX_VCPU_BASE_COEFFICIENT);
        } else {
            r.set(ModelDefinitions.COMMODITY_TYPE, MetricType.valueOf(type));
            r.set(ModelDefinitions.COMMODITY_KEY, key);
            r.set(ModelDefinitions.COMMODITY_CONSUMED, used);
            r.set(ModelDefinitions.COMMODITY_PEAK_CONSUMED, usedPeak);
            r.set(ModelDefinitions.COMMODITY_PROVIDER, producer);
        }
        r.set(ModelDefinitions.ENTITY_TYPE_ENUM, entityType);
        return r;
    }

    /**
     * Record a metric record for the given sold commodity structures, all of which are for the same
     * commodity type, with different commodity keys.
     *
     * <p>We aggregate the used and capacity metrics across all the sold commodity structures.</p>
     * @param oid            selling entity oid
     * @param entityType      type of the selling entity
     * @param type            commodity type
     * @param soldCommodities sold commodity structures
     */
    private void recordAggregatedSoldCommodity(final long oid, EntityType entityType, final MetricType type,
            final List<CommoditySoldDTO> soldCommodities) {
        // sum across commodity keys in case same commodity type appears with multiple keys
        final Double sumUsed = reduceCommodityCollection(soldCommodities, CommoditySoldDTO::hasUsed, CommoditySoldDTO::getUsed, Double::sum);
        final Double sumUsedPeak = reduceCommodityCollection(soldCommodities, CommoditySoldDTO::hasPeak, CommoditySoldDTO::getPeak, Double::sum);
        final Double sumCap = reduceCommodityCollection(soldCommodities, CommoditySoldDTO::hasCapacity, CommoditySoldDTO::getCapacity, Double::sum);
        metricRecords.add(getSoldCommodityRecord(oid, entityType, type.name(), null, sumUsed, sumUsedPeak, sumCap));
    }

    /**
     * Helper method to reduce commodity list when values available.
     * @param commodities collection of commodities
     * @param filter filter to determine if value is present on T
     * @param mapper maps T to value of interest
     * @param reducingFunction reducing operation applied to mapped T values
     * @param <T> type of object
     * @return reduced value from collection, null if no values present
     */
    @Nullable
    private static <T> Double reduceCommodityCollection(@Nonnull List<T> commodities,
                                                        @Nonnull Predicate<T> filter,
                                                        @Nonnull ToDoubleFunction<T> mapper,
                                                        @Nonnull DoubleBinaryOperator reducingFunction) {
        OptionalDouble value = commodities.stream()
                    .filter(filter)
                    .mapToDouble(mapper)
                    .reduce(reducingFunction);

        return value.isPresent() ? value.getAsDouble() : null;
    }

    /**
     * Record a metric record for each of the given sold commodity structures, all of which are for
     * the same commodity type, with different commodity keys.
     *  @param oid            selling entity oid
     * @param entityType      type of the selling entity
     * @param type            commodity type
     * @param soldCommodities sold commodity structures
     */
    private void recordUnaggregatedSoldCommodity(final long oid, EntityType entityType, final MetricType type,
            final List<CommoditySoldDTO> soldCommodities) {
        // sum across commodity keys in case same commodity type appears with multiple keys
        soldCommodities.stream()
                .map(cs -> getSoldCommodityRecord(oid,
                        entityType,
                        type.name(),
                        cs.getCommodityType().getKey(),
                        cs.hasUsed() ? cs.getUsed() : null,
                        cs.hasPeak() ? cs.getPeak() : null,
                        cs.hasCapacity() ? cs.getCapacity() : null))
                .forEach(metricRecords::add);
    }

    /**
     * Create a new metric record for a sold commodity.
     *
     * @param oid      selling entity oid
     * @param entityType type of the selling entity
     * @param type     commodity type
     * @param key      commodity key
     * @param used     used metric value
     * @param usedPeak peak of used metric value
     * @param capacity capacity metric value
     * @return new record
     */
    private Record getSoldCommodityRecord(final long oid, @Nonnull final EntityType entityType,
            final String type, String key, @Nullable final Double used,
            @Nullable final Double usedPeak, final Double capacity) {
        Record r = new Record(ModelDefinitions.METRIC_TABLE);
        r.set(ModelDefinitions.ENTITY_OID, oid);
        r.set(ModelDefinitions.ENTITY_TYPE_ENUM, entityType);
        r.set(ModelDefinitions.COMMODITY_TYPE, MetricType.valueOf(type));
        r.set(ModelDefinitions.COMMODITY_KEY, key);
        r.set(ModelDefinitions.COMMODITY_CAPACITY, capacity);
        r.set(ModelDefinitions.COMMODITY_CURRENT, used);
        r.set(ModelDefinitions.COMMODITY_PEAK_CURRENT, usedPeak);
        if (capacity != null && used != null) {
            r.set(ModelDefinitions.COMMODITY_UTILIZATION, capacity == 0 ? 0 : used / capacity);
        }
        return r;
    }


    /**
     * Create records for files on the given entity.
     *
     * @param entity {@link TopologyEntityDTO}
     */
    private void createFileRecords(@Nonnull TopologyEntityDTO entity) {
        // not volume entity or no volume info
        if (entity.getEntityType() != EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE
                || !entity.getTypeSpecificInfo().hasVirtualVolume()) {
            return;
        }

        final VirtualVolumeInfo volumeInfo = entity.getTypeSpecificInfo().getVirtualVolume();

        final Optional<Long> storage = TopologyDTOUtil.getVolumeProvider(entity);
        if (!storage.isPresent()) {
            logger.error("No storage provider for volume: {}:{}", entity.getOid(),
                    entity.getDisplayName());
            return;
        }

        final long storageId = storage.get();
        volumeInfo.getFilesList().forEach(file -> {
            Record fileRecord = new Record(FILE_TABLE);
            fileRecord.set(VOLUME_OID, entity.getOid());
            fileRecord.set(FILE_PATH, file.getPath());
            fileRecord.set(FILE_TYPE, FileType.valueOf(file.getType().name()));
            if (file.hasSizeKb()) {
                fileRecord.set(FILE_SIZE, file.getSizeKb());
            }
            if (file.hasModificationTimeMs()) {
                fileRecord.set(MODIFICATION_TIME,  new Timestamp(file.getModificationTimeMs()));
            }
            fileRecord.set(STORAGE_OID, storageId);
            // storage name will be added later in finish stage
            fileRecord.set(IS_ATTACHED, volumeInfo.getAttachmentState() != AttachmentState.UNATTACHED);
            filesByStorageId.computeIfAbsent(storageId, k -> new ArrayList<>()).add(fileRecord);
        });
    }

    @Override
    public int finish(final DataProvider dataProvider)
            throws UnsupportedDialectException, SQLException, InterruptedException {
        logger.info("Performing finish processing for topology {}", topologyLabel);
        // capture entity count before we add groups
        final int n = entityRecords.size();
        // compute scopes and persist any changes
        updateScopes(dataProvider);
        // create entity records for all groups
        recordGroupsAsEntities(dataProvider);
        // now write everything out!
        try (TableWriter entitiesUpserter = ENTITY_TABLE.open(
                getEntityUpsertSink(upsertConflicts, upsertUpdates),
                "Entities Upserter", logger);
             TableWriter metricInserter = METRIC_TABLE.open(
                     getMetricInserterSink(), "Metric Inserter", logger)) {
            writeEntityRecords(dataProvider, entitiesUpserter);
            writeClusterStats(dataProvider);
            writeMetricRecords(metricInserter);
            writeFileRecords(dataProvider);
        }
        return n;
    }

    private void updateScopes(DataProvider dataProvider)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        final Optional<RelatedEntitiesExtractor> extractorOptional =
                dataExtractionFactory.newRelatedEntitiesExtractor();
        if (!extractorOptional.isPresent()) {
            // this should not happen
            logger.error("Topology graph or supply chain is not ready, skipping updating scopes");
            return;
        }
        final RelatedEntitiesExtractor relatedEntitiesExtractor = extractorOptional.get();

        scopeManager.startTopology(topologyInfo);
        entityRecords.forEach(r -> updateScope(r.get(ENTITY_OID_AS_OID), relatedEntitiesExtractor));

        // Scope manager needs to know the types of all entities and groups appearing in
        // current topology, so we construct the needed map here.
        Int2IntMap entityTypes = new Int2IntOpenHashMap();
        entityRecords.forEach(r ->
                entityTypes.put(oidPack.toIndex(r.get(ENTITY_OID_AS_OID)),
                        r.get(ENTITY_TYPE_AS_TYPE_ENUM).ordinal()));
        dataProvider.getAllGroups().forEach(g -> {
            EntityType type = GroupTypeUtils.protoToDb(g.getDefinition().getType());
            entityTypes.put(oidPack.toIndex(g.getId()), type.ordinal());
        });
        // put every group in its own scope, so the overall scope relationship is reflexive.
        // this happens naturally for true entities due to supply chain calculation
        dataProvider.getAllGroups().forEach(g ->
                scopeManager.addInCurrentScope(g.getId(), g.getId()));
        scopeManager.finishTopology(entityTypes);
    }

    @VisibleForTesting
    DslRecordSink getMetricInserterSink() {
        return new DslRecordSink(dsl, METRIC_TABLE, config, pool);
    }

    @VisibleForTesting
    DslUpdateRecordSink getEntityUpdaterSink(final List<Column<?>> updateIncludes,
            final List<Column<?>> updateMatches, final List<Column<?>> updateUpdates) {
        return new DslUpdateRecordSink(dsl, ENTITY_TABLE, config, pool, "update",
                updateIncludes, updateMatches, updateUpdates);
    }

    @VisibleForTesting
    DslUpsertRecordSink getEntityUpsertSink(final ImmutableList<Column<?>> upsertConflicts, final ImmutableList<Column<?>> upsertUpdates) {
        return new DslUpsertRecordSink(dsl, ENTITY_TABLE, config, pool, "upsert",
                upsertConflicts, upsertUpdates);
    }

    private void recordGroupsAsEntities(final DataProvider dataProvider) {
        logger.info("Creating entity records for groups in topology {}", topologyLabel);
        dataProvider.getAllGroups()
                .forEach(group -> {
                    logger.debug("Creating record for group {}", group.getId());
                    Record r = new Record(ENTITY_TABLE);
                    final PartialRecordInfo rec = new PartialRecordInfo(
                            group.getId(), group.getDefinition().getType().getNumber(), r, new HashMap<>());
                    groupPatcher.patch(rec, group);
                    rec.finalizeAttrs();
                    entityRecords.add(r);
                });
    }

    private void writeEntityRecords(final DataProvider dataProvider, final TableWriter tableWriter) {
        logger.info("Upserting entity records for topology {}", topologyLabel);
        entityHashManager.open(topologyInfo, dsl);
        entityRecords.stream()
            .filter(entityHashManager::processEntity)
            .forEach(tableWriter::accept);
        entityHashManager.close();
    }

    private void writeClusterStats(final DataProvider dataProvider) {
        logger.info("Creating metric records for cluster properties");
        dataProvider.getClusterStats()
            .forEach(entityStats -> {
                final long oid = entityStats.getOid();
                logger.debug("Creating record for cluster {}", entityStats.getOid());
                entityStats.getStatSnapshotsList().forEach(snapshot -> snapshot.getStatRecordsList().forEach(record -> {
                    if (propsToDbType.containsKey(record.getName())) {
                        Record clusterRecord = createClusterRecord(oid, record,
                            snapshot.getSnapshotDate());
                        metricRecords.add(clusterRecord);
                    } else {
                        logger.error("Cluster property type {} can't be translated into a "
                            + "metric type", record.getName());
                    }
                }));
            });
    }

    private Record createClusterRecord(long oid, StatRecord record, long date) {
        Record r = new Record(ModelDefinitions.METRIC_TABLE);
        Double capacity = record.hasCapacity() ? (double)record.getCapacity().getAvg() : null;
        Double current = record.hasCapacity() && record.hasUsed()
                ? capacity - record.getUsed().getAvg() : null;
        Double utilization = null;
        if (capacity != null && current != null) {
            utilization = capacity == 0 ? 0 : current / capacity;
        }
        r.set(ModelDefinitions.ENTITY_TYPE_ENUM, EntityType.COMPUTE_CLUSTER);
        r.set(ModelDefinitions.COMMODITY_TYPE, propsToDbType.get(record.getName()));
        r.set(ModelDefinitions.ENTITY_OID, oid);
        r.set(ModelDefinitions.COMMODITY_KEY, record.hasStatKey() ? record.getStatKey() : null);
        r.set(ModelDefinitions.COMMODITY_CAPACITY, capacity);
        r.set(ModelDefinitions.COMMODITY_CURRENT, current);
        r.set(ModelDefinitions.COMMODITY_UTILIZATION,
            utilization);
        r.set(TIME, new Timestamp(date));
        return r;
    }

    private void updateScope(long oid, RelatedEntitiesExtractor relatedEntitiesExtractor) {
        // first collect oids for entities related to this one via supply chain
        final LongSet entitiesInScope = relatedEntitiesExtractor.getRelatedEntitiesByType(oid).values().stream()
                .flatMap(Collection::stream)
                .mapToLong(Long::longValue)
                .collect(LongOpenHashSet::new, LongSet::add, LongSet::addAll);
        logger.debug("Adding entities to scope for entity {}: {}", () -> oid, () -> entitiesInScope);
        scopeManager.addInCurrentScope(oid, entitiesInScope.toLongArray());
        // then we collect all the groups that any of our related entities belong to...
        LongSet groupsInScope = relatedEntitiesExtractor.getRelatedGroups(entitiesInScope.stream())
                .mapToLong(Grouping::getId)
                .collect(LongOpenHashSet::new, LongSet::add, LongSet::addAll);
        logger.debug("Adding groups to scope for entity {}: {}", () -> oid, () -> groupsInScope);
        // groups are added symmetrically to the entity scope
        scopeManager.addInCurrentScope(oid, true, groupsInScope.toLongArray());
        LongSet result = new LongOpenHashSet(entitiesInScope);
        result.addAll(groupsInScope);
    }

    private void writeMetricRecords(TableWriter tableWriter) {
        logger.info("Inserting metric records for topology {}", topologyLabel);
        final Timestamp time = new Timestamp(topologyInfo.getCreationTime());
        metricRecords.forEach(record -> {
            try (Record r = tableWriter.open(record)) {
                // For some metrics, such as headroom, we want to preserve their original
                // timestamp and not overwrite it with the topology creation time
                if (r.get(TIME) == null) {
                    r.set(TIME, time);
                }
            }
        });
    }

    /**
     * Write the wasted files records into the table.
     *
     * @param dataProvider data provider
     */
    private void writeFileRecords(DataProvider dataProvider) {
        logger.info("Writing file records for topology {}", topologyLabel);

        try (CloseableConsumer<Record> consumer = fileTableManager.open(dsl, config)) {
            filesByStorageId.long2ObjectEntrySet().forEach(entry ->
                    entry.getValue().forEach(consumer));
        }
    }
}
