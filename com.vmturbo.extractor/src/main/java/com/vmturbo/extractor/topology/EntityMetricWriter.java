package com.vmturbo.extractor.topology;

import static com.vmturbo.common.protobuf.topology.TopologyDTOUtil.QX_VCPU_BASE_COEFFICIENT;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_PATH;
import static com.vmturbo.extractor.models.ModelDefinitions.FILE_SIZE;
import static com.vmturbo.extractor.models.ModelDefinitions.METRIC_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.MODIFICATION_TIME;
import static com.vmturbo.extractor.models.ModelDefinitions.STORAGE_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.STORAGE_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.TIME;
import static com.vmturbo.extractor.models.ModelDefinitions.WASTED_FILE_TABLE;

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
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.RecordHashManager.SnapshotManager;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.patchers.GroupPrimitiveFieldsOnGroupingPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.patchers.TagsPatchers.EntityTagsPatcher;
import com.vmturbo.extractor.patchers.TagsPatchers.GroupTagsPatcher;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.search.EnumUtils.CommodityTypeUtils;
import com.vmturbo.extractor.search.EnumUtils.EntityTypeUtils;
import com.vmturbo.extractor.search.SearchEntityWriter.PartialRecordInfo;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.VirtualVolumeData.AttachmentState;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Writer that extracts entity and metric data from a topology and persists it to the database.
 */
// TODO 61163 Strengthen against Exceptions killing listener
public class EntityMetricWriter extends TopologyWriterBase {
    private static final Logger logger = LogManager.getLogger();

    /**
     * The name to persist in db for ready queue commodity bought by VM.
     */
    public static final MetricType VM_QX_VCPU_NAME = MetricType.CPU_READY;

    /**
     * Matches ready queue commodity like: Q16_VCPU, QN_VCPU.
     */
    private static final Pattern QX_VCPU_PATTERN = Pattern.compile("Q.*_VCPU");

    // configurations for upsert and update operations for entity table
    private static final ImmutableList<Column<?>> upsertConflicts = ImmutableList.of(
            ENTITY_OID_AS_OID, ModelDefinitions.ENTITY_HASH_AS_HASH);
    private static final ImmutableList<Column<?>> upsertUpdates = ImmutableList.of(ModelDefinitions.LAST_SEEN);
    private static final List<Column<?>> updateIncludes = ImmutableList
            .of(ModelDefinitions.ENTITY_HASH_AS_HASH, ModelDefinitions.LAST_SEEN);
    private static final List<Column<?>> updateMatches = ImmutableList.of(ModelDefinitions.ENTITY_HASH_AS_HASH);
    private static final List<Column<?>> updateUpdates = ImmutableList.of(ModelDefinitions.LAST_SEEN);

    private final Int2ObjectMap<Record> entityRecordsMap = new Int2ObjectOpenHashMap<>();

    private final Int2ObjectMap<List<Record>> metricRecordsMap = new Int2ObjectOpenHashMap<>();
    private final EntityHashManager entityHashManager;

    /**
     * List of wasted files records by storage oid.
     */
    private final Long2ObjectMap<List<Record>> wastedFileRecordsByStorageId = new Long2ObjectOpenHashMap<>();
    private final PrimitiveFieldsOnTEDPatcher tedPatcher = new PrimitiveFieldsOnTEDPatcher(true);
    private final EntityTagsPatcher entityTagsPatcher = new EntityTagsPatcher();
    private final GroupPrimitiveFieldsOnGroupingPatcher groupPatcher =
            new GroupPrimitiveFieldsOnGroupingPatcher();
    private final GroupTagsPatcher groupTagsPatcher = new GroupTagsPatcher();
    private final EntityIdManager entityIdManager;
    private final ScopeManager scopeManager;

    /**
     * Create a new writer instance.
     *
     * @param dbEndpoint        db endpoint for persisting data
     * @param entityHashManager to track entity hash evolution across topology broadcasts
     * @param scopeManager      scope manager
     * @param entityIdManager   entity id manager
     * @param pool              thread pool
     */
    public EntityMetricWriter(final DbEndpoint dbEndpoint, final EntityHashManager entityHashManager,
            final ScopeManager scopeManager, final EntityIdManager entityIdManager,
            final ExecutorService pool) {
        super(dbEndpoint, ModelDefinitions.REPORTING_MODEL, pool);
        this.entityHashManager = entityHashManager;
        this.scopeManager = scopeManager;
        this.entityIdManager = entityIdManager;
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(final TopologyInfo topologyInfo,
            final WriterConfig config, final MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
        super.startTopology(topologyInfo, config, timer);
        scopeManager.startTopology(topologyInfo);
        return this::writeEntity;
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO e) {
        if (EntityTypeUtils.protoIntToDb(e.getEntityType(), null) == null) {
            logger.error("Cannot map entity type {} for db storage for entity oid {}; skipping",
                    e.getEntityType(), e.getOid());
            return;
        }
        final long oid = e.getOid();
        final int iid = entityIdManager.toIid(oid);
        Record entitiesRecord = new Record(ENTITY_TABLE);
        final HashMap<String, Object> attrs = new HashMap<>();
        PartialRecordInfo rec = new PartialRecordInfo(e.getOid(), e.getEntityType(), entitiesRecord, attrs);
        tedPatcher.patch(rec, e);
        entityTagsPatcher.patch(rec, e);
        rec.finalizeAttrs();
        // supply chain will be added during finish processing
        entityRecordsMap.put(iid, entitiesRecord);
        writeMetrics(e, oid, iid);
        // cache wasted file records, since storage name can only be fetched later
        createWastedFileRecords(e);
    }

    @Override
    public boolean requireSupplyChainForAllEntities() {
        return true;
    }

    /**
     * Record metric records for this entity, including records for bought and sold commodities.
     *
     * @param e   entity
     * @param oid entity oid
     * @param iid entity iid (for same entity - we use both)
     */
    private void writeMetrics(final TopologyEntityDTO e, final long oid, final int iid) {
        final List<Record> metricRecords = metricRecordsMap.computeIfAbsent(iid, k -> new ArrayList<>());
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
                    recordAggregatedBoughtCommodity(oid, typeNo, cbs, producer, metricRecords);
                } else {
                    recordUnaggregatedBoughtCommodity(oid, typeNo, cbs, producer, metricRecords);
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
                recordAggregatedSoldCommodity(oid, type, css, metricRecords);
            } else {
                recordUnaggregatedSoldCommodity(oid, type, css, metricRecords);
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
     * @return true if commodity shoudl be aggregated across keys
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
     *
     * @param oid               consuming entity oid
     * @param typeNo            commodity type
     * @param boughtCommodities bought commodity structures
     * @param producer          oid of producer entity
     * @param metricRecords     where to add new metric record
     */
    private void recordAggregatedBoughtCommodity(final long oid, final Integer typeNo,
            final List<CommodityBoughtDTO> boughtCommodities, final long producer,
            final List<Record> metricRecords) {
        // sum across commodity keys in case same commodity type appears with multiple keys
        // and same provider
        final String type = CommodityType.forNumber(typeNo).name();
        final Double sumUsed = boughtCommodities.stream().mapToDouble(CommodityBoughtDTO::getUsed).sum();
        metricRecords.add(getBoughtCommodityRecord(oid, type, null, sumUsed, producer));
    }

    /**
     * Record a metric record for each of the given bought commodity structures, all of which are
     * for the same commodity type and bought from the same producer, with different commodity
     * keys.
     *
     * @param oid               consuming entity oid
     * @param typeNo            commodity type
     * @param boughtCommodities bought commodity structures
     * @param producer          oid of producer entity
     * @param metricRecords     where to add new metric records
     */
    private void recordUnaggregatedBoughtCommodity(final long oid, final Integer typeNo,
            final List<CommodityBoughtDTO> boughtCommodities, final long producer,
            final List<Record> metricRecords) {
        // record individual records for this bought commodity
        final String type = CommodityType.forNumber(typeNo).name();
        boughtCommodities.stream()
                .map(cb -> getBoughtCommodityRecord(oid, type, cb.getCommodityType().getKey(),
                        cb.getUsed(), producer))
                .forEach(metricRecords::add);
    }

    /**
     * Create a new metric record for a bought commodity.
     *
     * @param oid      consuming entity oid
     * @param type     commodity type
     * @param key      commodity key
     * @param used     used metric
     * @param producer producer oid
     * @return new metric record
     */
    private Record getBoughtCommodityRecord(final long oid, final String type, String key,
            final Double used, final long producer) {
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
            r.set(ModelDefinitions.COMMODITY_UTILIZATION, used / QX_VCPU_BASE_COEFFICIENT);
        } else {
            r.set(ModelDefinitions.COMMODITY_TYPE, MetricType.valueOf(type));
            r.set(ModelDefinitions.COMMODITY_KEY, key);
            r.set(ModelDefinitions.COMMODITY_CONSUMED, used);
            r.set(ModelDefinitions.COMMODITY_PROVIDER, producer);
        }
        return r;
    }

    /**
     * Record a metric record for the given sold commodity structures, all of which are for the same
     * commodity type, with different commodity keys.
     *
     * <p>We aggregate the used and capacity metrics across all the sold commodity structures.</p>
     *  @param oid             selling entity oid
     * @param type          commodity type
     * @param soldCommodities sold commodity structures
     * @param metricRecords   where to save new record
     */
    private void recordAggregatedSoldCommodity(final long oid, final MetricType type,
            final List<CommoditySoldDTO> soldCommodities, final List<Record> metricRecords) {
        // sum across commodity keys in case same commodity type appears with multiple keys
        final double sumUsed = soldCommodities.stream().mapToDouble(CommoditySoldDTO::getUsed).sum();
        final double sumCap = soldCommodities.stream().mapToDouble(CommoditySoldDTO::getCapacity).sum();

        metricRecords.add(getSoldCommodityRecord(oid, type.name(), null, sumUsed, sumCap));
    }

    /**
     * Record a metric record for each of the given sold commodity structures, all of which are for
     * the same commodity type, with different commodity keys.
     *  @param oid             selling entity oid
     * @param type          commodity type
     * @param soldCommodities sold commodity structures
     * @param metricRecords   where to save new records
     */
    private void recordUnaggregatedSoldCommodity(final long oid, final MetricType type,
            final List<CommoditySoldDTO> soldCommodities, final List<Record> metricRecords) {
        // sum across commodity keys in case same commodity type appears with multiple keys
        soldCommodities.stream()
                .map(cs -> getSoldCommodityRecord(oid, type.name(), cs.getCommodityType().getKey(),
                        cs.getUsed(), cs.getCapacity()))
                .forEach(metricRecords::add);
    }

    /**
     * Create a new metric record for a sold commodity.
     *
     * @param oid      selling entity oid
     * @param type     commodity type
     * @param key      commodity key
     * @param used     used metric value
     * @param capacity capacity metric value
     * @return new record
     */
    private Record getSoldCommodityRecord(final long oid, final String type, String key,
            final double used, final double capacity) {
        Record r = new Record(ModelDefinitions.METRIC_TABLE);
        r.set(ModelDefinitions.ENTITY_OID, oid);
        r.set(ModelDefinitions.COMMODITY_TYPE, MetricType.valueOf(type));
        r.set(ModelDefinitions.COMMODITY_KEY, key);
        r.set(ModelDefinitions.COMMODITY_CAPACITY, capacity);
        r.set(ModelDefinitions.COMMODITY_CURRENT, used);
        r.set(ModelDefinitions.COMMODITY_UTILIZATION, capacity == 0 ? 0 : used / capacity);
        return r;
    }


    /**
     * Create records for wasted files on the given entity.
     *
     * @param entity {@link TopologyEntityDTO}
     */
    private void createWastedFileRecords(@Nonnull TopologyEntityDTO entity) {
        // not volume entity or no volume info
        if (entity.getEntityType() != EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE
                || !entity.getTypeSpecificInfo().hasVirtualVolume()) {
            return;
        }

        final VirtualVolumeInfo volumeInfo = entity.getTypeSpecificInfo().getVirtualVolume();
        // not wasted files volume
        if (volumeInfo.getAttachmentState() != AttachmentState.UNATTACHED) {
            return;
        }

        final Optional<Long> storage = TopologyDTOUtil.getVolumeProvider(entity);
        if (!storage.isPresent()) {
            logger.error("No storage provider for volume: {}:{}", entity.getOid(),
                    entity.getDisplayName());
            return;
        }

        final Long storageId = storage.get();
        volumeInfo.getFilesList().forEach(file -> {
            Record wastedFileRecord = new Record(WASTED_FILE_TABLE);
            wastedFileRecord.set(FILE_PATH, file.getPath());
            wastedFileRecord.set(FILE_SIZE, file.getSizeKb());
            wastedFileRecord.set(MODIFICATION_TIME,  new Timestamp(file.getModificationTimeMs()));
            wastedFileRecord.set(STORAGE_OID, storageId);
            // storage name will be added later in finish stage
            wastedFileRecordsByStorageId.computeIfAbsent((long)storageId, k -> new ArrayList<>())
                    .add(wastedFileRecord);
        });
    }

    @Override
    public int finish(final DataProvider dataProvider)
            throws UnsupportedDialectException, SQLException, InterruptedException {
        // capture entity count before we add groups
        int n = entityRecordsMap.size();
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter entitiesUpserter = ENTITY_TABLE.open(
                     getEntityUpsertSink(dsl, upsertConflicts, upsertUpdates));
             TableWriter entitiesUpdater = ENTITY_TABLE.open(getEntityUpdaterSink(
                     dsl, updateIncludes, updateMatches, updateUpdates));
             TableWriter metricInserter = METRIC_TABLE.open(getMetricInserterSink(dsl));
             SnapshotManager snapshotManager = entityHashManager.open(topologyInfo.getCreationTime());
             TableWriter wastedFileReplacer = WASTED_FILE_TABLE.open(getWastedFileReplacerSink(dsl))) {

            // prepare and write all our entity and metric records
            writeGroupsAsEntities(dataProvider);
            upsertEntityRecords(dataProvider, entitiesUpserter, snapshotManager);
            writeMetricRecords(metricInserter);
            snapshotManager.processChanges(entitiesUpdater);
            scopeManager.finishTopology();
            // write wasted files records
            writeWastedFileRecords(wastedFileReplacer, dataProvider);
            return n;
        }
    }

    @VisibleForTesting
    DslRecordSink getMetricInserterSink(final DSLContext dsl) {
        return new DslRecordSink(dsl, METRIC_TABLE, config, pool);
    }

    @VisibleForTesting
    DslUpdateRecordSink getEntityUpdaterSink(final DSLContext dsl, final List<Column<?>> updateIncludes,
            final List<Column<?>> updateMatches, final List<Column<?>> updateUpdates) {
        return new DslUpdateRecordSink(dsl, ENTITY_TABLE, config, pool, "update",
                updateIncludes, updateMatches, updateUpdates);
    }

    @VisibleForTesting
    DslUpsertRecordSink getEntityUpsertSink(final DSLContext dsl,
            final ImmutableList<Column<?>> upsertConflicts, final ImmutableList<Column<?>> upsertUpdates) {
        return new DslUpsertRecordSink(dsl, ENTITY_TABLE, config, pool, "upsert",
                upsertConflicts, upsertUpdates);
    }

    @VisibleForTesting
    DslRecordSink getWastedFileReplacerSink(final DSLContext dsl) {
        return new DslReplaceRecordSink(dsl, WASTED_FILE_TABLE, config, pool, "replace");
    }

    private void writeGroupsAsEntities(final DataProvider dataProvider) {
        dataProvider.getAllGroups()
                .forEach(group -> {
                    Record r = new Record(ENTITY_TABLE);
                    final PartialRecordInfo rec = new PartialRecordInfo(
                            group.getId(), group.getDefinition().getType().getNumber(), r, new HashMap<>());
                    groupPatcher.patch(rec, group);
                    groupTagsPatcher.patch(rec, group);
                    rec.finalizeAttrs();
                    entityRecordsMap.put(entityIdManager.toIid(group.getId()), r);
                });
    }

    private void upsertEntityRecords(final DataProvider dataProvider, final TableWriter tableWriter,
            final SnapshotManager snapshotManager) {
        entityRecordsMap.int2ObjectEntrySet().forEach(entry -> {
            int iid = entry.getIntKey();
            Record record = entry.getValue();
            final LongSet scope = getRelatedEntitiesAndGroups(entityIdManager.toOid(iid), dataProvider);
            record.set(ModelDefinitions.SCOPED_OIDS, scope.stream().toArray(Long[]::new));
            // only store entity if hash changes
            Long newHash = snapshotManager.updateRecordHash(record);
            if (newHash != null) {
                try (Record r = tableWriter.open(record)) {
                    r.set(ModelDefinitions.ENTITY_HASH_AS_HASH, newHash);
                    snapshotManager.setRecordTimes(r);
                }
            }
        });
    }

    private LongSet getRelatedEntitiesAndGroups(long oid, DataProvider dataProvider) {
        // first collect iids for entities related to this one via spply chain
        final Set<Long> related = dataProvider.getRelatedEntities(oid);
        // those all go into our result set
        LongSet result = new LongOpenHashSet(related);
        // then we collect all the groups that any of oure related entities belong to...
        related.stream()
                .map(dataProvider::getGroupsForEntity)
                .flatMap(Collection::stream)
                .mapToLong(Grouping::getId)
                .distinct()
                // ... and add their iids to the result as well
                .forEach(result::add);
        scopeManager.addScope(oid, result.toLongArray());
        return result;
    }

    private void writeMetricRecords(TableWriter tableWriter) {
        final Timestamp time = new Timestamp(topologyInfo.getCreationTime());
        metricRecordsMap.int2ObjectEntrySet().forEach(entry -> {
            long oid = entityIdManager.toOid(entry.getIntKey());
            Long hash = entityHashManager.getEntityHash(oid);
            entry.getValue().forEach(partialMetricRecord -> {
                try (Record r = tableWriter.open(partialMetricRecord)) {
                    r.set(TIME, time);
                    r.set(ModelDefinitions.ENTITY_HASH, hash);
                }
            });
        });
    }

    /**
     * Write the wasted files records into the table.
     *
     * @param tableWriter table writer {@link DslReplaceRecordSink}
     * @param dataProvider data provider
     */
    private void writeWastedFileRecords(TableWriter tableWriter, DataProvider dataProvider) {
        wastedFileRecordsByStorageId.long2ObjectEntrySet().forEach(entry -> {
            final long storageId = entry.getLongKey();
            final String storageName = dataProvider.getDisplayName(storageId).orElseGet(() -> {
                // this should not happen, use empty string as default
                logger.error("No display name for storage {}", storageId);
                return "";
            });
            entry.getValue().forEach(partialWastedFileRecord -> {
                try (Record r = tableWriter.open(partialWastedFileRecord)) {
                    r.set(STORAGE_NAME, storageName);
                }
            });
        });
    }
}
