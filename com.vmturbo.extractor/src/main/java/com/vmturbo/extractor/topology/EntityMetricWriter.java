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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo.VirtualVolumeInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.schema.enums.EntityState;
import com.vmturbo.extractor.schema.enums.EntityType;
import com.vmturbo.extractor.schema.enums.EnvironmentType;
import com.vmturbo.extractor.schema.enums.MetricType;
import com.vmturbo.extractor.search.EnumUtils;
import com.vmturbo.extractor.topology.EntityHashManager.SnapshotManager;
import com.vmturbo.extractor.topology.mapper.GroupMappers;
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
    public static final String VM_QX_VCPU_NAME = "CPU_READY";

    /**
     * Matches ready queue commodity like: Q16_VCPU, QN_VCPU.
     */
    private static final Pattern QX_VCPU_PATTERN = Pattern.compile("Q.*_VCPU");

    private static final Long[] EMPTY_SCOPE = new Long[0];

    // configurations for upsert and update operations for entity table
    private static final ImmutableList<Column<?>> upsertConflicts = ImmutableList.of(
            ENTITY_OID_AS_OID, ModelDefinitions.ENTITY_HASH_AS_HASH);
    private static final ImmutableList<Column<?>> upsertUpdates = ImmutableList.of(ModelDefinitions.LAST_SEEN);
    private static final List<Column<?>> updateIncludes = ImmutableList
            .of(ModelDefinitions.ENTITY_HASH_AS_HASH, ModelDefinitions.LAST_SEEN);
    private static final List<Column<?>> updateMatches = ImmutableList.of(ModelDefinitions.ENTITY_HASH_AS_HASH);
    private static final List<Column<?>> updateUpdates = ImmutableList.of(ModelDefinitions.LAST_SEEN);

    private static final Printer jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .sortingMapKeys()
            .preservingProtoFieldNames();

    private static final ObjectMapper mapper = new ObjectMapper();

    private final Long2ObjectMap<Record> entityRecordsMap = new Long2ObjectOpenHashMap<>();

    private final Long2ObjectMap<List<Record>> metricRecordsMap = new Long2ObjectOpenHashMap<>();
    private final EntityHashManager entityHashManager;

    /**
     * List of wasted files records by storage oid.
     */
    private final Long2ObjectMap<List<Record>> wastedFileRecordsByStorageId = new Long2ObjectOpenHashMap<>();

    /**
     * Create a new writer instance.
     *
     * @param dbEndpoint        db endpoint for persisting data
     * @param entityHashManager to track entity hash evolution across topology broadcasts
     * @param pool              thread pool
     */
    public EntityMetricWriter(final DbEndpoint dbEndpoint, final EntityHashManager entityHashManager,
            final ExecutorService pool) {
        super(dbEndpoint, ModelDefinitions.REPORTING_MODEL, pool);
        this.entityHashManager = entityHashManager;
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(final TopologyInfo topologyInfo,
            final WriterConfig config, final MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException {
        super.startTopology(topologyInfo, config, timer);
        return this::writeEntity;
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO e) {
        final long oid = e.getOid();
        EntityType entityType = EnumUtils.entityTypeFromProtoIntToDb(e.getEntityType(), null);
        if (entityType == null) {
            logger.error("Cannot map entity type {} for db storage for entity oid {}; skipping",
                    e.getEntityType(), e.getOid());
            return;
        }
        Record entitiesRecord = new Record(ENTITY_TABLE);
        entitiesRecord.set(ENTITY_OID_AS_OID, oid);
        entitiesRecord.set(ModelDefinitions.ENTITY_TYPE_AS_TYPE, entityType.name());
        entitiesRecord.set(ModelDefinitions.ENTITY_NAME, e.getDisplayName());
        entitiesRecord.setIf(e.hasEnvironmentType(), ModelDefinitions.ENVIRONMENT_TYPE,
                () -> EnumUtils.environmentTypeFromProtoToDb(e.getEnvironmentType(),
                        EnvironmentType.UNKNOWN_ENV).name());
        entitiesRecord.setIf(e.hasEntityState(), ModelDefinitions.ENTITY_STATE,
                () -> EnumUtils.entityStateFromProtoToDb(e.getEntityState(),
                        EntityState.UNKNOWN).name());
        try {
            entitiesRecord.set(ModelDefinitions.ATTRS, getTypeSpecificInfoJson(e));
        } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
            logger.error("Failed to record type-specific info for entity {}", oid);
        }
        // supply chain will be added during finish processing
        entityRecordsMap.put(oid, entitiesRecord);
        writeMetrics(e, oid);
        // cache wasted file records, since storage name can only be fetched later
        createWastedFileRecords(e);
    }

    /**
     * Record metric records for this entity, including records for bought and sold commodities.
     *
     * @param e   entity
     * @param oid entity oid
     */
    private void writeMetrics(final TopologyEntityDTO e, final long oid) {
        final List<Record> metricRecords = metricRecordsMap.computeIfAbsent(oid, k -> new ArrayList<>());
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
                    logger.error("Skipping invalid bought commodity type {} for entity {}",
                            typeNo, e.getOid());
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
            MetricType type = EnumUtils.commodityTypeFromProtoIntToDb(typeNo, null);
            if (CommodityType.forNumber(typeNo) == null) {
                logger.error("Skipping invalid sold commodity type {} for entity {}",
                        typeNo, e.getOid());
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
            r.set(ModelDefinitions.COMMODITY_TYPE, type);
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
        r.set(ModelDefinitions.COMMODITY_TYPE, type);
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
                    final GroupDefinition def = group.getDefinition();
                    Record r = new Record(ENTITY_TABLE);
                    r.set(ENTITY_OID_AS_OID, group.getId());
                    r.set(ModelDefinitions.ENTITY_NAME, def.getDisplayName());
                    r.set(ModelDefinitions.ENTITY_TYPE_AS_TYPE,
                            GroupMappers.mapGroupTypeToName(def.getType()));
                    r.set(ModelDefinitions.SCOPED_OIDS, EMPTY_SCOPE);
                    final JsonString attrs;
                    try {
                        attrs = getGroupJson(group);
                        r.set(ModelDefinitions.ATTRS, attrs);
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to record group attributes for group {}", group.getId());
                    }
                    entityRecordsMap.put(group.getId(), r);
                });
    }

    private void upsertEntityRecords(final DataProvider dataProvider, final TableWriter tableWriter,
            final SnapshotManager snapshotManager) {
        entityRecordsMap.long2ObjectEntrySet().forEach(entry -> {
            long oid = entry.getLongKey();
            Record record = entry.getValue();
            final Long[] scope = getRelatedEntitiesAndGroups(oid, dataProvider);
            record.set(ModelDefinitions.SCOPED_OIDS, scope);

            // only store entity if hash changes
            Long newHash = snapshotManager.updateEntityHash(record);
            if (newHash != null) {
                try (Record r = tableWriter.open(record)) {
                    r.set(ModelDefinitions.ENTITY_HASH_AS_HASH, newHash);
                    snapshotManager.setEntityTimes(r);
                }
            }
        });
    }

    private Long[] getRelatedEntitiesAndGroups(long oid, DataProvider dataProvider) {
        final Set<Long> related = dataProvider.getRelatedEntities(oid);
        LongSet result = new LongOpenHashSet(related);
        related.stream()
                .map(dataProvider::getGroupsForEntity)
                .flatMap(Collection::stream)
                .mapToLong(Grouping::getId)
                .forEach(result::add);
        return result.toArray(new Long[0]);
    }

    private void writeMetricRecords(TableWriter tableWriter) {
        final Timestamp time = new Timestamp(topologyInfo.getCreationTime());
        metricRecordsMap.long2ObjectEntrySet().forEach(entry -> {
            long oid = entry.getLongKey();
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

    private JsonString getTypeSpecificInfoJson(TopologyEntityDTO entity) throws
            InvalidProtocolBufferException {
        final TypeSpecificInfo tsi = entity.hasTypeSpecificInfo() ? entity.getTypeSpecificInfo() : null;
        MessageOrBuilder trimmed = null;
        try {
            trimmed = tsi != null ? stripUnwantedFields(tsi) : null;
        } catch (IllegalArgumentException e) {
            logger.warn("Entity type {} not handled by type-specific-info stripper", tsi.getTypeCase());
        }
        final String json = trimmed != null ? jsonPrinter.print(trimmed) : null;
        return json != null ? new JsonString(json) : null;
    }

    private JsonString getGroupJson(Grouping group) throws JsonProcessingException {
        final List<String> expectedTypes = group.getExpectedTypesList().stream()
                .map(MemberType::getEntity)
                .map(EntityDTO.EntityType::forNumber)
                .map(Enum::name)
                .collect(Collectors.toList());

        Map<String, Object> obj = ImmutableMap.of(
                "expectedTypes", expectedTypes);
        return new JsonString(mapper.writeValueAsString(obj));
    }

    @VisibleForTesting
    static MessageOrBuilder stripUnwantedFields(TypeSpecificInfo tsi) {
        switch (tsi.getTypeCase()) {
            case APPLICATION:
                return tsi.getApplication();
            case BUSINESS_ACCOUNT:
                return tsi.getBusinessAccount().toBuilder()
                        .clearRiSupported();
            case BUSINESS_USER:
                return null;
            case COMPUTE_TIER:
                return tsi.getComputeTier().toBuilder()
                        .clearBurstableCPU()
                        .clearDedicatedStorageNetworkState()
                        .clearInstanceDiskType()
                        .clearInstanceDiskSizeGb()
                        .clearNumInstanceDisks()
                        .clearQuotaFamily()
                        .clearSupportedCustomerInfo();
            case DATABASE:
                return tsi.getDatabase();
            case DESKTOP_POOL:
                return tsi.getDesktopPool();
            case DISK_ARRAY:
                return tsi.getDiskArray();
            case LOGICAL_POOL:
                return tsi.getLogicalPool();
            case PHYSICAL_MACHINE:
                return tsi.getPhysicalMachine();
            case REGION:
                return null;
            case STORAGE:
                return tsi.getStorage().toBuilder()
                        .clearIgnoreWastedFiles()
                        .clearPolicy()
                        .clearRawCapacity();
            case STORAGE_CONTROLLER:
                return tsi.getStorageController();
            case VIRTUAL_MACHINE:
                return tsi.getVirtualMachine().toBuilder()
                        .clearDriverInfo()
                        .clearLocks()
                        .clearTenancy()
                        .clearArchitecture()
                        .clearBillingType()
                        .clearVirtualizationType();
            case VIRTUAL_VOLUME:
                return tsi.getVirtualVolume().toBuilder()
                        .clearFiles();
            case WORKLOAD_CONTROLLER:
                // TODO: evaluate whether we need these entities, and if so which attrs to strip
                return null;
            case DATABASE_TIER:
                // TODO: evaluate whether we need these entities, and if so which attrs to strip
                return null;
            case DATABASE_SERVER_TIER:
                // TODO: evaluate whether we need these entities, and if so which attrs to strip
                return null;
            case TYPE_NOT_SET:
                return null;
            default:
                throw new IllegalArgumentException("Unrecognized type-specific-info case: "
                        + tsi.getTypeCase().name());
        }
    }
}
