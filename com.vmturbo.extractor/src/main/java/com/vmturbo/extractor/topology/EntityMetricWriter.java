package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.METRIC_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.TIME;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import com.google.protobuf.util.JsonFormat.Printer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupDefinition;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TypeSpecificInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.DslRecordSink;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.EntityHashManager.SnapshotManager;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Writer that extracts entity and metric data from a topology and persists it to the database.
 */
public class EntityMetricWriter extends TopologyWriterBase {
    private static final Logger logger = LogManager.getLogger();

    private static final Set<GroupType> GROUP_TYPE_BLACKLIST = ImmutableSet.of(
            GroupType.RESOURCE, GroupType.BILLING_FAMILY);

    private static final Long[] EMPTY_SCOPE = new Long[0];

    // configurations for upsert and update operations for entity table
    private static final ImmutableList<Column<?>> upsertConflicts = ImmutableList.of(
            ENTITY_OID_AS_OID, ModelDefinitions.ENTITY_HASH_AS_HASH);
    private static final ImmutableList<Column<?>> upsertUpdates = ImmutableList.of(ModelDefinitions.LAST_SEEN);
    private static List<Column<?>> updateIncludes = ImmutableList
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
    private EntityHashManager entityHashManager;

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
        this.rand = ne Random();
        return this::writeEntity;
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO e) {
        if ()
        final long oid = e.getOid();
        Record entitiesRecord = new Record(ENTITY_TABLE);
        entitiesRecord.set(ENTITY_OID_AS_OID, oid);
        entitiesRecord.set(ModelDefinitions.ENTITY_TYPE_AS_TYPE, EntityType.forNumber(e.getEntityType()).name());
        entitiesRecord.set(ModelDefinitions.ENTITY_NAME, e.getDisplayName());
        entitiesRecord.setIf(e.hasEnvironmentType(), ModelDefinitions.ENVIRONMENT_TYPE, () -> e.getEnvironmentType().name());
        entitiesRecord.setIf(e.hasEntityState(), ModelDefinitions.ENTITY_STATE, () -> e.getEntityState().name());
        try {
            entitiesRecord.set(ModelDefinitions.ATTRS, getTypeSpecificInfoJson(e));
        } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
            logger.error("Failed to record type-specific info for entity {}", oid);
        }
        // supply chain will be added during finish processing
        entityRecordsMap.put(oid, entitiesRecord);
        writeMetrics(e, oid);
    }

    private void writeMetrics(final TopologyEntityDTO e, final long oid) {
        final List<Record> metricRecords = metricRecordsMap.computeIfAbsent(oid, k -> new ArrayList<>());
        e.getCommoditiesBoughtFromProvidersList().forEach(cbfp -> {
            final long producer = cbfp.getProviderId();
            Map<Integer, List<CommodityBoughtDTO>> cbByType = cbfp.getCommodityBoughtList().stream()
                    .filter(cb -> config.reportingCommodityWhitelist().contains(cb.getCommodityType().getType()))
                    .collect(Collectors.groupingBy(cb -> cb.getCommodityType().getType()));
            cbByType.forEach((typeNo, cbs) -> {
                // sum across commodity keys in case same commodity type appears with multiple keys
                // and same provider
                final String type = CommodityType.forNumber(typeNo).name();
                final Double sumUsed = cbs.stream().mapToDouble(CommodityBoughtDTO::getUsed).sum();

                Record r = new Record(ModelDefinitions.METRIC_TABLE);
                r.set(ModelDefinitions.ENTITY_OID, oid);
                r.set(ModelDefinitions.COMMODITY_PROVIDER, producer);
                r.set(ModelDefinitions.COMMODITY_TYPE, type);
                r.set(ModelDefinitions.COMMODITY_CONSUMED, sumUsed);
                r.set(ModelDefinitions.COMMODITY_PROVIDER, producer);
                metricRecords.add(r);
            });
        });
        Map<Integer, List<CommoditySoldDTO>> csByType = e.getCommoditySoldListList().stream()
                .filter(cs -> config.reportingCommodityWhitelist().contains(cs.getCommodityType().getType()))
                .collect(Collectors.groupingBy(cs -> cs.getCommodityType().getType()));
        csByType.forEach((typeNo, css) -> {
            // sum across commodity keys in case same commodity type appears with multiple keys
            final String type = CommodityType.forNumber(typeNo).name();
            final double sumUsed = css.stream().mapToDouble(CommoditySoldDTO::getUsed).sum();
            final double sumCap = css.stream().mapToDouble(CommoditySoldDTO::getCapacity).sum();

            Record r = new Record(ModelDefinitions.METRIC_TABLE);
            r.set(ModelDefinitions.ENTITY_OID, oid);
            r.set(ModelDefinitions.COMMODITY_TYPE, type);
            r.set(ModelDefinitions.COMMODITY_CAPACITY, sumCap);
            r.set(ModelDefinitions.COMMODITY_CURRENT, sumUsed);
            r.set(ModelDefinitions.COMMODITY_UTILIZATION, sumCap == 0 ? 0 : sumUsed / sumCap);
            metricRecords.add(r);
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
             SnapshotManager snapshotManager = entityHashManager.open(topologyInfo.getCreationTime())) {
            // prepare and write all our entity and metric records
            writeGroupsAsEntities(dataProvider);
            upsertEntityRecords(dataProvider, entitiesUpserter, snapshotManager);
            writeMetricRecords(metricInserter);
            snapshotManager.processChanges(entitiesUpdater);
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

    private void writeGroupsAsEntities(final DataProvider dataProvider) {
        dataProvider.getAllGroups()
                .filter(grouping -> !GROUP_TYPE_BLACKLIST.contains(grouping.getDefinition().getType()))
                .forEach(group -> {
                    final GroupDefinition def = group.getDefinition();
                    Record r = new Record(ENTITY_TABLE);
                    r.set(ENTITY_OID_AS_OID, group.getId());
                    r.set(ModelDefinitions.ENTITY_NAME, def.getDisplayName());
                    r.set(ModelDefinitions.ENTITY_TYPE_AS_TYPE, def.getType().name());
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
                .map(EntityType::forNumber)
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
