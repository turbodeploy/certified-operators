package com.vmturbo.extractor.topology;

import static com.vmturbo.extractor.models.ModelDefinitions.ATTRS;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CAPACITY;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CONSUMED;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_CURRENT;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_PROVIDER;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.COMMODITY_UTILIZATION;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_HASH_AS_HASH;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_NAME;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_STATE;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_TYPE_AS_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.ENVIRONMENT_TYPE;
import static com.vmturbo.extractor.models.ModelDefinitions.FIRST_SEEN;
import static com.vmturbo.extractor.models.ModelDefinitions.LAST_SEEN;
import static com.vmturbo.extractor.models.ModelDefinitions.METRIC_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.REPORTING_MODEL;
import static com.vmturbo.extractor.models.ModelDefinitions.SCOPED_OIDS;
import static com.vmturbo.extractor.models.ModelDefinitions.TIME;

import java.io.IOException;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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

import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
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
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.DslUpdateRecordSink;
import com.vmturbo.extractor.models.DslUpsertRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Writer that extracts entity and metric data from a topology and persists it to the database.
 */
public class EntityMetricWriter extends TopologyWriterBase {
    private static final Logger logger = LogManager.getLogger();

    private static final Long[] EMPTY_SCOPE = new Long[0];

    private static final Printer jsonPrinter = JsonFormat.printer()
            .omittingInsignificantWhitespace()
            .sortingMapKeys()
            .preservingProtoFieldNames();

    private static final ObjectMapper mapper = new ObjectMapper();

    private static final Set<String> includeColumnsForEntityHash = ImmutableSet.of(
            ENTITY_OID_AS_OID.getName(),
            ENTITY_NAME.getName(),
            ENTITY_TYPE_AS_TYPE.getName(),
            ENTITY_STATE.getName(),
            ENVIRONMENT_TYPE.getName(),
            ATTRS.getName(),
            SCOPED_OIDS.getName());

    // keep the previous hash to compare with current hash
    private static final Long2LongMap entityHash = new Long2LongOpenHashMap();
    // last seen timestamps for hashes seen since last update
    private static final Long2LongMap hashLastSeen = new Long2LongOpenHashMap();
    // TODO this should be probably in consul, maybe the maps too... how big can consul storage get?
    private static Long timeOfLastSeenUpdate;

    private final Long2ObjectMap<Record> entityRecordsMap = new Long2ObjectOpenHashMap<>();

    private final Long2ObjectMap<List<Record>> metricRecordsMap = new Long2ObjectOpenHashMap<>();
    private Timestamp firstSeenTime;
    private Timestamp lastSeenTime;


    /**
     * Create a new writer instance.
     *
     * @param dbEndpoint db endpoint for persisting data
     * @param pool       thread pool
     */
    public EntityMetricWriter(final DbEndpoint dbEndpoint, final ExecutorService pool) {
        super(dbEndpoint, REPORTING_MODEL, pool);
    }

    @Override
    public InterruptibleConsumer<TopologyEntityDTO> startTopology(final TopologyInfo topologyInfo,
            final Map<Long, List<Grouping>> entityToGroups, final WriterConfig config,
            final MultiStageTimer timer) throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
        final ImmutableList<String> conflictColumns
                = ImmutableList.of(ENTITY_OID_AS_OID.getName(), ENTITY_HASH_AS_HASH.getName());
        final ImmutableList<String> updateColumns = ImmutableList.of(LAST_SEEN.getName());
        final DSLContext dsl = dbEndpoint.dslContext();
        // super would attach an inserting sink to the entity table, but we need an upserting sink
        // so we do that here, and that will cause super to skip this table
        ENTITY_TABLE.attach(new DslUpsertRecordSink(dsl, ENTITY_TABLE, REPORTING_MODEL, config,
                pool, "upsert", conflictColumns, updateColumns), true);
        final InterruptibleConsumer<TopologyEntityDTO> result = super.startTopology(topologyInfo, entityToGroups, config, timer);
        computeTimestamps();
        return result;
    }

    private void computeTimestamps() {
        if (timeOfLastSeenUpdate == null) {
            timeOfLastSeenUpdate = topologyInfo.getCreationTime();
        }
        this.firstSeenTime = new Timestamp(topologyInfo.getCreationTime());
        this.lastSeenTime = new Timestamp(timeOfLastSeenUpdate + TimeUnit.MINUTES.toMillis(
                config.lastSeenUpdateIntervalMinutes() + config.lastSeenAdditionalFuzzMinutes()));
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO e) {
        final long oid = e.getOid();
        Record entitiesRecord = new Record(ENTITY_TABLE);
        entitiesRecord.set(ENTITY_OID_AS_OID, oid);
        entitiesRecord.set(ENTITY_TYPE_AS_TYPE, EntityType.forNumber(e.getEntityType()).name());
        entitiesRecord.set(ENTITY_NAME, e.getDisplayName());
        entitiesRecord.setIf(e.hasEnvironmentType(), ENVIRONMENT_TYPE, () -> e.getEnvironmentType().name());
        entitiesRecord.setIf(e.hasEntityState(), ENTITY_STATE, () -> e.getEntityState().name());
        try {
            entitiesRecord.set(ATTRS, getTypeSpecificInfoJson(e));
        } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
            logger.error("Failed to record type-specific info for entity {}", oid);
        }
        entitiesRecord.set(FIRST_SEEN, firstSeenTime);
        entitiesRecord.set(LAST_SEEN, lastSeenTime);
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

                Record r = new Record(METRIC_TABLE);
                r.set(TIME, firstSeenTime);
                r.set(ENTITY_OID, oid);
                r.set(COMMODITY_PROVIDER, producer);
                r.set(COMMODITY_TYPE, type);
                r.set(COMMODITY_CONSUMED, sumUsed);
                r.set(COMMODITY_PROVIDER, producer);
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

            Record r = new Record(METRIC_TABLE);
            r.set(TIME, firstSeenTime);
            r.set(ENTITY_OID, oid);
            r.set(COMMODITY_TYPE, type);
            r.set(COMMODITY_CAPACITY, sumCap);
            r.set(COMMODITY_CURRENT, sumUsed);
            r.set(COMMODITY_UTILIZATION, sumCap == 0 ? 0 : sumUsed / sumCap);
            metricRecords.add(r);
        });
    }

    @Override
    public int finish(final Map<Long, Set<Long>> entitiesToRelated)
            throws InterruptedException, UnsupportedDialectException, SQLException {
        writeGroupsAsEntities();
        upsertEntityRecords(entitiesToRelated);
        writeMetricRecords();
        // detach any sink we have on entity table (since we may need a new one before we're done)
        int n = super.finish(entitiesToRelated);
        updateLastSeenValues();
        return n;
    }

    private void writeGroupsAsEntities() {
        entityToGroups.values().stream()
                .flatMap(Collection::stream)
                .distinct().forEach(group -> {
            final GroupDefinition def = group.getDefinition();
            Record r = new Record(ENTITY_TABLE);
            r.set(ENTITY_OID_AS_OID, group.getId());
            r.set(ENTITY_NAME, def.getDisplayName());
            r.set(ENTITY_TYPE_AS_TYPE, def.getType().name());
            r.set(ENTITY_HASH_AS_HASH, r.getXxHash(includeColumnsForEntityHash));
            r.set(SCOPED_OIDS, EMPTY_SCOPE);
            r.set(FIRST_SEEN, firstSeenTime);
            r.set(LAST_SEEN, lastSeenTime);
            final JsonString attrs;
            try {
                attrs = getGroupJson(group);
                r.set(ATTRS, attrs);
            } catch (JsonProcessingException e) {
                logger.error("Failed to record group attributes for group {}", group.getId());
            }
            entityRecordsMap.put(group.getId(), r);
        });
    }

    private void upsertEntityRecords(final Map<Long, Set<Long>> entitiesToRelated) {
        entityRecordsMap.long2ObjectEntrySet().forEach(entry -> {
            long oid = entry.getLongKey();
            Record record = entry.getValue();
            final Long[] scope = getRelatedEntitiesAndGroups(oid, entitiesToRelated);
            record.set(SCOPED_OIDS, scope);

            // only store entity if hash changes
            long newHash = record.getXxHash(includeColumnsForEntityHash);
            Long oldHash = entityHash.containsKey(oid) ? entityHash.get(oid) : null;
            if (!Objects.equals(oldHash, newHash)) {
                try (Record r = ENTITY_TABLE.open(record)) {
                    r.set(ENTITY_HASH_AS_HASH, newHash);
                }
                entityHash.put(oid, newHash);
            }
            // keep track of when we last saw current hash
            hashLastSeen.put(newHash, topologyInfo.getCreationTime());
        });
    }

    private Long[] getRelatedEntitiesAndGroups(long oid, Map<Long, Set<Long>> entityToRelated) {
        final Set<Long> related = entityToRelated.getOrDefault(oid, Collections.emptySet());
        Set<Long> result = new LongOpenHashSet(related);
        related.stream()
                .map(id -> entityToGroups.getOrDefault(id, Collections.emptyList()))
                .flatMap(Collection::stream)
                .mapToLong(Grouping::getId)
                .forEach(result::add);
        return result.toArray(new Long[0]);
    }

    private void writeMetricRecords() {
        metricRecordsMap.long2ObjectEntrySet().forEach(entry -> {
            long oid = entry.getLongKey();
            Long hash = entityHash.get(oid);
            entry.getValue().forEach(partialMetricRecord -> {
                try (Record r = METRIC_TABLE.open(partialMetricRecord)) {
                    r.set(ENTITY_HASH, hash);
                }
            });
        });
    }

    private void updateLastSeenValues() throws UnsupportedDialectException, SQLException, InterruptedException {
        try {
            List<String> includeColumns = ImmutableList
                    .of(ENTITY_HASH_AS_HASH.getName(), LAST_SEEN.getName());
            final List<String> matchColumns = ImmutableList.of(ENTITY_HASH_AS_HASH.getName());
            final List<String> updateColumns = ImmutableList.of(LAST_SEEN.getName());
            final DSLContext dsl = dbEndpoint.dslContext();
            ENTITY_TABLE.attach(new DslUpdateRecordSink(dsl, ENTITY_TABLE, REPORTING_MODEL, config,
                    pool, "upd_times", includeColumns, matchColumns, updateColumns), true);
            updateLastSeenForDroppedEntities();
            maybeUpdateLastSeenForCurrentEntities();
        } finally {
            ENTITY_TABLE.detach();
        }
    }

    private void updateLastSeenForDroppedEntities() {
        LongSet droppedHashes = new LongOpenHashSet();
        hashLastSeen.long2LongEntrySet().stream()
                .filter(e -> e.getLongValue() != topologyInfo.getCreationTime())
                // here for entities that no longer appear in current topology, or appear with
                // a different hash. We can change their current last-seen estimate with a precise
                // value to improve query performance, and remove them from our map
                .peek(e -> droppedHashes.add(e.getLongKey()))
                .forEach(e -> {
                    try (Record r = ENTITY_TABLE.open()) {
                        r.set(ENTITY_HASH_AS_HASH, e.getLongKey());
                        r.set(LAST_SEEN, new Timestamp(e.getLongValue()));
                    }
                });
        // drop tracked hashes that disappeared this cycle
        LongStream.of(droppedHashes.toLongArray()).forEach(hashLastSeen::remove);
        // After that pruning, any orphaned entries in entity-hash map can be removed.
        LongSet orphanedOids = new LongOpenHashSet();
        entityHash.long2LongEntrySet().forEach(e -> {
            if (!hashLastSeen.containsKey(e.getLongValue())) {
                orphanedOids.add(e.getLongKey());
            }
        });
        LongStream.of(orphanedOids.toLongArray()).forEach(entityHash::remove);
    }

    private void maybeUpdateLastSeenForCurrentEntities() {
        final long thisCycleTime = topologyInfo.getCreationTime();
        final long lastSeenStaleness = thisCycleTime - timeOfLastSeenUpdate;
        if (lastSeenStaleness >= TimeUnit.MINUTES.toMillis(config.lastSeenUpdateIntervalMinutes())) {
            hashLastSeen.long2LongEntrySet().forEach(e -> {
                // update lastSeen for entities that were in this cycle
            });
            timeOfLastSeenUpdate = topologyInfo.getCreationTime();
        }
    }

    private JsonString getTypeSpecificInfoJson(TopologyEntityDTO entity) throws
            InvalidProtocolBufferException {
        final TypeSpecificInfo tsi = entity.hasTypeSpecificInfo() ? entity.getTypeSpecificInfo() : null;
        final MessageOrBuilder trimmed = tsi != null ? stripUnwantedFields(tsi) : null;
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

    private MessageOrBuilder stripUnwantedFields(TypeSpecificInfo tsi) {
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
            case TYPE_NOT_SET:
                return null;
            default:
                throw new IllegalArgumentException("Unrecognized type-specific-info case: "
                        + tsi.getTypeCase().name());
        }
    }
}
