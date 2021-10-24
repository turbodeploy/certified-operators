package com.vmturbo.extractor.search;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.Column;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.DslReplaceSearchRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.models.Table;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.patchers.CommoditiesPatcher;
import com.vmturbo.extractor.patchers.GroupAggregatedCommoditiesPatcher;
import com.vmturbo.extractor.patchers.GroupMemberFieldPatcher;
import com.vmturbo.extractor.patchers.GroupPrimitiveFieldsOnGroupingPatcher;
import com.vmturbo.extractor.patchers.GroupRelatedEntitiesPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsNotOnTEDPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher;
import com.vmturbo.extractor.patchers.PrimitiveFieldsOnTEDPatcher.PatchCase;
import com.vmturbo.extractor.patchers.RelatedEntitiesPatcher;
import com.vmturbo.extractor.patchers.RelatedGroupsPatcher;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.TopologyWriterBase;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.DbFieldDescriptor;
import com.vmturbo.search.metadata.DbFieldDescriptor.Location;
import com.vmturbo.search.metadata.SearchMetadataMapping;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Writer that extracts entity/group data from a topology and fetch all necessary aspects of the an
 * entity/group from other components, then persists them to the database for use by
 * search/sort/filter.
 */
public class SearchEntityWriter extends TopologyWriterBase {

    private static final Logger logger = LogManager.getLogger();

    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        // we want key order to be retained in attrs conversions, to prevent unneeded hash changes
        mapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    }

    /**
     * List of patchers for entity fields which are on {@link TopologyEntityDTO}.
     */
    private static final List<EntityRecordPatcher<TopologyEntityDTO>> ENTITY_PATCHERS_FOR_FIELDS_ON_TED =
            ImmutableList.of(
                    new PrimitiveFieldsOnTEDPatcher(PatchCase.SEARCH, null),
                    new CommoditiesPatcher()
            );

    /**
     * List of patchers for entity fields which are NOT on {@link TopologyEntityDTO}, like
     * num_actions, severity which come from ActionOrchestrator.
     */
    private static final List<EntityRecordPatcher<DataProvider>> ENTITY_PATCHERS_FOR_FIELDS_NOT_ON_TED =
            ImmutableList.of(
                    new PrimitiveFieldsNotOnTEDPatcher(),
                    new RelatedEntitiesPatcher(),
                    new RelatedGroupsPatcher()
                    // todo: add cost and more
            );

    /**
     * List of patchers for group fields which are available on {@link Grouping}, like name,
     * origin, member types, etc.
     */
    private static final List<EntityRecordPatcher<Grouping>> GROUP_PATCHERS_FOR_FIELDS_ON_GROUPING =
            ImmutableList.of(
                    new GroupPrimitiveFieldsOnGroupingPatcher(PatchCase.SEARCH, null)
            );

    /**
     * List of patchers for group fields which are NOT available on {@link Grouping}, like action
     * count, severity, commodities, etc.
     */
    private static final List<EntityRecordPatcher<DataProvider>> GROUP_PATCHERS_FOR_FIELDS_NOT_ON_GROUPING =
            ImmutableList.of(
                    new GroupMemberFieldPatcher(),
                    new GroupRelatedEntitiesPatcher(),
                    new GroupAggregatedCommoditiesPatcher()
            );

    /**
     * List of partial records for each entity, which also contain relevant info for use by patchers.
     */
    private final ObjectList<PartialEntityInfo> partialRecordInfos = new ObjectArrayList<>();

    /**
     * Create a new writer instance.
     *
     * @param dbEndpoint db endpoint for persisting data
     * @param pool thread pool
     */
    public SearchEntityWriter(final DbEndpoint dbEndpoint, final ExecutorService pool) {
         super(dbEndpoint, pool);
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(final TopologyInfo topologyInfo,
            final WriterConfig config, final MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException, InterruptedException {
        return super.startTopology(topologyInfo, config, timer);
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO entity) {
        if (!SearchMetadataUtils.hasMetadata(entity.getEntityType())) {
            // do not ingest if the entity type is not defined in search metadata
            return;
        }

        final PartialEntityInfo partialRecordInfo = new PartialEntityInfo(
                entity.getOid(), entity.getEntityType());
        // save partial records so other info (like related entities, actions, etc.) can be
        // added later during finish processing
        partialRecordInfos.add(partialRecordInfo);

        // add basic fields which are available on TopologyEntityDTO (TED), like name, state,
        // commodities, type specific info, etc.
        ENTITY_PATCHERS_FOR_FIELDS_ON_TED.forEach(patcher -> patcher.fetch(partialRecordInfo, entity));
    }

    /**
     * Add more info to the entity record, prepare group record, and then insert into database.
     * This needs to be done on finish stage, since they depends on the entire topology, like
     * supply chain relations.
     *
     * {@inheritDoc}
     */
    @Override
    public int finish(final DataProvider dataProvider)
            throws UnsupportedDialectException, SQLException, InterruptedException {
        final AtomicInteger counter = new AtomicInteger();
        // patch entities
        logger.info("Starting stage: Patch entities");
        timer.start("Patch entities");
        List<Record> entityRecords = Collections.synchronizedList(new ObjectArrayList<>());
        // TODO get rid of parallelstream and change to real executor with manageable threads
        partialRecordInfos.parallelStream().forEach(recordInfo -> {
            // add all other info which are not available on TopologyEntityDTO (TED)
            ENTITY_PATCHERS_FOR_FIELDS_NOT_ON_TED.forEach(patcher ->
                    patcher.fetch(recordInfo, dataProvider));
            entityRecords.addAll(recordInfo.createRecords(ModelDefinitions.SEARCH_ENTITY_TABLE, PatchCase.SEARCH));
        });
        counter.addAndGet(partialRecordInfos.size());
        timer.stop();

        // patch groups
        logger.info("Starting stage: Patch groups");
        timer.start("Patch groups");
        // TODO get rid of parallelstream and change to real executor with manageable threads
        dataProvider.getAllGroups().parallel().forEach(group -> {
            if (!SearchMetadataUtils.hasMetadata(group.getDefinition().getType())) {
                // do not ingest if no metadata defined for the group
                logger.trace("Skipping group {} of type {} due to lack of metadata definition",
                        group.getId(), group.getDefinition().getType());
                return;
            }

            final PartialEntityInfo partialRecordInfo = new PartialEntityInfo(
                    group.getId(), group.getDefinition().getType());
            // patch primitive fields whose values come from Grouping
            GROUP_PATCHERS_FOR_FIELDS_ON_GROUPING.forEach(patcher ->
                    patcher.fetch(partialRecordInfo, group));
            // patch other fields whose values come from other sources
            GROUP_PATCHERS_FOR_FIELDS_NOT_ON_GROUPING.forEach(patcher ->
                    patcher.fetch(partialRecordInfo, dataProvider));
            entityRecords.addAll(partialRecordInfo
                            .createRecords(ModelDefinitions.SEARCH_ENTITY_TABLE, PatchCase.SEARCH));
            counter.incrementAndGet();
        });
        timer.stop();

        logger.info("Starting stage: Write entities and groups");
        timer.start("Write entities and groups");
        Map<Table, TableWriter> tableWriters = new HashMap<>();
        try (DSLContext dsl = dbEndpoint.dslContext()) {
            // insert entities into db - single-threaded transactional batch insert
            // TODO change to backend-specific csv write when security concerns are addressed
            // (i.e. mysql 'load from infile')
            // NB this is not transactional connection, we will autocommit every batch
            dsl.connection(conn -> {
                try {
                    for (Record record : entityRecords) {
                        Table table = record.getTable();
                        TableWriter writer = tableWriters.computeIfAbsent(table,
                                        (t) -> table.open(getReplacerSink(dsl, table, conn),
                                                    table.getName() + " search rows replacer",
                                                    logger));
                        writer.accept(record);
                    }
                } finally {
                    tableWriters.values().forEach(TableWriter::close);
                }
            });
        }
        timer.stop();

        return counter.get();
    }

    @VisibleForTesting
    DslReplaceSearchRecordSink getReplacerSink(DSLContext dsl, Table table, Connection conn) {
        return new DslReplaceSearchRecordSink(dsl, table, Location.fromName(table.getName()),
                        conn, "new", config.searchBatchSize());
    }

    /**
     * Wrapper class containing the partial entity (or group) information needed for ingestion.
     */
    public static class PartialEntityInfo {
        /** oid of the entity (or group) for this record. */
        private final long oid;
        /** type of the entity for this record if this is an entity record. */
        private final int entityType;
        /** type of the group for this record if this is a group record. */
        private final GroupType groupType;
        /** attribute values for this entity, in db format. */
        private final Map<SearchMetadataMapping, Object> attrs = new HashMap<>();
        /** additional non-search-context attributes that can only go in json. */
        private final Map<String, Object> jsonAttrs = new HashMap<>();

        /**
         * Constructor for creating a wrapper object for entity record.
         *
         * @param oid        id of the entity this record is referring to
         * @param entityType type of the entity for this record
         */
        public PartialEntityInfo(long oid, int entityType) {
            this.oid = oid;
            this.entityType = entityType;
            this.groupType = null;
        }

        /**
         * Constructor for creating a wrapper object for group record.
         *
         * @param oid id of the group this record is referring to
         * @param groupType type of the group for this record
         */
        PartialEntityInfo(long oid, GroupType groupType) {
            this.oid = oid;
            this.groupType = groupType;
            this.entityType = -1;
        }

        /**
         * Constructor for creating a wrapper object for entity record.
         *
         * @param oid entity OID
         */
        public PartialEntityInfo(long oid) {
            this.oid = oid;
            this.entityType = -1;
            this.groupType = null;
        }

        /**
         * Create database record(s) for an entity.
         *
         * @param mainTable the table to create the 'main' row in
         * @param patchCase in non-search patch case, only main table record should be populated
         * @return should contain at least one entry in main table and possibly additional rows in dependent tables
         */
        public List<Record> createRecords(Table mainTable, PatchCase patchCase) {
            List<Record> records = new ObjectArrayList<>();
            Record entityRecord = new Record(mainTable);
            records.add(entityRecord);

            Map<String, Object> jsonColumns = new HashMap<>();
            for (Map.Entry<SearchMetadataMapping, Object> descriptor2value : attrs.entrySet()) {
                SearchMetadataMapping field = descriptor2value.getKey();
                DbFieldDescriptor<?> searchDescriptor = field.getDbDescriptor();
                Object value = descriptor2value.getValue();
                if (value != null) {
                    switch (searchDescriptor.getPlacement()) {
                        case Column:
                            if (mainTable.getColumn(searchDescriptor.getColumn()) != null) {
                                entityRecord.set(searchDescriptor.getColumn(), value);
                                break;
                            }
                            // otherwise place in a row
                        case Row:
                            if (patchCase == PatchCase.SEARCH) {
                                records.addAll(createValueTypeRows(field, value));
                                break;
                            }
                            // otherwise add to json
                        case Json:
                            jsonColumns.put(searchDescriptor.getColumn(), value);
                            break;
                    }
                }
            }

            // if jsonb column is not empty, add it to the record
            jsonColumns.putAll(jsonAttrs);
            if (!jsonColumns.isEmpty() && patchCase != PatchCase.SEARCH) {
                try {
                    entityRecord.set(ModelDefinitions.ATTRS, new JsonString(mapper.writeValueAsString(jsonColumns)));
                } catch (JsonProcessingException e) {
                    logger.error("Failed to record jsonb attributes for {}", oid, e);
                }
            }

            return records;
        }

        private List<Record> createValueTypeRows(SearchMetadataMapping field, Object value) {
            List<Record> rows = new ObjectArrayList<>();
            if (value instanceof String) {
                rows.add(createStringRow(field, (String)value));
            } else if (value instanceof Number) {
                rows.add(createNumericRow(field, ((Number)value).doubleValue()));
            } else if (value instanceof Enum) {
                rows.add(createNumericRow(field, ((Enum)value).ordinal()));
            } else if (value instanceof Boolean) {
                rows.add(createNumericRow(field, ((Boolean)value) ? 1 : 0));
            } else if (value instanceof Collection) {
                Collection attrs = (Collection)value;
                for (Object collAttr : attrs) {
                    rows.addAll(createValueTypeRows(field, collAttr));
                }
            } else if (value != null) {
                logger.error("Unsupported value type for field " + field.getDbDescriptor() + ": " + value);
            }
            return rows;
        }

        private Record createNumericRow(SearchMetadataMapping field, double value) {
            return createValueTypeRow(ModelDefinitions.SEARCH_ENTITY_NUMERIC_TABLE, field,
                            ModelDefinitions.DOUBLE_VALUE, value);
        }

        private Record createStringRow(SearchMetadataMapping field, String value) {
            return createValueTypeRow(ModelDefinitions.SEARCH_ENTITY_STRING_TABLE, field,
                            ModelDefinitions.STRING_VALUE, value);
        }

        private <ValueT> Record createValueTypeRow(Table table, SearchMetadataMapping field,
                        Column<ValueT> col, ValueT value) {
            Record record = new Record(table);
            record.set(ModelDefinitions.ENTITY_OID_AS_OID, oid);
            record.set(ModelDefinitions.FIELD_NAME, field.ordinal());
            record.set(col, value);
            return record;
        }

        public long getOid() {
            return oid;
        }

        public int getEntityType() {
            return entityType;
        }

        public GroupType getGroupType() {
            return groupType;
        }

        /**
         * Set the value for an entity search field.
         *
         * @param key   attrs key
         * @param value value for key
         */
        public void putAttr(SearchMetadataMapping key, final Object value) {
            if (value != null) {
                attrs.put(key, value);
            }
        }

        /**
         * Set the value for a non-search entity field.
         *
         * @param key   attrs key
         * @param value value for key
         */
        public void putJsonAttr(String key, final Object value) {
            if (value != null) {
                jsonAttrs.put(key, value);
            }
        }

        /**
         * Get the attribute values in a free-form map for json.
         *
         * @return field name to value map
         */
        public Map<String, Object> getAttrs() {
            Map<String, Object> searchAttrs = attrs.entrySet().stream()
                            .collect(Collectors.toMap(
                                            entry -> entry.getKey().getDbDescriptor().getColumn(),
                                            entry -> entry.getValue()));
            searchAttrs.putAll(jsonAttrs);
            return searchAttrs;
        }
    }

    /**
     * Represents an operation which takes data from source {@link D} and patch it onto the field
     * {@link PartialEntityInfo#record}.
     *
     * @param <D> type of the source which provides the data for patching
     */
    @FunctionalInterface
    public interface EntityRecordPatcher<D> {
        /**
         * Get the data from context to the entity record, before sending it to DB.
         *
         * @param recordInfo   contains partial record and all helpful info for patching
         * @param dataProvider the object which provides the required data
         */
        void fetch(PartialEntityInfo recordInfo, D dataProvider);
    }
}
