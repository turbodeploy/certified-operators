package com.vmturbo.extractor.search;

import static com.vmturbo.extractor.models.ModelDefinitions.ATTRS;
import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_ENTITY_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_MODEL;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.models.Column.JsonString;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.TopologyWriterBase;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;

/**
 * Writer that extracts entity/group data from a topology and fetch all necessary aspects of the an
 * entity/group from other components, then persists them to the database for use by search/sort/filter.
 */
public class SearchEntityWriter extends TopologyWriterBase {

    private static final Logger logger = LogManager.getLogger();

    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * List of patchers for entity fields which are on {@link TopologyEntityDTO}.
     */
    private static final List<EntityRecordPatcher<TopologyEntityDTO>> ENTITY_PATCHERS_FOR_FIELDS_ON_TED =
            ImmutableList.of(
                    new PrimitiveFieldsOnTEDPatcher(),
                    new CommoditiesPatcher()
            );

    /**
     * List of patchers for entity fields which are NOT on {@link TopologyEntityDTO}, like
     * num_actions, severity which come from ActionOrchestrator.
     */
    private static final List<EntityRecordPatcher<DataProvider>> ENTITY_PATCHERS_FOR_FIELDS_NOT_ON_TED =
            ImmutableList.of(
                    new PrimitiveFieldsNotOnTEDPatcher(),
                    new RelatedActionsPatcher(),
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
                    new GroupPrimitiveFieldsOnGroupingPatcher()
            );

    /**
     * List of patchers for group fields which are NOT available on {@link Grouping}, like action
     * count, severity, commodities, etc.
     */
    private static final List<EntityRecordPatcher<DataProvider>> GROUP_PATCHERS_FOR_FIELDS_NOT_ON_GROUPING =
            ImmutableList.of(
                    new GroupRelatedActionsPatcher(),
                    new GroupPrimitiveFieldsNotOnGroupingPatcher(),
                    new GroupMemberFieldPatcher(),
                    new GroupAggregatedCommoditiesPatcher()
            );

    /**
     * List of partial records for each entity, which also contain relevant info for use by patchers.
     */
    private final ObjectList<PartialRecordInfo> partialRecordInfos = new ObjectArrayList<>();

    /**
     * Create a new writer instance.
     *
     * @param dbEndpoint db endpoint for persisting data
     * @param pool thread pool
     */
    public SearchEntityWriter(final DbEndpoint dbEndpoint, final ExecutorService pool) {
        super(dbEndpoint, SEARCH_MODEL, pool);
    }

    @Override
    public Consumer<TopologyEntityDTO> startTopology(final TopologyInfo topologyInfo,
            final WriterConfig config, final MultiStageTimer timer)
            throws IOException, UnsupportedDialectException, SQLException {
        return super.startTopology(topologyInfo, config, timer);
    }

    @Override
    protected void writeEntity(final TopologyEntityDTO entity) {
        if (!SearchMetadataUtils.hasMetadata(entity.getEntityType())) {
            // this is legitimate, since not all entities are ingested
            logger.trace("Skipping entity {} of type {} due to lack of metadata definition",
                    entity.getOid(), EntityType.forNumber(entity.getEntityType()));
            return;
        }

        // create a new record for this entity
        final Record entityRecord = new Record(SEARCH_ENTITY_TABLE);
        // create an attrs map for the jsonb column
        final Map<String, Object> attrs = new HashMap<>();
        final PartialRecordInfo partialRecordInfo = new PartialRecordInfo(
                entity.getOid(), entity.getEntityType(), entityRecord, attrs);
        // save partial records so other info (like related entities, actions, etc.) can be
        // added later during finish processing
        partialRecordInfos.add(partialRecordInfo);

        // add basic fields which are available on TopologyEntityDTO (TED), like name, state,
        // commodities, type specific info, etc.
        ENTITY_PATCHERS_FOR_FIELDS_ON_TED.forEach(patcher -> patcher.patch(partialRecordInfo, entity));
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
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter entitiesReplacer = SEARCH_ENTITY_TABLE.open(getEntityReplacerSink(dsl))) {
            // write entities
            partialRecordInfos.forEach(recordInfo -> {
                // add all other info which are not available on TopologyEntityDTO (TED)
                ENTITY_PATCHERS_FOR_FIELDS_NOT_ON_TED.forEach(patcher ->
                        patcher.patch(recordInfo, dataProvider));
                // insert into db
                try (Record r = entitiesReplacer.open(recordInfo.record)) {
                    // if jsonb column is not empty, add it to the record
                    if (!recordInfo.attrs.isEmpty()) {
                        r.set(ATTRS, new JsonString(mapper.writeValueAsString(recordInfo.attrs)));
                    }
                } catch (JsonProcessingException e) {
                    logger.error("Failed to record jsonb attributes for entity {}", recordInfo.oid, e);
                }
            });
            counter.addAndGet(partialRecordInfos.size());

            // write groups
            dataProvider.getAllGroups().forEach(group -> {
                if (!SearchMetadataUtils.hasMetadata(group.getDefinition().getType())) {
                    // do not ingest if no metadata defined for the group
                    logger.trace("Skipping group {} of type {} due to lack of metadata definition",
                            group.getId(), group.getDefinition().getType());
                    return;
                }

                final Record groupRecord = new Record(SEARCH_ENTITY_TABLE);
                final Map<String, Object> attrs = new HashMap<>();
                final PartialRecordInfo partialRecordInfo = new PartialRecordInfo(
                        group.getId(), group.getDefinition().getType(), groupRecord, attrs);
                // patch primitive fields whose values come from Grouping
                GROUP_PATCHERS_FOR_FIELDS_ON_GROUPING.forEach(patcher ->
                        patcher.patch(partialRecordInfo, group));
                // patch other fields whose values come from other sources
                GROUP_PATCHERS_FOR_FIELDS_NOT_ON_GROUPING.forEach(patcher ->
                        patcher.patch(partialRecordInfo, dataProvider));
                // set attrs to record if not empty
                if (!attrs.isEmpty()) {
                    try {
                        groupRecord.set(ATTRS, new JsonString(mapper.writeValueAsString(attrs)));
                    } catch (JsonProcessingException e) {
                        logger.error("Failed to record group attributes for group {}", group.getId());
                    }
                }
                // insert into db
                entitiesReplacer.accept(groupRecord);
                counter.incrementAndGet();
            });
        }
        return counter.get();
    }

    @VisibleForTesting
    DslReplaceRecordSink getEntityReplacerSink(final DSLContext dsl) {
        return new DslReplaceRecordSink(dsl, SEARCH_ENTITY_TABLE, config, pool, "replace");
    }

    /**
     * Wrapper class containing the partial entity (or group) record, incomplete jsonb column
     * attributes and other entity (or group) information needed for ingestion.
     */
    protected static class PartialRecordInfo {
        /** oid of the entity (or group) for this record. */
        final long oid;
        /** type of the entity for this record if this is an entity record. */
        final int entityType;
        /** type of the group for this record if this is a group record. */
        final GroupType groupType;
        /** the partial record for an entity (or group) to be sent to database. */
        final Record record;
        /** attrs for the jsonb column in this record. */
        final Map<String, Object> attrs;

        /**
         * Constructor for creating a wrapper object for entity record.
         *
         * @param oid id of the entity this record is referring to
         * @param entityType type of the entity for this record
         * @param record the partial record for an entity to be sent to database
         * @param attrs attrs for the jsonb column in this record
         */
        PartialRecordInfo(long oid, int entityType, Record record, Map<String, Object> attrs) {
            this.oid = oid;
            this.entityType = entityType;
            this.record = record;
            this.attrs = attrs;
            this.groupType = null;
        }

        /**
         * Constructor for creating a wrapper object for group record.
         *
         * @param oid id of the group this record is referring to
         * @param groupType type of the group for this record
         * @param record the partial record for an group to be sent to database
         * @param attrs attrs for the jsonb column in this record
         */
        PartialRecordInfo(long oid, GroupType groupType, Record record, Map<String, Object> attrs) {
            this.oid = oid;
            this.groupType = groupType;
            this.record = record;
            this.attrs = attrs;
            this.entityType = -1;
        }
    }

    /**
     * Represents an operation which takes data from source {@link D} and patch it onto the field
     * {@link PartialRecordInfo#record}.
     *
     * @param <D> type of the source which provides the data for patching
     */
    @FunctionalInterface
    protected interface EntityRecordPatcher<D> {
        /**
         * Patch data from source to the entity record, before sending it to DB.
         *
         * @param recordInfo contains partial record and all helpful info for patching
         * @param dataProvider the object which provides the required data
         */
        void patch(PartialRecordInfo recordInfo, D dataProvider);
    }
}
