package com.vmturbo.extractor.action;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.action.InvolvedEntityExpansionUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.severity.SeverityMap;
import com.vmturbo.common.protobuf.severity.SeverityMapper;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.PendingActionWriter.IActionWriter;
import com.vmturbo.extractor.models.DslReplaceSearchRecordSink;
import com.vmturbo.extractor.models.ModelDefinitions;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.search.EnumUtils.SearchEntityTypeUtils;
import com.vmturbo.extractor.search.SearchMetadataUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher;
import com.vmturbo.extractor.topology.fetcher.SupplyChainFetcher.SupplyChain;
import com.vmturbo.platform.common.dto.CommonDTO.GroupDTO.GroupType;
import com.vmturbo.search.metadata.DbFieldDescriptor.Location;
import com.vmturbo.search.metadata.SearchMetadataMapping;
import com.vmturbo.search.metadata.utils.SearchFiltersMapper;
import com.vmturbo.search.metadata.utils.SearchFiltersMapper.SearchFilterSpec;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Write action data related to search.
 */
class SearchPendingActionWriter implements IActionWriter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The action states which we care for search. This is based on the parameters in action stats
     * API request coming from UI.
     */
    private static final Set<Integer> SEARCH_INTERESTED_ACTION_STATES = ImmutableSet.of(
            ActionState.READY.getNumber(), ActionState.IN_PROGRESS.getNumber(),
            ActionState.QUEUED.getNumber(), ActionState.ACCEPTED.getNumber());

    private final TopologyGraph<SupplyChainEntity> topologyGraph;
    private final SupplyChain supplyChain;
    private final Long2ObjectMap<List<Long>> groupToLeafEntityIds;
    private final DbEndpoint dbEndpoint;
    private final WriterConfig writerConfig;
    private final ExecutorService pool;
    private final Map<Long, GroupType> groupToType;
    private SeverityMap severityMap = SeverityMapper.empty();

    private final Long2ObjectMap<EnumMap<InvolvedEntityCalculation, LongSet>>
            actionsByEntityIdAndCalcType = new Long2ObjectArrayMap<>();

    SearchPendingActionWriter(DataProvider dataProvider,
            DbEndpoint dbEndpoint, WriterConfig writerConfig, ExecutorService pool) {
        this.topologyGraph = dataProvider.getTopologyGraph();
        this.supplyChain = dataProvider.getSupplyChain();
        this.groupToLeafEntityIds = dataProvider.getGroupToLeafEntities();
        this.dbEndpoint = dbEndpoint;
        this.writerConfig = writerConfig;
        this.pool = pool;
        this.groupToType = dataProvider.getAllGroups().collect(Collectors.toMap(Grouping::getId,
                        group -> group.getDefinition().getType()));
    }

    @Override
    public void recordAction(ActionOrchestratorAction aoAction) {
        ActionSpec actionSpec = aoAction.getActionSpec();
        if (!SEARCH_INTERESTED_ACTION_STATES.contains(actionSpec.getActionState().getNumber())) {
            // do not care actions of these states
            return;
        }
        try {
            for (InvolvedEntityCalculation calcType : InvolvedEntityCalculation.values()) {
                ActionDTOUtil.getInvolvedEntityIds(actionSpec.getRecommendation(), calcType)
                        .forEach(involvedEntityId -> actionsByEntityIdAndCalcType
                                .computeIfAbsent((long)involvedEntityId,
                                        k -> new EnumMap<>(InvolvedEntityCalculation.class))
                                .computeIfAbsent(calcType, s -> new LongOpenHashSet())
                                .add(aoAction.getActionId()));
            }
        } catch (UnsupportedActionException e) {
            // this should not happen
            logger.error("Unsupported action {}", aoAction, e);
        }
    }

    @Override
    public boolean requireSeverity() {
        return true;
    }

    @Override
    public void acceptSeverity(SeverityMap severityMap) {
        this.severityMap = severityMap;
    }

    @Override
    public void write(MultiStageTimer timer)
            throws UnsupportedDialectException, InterruptedException, SQLException {
        // TODO parallelize calculation and processing using pool
        // then write fully prepared data sequentially with bulk import
        final SupplyChain supplyChain = calculateRelatedEntities(timer);

        // process and write to db
        try (DSLContext dsl = dbEndpoint.dslContext()) {
            // TODO change to backend-specific csv write when security concerns are addressed
            // (i.e. mysql 'load from infile')
            // NB this is not transactional connection, we will autocommit every batch
            dsl.connection(conn -> {
                try (TableWriter actionsReplacer = ModelDefinitions.SEARCH_ENTITY_ACTION_TABLE.open(
                                getSearchActionReplacerSink(dsl, conn), "Action Replacer", logger)) {
                    // write action data for entities (only write those with actions)
                    timer.start("Write action data for search entities");
                    for (SupplyChainEntity entity : topologyGraph.entities()
                                    .filter(e -> SearchMetadataUtils.hasMetadata(e.getEntityType()))
                                    .collect(Collectors.toSet())) {
                        final long entityId = entity.getOid();
                        final int entityType = entity.getEntityType();
                        final InvolvedEntityCalculation calcType = getInvolvedEntityCalculation(entityType);
                        final int count = (int)getActionsForEntity(entityId, entityType, calcType,
                                        supplyChain).count();
                        if (count > 0) {
                            actionsReplacer.accept(newActionRecord(entityId, entityType, count,
                                    severityMap.getSeverity(entityId)));
                        }
                    }
                    timer.stop();

                    // write action data for all groups
                    timer.start("Write action data for search groups");
                    if (groupToLeafEntityIds != null) {
                        for (Long2ObjectMap.Entry<List<Long>> entry : groupToLeafEntityIds.long2ObjectEntrySet()) {
                            final int count = getActionCountForGroup(entry.getValue(), topologyGraph, supplyChain);
                            if (count > 0) {
                                final Record record = newActionRecord(entry.getLongKey(),
                                                groupToType.getOrDefault(entry.getLongKey(),
                                                                GroupType.REGULAR).ordinal(),
                                                count,
                                                severityMap.calculateSeverity(entry.getValue()));
                                actionsReplacer.accept(record);
                            }
                        }
                    }
                    timer.stop();
                }
            });
        }
    }

    /**
     * Get the related entities required for ARM entities and some aggregated entities, whose
     * action count should be retrieved from related entities.
     *
     * @param timer timer
     * @return partial calculated supply chain
     */
    private SupplyChain calculateRelatedEntities(MultiStageTimer timer) {
        if (supplyChain != null && supplyChain.isFull()) {
            // use the cached supply chain if it's full
            return supplyChain;
        }
        // calculate supply chain on demand if it's not ready or partially calculated
        final Map<Long, Map<Integer, Set<Long>>> entityToRelated = new Long2ObjectOpenHashMap<>();
        final Map<Long, Map<Integer, Set<Long>>> syncEntityToRelated =
                Collections.synchronizedMap(entityToRelated);
        // calculate related entities for ARM entities first
        // TODO (OM-63758): maybe only calculate the supply chains from the top ARM entities
        timer.start("Calculate related entities for ARM entities");
        InvolvedEntityExpansionUtil.ENTITY_WITH_EXPAND_ENTITY_SET_MAP.keySet().forEach(type -> {
            topologyGraph.entitiesOfType(type).parallel().forEach(entity ->
                    SupplyChainFetcher.calculateFullSupplyChain(entity, topologyGraph, syncEntityToRelated));
        });
        timer.stop();

        // then process aggregated entities
        timer.start("Calculate related entities for aggregated entities");
        ApiEntityType.PROTO_ENTITY_TYPES_TO_EXPAND.forEach((type, relatedTypes) -> {
            final Map<Integer, SearchFilterSpec> searchFilterSpecs = getSearchFilterSpecs(type, relatedTypes);
            // if we have search filters defined for all related types, then use it.
            // these are for entity types (like DataCenter) with very few expanded types
            if (searchFilterSpecs.keySet().containsAll(relatedTypes)) {
                topologyGraph.entitiesOfType(type).parallel().forEach(entity ->
                        SupplyChainFetcher.calculatePartialSupplyChain(
                                entity, topologyGraph, searchFilterSpecs, syncEntityToRelated));
            } else {
                // otherwise use SupplyChainCalculator, these are usually for cloud entities
                // it's fine since there should not be many regions, zones, etc.
                topologyGraph.entitiesOfType(type).parallel().forEach(entity ->
                        SupplyChainFetcher.calculateFullSupplyChain(entity, topologyGraph, syncEntityToRelated));
            }
        });
        timer.stop();
        return new SupplyChain(entityToRelated, false);
    }

    /**
     * Get search filter specs for the related types of a given entity type.
     *
     * @param fromType from entity type
     * @param relatedTypes related entity types
     * @return map from related type to corresponding search filters
     */
    private Map<Integer, SearchFilterSpec> getSearchFilterSpecs(int fromType, Set<Integer> relatedTypes) {
        return relatedTypes.stream()
                .filter(related -> related != fromType)
                .map(related -> new AbstractMap.SimpleEntry<>(related, SearchFiltersMapper.getSearchFilterSpec(
                        SearchEntityTypeUtils.protoIntToApi(fromType),
                        SearchEntityTypeUtils.protoIntToApi(related))))
                .filter(entry -> entry.getValue() != null)
                .collect(Collectors.toMap(SimpleEntry::getKey, SimpleEntry::getValue));
    }

    @VisibleForTesting
    DslReplaceSearchRecordSink getSearchActionReplacerSink(DSLContext dsl, Connection conn) {
        return new DslReplaceSearchRecordSink(dsl, ModelDefinitions.SEARCH_ENTITY_ACTION_TABLE,
                        Location.Actions, conn, "new", writerConfig.searchBatchSize());
    }

    /**
     * Get the involved action ids for the given entity.
     *
     * @param entityId id of the entity
     * @param entityType type of the entity
     * @param calcType type of the InvolvedEntity calculation
     * @param supplyChain latest calculated supply chain
     * @return stream of action ids
     */
    private Stream<Long> getActionsForEntity(long entityId, int entityType,
            InvolvedEntityCalculation calcType, SupplyChain supplyChain) {
        // check if we need to expand to related types
        final Set<Integer> expandedEntityTypes;
        if (InvolvedEntityExpansionUtil.expansionRequiredEntityType(entityType)) {
            expandedEntityTypes = InvolvedEntityExpansionUtil.ENTITY_WITH_EXPAND_ENTITY_SET_MAP.get(entityType);
        } else {
            expandedEntityTypes = ApiEntityType.PROTO_ENTITY_TYPES_TO_EXPAND.getOrDefault(
                    entityType, Collections.emptySet());
        }
        // always add the entity itself
        final Set<Long> entities = Sets.newHashSet(entityId);
        if (!expandedEntityTypes.isEmpty()) {
            // need to expand this entity to related entities in the supply chain, like: expand
            // Region to all workloads in the region. we should also include the region itself,
            // since actions like buyRi involves region id in its action spec
            expandedEntityTypes.forEach(expandedEntityType -> entities.addAll(
                    supplyChain.getRelatedEntitiesOfType(entityId, expandedEntityType)));
        }
        return entities.stream().flatMap(entity -> {
            EnumMap<InvolvedEntityCalculation, LongSet> relatedActionsByCalcType =
                    actionsByEntityIdAndCalcType.get((long)entity);
            if (relatedActionsByCalcType == null) {
                return Stream.empty();
            }
            final Set<Long> actionIds = relatedActionsByCalcType.get(calcType);
            // do not use getOrDefault since Long2IntArrayMap may call both 'get'
            // and 'containsKey' which is expensive for large topology
            return actionIds == null ? Stream.empty() : actionIds.stream();
        }).distinct();
    }

    /**
     * Create a new action record.
     *
     * @param oid oid of the entity or group
     * @param type entity type
     * @param count count of the actions
     * @param severity severity of the action
     * @return new action record
     */
    private Record newActionRecord(long oid, int type, int count, ActionDTO.Severity severity) {
        final Record actionRecord = new Record(ModelDefinitions.SEARCH_ENTITY_ACTION_TABLE);
        actionRecord.set(SearchMetadataMapping.PRIMITIVE_OID.getDbDescriptor().getColumn(), oid);
        actionRecord.set(SearchMetadataMapping.PRIMITIVE_ENTITY_TYPE.getDbDescriptor().getColumn(), type);
        actionRecord.set(SearchMetadataMapping.RELATED_ACTION_COUNT.getDbDescriptor().getColumn(), count);
        actionRecord.set(SearchMetadataMapping.PRIMITIVE_SEVERITY.getDbDescriptor().getColumn(),
                        severity.ordinal());
        return actionRecord;
    }

    /**
     * Get actions for the given group.
     *
     * @param leafEntities leaf entities of the group
     * @param graph latest topology
     * @param supplyChain latest calculated supply chain
     * @return action count for the group
     */
    private int getActionCountForGroup(List<Long> leafEntities,
            TopologyGraph<SupplyChainEntity> graph, SupplyChain supplyChain) {
        // for groups, we need to check if all members are ARM entities
        final boolean areAllEntitiesRequiringExpansion = leafEntities.stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(SupplyChainEntity::getEntityType)
                .allMatch(InvolvedEntityExpansionUtil::expansionRequiredEntityType);
        final InvolvedEntityCalculation calcType = getInvolvedEntityCalculation(areAllEntitiesRequiringExpansion);
        return (int)leafEntities.stream()
                .flatMap(entityId -> graph.getEntity(entityId)
                        .map(entity -> getActionsForEntity(entityId, entity.getEntityType(),
                                calcType, supplyChain))
                        .orElse(Stream.empty()))
                // De-dupe the action ids.
                .distinct()
                .count();
    }

    /**
     * Get InvolvedEntityCalculation based on whether this is an entity that requires expansion.
     *
     * @param isExpansionRequiredEntity whether this is arm entity
     * @return InvolvedEntityCalculation
     */
    private static InvolvedEntityCalculation getInvolvedEntityCalculation(boolean isExpansionRequiredEntity) {
        return isExpansionRequiredEntity ? InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS
                : InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES;
    }

    /**
     * Get InvolvedEntityCalculation based on given entity type.
     *
     * @param entityType entity type
     * @return InvolvedEntityCalculation
     */
    private static InvolvedEntityCalculation getInvolvedEntityCalculation(int entityType) {
        return getInvolvedEntityCalculation(InvolvedEntityExpansionUtil.expansionRequiredEntityType(entityType));
    }
}
