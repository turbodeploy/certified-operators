package com.vmturbo.extractor.action;

import static com.vmturbo.extractor.models.ModelDefinitions.ENTITY_OID_AS_OID;
import static com.vmturbo.extractor.models.ModelDefinitions.NUM_ACTIONS;
import static com.vmturbo.extractor.models.ModelDefinitions.SEARCH_ENTITY_ACTION_TABLE;
import static com.vmturbo.extractor.models.ModelDefinitions.SEVERITY_ENUM;

import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;

import com.vmturbo.common.protobuf.action.ARMEntityUtil;
import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionOrchestratorAction;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionPlanInfo.TypeInfoCase;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionSpec;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.InvolvedEntityCalculation;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.severity.SeverityMap;
import com.vmturbo.common.protobuf.severity.SeverityMapper;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.components.common.utils.MultiStageTimer;
import com.vmturbo.extractor.action.ActionWriter.IActionWriter;
import com.vmturbo.extractor.models.DslReplaceRecordSink;
import com.vmturbo.extractor.models.Table.Record;
import com.vmturbo.extractor.models.Table.TableWriter;
import com.vmturbo.extractor.search.EnumUtils.SeverityUtils;
import com.vmturbo.extractor.topology.DataProvider;
import com.vmturbo.extractor.topology.SupplyChainEntity;
import com.vmturbo.extractor.topology.WriterConfig;
import com.vmturbo.sql.utils.DbEndpoint;
import com.vmturbo.sql.utils.DbEndpoint.UnsupportedDialectException;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Write action data related to search.
 */
class SearchActionWriter implements IActionWriter {

    private static final Logger logger = LogManager.getLogger();

    /**
     * The action states which we care for search. This is based on the parameters in action stats
     * API request coming from UI.
     */
    private static final Set<Integer> SEARCH_INTERESTED_ACTION_STATES = ImmutableSet.of(
            ActionState.READY.getNumber(), ActionState.IN_PROGRESS.getNumber(),
            ActionState.QUEUED.getNumber(), ActionState.ACCEPTED.getNumber());

    /**
     * Types of InvolvedEntityCalculation which we need to calculate to ensure correct action count.
     */
    private static final List<InvolvedEntityCalculation> CALC_TYPES = ImmutableList.of(
            InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES,
            InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS
    );

    private final DataProvider dataProvider;
    private final DbEndpoint dbEndpoint;
    private final WriterConfig writerConfig;
    private final ExecutorService pool;
    private SeverityMap severityMap = SeverityMapper.empty();

    private final Long2ObjectMap<EnumMap<InvolvedEntityCalculation, LongSet>>
            actionsByEntityIdAndCalcType = new Long2ObjectArrayMap<>();

    SearchActionWriter(DataProvider dataProvider,
            DbEndpoint dbEndpoint, WriterConfig writerConfig, ExecutorService pool) {
        this.dataProvider = dataProvider;
        this.dbEndpoint = dbEndpoint;
        this.writerConfig = writerConfig;
        this.pool = pool;
    }

    @Override
    public void accept(ActionOrchestratorAction aoAction) {
        ActionSpec actionSpec = aoAction.getActionSpec();
        if (!SEARCH_INTERESTED_ACTION_STATES.contains(actionSpec.getActionState().getNumber())) {
            // do not care actions of these states
            return;
        }
        try {
            for (InvolvedEntityCalculation calcType : CALC_TYPES) {
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
    public void acceptSeverity(SeverityMap severityMap) {
        this.severityMap = severityMap;
    }

    @Override
    public void write(Map<TypeInfoCase, Long> lastActionWrite, TypeInfoCase actionPlanType,
            MultiStageTimer timer) throws UnsupportedDialectException, InterruptedException, SQLException {
        // get latest topology and supply chain
        final TopologyGraph<SupplyChainEntity> topology = dataProvider.getTopologyGraph();
        if (topology == null) {
            logger.error("Topology graph is not ready, skipping writing search actions for this cycle");
            return;
        }
        final Map<Long, Map<Integer, Set<Long>>> supplyChain = dataProvider.getSupplyChain();
        if (supplyChain == null) {
            logger.error("Supply chain is not ready, skipping writing search actions for this cycle");
            return;
        }
        final Long2ObjectMap<List<Long>> groupToLeafEntityIds = dataProvider.getGroupToLeafEntities();
        // process and write to db
        try (DSLContext dsl = dbEndpoint.dslContext();
             TableWriter actionsReplacer = SEARCH_ENTITY_ACTION_TABLE.open(getSearchActionReplacerSink(dsl))) {
            // write action data for entities (only write those with actions)
            timer.start("Write action data for search entities");
            topology.entities().parallel()
                    .forEach(entity -> {
                final long entityId = entity.getOid();
                final int entityType = entity.getEntityType();
                final InvolvedEntityCalculation calcType = getInvolvedEntityCalculation(entityType);
                final int count = (int)getActionsForEntity(entityId, entityType, calcType,
                        supplyChain).count();
                if (count > 0) {
                    actionsReplacer.accept(newActionRecord(entityId, count,
                            severityMap.getSeverity(entityId)));
                }
            });
            timer.stop();

            // write action data for all groups
            timer.start("Write action data for search groups");
            if (groupToLeafEntityIds != null) {
                groupToLeafEntityIds.long2ObjectEntrySet().parallelStream()
                        .forEach(entry -> {
                            final Record record = newActionRecord(entry.getLongKey(),
                                    getActionCountForGroup(entry.getValue(), topology, supplyChain),
                                    severityMap.calculateSeverity(entry.getValue()));
                            actionsReplacer.accept(record);
                        });
            }
            timer.stop();
        }
    }

    @VisibleForTesting
    DslReplaceRecordSink getSearchActionReplacerSink(final DSLContext dsl) {
        return new DslReplaceRecordSink(dsl, SEARCH_ENTITY_ACTION_TABLE, writerConfig, pool, "replace");
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
            InvolvedEntityCalculation calcType, Map<Long, Map<Integer, Set<Long>>> supplyChain) {
        // check if we need to expand to related types
        final Set<Integer> expandedEntityTypes;
        if (ARMEntityUtil.isARMEntityType(entityType)) {
            expandedEntityTypes = ARMEntityUtil.ENTITY_TYPES_BELOW_ARM;
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
                    supplyChain.getOrDefault(entityId, Collections.emptyMap())
                            .getOrDefault(expandedEntityType, Collections.emptySet())));
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
     * @param count count of the actions
     * @param severity severity of the action
     * @return new action record
     */
    private Record newActionRecord(long oid, int count, ActionDTO.Severity severity) {
        final Record actionRecord = new Record(SEARCH_ENTITY_ACTION_TABLE);
        actionRecord.set(ENTITY_OID_AS_OID, oid);
        actionRecord.set(NUM_ACTIONS, count);
        actionRecord.set(SEVERITY_ENUM, SeverityUtils.protoToDb(severity));
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
            TopologyGraph<SupplyChainEntity> graph,
            Map<Long, Map<Integer, Set<Long>>> supplyChain) {
        // for groups, we need to check if all members are ARM entities
        final boolean areAllARMEntities = leafEntities.stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(SupplyChainEntity::getEntityType)
                .allMatch(ARMEntityUtil::isARMEntityType);
        final InvolvedEntityCalculation calcType = getInvolvedEntityCalculation(areAllARMEntities);
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
     * Get InvolvedEntityCalculation based on whether this is arm entity.
     *
     * @param isARMEntity whether this is arm entity
     * @return InvolvedEntityCalculation
     */
    private static InvolvedEntityCalculation getInvolvedEntityCalculation(boolean isARMEntity) {
        return isARMEntity ? InvolvedEntityCalculation.INCLUDE_SOURCE_PROVIDERS_WITH_RISKS
                : InvolvedEntityCalculation.INCLUDE_ALL_STANDARD_INVOLVED_ENTITIES;
    }

    /**
     * Get InvolvedEntityCalculation based on given entity type.
     *
     * @param entityType entity type
     * @return InvolvedEntityCalculation
     */
    private static InvolvedEntityCalculation getInvolvedEntityCalculation(int entityType) {
        return getInvolvedEntityCalculation(ARMEntityUtil.isARMEntityType(entityType));
    }
}
