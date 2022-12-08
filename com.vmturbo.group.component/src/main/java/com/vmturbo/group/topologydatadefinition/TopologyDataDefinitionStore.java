package com.vmturbo.group.topologydatadefinition;

import static com.vmturbo.group.db.Tables.ALL_TOPO_DATA_DEFS;
import static com.vmturbo.group.db.Tables.MAN_DATA_DEFS_ASSOC_ENTITIES;
import static com.vmturbo.group.db.Tables.MAN_DATA_DEFS_ASSOC_GROUPS;
import static com.vmturbo.group.db.Tables.MAN_DATA_DEFS_DYN_CONNECT_FILTERS;
import static com.vmturbo.group.db.tables.AutoTopoDataDefs.AUTO_TOPO_DATA_DEFS;
import static com.vmturbo.group.db.tables.ManualTopoDataDefs.MANUAL_TOPO_DATA_DEFS;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.protobuf.InvalidProtocolBufferException;

import io.grpc.Status;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.DSLContext;
import org.jooq.Query;
import org.jooq.Record1;
import org.jooq.TableRecord;
import org.jooq.conf.StatementType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.springframework.util.StopWatch;

import com.vmturbo.common.protobuf.group.GroupDTO.GroupID;
import com.vmturbo.common.protobuf.group.GroupDTO.MemberType;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers;
import com.vmturbo.common.protobuf.group.GroupDTO.StaticMembers.StaticMembersByType;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.AutomatedEntityDefinition.TagBasedGenerationAndConnection;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.DynamicConnectionFilters;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinition.ManualEntityDefinition.AssociatedEntitySelectionCriteria;
import com.vmturbo.common.protobuf.group.TopologyDataDefinitionOuterClass.TopologyDataDefinitionEntry;
import com.vmturbo.components.api.ComponentGsonFactory;
import com.vmturbo.components.api.SetOnce;
import com.vmturbo.components.common.diagnostics.DiagnosticsAppender;
import com.vmturbo.components.common.diagnostics.DiagnosticsException;
import com.vmturbo.components.common.diagnostics.DiagsRestorable;
import com.vmturbo.group.db.tables.pojos.AutoTopoDataDefs;
import com.vmturbo.group.db.tables.pojos.ManDataDefsAssocEntities;
import com.vmturbo.group.db.tables.pojos.ManDataDefsAssocGroups;
import com.vmturbo.group.db.tables.pojos.ManDataDefsDynConnectFilters;
import com.vmturbo.group.db.tables.pojos.ManualTopoDataDefs;
import com.vmturbo.group.group.GroupDAO;
import com.vmturbo.group.service.StoreOperationException;
import com.vmturbo.identity.exceptions.IdentifierConflictException;
import com.vmturbo.identity.exceptions.IdentityStoreException;
import com.vmturbo.identity.store.IdentityStore;
import com.vmturbo.identity.store.IdentityStoreUpdate;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * DB backed store for TopologyDataDefinitions.
 */
public class TopologyDataDefinitionStore implements DiagsRestorable<DSLContext> {

    /**
     * The file name for the TopolodyDataDefinition dump collected from the
     * {@link TopologyDataDefinitionStore}.
     */
    private static final String TOPOLOGY_DATA_DEFINITION_DUMP_FILE = "topology_data_definition_dump";

    private static final Logger logger = LogManager.getLogger();

    private static final String NAME_ALREADY_IN_USE =
            "Cannot create topology entity definition '%s' because a definition with that "
                    + "name already exists.";

    private static final String MANUAL_TYPE = "MANUAL";

    private static final String AUTO_TYPE = "AUTO";

    private final DSLContext dslContext;

    private final IdentityStore<TopologyDataDefinition> identityStore;

    private final GroupDAO groupStore;

    /**
     * Create a TopologyDataDefinitionStore.
     *
     * @param dslContext for interacting with DB.
     * @param identityStore to create OIDs and get existing OIDs of TopologyDataDefinitions.
     * @param groupStore the groupStore for getting group related information.
     */
    public TopologyDataDefinitionStore(@Nonnull DSLContext dslContext,
                                       @Nonnull IdentityStore<TopologyDataDefinition> identityStore,
                                       @Nonnull GroupDAO groupStore) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.identityStore = Objects.requireNonNull(identityStore);
        this.groupStore = groupStore;
    }

    /**
     * Create a new TopologyDataDefinition and persist it.
     *
     * @param definition the {@link TopologyDataDefinition} to create.
     * @return {@link TopologyDataDefinitionEntry} that includes the OID and the
     * TopologyDataDefinition.
     * @throws IdentityStoreException if there is a problem fetching an OID for this
     * {@link TopologyDataDefinition}.
     * @throws StoreOperationException if there is already an existing {@link TopologyDataDefinition}
     * with the same matching attributes as this one.
     */
    public TopologyDataDefinitionEntry createTopologyDataDefinition(
            @Nonnull TopologyDataDefinition definition)
            throws IdentityStoreException, StoreOperationException {
        IdentityStoreUpdate<TopologyDataDefinition> update = identityStore.fetchOrAssignItemOids(
                Collections.singletonList(Objects.requireNonNull(definition)));
        final long oid = update.getOldItems().isEmpty() ? update.getNewItems().get(definition)
                : update.getOldItems().get(definition);
        final SetOnce<StoreOperationException> storeException = new SetOnce<>();
        final SetOnce<TopologyDataDefinitionEntry> result = new SetOnce<>();
        logger.debug("Creating topology data definition with oid {}", () -> oid);
        dslContext.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            try {
                result.trySetValue(createTopologyDataDefinition(transactionDsl, oid, definition));
            } catch (StoreOperationException e) {
                storeException.trySetValue(e);
            }
        });
        if (storeException.getValue().isPresent()) {
            logger.debug("Exception creating topology data definition {}", () ->
                    storeException.getValue().get());
            throw storeException.getValue().get();
        }
        logger.debug("Created topology data definition {}",  () -> result.getValue().get());
        return result.getValue().get();
    }

    private @Nonnull TopologyDataDefinitionEntry createTopologyDataDefinition(
            @Nonnull DSLContext context,
            long oid,
            @Nonnull TopologyDataDefinition definition) throws StoreOperationException {
        switch (definition.getTopologyDataDefinitionDetailsCase()) {
            case MANUAL_ENTITY_DEFINITION:
                createManualTopologyDataDefinition(context, oid, definition.getManualEntityDefinition());
                break;
            case AUTOMATED_ENTITY_DEFINITION:
                createAutomaticTopologyDataDefintion(context, oid,
                        definition.getAutomatedEntityDefinition());
                break;
            default:
                throw new StoreOperationException(Status.INVALID_ARGUMENT,
                        "Topology Data Definition is missing details: Must include either manual or"
                                + " automated topology data definition details");
        }
        return TopologyDataDefinitionEntry.newBuilder()
                .setId(oid)
                .setDefinition(definition)
                .build();

    }

    private void createAutomaticTopologyDataDefintion(@Nonnull DSLContext context, long oid,
            AutomatedEntityDefinition automatedEntityDefinition) throws StoreOperationException {
        if (oidExistsInDb(context, oid)) {
            throw new StoreOperationException(Status.ALREADY_EXISTS,
                    String.format(NAME_ALREADY_IN_USE, automatedEntityDefinition.getNamingPrefix()));
        }
        AutoTopoDataDefs definitionPojo = createAutoDefinitionPojo(oid,
                automatedEntityDefinition);
        context.newRecord(AUTO_TOPO_DATA_DEFS, definitionPojo).store();
    }

    private boolean oidExistsInDb(@Nonnull DSLContext context, long oid) {
        return context.fetchExists(context.selectFrom(ALL_TOPO_DATA_DEFS)
                .where(ALL_TOPO_DATA_DEFS.ID.eq(oid)));
    }

    private void createManualTopologyDataDefinition(@Nonnull DSLContext context, long oid,
            @Nonnull ManualEntityDefinition manualEntityDefinition) throws StoreOperationException {
        if (oidExistsInDb(context, oid)) {
            throw new StoreOperationException(Status.ALREADY_EXISTS,
                    String.format(NAME_ALREADY_IN_USE, manualEntityDefinition.getEntityName()));
        }
        ManualTopoDataDefs definitionPojo = createManualDefinitionPojo(oid,
                manualEntityDefinition, context);
        logger.debug("Created POJO for manual topology data definition {}", () -> definitionPojo);
        context.newRecord(MANUAL_TOPO_DATA_DEFS, definitionPojo).store();
        context.batchInsert(
                generateInsertsFromManualDefinition(context, oid,
                        manualEntityDefinition)).execute();
    }

    private Collection<TableRecord<?>> generateInsertsFromManualDefinition(
            @Nonnull DSLContext context,
            long oid,
            @Nonnull ManualEntityDefinition manualEntityDefinition) {
        final Collection<TableRecord<?>> inserts = Lists.newArrayList();
        manualEntityDefinition.getAssociatedEntitiesList().forEach(
                associatedEntitySelectionCriterion -> {
                    switch (associatedEntitySelectionCriterion.getSelectionTypeCase()) {
                        case STATIC_ASSOCIATED_ENTITIES:
                            inserts.addAll(insertStaticMembers(context, oid,
                                    associatedEntitySelectionCriterion.getConnectedEntityType(),
                                    associatedEntitySelectionCriterion.getStaticAssociatedEntities()));
                            break;
                        case ASSOCIATED_GROUP:
                            inserts.add(insertAssociatedGroups(context, oid,
                                    associatedEntitySelectionCriterion.getConnectedEntityType(),
                                    associatedEntitySelectionCriterion.getAssociatedGroup()));
                            break;
                        case DYNAMIC_CONNECTION_FILTERS:
                            inserts.add(insertDynamicConnectionFilters(context, oid,
                                    associatedEntitySelectionCriterion.getConnectedEntityType(),
                                    associatedEntitySelectionCriterion.getDynamicConnectionFilters()));
                            break;
                    }
                });
        return inserts;
    }

    private Collection<? extends TableRecord<?>> insertStaticMembers(@Nonnull DSLContext context,
            long oid, @Nonnull EntityType entityType,
            @Nonnull StaticMembers staticMembers) {
        final Collection<TableRecord<?>> records = Lists.newArrayList();
        for (StaticMembersByType staticMember : staticMembers.getMembersByTypeList()) {
            for (Long memberId : staticMember.getMembersList()) {
                final ManDataDefsAssocEntities entityChild = new ManDataDefsAssocEntities(oid,
                        staticMember.getType().getEntity(), memberId);
                records.add(context.newRecord(MAN_DATA_DEFS_ASSOC_ENTITIES, entityChild));
            }
        }
        return records;
    }

    private TableRecord<?> insertAssociatedGroups(@Nonnull DSLContext context, long oid,
            @Nonnull EntityType entityType, @Nonnull GroupID groupId) {
        final ManDataDefsAssocGroups groupChild = new ManDataDefsAssocGroups(oid, groupId.getId(),
                entityType.getNumber());
        return context.newRecord(MAN_DATA_DEFS_ASSOC_GROUPS, groupChild);
    }

    private TableRecord<?> insertDynamicConnectionFilters(@Nonnull DSLContext context, long oid,
            @Nonnull EntityType entityType,
            @Nonnull DynamicConnectionFilters dynamicConnectionFilters) {
        final com.vmturbo.group.db.tables.pojos.ManDataDefsDynConnectFilters filterChild =
                new ManDataDefsDynConnectFilters(oid, entityType.getNumber(),
                        dynamicConnectionFilters.toByteArray());
        return context.newRecord(MAN_DATA_DEFS_DYN_CONNECT_FILTERS, filterChild);
    }

    private AutoTopoDataDefs createAutoDefinitionPojo(long oid,
            @Nonnull AutomatedEntityDefinition definition) {
        final AutoTopoDataDefs automatedDefinition = new AutoTopoDataDefs();
        automatedDefinition.setId(oid);
        automatedDefinition.setNamePrefix(definition.getNamingPrefix());
        automatedDefinition.setEntityType(definition.getEntityType().getNumber());
        automatedDefinition.setConnectedEntityType(definition.getConnectedEntityType().getNumber());
        automatedDefinition.setTagKey(definition.getTagGrouping().getTagKey());
        return automatedDefinition;
    }

    private ManualTopoDataDefs createManualDefinitionPojo(long oid,
            @Nonnull ManualEntityDefinition definition, @Nonnull DSLContext context)
            throws StoreOperationException {
        final ManualTopoDataDefs manualDefinitionPojo = new ManualTopoDataDefs();
        manualDefinitionPojo.setId(oid);
        manualDefinitionPojo.setName(definition.getEntityName());
        manualDefinitionPojo.setEntityType(definition.getEntityType().getNumber());
        manualDefinitionPojo.setContextBased(definition.getContextBased());
        if (definition.getAssociatedEntitiesList().isEmpty()) {
            logger.debug("Incorrectly formatted manual topology data definition for OID " + oid
                    + ". No associated entities defined.");
            throw new StoreOperationException(Status.INVALID_ARGUMENT,
                    "No associated entities specified for manual topology data definition.");
        }
        for (AssociatedEntitySelectionCriteria associatedEntitySelectionCriteria
                : definition.getAssociatedEntitiesList()) {
            // Validate associatedEntitySelectionCriteria types that we can validate at this point
            switch (associatedEntitySelectionCriteria.getSelectionTypeCase()) {
                case STATIC_ASSOCIATED_ENTITIES:
                    validateStaticMembers(
                            associatedEntitySelectionCriteria.getStaticAssociatedEntities(),
                            associatedEntitySelectionCriteria.getConnectedEntityType());
                    break;
                case ASSOCIATED_GROUP:
                    validateGroupMembers(context,
                            associatedEntitySelectionCriteria.getAssociatedGroup(),
                            associatedEntitySelectionCriteria.getConnectedEntityType());
                    break;
            }
        }
        return manualDefinitionPojo;
    }

    private void validateGroupMembers(@Nonnull DSLContext context, @Nonnull GroupID associatedGroup,
            @Nonnull EntityType connectedEntityType) throws StoreOperationException {
        // Look at the expected member types of the group and see if any don't match the
        // connectedEntityType
        Table<Long, MemberType, Boolean> expectedTypes =
                groupStore.getExpectedMemberTypes(context,
                        Collections.singleton(associatedGroup.getId()));
        if (expectedTypes.row(associatedGroup.getId()).keySet().stream()
                .anyMatch(memberType -> !memberType.hasEntity()
                        || memberType.getEntity() != connectedEntityType.getNumber())) {
            final String msg = "One or more members of the associated group are not of the "
                    + "specified entity type: " + connectedEntityType.name();
            logger.error(msg);
            throw new StoreOperationException(Status.INVALID_ARGUMENT, msg);
        }
    }

    private void validateStaticMembers(@Nonnull StaticMembers staticAssociatedEntities,
            @Nonnull EntityType entityType) throws StoreOperationException {
        // Check that no static members are groups and that all static entities are of the correct
        // entity type
        if (staticAssociatedEntities.getMembersByTypeList().stream()
                .filter(StaticMembersByType::hasType)
                .map(StaticMembersByType::getType)
                .anyMatch(memberType -> !memberType.hasEntity()
                        || memberType.getEntity() != entityType.getNumber())) {
            final String msg = "One or more manual topology data definition static members are "
                    + "not of the specified entity type: " + entityType.name();
            logger.error(msg);
            throw new StoreOperationException(Status.INVALID_ARGUMENT, msg);
        }
    }

    /**
     * Get the {@link TopologyDataDefinition} with the given OID.
     *
     * @param id the OID of the {@link TopologyDataDefinition}.
     * @return Optional of {@link TopologyDataDefinition} if it exist or Optional.empty otherwise.
     */
    public Optional<TopologyDataDefinition> getTopologyDataDefinition(long id) {
        return dslContext.transactionResult(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            return getTopologyDataDefinition(transactionDsl, id);
        });
   }

    private Optional<TopologyDataDefinition> getTopologyDataDefinition(@Nonnull DSLContext context,
            long id) {
        return Optional.ofNullable(getTopologyDataDefinitionsInternal(context,
                Collections.singleton(id)).get(id));
    }

    private Map<Long, TopologyDataDefinition> getTopologyDataDefinitionsInternal(
            @Nonnull DSLContext context,
            @Nonnull Collection<Long> oids) {
        if (oids.isEmpty()) {
            return Collections.emptyMap();
        }
        final Set<Long> manualDefOids = Sets.newHashSet();
        final Set<Long> autoDefOids = Sets.newHashSet();
        context.select(ALL_TOPO_DATA_DEFS.ID, ALL_TOPO_DATA_DEFS.TYPE)
                .from(ALL_TOPO_DATA_DEFS)
                .where(ALL_TOPO_DATA_DEFS.ID.in(oids))
                .forEach(record -> {
                    if (MANUAL_TYPE.equals(record.value2())) {
                        manualDefOids.add(record.value1());
                    } else if (AUTO_TYPE.equals(record.value2())) {
                        autoDefOids.add(record.value1());
                    } else {
                        logger.error("Unrecognized topology data definition type {} in table."
                                        + " Oid: {}", record.value2(), record.value1());
                    }
                });
        final StopWatch stopWatch = new StopWatch("Retrieving " + (manualDefOids.size()
                + autoDefOids.size()) + " topology data definitions");
        stopWatch.start("manual topology data definitions table");
        final Map<Long, ManualTopoDataDefs> manualTopoDataDefsMap =
                context.selectFrom(MANUAL_TOPO_DATA_DEFS)
                    .where(MANUAL_TOPO_DATA_DEFS.ID.in(manualDefOids))
                .fetchInto(ManualTopoDataDefs.class)
                .stream()
                .collect(Collectors.toMap(ManualTopoDataDefs::getId, Function.identity()));
        stopWatch.stop();
        stopWatch.start("associated entities");
        final Map<Long, Set<AssociatedEntitySelectionCriteria>> associatedEntitiesMap =
                getManualTopoDefAssocEntities(context, manualTopoDataDefsMap.keySet());
        stopWatch.stop();
        stopWatch.start("associated groups");
        final Map<Long, Set<AssociatedEntitySelectionCriteria>> associatedGroupIdMap =
                getManualTopoDefAssociatedGroups(context, manualTopoDataDefsMap.keySet());
        stopWatch.stop();
        stopWatch.start("associated entity filters");
        final Map<Long, Set<AssociatedEntitySelectionCriteria>> dynamicFiltersMap =
                getManualTopoDefDynConnectionFilters(context, manualTopoDataDefsMap.keySet());
        stopWatch.stop();

        stopWatch.start("building " +  manualTopoDataDefsMap.size()
                + "manual topology definitions");
        Map<Long, TopologyDataDefinition> returnMap = Maps.newHashMap();
        for (Map.Entry<Long, ManualTopoDataDefs> nxtEntry : manualTopoDataDefsMap.entrySet()) {
            final ManualEntityDefinition.Builder manualTopoDefBuilder =
                    ManualEntityDefinition.newBuilder();
            manualTopoDefBuilder.setEntityName(nxtEntry.getValue().getName());
            manualTopoDefBuilder.setContextBased(nxtEntry.getValue().getContextBased());
            manualTopoDefBuilder.setEntityType(EntityType.forNumber(
                    nxtEntry.getValue().getEntityType()));
            associatedEntitiesMap.getOrDefault(nxtEntry.getKey(), Collections.emptySet())
                    .forEach(staticMembers ->
                            manualTopoDefBuilder.addAssociatedEntities(staticMembers));
            associatedGroupIdMap.getOrDefault(nxtEntry.getKey(), Collections.emptySet())
                    .forEach(associatedGroup ->
                            manualTopoDefBuilder.addAssociatedEntities(associatedGroup));
            dynamicFiltersMap.getOrDefault(nxtEntry.getKey(), Collections.emptySet())
                    .forEach(dynConnectionFilters ->
                            manualTopoDefBuilder.addAssociatedEntities(dynConnectionFilters));
            final TopologyDataDefinition.Builder builder = TopologyDataDefinition.newBuilder();
            builder.setManualEntityDefinition(manualTopoDefBuilder);
            returnMap.put(nxtEntry.getKey(), builder.build());
        }
        stopWatch.stop();
        stopWatch.start("automatic topology data definitions table");
        final Map<Long, AutoTopoDataDefs> autoTopoDataDefsMap =
                context.selectFrom(AUTO_TOPO_DATA_DEFS)
                            .where(AUTO_TOPO_DATA_DEFS.ID.in(autoDefOids))
                        .fetchInto(AutoTopoDataDefs.class)
                        .stream()
                        .collect(Collectors.toMap(AutoTopoDataDefs::getId, Function.identity()));
        stopWatch.stop();
        stopWatch.start("building " + autoTopoDataDefsMap.size()
                + " automated topology data definitions");
        autoTopoDataDefsMap.entrySet().forEach(entry -> {
            final AutoTopoDataDefs autoTopoDataDefs = entry.getValue();
            returnMap.put(entry.getKey(), TopologyDataDefinition.newBuilder()
                    .setAutomatedEntityDefinition(AutomatedEntityDefinition.newBuilder()
                            .setEntityType(EntityType.forNumber(autoTopoDataDefs.getEntityType()))
                            .setNamingPrefix(autoTopoDataDefs.getNamePrefix())
                            .setConnectedEntityType(EntityType.forNumber(autoTopoDataDefs
                                    .getConnectedEntityType()))
                            .setTagGrouping(TagBasedGenerationAndConnection.newBuilder()
                                    .setTagKey(autoTopoDataDefs.getTagKey())
                                    .build())
                            .build())
                    .build());
        });
        stopWatch.stop();
        logger.debug(stopWatch::prettyPrint);
        return returnMap;
    }

    private Map<Long, Set<AssociatedEntitySelectionCriteria>> getManualTopoDefAssociatedGroups(
            @Nonnull DSLContext context,
            @Nonnull Set<Long> definitionIds) {
        return context.select(MAN_DATA_DEFS_ASSOC_GROUPS.DEFINITION_ID,
                        MAN_DATA_DEFS_ASSOC_GROUPS.GROUP_OID,
                        MAN_DATA_DEFS_ASSOC_GROUPS.ENTITY_TYPE)
                .from(MAN_DATA_DEFS_ASSOC_GROUPS)
                .where(MAN_DATA_DEFS_ASSOC_GROUPS.DEFINITION_ID.in(definitionIds))
                .fetch().stream()
                .collect(Collectors.toMap(record -> record.value1(), record -> Sets.newHashSet(
                        AssociatedEntitySelectionCriteria.newBuilder()
                                .setConnectedEntityType(EntityType.forNumber(record.value3()))
                                .setAssociatedGroup(GroupID.newBuilder()
                                        .setId(record.value2())
                                        .build())
                                .build()),
                        (oldVal, newVal) -> Sets.union(oldVal, newVal)));
    }

    private AssociatedEntitySelectionCriteria createAssociatedEntitySelectionCriteria(
            int entityNumber, @Nonnull Set<Long> associatedEntityIds) {
        return AssociatedEntitySelectionCriteria.newBuilder()
                .setConnectedEntityType(EntityType.forNumber(entityNumber))
                .setStaticAssociatedEntities(StaticMembers.newBuilder()
                        .addAllMembersByType(Collections.singletonList(
                                StaticMembersByType.newBuilder()
                                        .addAllMembers(associatedEntityIds)
                                        .setType(MemberType.newBuilder()
                                                .setEntity(entityNumber)
                                                .build())
                                        .build()))
                        .build())
                .build();
    }

    private Map<Long, Set<AssociatedEntitySelectionCriteria>> getManualTopoDefAssocEntities(
            @Nonnull DSLContext context,
            @Nonnull Collection<Long> definitionIds) {
        final Map<Long, Map<Integer, Set<Long>>> associatedEntities = new HashMap<>();
        // Read all the associated entities into a map that takes oid -> map of entity type to a set
        // of associated entity oids for that entity type
        context.select(MAN_DATA_DEFS_ASSOC_ENTITIES.DEFINITION_ID,
                    MAN_DATA_DEFS_ASSOC_ENTITIES.ENTITY_TYPE,
                    MAN_DATA_DEFS_ASSOC_ENTITIES.ENTITY_ID)
                .from(MAN_DATA_DEFS_ASSOC_ENTITIES)
                .where(MAN_DATA_DEFS_ASSOC_ENTITIES.DEFINITION_ID.in(definitionIds))
                .fetch()
                .forEach(record -> associatedEntities.computeIfAbsent(record.value1(), key -> new HashMap<>())
                        .computeIfAbsent(record.value2(), key -> new HashSet<>())
                        .add(record.value3()));

        // convert the map we just created into a map of topology data def oid to a set of
        // AssociatedEntitySelectionCriteria representing the associated static entities for that
        // topology data definition and return it
        return associatedEntities.entrySet().stream().collect(
                Collectors.toMap(entry -> entry.getKey(), entry -> entry.getValue()
                        .entrySet()
                        .stream()
                        .map(entityTypeEntry -> createAssociatedEntitySelectionCriteria(
                                entityTypeEntry.getKey(), entityTypeEntry.getValue()))
                        .collect(Collectors.toSet())));
    }

    private Map<Long, Set<AssociatedEntitySelectionCriteria>> getManualTopoDefDynConnectionFilters(
            @Nonnull DSLContext context, @Nonnull Collection<Long> definitionIds) {
        final List<ManDataDefsDynConnectFilters> filtersMap =
                context.selectFrom(MAN_DATA_DEFS_DYN_CONNECT_FILTERS)
                        .where(MAN_DATA_DEFS_DYN_CONNECT_FILTERS.DEFINITION_ID.in(definitionIds))
                        .fetchInto(ManDataDefsDynConnectFilters.class);

        final Map<Long, Set<AssociatedEntitySelectionCriteria>> oidToFiltersMap = Maps.newHashMap();
        for (ManDataDefsDynConnectFilters filters : filtersMap) {
            try {
                oidToFiltersMap.computeIfAbsent(filters.getDefinitionId(), id -> Sets.newHashSet())
                        .add(AssociatedEntitySelectionCriteria.newBuilder()
                                .setDynamicConnectionFilters(DynamicConnectionFilters.parseFrom(
                                        filters.getDynamicConnectionFilters()))
                                .setConnectedEntityType(
                                        EntityType.forNumber(filters.getEntityType()))
                                .build());
            } catch (InvalidProtocolBufferException e) {
                logger.error("Could not parse filters for manual topology data definition"
                        + " with oid {}.", filters.getDefinitionId(), e);
            }
        }
        return oidToFiltersMap;
    }

    /**
     * Return a collection of all {@link TopologyDataDefinitionEntry}s.
     *
     * @return {@link Collection} of all {@link TopologyDataDefinitionEntry}s.
     */
    public Collection<TopologyDataDefinitionEntry> getAllTopologyDataDefinitions() {
        final List<TopologyDataDefinitionEntry> allDefs = Lists.newArrayList();
        dslContext.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            List<Long> oids = transactionDsl.select(ALL_TOPO_DATA_DEFS.ID)
                    .from(ALL_TOPO_DATA_DEFS)
                    .fetch()
                    .stream()
                    .map(Record1::value1)
                    .collect(Collectors.toList());

            allDefs.addAll(getTopologyDataDefinitionsInternal(transactionDsl, oids).entrySet()
                    .stream()
                    .map(entry -> TopologyDataDefinitionEntry.newBuilder()
                            .setDefinition(entry.getValue())
                            .setId(entry.getKey())
                            .build())
                    .collect(Collectors.toList()));
        });
        return allDefs;
    }

    /**
     * Update an existing {@link TopologyDataDefinition} with a new value.
     *
     * @param id the OID of the existing {@link TopologyDataDefinition}.
     * @param updatedDefinition the new value of the {@link TopologyDataDefinition}.
     * @return Optional of {@link TopologyDataDefinition} if it exists or Optional.empty if not.
     * @throws StoreOperationException if topo data definition with id does not exist or if the
     * updated definition would conflict with an existing definition.
     * @throws IdentityStoreException if there is an exception updating the OID table in the
     * IdentityStore.
     */
    public Optional<TopologyDataDefinition> updateTopologyDataDefinition(long id,
                                                                    @Nonnull TopologyDataDefinition
                                                                    updatedDefinition)
            throws StoreOperationException, IdentityStoreException {
        final SetOnce<Optional<TopologyDataDefinition>> optTopoDataDef = new SetOnce<>();
        final SetOnce<StoreOperationException> exception = new SetOnce<>();
        dslContext.transaction(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            Optional<TopologyDataDefinition> optExistingDef = getTopologyDataDefinition(
                    transactionDsl, id);
            if (!optExistingDef.isPresent()) {
                logger.error("Update failed: no TopologyEntityDefinition with OID {} found.", id);
                exception.trySetValue(new StoreOperationException(Status.NOT_FOUND,
                        "Topology data definition " + id + " not found"));
                return;
            }

            // make sure that the updated definition does not conflict with any existing definition
            try {
                IdentityStoreUpdate<TopologyDataDefinition> update = identityStore.fetchOrAssignItemOids(
                        Collections.singletonList(Objects.requireNonNull(updatedDefinition)));
                // if this definition matches an existing oid, make sure that it is this same
                // definition.  If it is a different definition, it means we are trying to update
                // a definition in a way that conflicts with an already existing definition.
                if (!update.getOldItems().isEmpty()
                        && update.getOldItems().get(updatedDefinition) != id) {
                    if (oidExistsInDb(transactionDsl, update.getOldItems().get(updatedDefinition))) {
                        exception.trySetValue(new StoreOperationException(Status.ALREADY_EXISTS,
                                "A topology data definition that conflicts with the updated definition"
                                        + " already exists. Its oid is "
                                        + update.getOldItems().get(updatedDefinition)));
                        return;
                    }
                } else if (!update.getNewItems().isEmpty()) {
                    // if the updated definition created a new oid in the identityStore, remove it,
                    // since we won't be adding the updated definition under the new OID but will
                    // update the existing oid in the store.
                    identityStore.removeItemOids(Collections.singleton(update.getNewItems()
                            .get(updatedDefinition)));
                }
            } catch (IdentityStoreException e) {
                exception.trySetValue(new StoreOperationException(Status.INVALID_ARGUMENT,
                        e.getMessage(), e));
                return;
            }
            // if existing definition is manual, clean up related tables first
            final TopologyDataDefinition existingDef = optExistingDef.get();
            // update the existing definition if it is the same type as the updated definition.
            // if the update is a different type that the existing, delete the existing one and
            // create the updated one.
            if (!updateExistingDefinition(transactionDsl, existingDef, updatedDefinition, id)) {
                deleteTopologyDataDefinition(transactionDsl, id);
                createTopologyDataDefinition(transactionDsl, id, updatedDefinition);
            }
            optTopoDataDef.trySetValue(getTopologyDataDefinition(transactionDsl, id));
        });
        if (exception.getValue().isPresent()) {
            throw exception.getValue().get();
        }
        // If we set the TopoDataDef SetOnce to a non empty value, update the identity store to
        // make sure the oid reflects the updated definition and return the
        // Optional<TopologyDataDefinition>.
        if (optTopoDataDef.getValue().isPresent() && optTopoDataDef.getValue().get().isPresent()) {
            logger.debug("Updated TopologyDataDefinition with OID {}.  New value: {}.", () -> id,
                    () -> optTopoDataDef.getValue().get());
            // Update identity store so that it properly reflects the new attributes of the definition
            try {
                identityStore.updateItemAttributes(Collections.singletonMap(id, updatedDefinition));
            } catch (IdentifierConflictException e) {
                // this should never happen since we already checked for this condition earlier
                // in the method.
                throw new StoreOperationException(Status.ALREADY_EXISTS, "A topology data "
                        + "definition that conflicts with the updated definition already exists.");
            }
            return optTopoDataDef.getValue().get();
        } else {
            return Optional.empty();
        }
    }

    private boolean updateExistingDefinition(@Nonnull DSLContext dslContext,
            @Nonnull TopologyDataDefinition existingDef,
            @Nonnull TopologyDataDefinition updatedDef,
            long oid) throws StoreOperationException {
        if (existingDef.hasManualEntityDefinition()) {
            if (updatedDef.hasAutomatedEntityDefinition()) {
                return false;
            } else {
                // existing def was also manual, so just update it
                cleanManualDataDefChildTables(dslContext, oid);
                final Collection<TableRecord<?>> children = generateInsertsFromManualDefinition(
                        dslContext, oid, updatedDef.getManualEntityDefinition());
                // create update query
                createManualEntityDefinitionUpdate(dslContext, createManualDefinitionPojo(oid,
                        updatedDef.getManualEntityDefinition(), dslContext)).execute();
                dslContext.batchInsert(children).execute();
                return true;
            }
        } else {
            if (updatedDef.hasManualEntityDefinition()) {
                return false;
            } else {
                createAutomaticDefinitionUpdate(dslContext, createAutoDefinitionPojo(oid,
                        updatedDef.getAutomatedEntityDefinition())).execute();
                return true;
            }
        }
    }

    private Query createAutomaticDefinitionUpdate(@Nonnull DSLContext context,
            @Nonnull AutoTopoDataDefs autoDefinitionPojo) {
        Objects.requireNonNull(autoDefinitionPojo);
        return context.update(AUTO_TOPO_DATA_DEFS)
                .set(AUTO_TOPO_DATA_DEFS.ENTITY_TYPE, autoDefinitionPojo.getEntityType())
                .set(AUTO_TOPO_DATA_DEFS.NAME_PREFIX, autoDefinitionPojo.getNamePrefix())
                .set(AUTO_TOPO_DATA_DEFS.CONNECTED_ENTITY_TYPE,
                        autoDefinitionPojo.getConnectedEntityType())
                .set(AUTO_TOPO_DATA_DEFS.TAG_KEY, autoDefinitionPojo.getTagKey())
                .where(AUTO_TOPO_DATA_DEFS.ID.eq(autoDefinitionPojo.getId()));
    }

    private Query createManualEntityDefinitionUpdate(@Nonnull DSLContext context,
            @Nonnull ManualTopoDataDefs manualDataDefPojo) {
        Objects.requireNonNull(manualDataDefPojo);
        return context.update(MANUAL_TOPO_DATA_DEFS)
                .set(MANUAL_TOPO_DATA_DEFS.ENTITY_TYPE, manualDataDefPojo.getEntityType())
                .set(MANUAL_TOPO_DATA_DEFS.NAME, manualDataDefPojo.getName())
                .set(MANUAL_TOPO_DATA_DEFS.CONTEXT_BASED, manualDataDefPojo.getContextBased())
                .where(MANUAL_TOPO_DATA_DEFS.ID.eq(manualDataDefPojo.getId()));
    }

    private void cleanManualDataDefChildTables(@Nonnull DSLContext context, long oid) {
        context.deleteFrom(MAN_DATA_DEFS_ASSOC_ENTITIES)
                .where(MAN_DATA_DEFS_ASSOC_ENTITIES.DEFINITION_ID.eq(oid))
                .execute();
        context.deleteFrom(MAN_DATA_DEFS_DYN_CONNECT_FILTERS)
                .where(MAN_DATA_DEFS_DYN_CONNECT_FILTERS.DEFINITION_ID.eq(oid))
                .execute();
        context.deleteFrom(MAN_DATA_DEFS_ASSOC_GROUPS)
                .where(MAN_DATA_DEFS_ASSOC_GROUPS.DEFINITION_ID.eq(oid))
                .execute();
    }

    /**
     * Delete the {@link TopologyDataDefinition} with the given OID.
     *
     * @param id the OID of the {@link TopologyDataDefinition} to delete.
     * @return boolean indicating whether or not the {@link TopologyDataDefinition} was deleted.
     */
    public boolean deleteTopologyDataDefinition(long id) throws StoreOperationException {
        final SetOnce<StoreOperationException> exception = new SetOnce<>();
        return dslContext.transactionResult(configuration -> {
            DSLContext transactionDsl = DSL.using(configuration);
            boolean result = false;
            try {
                result = deleteTopologyDataDefinition(transactionDsl, id);
            } catch (StoreOperationException e) {
                throw new StoreOperationException(Status.ALREADY_EXISTS, "A topology data "
                        + "definition that conflicts with the updated definition already exists.");
            }
            return result;
        });
    }

    private boolean deleteTopologyDataDefinition(@Nonnull DSLContext context, final long id)
            throws StoreOperationException {
        Optional<TopologyDataDefinition> existingDef = getTopologyDataDefinition(context, id);
        if (!existingDef.isPresent()) {
            logger.warn("Attempt to delete non-existing TopologyDataDefinition with OID {}.",
                    id);
            return false;
        }

        if (existingDef.get().hasManualEntityDefinition()) {
            cleanManualDataDefChildTables(context, id);
            context.deleteFrom(MANUAL_TOPO_DATA_DEFS)
                    .where(MANUAL_TOPO_DATA_DEFS.ID.eq(id))
                    .execute();
            try {
                this.identityStore.removeItemOids(Collections.singleton(id));
            } catch (IdentityStoreException e) {
                throw new StoreOperationException(Status.INVALID_ARGUMENT,
                        e.getMessage(), e);
            }
        } else {
            context.deleteFrom(AUTO_TOPO_DATA_DEFS)
                    .where(AUTO_TOPO_DATA_DEFS.ID.eq(id))
                    .execute();
        }

        return true;
    }

    /**
     * Restore topology data definitions from diags.
     *
     * @param collectedDiags previously collected diagnostics
     * @param context the dsl context for transaction
     * @throws DiagnosticsException when an error is encountered.
     */
    @Override
    public void restoreDiags(@Nonnull final List<String> collectedDiags, @Nonnull DSLContext context)
            throws DiagnosticsException {
        try {
            final StatementType initialType = context.settings().getStatementType();
            context.settings().setStatementType(StatementType.STATIC_STATEMENT);
            final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
            // Replace all existing topology data definitions with those from the diags
            // clear any existing topology data definitions from the DB
            context.deleteFrom(MANUAL_TOPO_DATA_DEFS).execute();
            context.deleteFrom(AUTO_TOPO_DATA_DEFS).execute();
            logger.info("All existing topology data definitions deleted.");
            logger.info("Attempting to parse {} topology data definitions.",
                collectedDiags.size());
            final Collection<TopologyDataDefinitionEntry> allDefinitions = collectedDiags.stream()
                .map(string -> gson.fromJson(string, TopologyDataDefinitionEntry.class))
                .collect(Collectors.toList());
            logger.info("Parsed {} topology data definitions.", allDefinitions.size());
            logger.info("Creating topology data defintions.");
            int num_created = 0;
            int num_errors = 0;
            for (TopologyDataDefinitionEntry entry : allDefinitions) {
                try {
                    createTopologyDataDefinition(context, entry.getId(),
                        entry.getDefinition());
                    num_created += 1;
                } catch (StoreOperationException e) {
                    logger.error("Could not create topology data definition {} from diags.",
                        entry, e);
                    num_errors += 1;
                }
            }
            context.settings().setStatementType(initialType);
            logger.info("{} topology data definitions created. {} errors encountered.", num_created,
                num_errors);
        } catch (DataAccessException ex) {
            throw new DiagnosticsException("There was an error accessing database while restoring"
                + " topology data definitions.", ex);
        }

    }

    /**
     * Write out topology data definitions to diags.
     *
     * @param appender an appender to put diagnostics to. String by string.
     * @throws DiagnosticsException when an error is encountered.
     */
    @Override
    public void collectDiags(@Nonnull final DiagnosticsAppender appender)
            throws DiagnosticsException {
        final Gson gson = ComponentGsonFactory.createGsonNoPrettyPrint();
        final Iterator<TopologyDataDefinitionEntry> definitions =
                getAllTopologyDataDefinitions().iterator();
        int counter = 0;
        while (definitions.hasNext()) {
            final TopologyDataDefinitionEntry nextDef = definitions.next();
            appender.appendString(gson.toJson(nextDef));
            counter++;
        }
        logger.info("Collected diags for {} topology data definitions.", counter);
    }

    @Nonnull
    @Override
    public String getFileName() {
        return TOPOLOGY_DATA_DEFINITION_DUMP_FILE;
    }
}
