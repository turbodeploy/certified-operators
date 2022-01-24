
package com.vmturbo.topology.processor.topology;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.PlanChanges.UtilizationLevel;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyAddition;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyMigration.MigrationReference;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyRemoval;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.TopologyReplace;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Edit;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Removed;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Replaced;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.platform.common.dto.CommonDTO.CommodityDTO.CommodityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.stitching.TopologyEntity.Builder;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.identity.IdentityProvider;
import com.vmturbo.topology.processor.template.TemplateConverterFactory;
import com.vmturbo.topology.processor.topology.clone.DefaultEntityCloneEditor;
import com.vmturbo.topology.processor.topology.clone.EntityCloneEditorFactory;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

/**
 * The {@link TopologyEditor} is responsible for applying a set of changes (reflected
 * by {@link ScenarioChange} objects) to a topology.
 *
 * <p>Topology editing is an important phase of the plan lifecycle, since a key part of plans
 * is testing the addition/removal/replacement of entities.
 */
public class TopologyEditor {
    private final Logger logger = LogManager.getLogger();

    private final TemplateConverterFactory templateConverterFactory;

    private final GroupServiceBlockingStub groupServiceClient;

    private final EntityCloneEditorFactory entityCloneEditorFactory;

    private static final Set<Integer> UTILIZATION_LEVEL_TYPES = ImmutableSet
            .of(CommodityType.CPU_VALUE, CommodityType.MEM_VALUE);

    TopologyEditor(@Nonnull final IdentityProvider identityProvider,
                   @Nonnull final TemplateConverterFactory templateConverterFactory,
                   @Nonnull final GroupServiceBlockingStub groupServiceClient) {
        Objects.requireNonNull(identityProvider);
        this.templateConverterFactory = Objects.requireNonNull(templateConverterFactory);
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        entityCloneEditorFactory = new EntityCloneEditorFactory(identityProvider);
    }

    /**
     * Apply a set of changes to a topology. The method will edit the
     * input topology in-place.
     *
     * @param topology The entities in the topology, arranged by ID.
     * @param changes The list of changes to make. Some of these changes may not be topology-related.
     *                We ignore those.
     * @param context Context containing topology info.
     * @param groupResolver The resolver to use when resolving group membership.
     * @param sourceEntities The source entities for the plan.
     * @param destinationEntities The destination entities for the plan.
     * @throws GroupResolutionException Thrown when we could not resolve groups.
     */
    public void editTopology(@Nonnull final Map<Long, TopologyEntity.Builder> topology,
                             @Nonnull final List<ScenarioChange> changes,
                             @Nonnull final TopologyPipelineContext context,
                             @Nonnull final GroupResolver groupResolver,
                             @Nonnull final Set<Long> sourceEntities,
                             @Nonnull final Set<Long> destinationEntities) throws GroupResolutionException {
        final TopologyInfo topologyInfo = context.getTopologyInfo();
        // Set shopTogether to false for all entities if it's not a alleviate pressure plan,
        // so SNM is not performed by default.
        // 1. For full scope custom plan,
        //    since shopTogether is false for all entities, we will not perform SNM on any entities.
        // 2. For add workload plans (VM template),
        //    the new entities created later from VM template will shop together by default,
        //    which means we will perform SNM on those new entities.
        // 3. For add workload plans (VM copy),
        //    the new entities created later from VM copy have the same settings as the VM copy.
        //    Since shopTogether is false for VM copy, shopTogether is also false for new entities,
        //    which means we will not perform SNM on those new entities.
        // 4. For hardware refresh plans,
        //    shopTogether will be reset to true later for all consumers of an entity to be replaced,
        //    which means we will perform SNM on those consumers.
        // 5. For alleviate pressure plans,
        //    set shopTogether to true for all entities. This plan is a scoped plan, so entities
        //    not related to the hot or cold clusters will be discarded later.
        // Related story: OM-44989
        boolean isAlleviatePressurePlan = TopologyDTOUtil.isAlleviatePressurePlan(topologyInfo);
        topology.forEach((oid, entity) ->
            entity.getEntityBuilder().getAnalysisSettingsBuilder()
                .setShopTogether(isAlleviatePressurePlan));

        Map<Long, Long> entityAdditions = new HashMap<>();
        Set<Long> entitiesToRemove = new HashSet<>();
        Set<Long> entitiesToReplace = new HashSet<>();
        final Map<Long, Long> templateToAdd = new HashMap<>();
        // Map key is template id, and value is the replaced topologyEntity.
        final Multimap<Long, Long> templateToReplacedEntity =
            ArrayListMultimap.create();
        final Map<Long, Grouping> groupIdToGroupMap = getGroups(changes);
        final TopologyGraph<TopologyEntity> topologyGraph =
            TopologyEntityTopologyGraphCreator.newGraph(topology);

        for (ScenarioChange change : changes) {
            if (change.hasTopologyAddition()) {
                final TopologyAddition addition = change.getTopologyAddition();
                int targetType = addition.getTargetEntityType();
                long additionCount = addition.hasAdditionCount() ? addition.getAdditionCount() : 1L;
                // user can choose a group whose member may not directly match with the addition type,
                // in such cases, we need to traverse up/down supply chain to find the real entities they
                // want to add
                if (addition.hasEntityId()) {
                    TopologyEntity.Builder entityToAdd = topology.get(addition.getEntityId());
                    if (addition.hasTargetEntityType() && entityToAdd.getEntityType() != targetType) {
                        // for instance: user can add VM from a pm cluster
                        addEntitiesToMap(targetType, additionCount, entityToAdd,
                                topologyGraph, entityAdditions)
                                .entrySet()
                                .forEach(entry -> entityAdditions.put(entry.getKey(), entry.getValue()));
                    } else {
                        addTopologyAdditionCount(entityAdditions, addition, addition.getEntityId());
                    }
                } else if (addition.hasTemplateId()) {
                    addTopologyAdditionCount(templateToAdd, addition, addition.getTemplateId());
                } else if (addition.hasGroupId()) {
                    Set<Long> entityToAddId = groupResolver.resolve(groupIdToGroupMap.get(addition.getGroupId()),
                            topologyGraph).getAllEntities();
                    for (long id : entityToAddId) {
                        TopologyEntity.Builder entity = topology.get(id);
                        if (addition.hasTargetEntityType() && entity.getEntityType() != targetType) {
                            // remove host from storage cluster group that user selected
                            addEntitiesToMap(targetType, additionCount, entity, topologyGraph, entityAdditions)
                                    .entrySet()
                                    .forEach(entry -> entityAdditions.put(entry.getKey(), entry.getValue()));
                        } else {
                            addTopologyAdditionCount(entityAdditions, addition, id);
                        }
                    }
                } else {
                    logger.warn("Unimplemented handling for topology addition with {}",
                            addition.getAdditionTypeCase());
                }
            } else if (change.hasTopologyMigration()) {
                // also consider on-prem migration use case
                final TopologyMigration topologyMigration = change.getTopologyMigration();
                final Integer migratingEntityType = topologyMigration.getDestinationEntityType()
                        == TopologyMigration.DestinationEntityType.VIRTUAL_MACHINE
                        ? EntityType.VIRTUAL_MACHINE_VALUE
                        : EntityType.DATABASE_SERVER_VALUE;

                final Set<Long> entitiesToMigrate = expandAndFlattenReferences(
                        topologyMigration.getSourceList(), migratingEntityType, groupIdToGroupMap,
                        groupResolver, topologyGraph);
                sourceEntities.clear();
                sourceEntities.addAll(entitiesToMigrate);

                final Set<Long> migratingToRegions = expandAndFlattenReferences(
                        topologyMigration.getDestinationList(),
                        migratingEntityType,
                        groupIdToGroupMap,
                        groupResolver,
                        topologyGraph);

                destinationEntities.clear();
                destinationEntities.addAll(migratingToRegions);
            } else if (change.hasTopologyRemoval()) {
                final TopologyRemoval removal = change.getTopologyRemoval();
                int targetType = removal.getTargetEntityType();
                Set<Long> entities = removal.hasEntityId()
                    ? Collections.singleton(removal.getEntityId())
                    : groupResolver.resolve(
                        groupIdToGroupMap.get(removal.getGroupId()),
                        topologyGraph).getAllEntities();
                int nonExistentEntitiesCount = 0;
                for (Long id : entities) {
                    TopologyEntity.Builder entity = topology.get(id);
                    if (entity == null) {
                        // When there are entities exist in a group but not in the system, removal
                        // group in plan should still proceed.
                        nonExistentEntitiesCount++;
                        continue;
                    }
                    // user can choose a group whose member may not directly match with the removal type,
                    // in such cases, we need to traverse up/down supply chain to find the real entities they
                    // want to remove
                    if (removal.hasTargetEntityType() && entity.getEntityType() != targetType) {
                        Set<TopologyEntity.Builder> targetEntities = getTargetEntities(targetType, entity, topologyGraph);
                        for (TopologyEntity.Builder targetEntity : targetEntities) {
                            entitiesToRemove.add(targetEntity.getOid());
                            if (targetEntity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                                // Remove the daemonset pod consumers of a VM being removed, else
                                // they will remain unplaced. The daemonset pod on a VM can be placed
                                // only on that VM.
                                RemoveDaemonPodConsumersWithAllTheirConsumers(entitiesToRemove, targetEntity);
                            }
                        }
                    } else if (removal.hasTargetEntityType() && entity.getEntityType() == targetType) {
                        entitiesToRemove.add(entity.getOid());
                        if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                            // Remove the daemonset pod consumers of a VM being removed, else
                            // they will remain unplaced. The daemonset pod on a VM can be placed
                            // only on that VM.
                            RemoveDaemonPodConsumersWithAllTheirConsumers(entitiesToRemove, entity);
                        }
                    }
                }
                if (nonExistentEntitiesCount != 0) {
                    logger.warn("{} entities to be removed no longer exist in topology",
                            nonExistentEntitiesCount);
                }
            } else if (change.hasTopologyReplace()) {
                final TopologyReplace replace = change.getTopologyReplace();
                int targetType = replace.getTargetEntityType();
                Set<Long> entities = replace.hasRemoveEntityId()
                    ? Collections.singleton(replace.getRemoveEntityId())
                    : groupResolver.resolve(
                        groupIdToGroupMap.get(replace.getRemoveGroupId()),
                        topologyGraph).getAllEntities();
                entities.forEach(id -> {
                    TopologyEntity.Builder entity = topology.get(id);
                    if (entity == null) {
                        throwEntityNotFoundException(id);
                    }
                    // user can choose a group whose member may not directly match with the replace entity
                    // type,in  such cases, we need to traverse up/down supply chain to find the real entities they
                    // want to replace
                    if (replace.hasTargetEntityType() && entity.getEntityType() != targetType) {
                        Set<Long> removalSet = getTargetEntities(targetType, entity, topologyGraph)
                            .stream().map(TopologyEntity.Builder::getOid).collect(Collectors.toSet());
                        for (long oid : removalSet) {
                            entitiesToReplace.add(oid);
                            templateToReplacedEntity.put(replace.getAddTemplateId(), oid);
                        }
                    } else {
                        entitiesToReplace.add(id);
                        templateToReplacedEntity.put(replace.getAddTemplateId(), id);
                    }
                });
            // only change utilization when plan changes have utilization level message.
            } else if (change.hasPlanChanges() && change.getPlanChanges().hasUtilizationLevel()) {
                final UtilizationLevel utilizationLevel =
                        change.getPlanChanges().getUtilizationLevel();
                changeUtilization(topology, utilizationLevel.getPercentage());

            } else {
                logger.warn("Unimplemented handling for change of type {}", change.getDetailsCase());
            }
        }
        // Clone the added entities and add the cloned entities into the topology
        entityAdditions.forEach((oid, addCount) -> Optional.ofNullable(topology.get(oid))
                .ifPresent(entity -> {
                    final DefaultEntityCloneEditor cloneFunction = entityCloneEditorFactory
                            .createEntityCloneFunction(entity, topologyInfo);
                    LongStream.range(0, addCount).forEach(i -> cloneFunction
                            .clone(entity.getEntityBuilder(), i, topology));
        }));

        // Prepare any entities that are getting removed as part of the plan, for removal from the
        // analysis topology. This process will unplace any current buyers of these entities
        // commodities, then mark the entities as "removed", so they can be removed from the Analysis
        // entities set.
        Edit removalEdit = Edit.newBuilder()
                .setRemoved(Removed.newBuilder()
                        .setPlanId(topologyInfo.getTopologyContextId()))
                .build();
        prepareEntitiesForRemoval(entitiesToRemove, topology, removalEdit);

        // Like we just did for "removed" entities, we will prepare any entities "replaced" as part
        // of a plan to be removed from the analysis topology. The steps are the same as with the
        // "removed" entities, except the Edit that is recorded on them is a Replacement, rather
        // than a Removal.
        Edit replacementEdit = Edit.newBuilder()
                .setReplaced(Replaced.newBuilder()
                        .setPlanId(topologyInfo.getTopologyContextId()))
                .build();
        prepareEntitiesForRemoval(entitiesToReplace, topology, replacementEdit);

        // Mark added entities with the Plan Origin so they aren't counted in "before" plan
        // stats
        addTemplateTopologyEntities(templateToAdd, templateToReplacedEntity, topology,
                topologyInfo.getTopologyContextId()).forEach(
                entity -> topology.put(entity.getOid(), TopologyEntity.newBuilder(entity)));
    }

    /**
     * Given a collection of {@link MigrationReference}, resolve the members of all groups and return a new collection
     * of all entities in the given scope.
     *
     * @param migrationReferences a collection of entities/groups to migrate from/to
     * @param migratingEntityType the numeric {@link TopologyMigration.DestinationEntityType} of the migration
     * @param groupIdToGroupMap a map of OID to {@link Grouping} representing all groups in the source/destination scopes
     * @param groupResolver a {@link GroupResolver} reference used to expand group membership
     * @param topologyGraph used in tandem with {@param groupResolver} to expand group membership
     * @return a set of all entities represented by {@param migrationReferences}
     * @throws GroupResolutionException when a group by a migration source/destination is not resolved
     */
    @VisibleForTesting
    protected static Set<Long> expandAndFlattenReferences(@Nonnull final List<MigrationReference> migrationReferences,
                                                 @Nonnull final Integer migratingEntityType,
                                                 @Nonnull final Map<Long, Grouping> groupIdToGroupMap,
                                                 @Nonnull final GroupResolver groupResolver,
                                                 @Nonnull final TopologyGraph<TopologyEntity> topologyGraph)
            throws GroupResolutionException {
        boolean areGroups = true;
        Map<Boolean, Set<Long>> areGroupsToEntityOids = migrationReferences.stream()
                .collect(Collectors.groupingBy(reference -> reference.hasGroupType(),
                        Collectors.mapping(MigrationReference::getOid, Collectors.toSet())));

        final Set<Long> migrationEntities = Sets.newHashSet();
        if (CollectionUtils.isNotEmpty(areGroupsToEntityOids.get(areGroups))) {
            // Add the members of expanded groups
            for (Long groupOid : areGroupsToEntityOids.get(areGroups)) {
                final Map<ApiEntityType, Set<Long>> entityTypeToMembers = groupResolver.resolve(groupIdToGroupMap.get(groupOid), topologyGraph)
                        .getEntitiesByType();
                final ApiEntityType apiEntityType = ApiEntityType.fromType(migratingEntityType);
                // Add members of migratingEntityType
                if (entityTypeToMembers.containsKey(apiEntityType)) {
                    migrationEntities.addAll(entityTypeToMembers.get(apiEntityType));
                }
                // We're dealing with a cluster, get PMs that are part of that cluster.
                Set<Long> pms = entityTypeToMembers.get(ApiEntityType.PHYSICAL_MACHINE);
                final Set<Long> dcs = entityTypeToMembers.get(ApiEntityType.DATACENTER);
                if (CollectionUtils.isNotEmpty(dcs)) {
                    // If it is a group of DCs, we first need to resolve that and get the PMs
                    // that are part of those DCs.
                    if (pms == null) {
                        pms = new HashSet<>();
                    }
                    pms.addAll(getConsumersOfType(topologyGraph.getEntities(dcs),
                            EntityType.PHYSICAL_MACHINE_VALUE));
                }
                // Finally get the VMs that consume from those set of PMs.
                if (CollectionUtils.isNotEmpty(pms)) {
                    final Set<Long> clusterWorkloads = getConsumersOfType(
                            topologyGraph.getEntities(pms),
                            migratingEntityType);
                    migrationEntities.addAll(clusterWorkloads);
                }
            }
        }
        final Set<Long> nonGroupSources = areGroupsToEntityOids.get(!areGroups);
        if (CollectionUtils.isNotEmpty(nonGroupSources)) {
            // Add individual entities
            // If these are workloads, add them (VMs, DBs, DBSs)
            final Map<Integer, Set<TopologyEntity>> typeToEntity = topologyGraph.getEntities(nonGroupSources)
                    .collect(Collectors.groupingBy(TopologyEntity::getEntityType,
                            Collectors.mapping(Function.identity(), Collectors.toSet())));

            final Set<Long> dataCenterOids = Sets.newHashSet();
            final Set<TopologyEntity> dataCenters = typeToEntity.get(EntityType.DATACENTER_VALUE);
            if (CollectionUtils.isNotEmpty(dataCenters)) {
                // Get migratingEntityType entities from the DC...
                Set<Long> entitiesAggregatedByDCs = getConsumersOfType(
                        dataCenters.stream()
                            .flatMap(dataCenter -> dataCenter.getConsumers().stream()),
                        migratingEntityType);

                migrationEntities.addAll(entitiesAggregatedByDCs);
                dataCenterOids.addAll(dataCenters.stream()
                        .collect(Collectors.mapping(TopologyEntity::getOid, Collectors.toSet())));
            }
            final Set<Long> nonGroupSourcesToAdd = nonGroupSources.stream()
                    .filter(nonGroupSource -> !dataCenterOids.contains(nonGroupSource))
                    .collect(Collectors.toSet());
            migrationEntities.addAll(nonGroupSourcesToAdd);
        }
        return migrationEntities;
    }

    /**
     * Get consumers of a type specified by {@param type} from {@param topologyEntityStream}.
     *
     * @param topologyEntityStream a stream of {@link TopologyEntity} retrieved from a {@link TopologyGraph} instance
     * @param type a numeric {@link EntityType} specifying the types of consumers to retrieve
     * @return a set of OIDs representing consumers of a specified type
     */
    @Nonnull
    private static Set<Long> getConsumersOfType(Stream<TopologyEntity> topologyEntityStream, Integer type) {
        return topologyEntityStream.flatMap(pm -> pm.getConsumers().stream())
                .filter(consumer -> type.equals(consumer.getEntityType()))
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
    }

    /**
     * Return a set of entities owned by the set of {@param seedOids} provided, of a type specified by
     * {@param connectedEntityTypesToGather}. This implementation assumes no groups are included in {@param seedOids}.
     *
     * @param seedOids a set of entity OIDs
     * @param connectedEntityTypesToGather a set of numeric entity types for which to retrieve entity OIDs
     * @param topologyGraph a {@link TopologyGraph} data structure representing the topology being operated on
     * @return a map of numeric {@link EntityType} to the OIDs that correspond to it
     */
    @Nonnull
    private Set<Long> getOwnedEntities(
            @Nonnull final Set<Long> seedOids,
            @Nonnull final Set<Integer> connectedEntityTypesToGather,
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        return topologyGraph.getEntities(seedOids)
                .flatMap(topologyEntity -> topologyEntity.getOwnedEntities().stream())
                .filter(topologyEntity -> connectedEntityTypesToGather.contains(topologyEntity.getEntityType()))
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
    }

    /**
     * Using {@param topologyGraph}, get all entities aggregated by those specified by {@param seedOids} of a type specified
     * in {@param workloadTypes}, and return their OIDs for removal.
     *
     * @param seedOids if an entity is connected to one of the entities represented by this set, remove it
     * @param workloadTypes all entity types to target for removal
     * @param topologyGraph a {@link TopologyGraph} data structure representing the topology being operated on
     * @return a set of entities connected from {@param seedOids} of a type represented by {@param workloadTypes}
     */
    @Nonnull
    private Set<Long> getAggregatedEntities(
            @Nonnull final Set<Long> seedOids,
            @Nonnull final Set<Integer> workloadTypes,
            @Nonnull final TopologyGraph<TopologyEntity> topologyGraph) {
        return topologyGraph.getEntities(seedOids)
                .flatMap(topologyEntity -> topologyGraph.getAggregatedEntities(topologyEntity)
                        .filter(aggregatedEntity -> workloadTypes.contains(aggregatedEntity.getEntityType())))
                .map(TopologyEntity::getOid)
                .collect(Collectors.toSet());
    }

    /**
     * Add entities into a map with entity oid as key and the number of copied to be clones as value.
     *
     * @param targetType the target entity type
     * @param additionCount the number of clones to be added
     * @param entity the entity to be cloned
     * @param topologyGraph the topology graph
     * @param entityAdditions the mapping of entity oid to the number of clones to be added
     * @return an oid to entity addition count mapping
     */
    private Map<Long, Long> addEntitiesToMap(final int targetType,
                                             final long additionCount,
                                             final @Nonnull TopologyEntity.Builder entity,
                                             final @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                             final @Nonnull Map<Long, Long> entityAdditions) {
        for (final Builder targetEntity : getTargetEntities(targetType, entity, topologyGraph)) {
            entityAdditions.put(targetEntity.getOid(), additionCount);
        }
        return entityAdditions;
    }

    /**
     * Obtain the associated entity of a given type starting from a given entity.
     *
     * @param targetType the type of entity to be found
     * @param entity the given entity candidate
     * @param topologyGraph the topology graph
     * @return a set of TopologyEntity.Builder
     */
    private Set<TopologyEntity.Builder> getTargetEntities(final int targetType,
                                                          final @Nonnull TopologyEntity.Builder entity,
                                                          final @Nonnull TopologyGraph<TopologyEntity> topologyGraph) {
        // traver supply chain up to find target entities based on consumer relation, if none exists,
        // traverse down supply chain to find target entities based on provider relation
        Set<TopologyEntity.Builder> topologyentities = traverseSupplyChain(targetType, entity, topologyGraph, true);
        if (topologyentities.isEmpty()) {
            topologyentities = traverseSupplyChain(targetType, entity, topologyGraph, false);
        }
        return topologyentities;
    }

    /**
     * Add the daemon consumer pods and the pods consumers subsequently for a given VM entity
     * to the set of entities that should be removed from plan.
     * This is applicable only for VMs.
     *
     * @param entitiesToRemove the set to which to add the entities
     * @param entity the given entity candidate whose consumers to be searched and added to remove list
     */
    private void RemoveDaemonPodConsumersWithAllTheirConsumers(final @Nonnull Set<Long> entitiesToRemove,
                                                             final @Nonnull TopologyEntity.Builder entity) {
        entity.getConsumers().stream().forEach(e -> {
            if ((e.getEntityType() == EntityType.CONTAINER_POD_VALUE)
                    && (e.getTopologyEntityDtoBuilder().getAnalysisSettings().getDaemon())) {
                entitiesToRemove.add(e.getOid());
                RemoveAllConsumersRecursive(entitiesToRemove, e);
            }
        });
    }

    /**
     * Add the consumers who won't have any other provider apart from the current one
     * recursively traversing up the supply chain. Skipping those consumers which can
     * have another provider will ensure not to remove entities which can have multiple
     * providers, for example a single service over multiple pods.
     *
     * @param entitiesToRemove the set to which to add the entities
     * @param entity the given entity candidate whose consumers to be searched and added to remove list
     */
    private void RemoveAllConsumersRecursive(final @Nonnull Set<Long> entitiesToRemove,
                                          final @Nonnull TopologyEntity entity) {
        entity.getConsumers().stream().forEach(consumer -> {
            if (consumer.getProviders().stream().noneMatch(provider -> provider
                    .getOid() != entity.getOid())) {
                // Add this consumer if it has no other provider apart from the current one.
                entitiesToRemove.add(consumer.getOid());
            }
            RemoveAllConsumersRecursive(entitiesToRemove, consumer);
        });
    }

    /**
     * Recursively looking for the consumers or providers of a type from the given entity.
     *
     * @param targetType the type of entity to be found
     * @param entity the given entity candidate
     * @param topologyGraph the topology graph
     * @param traverseUp traverse up or down the supply chain to find related entities
     * @return a set of TopologyEntity.Builder
     */
    private Set<TopologyEntity.Builder> traverseSupplyChain(final int targetType,
                                                            final @Nonnull TopologyEntity.Builder entity,
                                                            final @Nonnull TopologyGraph<TopologyEntity> topologyGraph,
                                                            final boolean traverseUp) {
        Set<TopologyEntity.Builder> targetEntities = new HashSet<>();
        Set<TopologyEntity> directRelatedEntities = new HashSet<>();
        if (traverseUp) {
            directRelatedEntities = topologyGraph.getEntity(entity.getOid())
                .get().getConsumers().stream()
                .collect(Collectors.toSet());
        } else {
            directRelatedEntities = topologyGraph.getEntity(entity.getOid())
                .get().getProviders().stream()
                .collect(Collectors.toSet());
        }
        if (directRelatedEntities.isEmpty()) {
            return targetEntities;
        }
        targetEntities = directRelatedEntities.stream()
                .filter(e -> targetType == e.getEntityType())
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .map(TopologyEntity::newBuilder)
                .collect(Collectors.toSet());
        if (targetEntities.isEmpty()) {
            for (TopologyEntity c : directRelatedEntities) {
                targetEntities.addAll(traverseSupplyChain(targetType,
                        TopologyEntity.newBuilder(c.getTopologyEntityDtoBuilder()), topologyGraph, traverseUp));
            }
        }
        return targetEntities;
    }

    /**
     * Marks an entity with an Edit attribute. When the entity is processed in the Market, entities
     * with "Removed" or "Replaced" edits on them will be removed from the set of entities being
     * analyzed.
     **/
    private void tagEntityWithEdit(Long oid,
                                   @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                   Edit edit) {
        TopologyEntity.Builder entity = topology.get(oid);
        if (entity != null) {
            entity.getEntityBuilder().setEdit(edit);
        }
    }

    private void changeUtilization(@Nonnull Map<Long, TopologyEntity.Builder> topology, int percentage) {
        final Predicate<TopologyEntity.Builder> isVm =
                entity -> entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE;
        final Set<TopologyEntity.Builder> topologyVms = topology.values().stream().filter(isVm)
                .collect(Collectors.toSet());
        for (TopologyEntity.Builder vm : topologyVms) {
            final List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders =
                    vm.getEntityBuilder().getCommoditiesBoughtFromProvidersList();
            final List<CommoditiesBoughtFromProvider> increasedCommodities =
                    increaseProviderCommodities(topology, percentage, vm, commoditiesBoughtFromProviders);
            vm.getEntityBuilder().clearCommoditiesBoughtFromProviders();
            vm.getEntityBuilder().addAllCommoditiesBoughtFromProviders(increasedCommodities);
        }
    }

    @Nonnull
    private List<CommoditiesBoughtFromProvider> increaseProviderCommodities(
            @Nonnull Map<Long, TopologyEntity.Builder> topology, int percentage,
            @Nonnull TopologyEntity.Builder vm, List<CommoditiesBoughtFromProvider> commoditiesBoughtFromProviders) {
        final ImmutableList.Builder<CommoditiesBoughtFromProvider> increasedProviderCommodities =
                ImmutableList.builder();
        for (CommoditiesBoughtFromProvider providerCommodities : commoditiesBoughtFromProviders) {
            List<CommodityBoughtDTO> increasedCommodities =
                    increaseCommodities(topology, percentage, vm.getEntityBuilder(), providerCommodities);
            increasedProviderCommodities.add(CommoditiesBoughtFromProvider
                    .newBuilder(providerCommodities)
                    .clearCommodityBought()
                    .addAllCommodityBought(increasedCommodities)
                    .build());
        }
        return increasedProviderCommodities.build();
    }

    @Nonnull
    private List<CommodityBoughtDTO> increaseCommodities(
        @Nonnull Map<Long, TopologyEntity.Builder> topology,
        int percentage, @Nonnull TopologyEntityDTO.Builder vm,
        @Nonnull CommoditiesBoughtFromProvider providerCommodities) {
        final ImmutableList.Builder<CommodityBoughtDTO> changedCommodities = ImmutableList.builder();
        for (CommodityBoughtDTO commodity : providerCommodities.getCommodityBoughtList()) {
            final int commodityType = commodity.getCommodityType().getType();
            if (UTILIZATION_LEVEL_TYPES.contains(commodityType)) {
                final double changedUtilization = increaseByPercent(commodity.getUsed(), percentage);
                final double changedAmount = changedUtilization - commodity.getUsed();
                changedCommodities.add(CommodityBoughtDTO.newBuilder(commodity)
                        .setUsed(changedUtilization).build());
                // increase provider's commodity sold utilization only when it has provider
                if (providerCommodities.hasProviderId()) {
                    increaseCommoditySoldByProvider(topology, providerCommodities.getProviderId(),
                            vm.getOid(), commodityType, changedAmount);
                }
            } else {
                changedCommodities.add(commodity);
            }
        }
        return changedCommodities.build();
    }

    private void increaseCommoditySoldByProvider(@Nonnull Map<Long, TopologyEntity.Builder> topology,
            long providerId, long consumerId, int commodityType, double adjustmentAmount) {
        final ImmutableList.Builder<CommoditySoldDTO> changedSoldCommodities =
                ImmutableList.builder();
        final TopologyEntity.Builder provider = topology.get(providerId);
        if (provider == null) {
            throw new IllegalArgumentException("Topology doesn't contain entity with id " + providerId);
        }
        for (CommoditySoldDTO sold : provider.getEntityBuilder().getCommoditySoldListList()) {
            if (((sold.getAccesses() == 0 ) || (sold.getAccesses() == consumerId))
                    && sold.getCommodityType().getType() == commodityType) {
                // increase the commodity by the adjustment amount
                final CommoditySoldDTO increasedCommodity = CommoditySoldDTO.newBuilder(sold)
                        .setUsed(adjustmentAmount + sold.getUsed())
                        .build();
                changedSoldCommodities.add(increasedCommodity);
            } else {
                changedSoldCommodities.add(sold);
            }
        }
        provider.getEntityBuilder()
            .clearCommoditySoldList()
            .addAllCommoditySoldList(changedSoldCommodities.build());
    }

    private double increaseByPercent(double value, int percentage) {
        return value + value * percentage / 100;
    }

    /**
     * The entities being removed (or replaced) will be marked with an Edit property
     * that flags these entities for removal in the Market.
     *
     * @param entitiesToRemove a set of replaced entity oids.
     * @param topology the entities in the topology, arranged by ID.
     * @param edit the Edit to assign to the entity, marking it as removed/replaced
     */
    private void prepareEntitiesForRemoval(@Nonnull Set<Long> entitiesToRemove,
                                           @Nonnull final Map<Long, TopologyEntity.Builder> topology,
                                           @Nonnull Edit edit) {
        /**
         * Mark the entities as edited. The entities remain in the topology and all related entities
         * maintain their relationships to the entities marked for removal. The marked entities will
         * actually be removed in the market component prior to analysis.
         *
         * It is important to retain the entities in the topology and the relationships to those entities
         * for correctness. For example, consider the scoping algorithm which occurs after these
         * edits take place. If we were to run a plan on a cluster (ie the scope seeds are the hosts in
         * the cluster) and then replace those hosts, if we removed the hosts now, the scoping algorithm
         * would be unable to scope from the seeds. Further, even if we retained the hosts but unplaced
         * the VMs buying from those hosts, the scoping algorithm would be unable to traverse from
         * the hosts to their consumers for scoping purposes because no entities buy from the hosts being
         * removed. Another example of why we should not unplace due to removal at this time is because
         * if a group applies to the entities consuming from or providing to the entities being removed,
         * that group would be invalidated by removing the relationship to the entity being removed. As
         * a result, policy and setting application would do the wrong thing.
         */
        entitiesToRemove.forEach(entityOid -> tagEntityWithEdit(entityOid, topology, edit));
    }

    /**
     * Add all addition or replaced topology entities which converted from templates.
     *
     * @param templateAdditions a map which key is template id, value is the addition count.
     * @param templateToReplacedEntity a map which key is template id, value is a list of replaced entity.
     * @param topology The entities in the topology, arranged by ID.
     *
     * @return a stream of builders of entities created from the specified templates
     */
    private Stream<TopologyEntityDTO.Builder> addTemplateTopologyEntities(
        @Nonnull Map<Long, Long> templateAdditions,
        @Nonnull Multimap<Long, Long> templateToReplacedEntity,
        @Nonnull Map<Long, TopologyEntity.Builder> topology, long topologyId) {
        // Check if there are templates additions or replaced
        if (templateAdditions.isEmpty() && templateToReplacedEntity.isEmpty()) {
            return Stream.empty();
        } else {
            return templateConverterFactory.generateTopologyEntityFromTemplates(templateAdditions,
                templateToReplacedEntity, topology, topologyId);
        }
    }

    private static void addTopologyAdditionCount(@Nonnull final Map<Long, Long> additionMap,
                                                 @Nonnull TopologyAddition addition,
                                                 long key) {
        final long additionCount =
                addition.hasAdditionCount() ? addition.getAdditionCount() : 1L;
        additionMap.put(key, additionMap.getOrDefault(key, 0L) + additionCount);

    }


    private Map<Long, Grouping> getGroups(List<ScenarioChange> changes) {
        final Set<Long> groupIds = PlanDTOUtil.getInvolvedGroups(changes);
        final Map<Long, Grouping> groupIdToGroupMap = new HashMap<>();

        if (!groupIds.isEmpty()) {
            final GetGroupsRequest request =
                            GetGroupsRequest.newBuilder()
                            .setGroupFilter(GroupFilter.newBuilder()
                                            .addAllId(groupIds))
                            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                            .build();

            groupServiceClient.getGroups(request)
                    .forEachRemaining(
                            group -> groupIdToGroupMap.put(group.getId(), group)
                    );
        }

        return groupIdToGroupMap;
    }

    private void throwEntityNotFoundException(long id) {
        logger.error("Cannot find entity: {} in current topology", id);
        throw TopologyEditorException.notFoundEntityException(id);
    }
}
