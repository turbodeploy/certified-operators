package com.vmturbo.topology.processor.topology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.StitchingEntity;
import com.vmturbo.stitching.journal.IStitchingJournal;
import com.vmturbo.stitching.journal.JournalableEntity;
import com.vmturbo.topology.processor.stitching.StitchingContext;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.stitching.TopologyStitchingGraph;
import com.vmturbo.topology.processor.stitching.journal.StitchingJournalFactory;

/**
 * Applying scoping logic for cloud plan topologies.
 */
public class CloudTopologyScopeEditor {
    private static final Set<EntityType> serviceTiers = new HashSet<>(Arrays
            .asList(EntityType.DATABASE_TIER, EntityType.DATABASE_SERVER_TIER,
                    EntityType.COMPUTE_TIER, EntityType.STORAGE_TIER));

    /**
     * Filter entities in the stitchinContext based on planScope.
     *
     * @param stitchingContext the context containing {@link TopologyStitchingEntity}
     * @param scope the scope user defined in cloud plan
     * @param journalFactory the journal for use to tracing changes
     * @return {@link StitchingContext}
     */
    public StitchingContext scope(@Nonnull final StitchingContext stitchingContext,
                                  @Nullable final PlanScope scope,
                                  @Nonnull final StitchingJournalFactory journalFactory) {
        // record a count of the number of entities by their entity type in the context.
        final IStitchingJournal<StitchingEntity> journal = journalFactory.stitchingJournal(stitchingContext);
        TopologyStitchingGraph graph = stitchingContext.getStitchingGraph();
        Set<TopologyStitchingEntity> totalEntitySet = graph.entities().collect(Collectors.toSet());
        journal.recordTopologySizes(stitchingContext.entityTypeCounts());
        journal.recordMessage("Tracing scope before filtering by target to the following entities: " +
                        totalEntitySet.stream()
                        .map(JournalableEntity::getDisplayName)
                        .collect(Collectors.toList()));
        Set<Long> targetSet = new HashSet<>();
        for (PlanScopeEntry planScope : scope.getScopeEntriesList()) {
            totalEntitySet.stream().filter(e -> e.getOid()==planScope.getScopeObjectOid())
                            .map(TopologyStitchingEntity::getTargetId)
                            .forEach(t -> targetSet.add(t));
        }
        // first filter all entities discovered by the target which discover the scope seed
        // TODO: Cloud migration plan has to keep onprem entity selected, which will not have same
        // target as the scope
        Map<Long, TopologyStitchingEntity> filteredEntities = filterEntitiesByTarget(targetSet, totalEntitySet);
        journal.recordMessage("Tracing scope after filtering by target to the following entities: " +
                        filteredEntities.values().stream()
                        .map(JournalableEntity::getDisplayName)
                        .collect(Collectors.toList()));
        final Set<TopologyStitchingEntity> cloudProviders = new HashSet<>();
        final Set<TopologyStitchingEntity> cloudConsumers = new HashSet<>();
        for (PlanScopeEntry planScope : scope.getScopeEntriesList()) {
            TopologyStitchingEntity seed = filteredEntities.get(planScope.getScopeObjectOid());
            // if user specify a scope on region or business account
            if (seed.getEntityType().equals(EntityType.REGION)
                    || seed.getEntityType().equals(EntityType.BUSINESS_ACCOUNT)) {
                // if starting from region, traverse downward we can find all AZ
                // if starting from BA, traverse downward we can find all DB, DBS, VM, VV and AZ
                cloudConsumers.addAll(findConnectedEntity(seed, filteredEntities, false));
                cloudConsumers.add(seed);
                journal.recordMessage("Tracing cloud consumers after scoping on the following entities: " +
                                cloudConsumers.stream()
                                .map(JournalableEntity::getDisplayName)
                                .collect(Collectors.toList()));
                Set<TopologyStitchingEntity> azSet = cloudConsumers.stream()
                                .filter(e -> e.getEntityType() == EntityType.AVAILABILITY_ZONE)
                                .collect(Collectors.toSet());
                Set<TopologyStitchingEntity> regionSet = cloudConsumers.stream()
                                .filter(e -> e.getEntityType() == EntityType.REGION)
                                .collect(Collectors.toSet());
                if (!azSet.isEmpty()) {
                    // we need to add all cloud providers which are the service tiers and regions
                    // we can start from AZ to go upward to get them
                    for (TopologyStitchingEntity a : azSet) {
                        cloudProviders.addAll(findConnectedEntity(a, filteredEntities, true));
                    }
                    // for service tiers such as storage and compute, they are generally connected
                    // with all region and az, therefore, virtual volumes and virtual machines
                    // from outside of scope can be included. For examples, GP2 may bring VM
                    // from London into scope even if user selects only Ohio. We need to verify
                    // entities from cloudProviders to filter out unnecessary VM and VV.
                    Set<TopologyStitchingEntity> entityBeyondScope =
                            entityBeyondScope(azSet, EntityType.AVAILABILITY_ZONE, cloudProviders);
                    cloudProviders.removeAll(entityBeyondScope);
                } else if (!regionSet.isEmpty()) {
                    // in case of azure, there could be no availability zone, if the scope starts
                    // with region, we can use region to traverse upwards to get service tiers
                    for (TopologyStitchingEntity r : regionSet) {
                        cloudProviders.addAll(findConnectedEntity(r, filteredEntities, true));
                    }
                    // for service tiers such as storage and compute, they are generally connected
                    // with all region and az, therefore, virtual volumes and virtual machines
                    // from outside of scope can be included. For examples, GP2 may bring VM
                    // from London into scope even if user selects only Ohio. We need to verify
                    // entities from cloudProviders to filter out unnecessary VM and VV.
                    Set<TopologyStitchingEntity> entityBeyondScope =
                                    entityBeyondScope(regionSet, EntityType.REGION, cloudProviders);
                    cloudProviders.removeAll(entityBeyondScope);
                } else {
                    // in case of azure, there could be no availability zone, if the scope starts
                    // with ba, we add all service tiers into topology. VM will find the service tier
                    // with correct ba in market analysis
                    cloudProviders.addAll(filteredEntities.values().stream()
                            .filter(e -> serviceTiers.contains(e.getEntityType()))
                            .collect(Collectors.toSet()));
                }
                journal.recordMessage("Tracing cloud service tiers after scoping on the following entities: " +
                                cloudProviders.stream()
                                .map(JournalableEntity::getDisplayName)
                                .collect(Collectors.toList()));
            }
        }
        Set<TopologyStitchingEntity> entityToRemove = totalEntitySet.stream()
                .filter(e -> !cloudConsumers.contains(e) && !cloudProviders.contains(e))
                .collect(Collectors.toSet());
        for (TopologyStitchingEntity entity : entityToRemove) {
            stitchingContext.removeEntity(entity);
        }
        journal.recordMessage("Tracing cloud topology scope output to the following entities: " +
                        totalEntitySet.stream()
                        .map(JournalableEntity::getDisplayName)
                        .collect(Collectors.toList()));
        journal.recordTopologySizes(stitchingContext.entityTypeCounts());
        return stitchingContext;
    }

    /**
     * Filter {@link TopologyStitchingEntity} that are connectedTo other entities with same types as scopedEntity.
     *
     * @param scopedEntity the scoped entity set
     * @param type the type of scoped entity
     * @param candidates the {@link TopologyStitchingEntity} to be filtered
     * @return a set of TopologyStitchingEntity
     */
    private @Nonnull Set<TopologyStitchingEntity>
            entityBeyondScope(@Nonnull Set<TopologyStitchingEntity> scopedEntity,
                              @Nonnull EntityType type,
                              @Nonnull Set<TopologyStitchingEntity> candidates) {
        Set<TopologyStitchingEntity> beyondScopeSet = new HashSet<>();
        for (TopologyStitchingEntity entity : candidates) {
            if (entity.getConnectedToByType().values().stream()
                    .flatMap(set -> set.stream())
                    .filter(e -> e.getEntityType() == type)
                    .anyMatch(c -> !scopedEntity.contains(c))) {
                beyondScopeSet.add(entity);
            }
        }
        return beyondScopeSet;
    }

    /**
     * Traverse up or down supply chain to recursively finds seed's TopologyStitchingEntity.
     *
     * @param seed the start point of traverse
     * @param input oid to TopologyStitchingEntity mapping
     * @param traverseUp whether to go up or go down supply chain
     */
    private @Nonnull Set<TopologyStitchingEntity>
            findConnectedEntity(@Nonnull final TopologyStitchingEntity seed,
                                @Nonnull final Map<Long, TopologyStitchingEntity> input,
                                boolean traverseUp) {
        Set<TopologyStitchingEntity> seedConnectedSet = new HashSet<>();
        if (!traverseUp && seed.getConnectedToByType().isEmpty()) {
            return seedConnectedSet;
        } else if (traverseUp && seed.getConnectedFromByType().isEmpty()){
            return seedConnectedSet;
        } else {
            Set<Long> connectedEntityOid = new HashSet<>();
            connectedEntityOid.addAll((traverseUp ? seed.getConnectedFromByType().values()
                     : seed.getConnectedToByType().values())
                    .stream()
                    .flatMap(set -> set.stream())
                    .map(StitchingEntity::getOid)
                    .collect(Collectors.toSet()));
            for (long oid : connectedEntityOid) {
                TopologyStitchingEntity connectedEntity = input.get(oid);
                if (connectedEntity != null) {
                    seedConnectedSet.add(connectedEntity);
                    seedConnectedSet.addAll(findConnectedEntity(connectedEntity, input, traverseUp));
                }
            }
            return seedConnectedSet;
        }
    }
    /**
     * Filter input entities by target id.
     *
     * @param targetId a set of target ids
     * @param input
     * @return
     */
    private Map<Long, TopologyStitchingEntity> filterEntitiesByTarget(@Nonnull Set<Long> targetId,
                                                                      @Nonnull Set<TopologyStitchingEntity> entities) {
        Map<Long, TopologyStitchingEntity> entitiesFilterByTarget = new HashMap<>();
        entities.forEach(e -> {
            if (targetId.contains(e.getTargetId())) {
                entitiesFilterByTarget.put(e.getOid(), e);
            };
        });
      return entitiesFilterByTarget;
    }
}
