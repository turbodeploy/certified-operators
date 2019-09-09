package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScope;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.stitching.TopologyStitchingEntity;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;

public class PlanTopologyScopeEditor {

    private static final Logger logger = LogManager.getLogger();

    private final GroupServiceBlockingStub groupServiceClient;

    // a set of entity type that represent cloud service tiers
    private static final Set<EntityType> serviceTiers = new HashSet<>(Arrays
            .asList(EntityType.DATABASE_TIER, EntityType.DATABASE_SERVER_TIER,
                    EntityType.COMPUTE_TIER, EntityType.STORAGE_TIER));
    // a set of on-prem application entity type
    private static final Set<Integer> applicationEntityTypes = new HashSet<>(Arrays
                    .asList(EntityType.APPLICATION_VALUE, EntityType.APPLICATION_SERVER_VALUE,
                            EntityType.BUSINESS_APPLICATION_VALUE, EntityType.DATABASE_SERVER_VALUE,
                            EntityType.DATABASE_VALUE, EntityType.CONTAINER_VALUE,
                            EntityType.CONTAINER_POD_VALUE));
    // a set of entity type which indicates the entities to be preserved in scope
    private static final Set<Integer> inScopeConnectedEntityTypes = new HashSet<>(Arrays
            .asList(EntityType.VIRTUAL_VOLUME_VALUE));
    // The overlapping entity type in infrastructure and application layer. Currently, VM is
    // used to find application entities if user scoped in infrastructure layer, or is used
    // to find infrastructure entities if user scoped in application layer.
    private static final int OVERLAPPING_ENTITY_TYPE_VALUE = EntityType.VIRTUAL_MACHINE_VALUE;

    public PlanTopologyScopeEditor(@Nonnull final GroupServiceBlockingStub groupServiceClient) {
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
    }

    /**
     * Filter entities in cloud plans based on user defined scope.
     *
     * @param graph the topology entity graph
     * @param scope the scope user defined in cloud plan
     * @return {@link TopologyGraph}
     */
    public TopologyGraph<TopologyEntity> scopeCloudTopology(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                                            @Nonnull final PlanScope scope) {
        logger.info("Entering scoping stage for cloud topology .....");
        Set<TopologyEntity> totalEntitySet = graph.entities().collect(Collectors.toSet());
        // record a count of the number of entities by their entity type in the context.
        logger.info("Initial entity graph total count is {}", graph.size());
        entityCountMsg(graph.entities().collect(Collectors.toSet()));
        Set<Long> targetSet = new HashSet<>();
        for (PlanScopeEntry planScope : scope.getScopeEntriesList()) {
            totalEntitySet.stream().filter(e -> e.getOid() == planScope.getScopeObjectOid())
                            .flatMap(TopologyEntity::getDiscoveringTargetIds)
                            .forEach(t -> targetSet.add(t));
        }
        // first filter all entities discovered by the target which discover the scope seed
        // TODO: Cloud migration plan has to keep onprem entity selected, which will not have same
        // target as the scope
        Map<Long, TopologyEntity> filteredEntities = filterEntitiesByTarget(targetSet, totalEntitySet);
        logger.info("Entities with the same discovery target count is {}", filteredEntities.size());
        entityCountMsg(filteredEntities.values().stream().collect(Collectors.toSet()));
        final Set<TopologyEntity> cloudProviders = new HashSet<>();
        final Set<TopologyEntity> cloudConsumers = new HashSet<>();
        for (PlanScopeEntry planScope : scope.getScopeEntriesList()) {
            TopologyEntity seed = filteredEntities.get(planScope.getScopeObjectOid());
            // if user specify a scope on region or business account
            if (seed.getEntityType() == EntityType.REGION_VALUE
                    || seed.getEntityType() == EntityType.BUSINESS_ACCOUNT_VALUE) {
                // if starting from region, traverse downward we can find all AZ
                // if starting from BA, traverse downward we can find all DB, DBS, VM, VV and AZ
                cloudConsumers.addAll(findConnectedEntities(seed, filteredEntities, false));
                cloudConsumers.add(seed);
                logger.trace("Tracing cloud consumers after scoping on the following entities: "
                        + cloudConsumers.stream()
                        .map(TopologyEntity::getDisplayName)
                        .collect(Collectors.toList()));
                Set<TopologyEntity> azSet = cloudConsumers.stream()
                                .filter(e -> e.getEntityType() == EntityType.AVAILABILITY_ZONE_VALUE)
                                .collect(Collectors.toSet());
                Set<TopologyEntity> regionSet = cloudConsumers.stream()
                                .filter(e -> e.getEntityType() == EntityType.REGION_VALUE)
                                .collect(Collectors.toSet());
                if (!azSet.isEmpty()) {
                    // we need to add all cloud providers which are the service tiers and regions
                    // we can start from AZ to go upward to get them
                    for (TopologyEntity a : azSet) {
                        cloudProviders.addAll(findConnectedEntities(a, filteredEntities, true));
                    }
                    // for service tiers such as storage and compute, they are generally connected
                    // with all region and az, therefore, virtual volumes and virtual machines
                    // from outside of scope can be included. For examples, GP2 may bring VM
                    // from London into scope even if user selects only Ohio. We need to verify
                    // entities from cloudProviders to filter out unnecessary VM and VV.
                    Set<TopologyEntity> entityBeyondScope =
                            entityBeyondScope(azSet, EntityType.AVAILABILITY_ZONE, cloudProviders);
                    cloudProviders.removeAll(entityBeyondScope);
                } else if (!regionSet.isEmpty()) {
                    // in case of azure, there could be no availability zone, if the scope starts
                    // with region, we can use region to traverse upwards to get service tiers
                    for (TopologyEntity r : regionSet) {
                        cloudProviders.addAll(findConnectedEntities(r, filteredEntities, true));
                    }
                    // for service tiers such as storage and compute, they are generally connected
                    // with all region and az, therefore, virtual volumes and virtual machines
                    // from outside of scope can be included. For examples, GP2 may bring VM
                    // from London into scope even if user selects only Ohio. We need to verify
                    // entities from cloudProviders to filter out unnecessary VM and VV.
                    Set<TopologyEntity> entityBeyondScope =
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
                logger.trace("Tracing cloud service tiers after scoping on the following entities: " +
                        cloudProviders.stream().map(TopologyEntity::getDisplayName)
                        .collect(Collectors.toList()));
            }
        }
        Set<TopologyEntity> entityToKeep = totalEntitySet.stream()
                .filter(e -> cloudConsumers.contains(e) || cloudProviders.contains(e))
                .collect(Collectors.toSet());
        Map<Long, TopologyEntity.Builder> resultEntityMap = entityToKeep.stream()
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toMap(Builder::getOid, TopologyEntity::newBuilder));
        // record the scoped result
        logger.info("Final scoped entity set total count is {}", entityToKeep.size());
        entityCountMsg(entityToKeep);
        return new TopologyGraphCreator<>(resultEntityMap).build();
    }

    /**
     * A helper method to print the entities by type count in log.
     * @param entities the list of entities
     */
    private void entityCountMsg(Set<TopologyEntity> entities) {
        Map<Integer, Long> originEntityByType = entities.stream()
                .collect(Collectors.groupingBy(e -> e.getEntityType(), Collectors.counting()));
        StringBuilder infoMsg = new StringBuilder().append("Entity set contains the following :");
        originEntityByType.entrySet().forEach(entry -> infoMsg.append(" Entity type ")
                .append(EntityType.forNumber(entry.getKey())).append(" count is ").append(entry.getValue()));
        logger.info(infoMsg.toString());
    }

    /**
     * Filter {@link TopologyStitchingEntity} that are connectedTo other entities with same types as scopedEntity.
     *
     * @param scopedEntity the scoped entity set
     * @param type the type of scoped entity
     * @param candidates the {@link TopologyStitchingEntity} to be filtered
     * @return a set of TopologyStitchingEntity
     */
    private @Nonnull Set<TopologyEntity> entityBeyondScope(@Nonnull Set<TopologyEntity> scopedEntity,
                                                           @Nonnull EntityType type,
                                                           @Nonnull Set<TopologyEntity> candidates) {
        Set<TopologyEntity> beyondScopeSet = new HashSet<>();
        for (TopologyEntity entity : candidates) {
            if (entity.getConnectedToEntities().stream()
                    .filter(e -> e.getEntityType() == type.getNumber())
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
    private @Nonnull Set<TopologyEntity> findConnectedEntities(@Nonnull final TopologyEntity seed,
                                                             @Nonnull final Map<Long, TopologyEntity> input,
                                                             boolean traverseUp) {
        Set<TopologyEntity> seedConnectedSet = new HashSet<>();
        if (!traverseUp && seed.getConnectedToEntities().isEmpty()) {
            return seedConnectedSet;
        } else if (traverseUp && seed.getConnectedFromEntities().isEmpty()){
            return seedConnectedSet;
        } else {
            Set<Long> connectedEntityOid = new HashSet<>();
            connectedEntityOid.addAll(traverseUp ? seed.getConnectedFromEntities()
                    .stream().map(TopologyEntity::getOid).collect(Collectors.toSet())
                    : seed.getConnectedToEntities().stream().map(TopologyEntity::getOid)
                    .collect(Collectors.toSet()));
            for (long oid : connectedEntityOid) {
                TopologyEntity connectedEntity = input.get(oid);
                if (connectedEntity != null) {
                    seedConnectedSet.add(connectedEntity);
                    seedConnectedSet.addAll(findConnectedEntities(connectedEntity, input, traverseUp));
                }
            }
            return seedConnectedSet;
        }
    }
    /**
     * Filter input entities by target id.
     *
     * @param targetId a set of target ids
     * @param entities s list of entities to be filtered
     * @return a map of entities by oid
     */
    private Map<Long, TopologyEntity> filterEntitiesByTarget(@Nonnull Set<Long> targetId,
                                                             @Nonnull Set<TopologyEntity> entities) {
        Map<Long, TopologyEntity> entitiesFilterByTarget = new HashMap<>();
        entities.forEach(e -> {
            if (e.getDiscoveringTargetIds().anyMatch(t -> targetId.contains(t))) {
                entitiesFilterByTarget.put(e.getOid(), e);
            };
        });
      return entitiesFilterByTarget;
    }

    /**
     * Filter entities in on prem plans based on user defined scope. If user scoped on any
     * infrastructure level entities, first finding all entities in infrastructure layer that are
     * connected with the seed via provider or consumer relationship, then using the result from
     * first pass to find application entities connected with the result entities via consumer
     * relationship and include them into scope. If user scoped on any application level entities,
     * finding all entities in application layer that are connected with the seed via provider or
     * consumer relationship, then using the result from first pass to find infrastructure level
     * entities that are connected with the result entities via consumer or provider relationship.
     *
     * @param topologyInfo the topologyInfo which contains topology relevant properties
     * @param graph the topology entity graph the topology entity graph
     * @param planScope the user defined plan scope
     * @param groupResolver the group resolver
     * @param changes a list of scenario changes user made
     * @throws PipelineStageException
     */
    public TopologyGraph<TopologyEntity> scopeOnPremTopology(@Nonnull final TopologyInfo topologyInfo,
                                                             @Nonnull final TopologyGraph<TopologyEntity> graph,
                                                             @Nonnull final PlanScope planScope,
                                                             @Nonnull final GroupResolver groupResolver,
                                                             @Nonnull final List<ScenarioChange> changes)
                                                             throws PipelineStageException {
        logger.info("Entering scoping stage for on prem topology .....");
        // record a count of the number of entities by their entity type in the context.
        logger.info("Initial entity graph total count is {}", graph.size());
        entityCountMsg(graph.entities().collect(Collectors.toSet()));
        // divide entities into two sets, one for application layer and another for infrastructure
        // layer. VM is the overlapping entities in both which will be used to connect two layers.
        Set<TopologyEntity> planAddedEntities = new HashSet<>();
        Set<TopologyEntity> appLayerEntities = new HashSet<>();
        Set<TopologyEntity> infraLayerEntities = new HashSet<>();
        graph.entities().forEach(e -> {
            if (e.getEntityType() == OVERLAPPING_ENTITY_TYPE_VALUE) {
                appLayerEntities.add(e);
                infraLayerEntities.add(e);
            } else if (applicationEntityTypes.contains(e.getEntityType())) {
                appLayerEntities.add(e);
            } else {
                infraLayerEntities.add(e);
            }

            // find entities that are added as part of plan configuration changes
            if (e.getOrigin().isPresent() && e.getOrigin().get().hasPlanScenarioOrigin()
                    && e.getOrigin().get().getPlanScenarioOrigin().getPlanId()
                    == topologyInfo.getTopologyContextId()) {
                planAddedEntities.add(e);
            }
        });
        // Parse the entities in infra layer and app layer into different subsets.
        // Each subset contains entities that are connected with each other via consumer
        // or provider relationship. We are doing this two layer separation because empirically
        // applications are tend to have closer connection across different infrastructures.
        // Without the layer separation, excessive infrastructure level entities may be dragged into
        // scope due to the connection at application level, thus we may fail our goal of reducing
        // unnecessary entities by adding this scoping logic.
        // For example, when user scoped to a host cluster, usually they dont care about the other
        // clusters, even though some business applications may consume on appServers that consume
        // VM from both host clusters.
        // TODO: On the other hand, when user scoped to a business application, we may want to bring
        // all appServers for it and all VMs hosting those appServers. But we are not sure if we
        // should bring the hosts that are currently hosting those VM, or all hosts that can
        // potentially be providers of those VM.
        logger.info("============== segregate topology graph starts ===============");
        Map<Integer, Set<TopologyEntity>> infraEntitySubsetMap = new HashMap<>();
        Map<Integer, Set<TopologyEntity>> appEntitySubsetMap = new HashMap<>();
        int subsetCount = 0;
        for (Set<TopologyEntity> set : segregateEntities(infraLayerEntities)) {
            infraEntitySubsetMap.put(subsetCount++, set);
        }
        logger.info("{} subsets created for infrastructure layer", subsetCount);
        for (Set<TopologyEntity> set : segregateEntities(appLayerEntities)) {
            appEntitySubsetMap.put(subsetCount++, set);
        }
        logger.info("{} subsets created in total", subsetCount);
        logger.info("============== segregate topology graph ends ===============");

        // a list of ids to keep track of subsets that are included in the resultEntityMap
        Set<Integer> alreadyIncludedSubsetIds = new HashSet<>();
        Map<Long, TopologyEntity.Builder> scopingResult = new HashMap<>();
        Map<EntityType, Set<TopologyEntity>> allSeed = getSeedEntities(groupResolver, planScope, graph);
        // application layer seed
        Set<TopologyEntity> appSeed = allSeed.entrySet().stream()
                .filter(e -> applicationEntityTypes.contains(e.getKey().getNumber()))
                .map(Entry::getValue)
                .flatMap(set -> set.stream())
                .collect(Collectors.toSet());
        // infrastructure layer seed, which also includes VM
        Set<TopologyEntity> infraSeed = allSeed.entrySet().stream()
                .filter(e -> !appSeed.contains(e))
                .map(Entry::getValue)
                .flatMap(set -> set.stream())
                .collect(Collectors.toSet());
        // we start to find the application layer subsets containing the appSeed, then filter out VM,
        // use those VM to find subsets containing those VM in infrastructure layer
        // TODO: now we are trying to bring all possible providers for the VM.
        Map<Long, TopologyEntity> appResult = findConnectedEntitiesForAppSeed(graph,
                appSeed, infraEntitySubsetMap, appEntitySubsetMap, alreadyIncludedSubsetIds);
        appResult.entrySet().forEach( e-> {
            scopingResult.put(e.getKey(), TopologyEntity.newBuilder(e.getValue().getTopologyEntityDtoBuilder()));
        });
        // we start to find the subsets containing the infraSeed, then filter out VM,
        // traverse up supply chain recursively for those VM using consumer relationship
        // to include the needed application layer entities.
        // Note: we dont use the application subsets to find the VM's connected entities because
        // those subsets may contain excessive entities especially if there is BA on the top of VM's
        // supply chain
        Map<Long, TopologyEntity> infraResult = findConnectedEntitiesForInfraSeed(graph,
                infraSeed, infraEntitySubsetMap, alreadyIncludedSubsetIds);
        infraResult.entrySet().forEach( e-> {
            scopingResult.put(e.getKey(), TopologyEntity.newBuilder(e.getValue().getTopologyEntityDtoBuilder()));
        });
        // populate entities added by plan configuration
        planAddedEntities.forEach(a -> scopingResult.put(a.getOid(), TopologyEntity
                .newBuilder(a.getTopologyEntityDtoBuilder())));
        logger.debug("entities added by user total count {}", planAddedEntities.size());
        // record the result after scoping
        logger.info("Final scoped entity set total count is {}", scopingResult.size());
        entityCountMsg(scopingResult.values().stream()
                .map(TopologyEntity.Builder::build)
                .collect(Collectors.toSet()));
        return new TopologyGraphCreator<>(scopingResult).build();
    }

    /**
     * Find connected entities for a set of seed entities that belong to infrastructure layer.
     * First start from the seed to find all infrastructure layer subsets that contains seed.
     * Then filter out the overlapping entities from first pass, recursively find all application
     * layer consumers of those overlapping entities. The result is all those consumers plus all the
     * entities in subsets of the first pass.
     *
     * @param graph the topology entity graph
     * @param infraSeed the entities in the plan scope at infrastructure layer
     * @param infraEntitySubsetMap the parsed infrastructure layer entities
     * @param alreadyIncludedSubsetIds ids of subset that are already included in result
     * @return entity by oid map
     */
    private Map<Long, TopologyEntity> findConnectedEntitiesForInfraSeed(
                                              @Nonnull final TopologyGraph<TopologyEntity> graph,
                                              @Nonnull final Set<TopologyEntity> infraSeed,
                                              @Nonnull final Map<Integer, Set<TopologyEntity>> infraEntitySubsetMap,
                                              @Nonnull final Set<Integer> alreadyIncludedSubsetIds) {
        Map<Long, TopologyEntity> result = new HashMap<>();
        if (infraSeed.isEmpty()) {
            return result;
        }
        logger.info("populating scoped infrastrucutre layer entities in first pass");
        Map<Long, TopologyEntity> infraLayerResult = populateResultMap(infraSeed,
                infraEntitySubsetMap, alreadyIncludedSubsetIds);
        infraLayerResult.entrySet().forEach( e-> {
            result.put(e.getKey(), e.getValue());
        });
        Set<TopologyEntity> overlappingEntities = infraLayerResult.values().stream()
                .filter(i -> i.getEntityType() == OVERLAPPING_ENTITY_TYPE_VALUE)
                .collect(Collectors.toSet());
        logger.trace("overlapping entities {}", overlappingEntities);
        Set<TopologyEntity> consumersInAppLayer = new HashSet<>();
        logger.info("populating scoped infrastructure layer entities in second pass");
        overlappingEntities.forEach(e -> {
            consumersInAppLayer.addAll(findAllConsumers(e));
        });
        consumersInAppLayer.forEach(c -> {
            result.put(c.getOid(), c);
        });
        return result;
    }

    /**
     * Recursively traverse up supply chain to finds an entity's consumer.
     *
     * @param entity the given entity
     * @return a set of consumer entities
     */
    private Set<TopologyEntity> findAllConsumers(TopologyEntity entity) {
        Set<TopologyEntity> consumers = new HashSet<>();
        entity.getConsumers().forEach(c -> {
            logger.trace("adding {} as consumer for {}", c, entity);
            consumers.add(c);
            consumers.addAll(findAllConsumers(c));
        });
        return consumers;
    }

    /**
     * Find connected entities for a set of seed entities that belong to application layer.
     * First start from the seed to find all application layer subsets that contains seed.
     * Then filter out the overlapping entities from first pass, use them as seed to find all
     * infrastructure layer subsets that contains them in the second pass. Combining result
     * from both passes and return an entity by oid map.
     *
     * @param graph the topology entity graph
     * @param appSeed the entities in the plan scope that are at application layer
     * @param infraEntitySubsetMap the parsed infrastructure layer entities
     * @param appEntitySubsetMap the parsed apllication layer entities
     * @param alreadyIncludedSubsetIds ids of subset that are already included in result
     * @return entity by oid map
     */
    private Map<Long, TopologyEntity> findConnectedEntitiesForAppSeed(
                                      @Nonnull final TopologyGraph<TopologyEntity> graph,
                                      @Nonnull final Set<TopologyEntity> appSeed,
                                      @Nonnull final Map<Integer, Set<TopologyEntity>> infraEntitySubsetMap,
                                      @Nonnull final Map<Integer, Set<TopologyEntity>> appEntitySubsetMap,
                                      @Nonnull final Set<Integer> alreadyIncludedSubsetIds) {
        Map<Long, TopologyEntity> result = new HashMap<>();
        if (appSeed.isEmpty()) {
            return result;
        }
        logger.info("populating scoped application layer entities in first pass");
         // find the subsets that contains the consumer entities from plan scope, populate all entities
        // from those subsets
        Map<Long, TopologyEntity> appLayerResult = populateResultMap(appSeed, appEntitySubsetMap,
                alreadyIncludedSubsetIds);
        // filter out VMs from the result
        Set<TopologyEntity> overlappingEntities = appLayerResult.values().stream()
                .filter(r -> r.getEntityType() == OVERLAPPING_ENTITY_TYPE_VALUE)
                .collect(Collectors.toSet());
        logger.debug("overlapping entities {}", overlappingEntities);
        logger.info("populating scoped infrastructure layer entities in second pass");
        // use the VM to find subsets that contains those VM, populate all entities from those subsets
        Map<Long, TopologyEntity> infraLayerResult = populateResultMap(overlappingEntities,
                infraEntitySubsetMap, alreadyIncludedSubsetIds);
        appLayerResult.entrySet().forEach( e-> {
            result.put(e.getKey(), e.getValue());
        });
        infraLayerResult.entrySet().forEach( e-> {
            result.put(e.getKey(), e.getValue());
        });
        return result;
    }

    /**
     * Returns a map of entity set by entity type.
     *
     * @param resultEntityMap the entities to be counted.
     * @return entity count by entity type
     */
    private Map<Integer, Long> entityByType(Map<Long, TopologyEntity.Builder> resultEntityMap) {
        return resultEntityMap.values().stream()
                .collect(Collectors.groupingBy(TopologyEntity.Builder::getEntityType,
                        Collectors.counting()));
    }

    /**
     * A helper method to iterate the entity subsets and find the ones containing a given topology entity.
     *
     * @param ent a given topology entity
     * @param entitySubsetMap a map of entity sets
     * @param alreadyIncludedSubsetIds the list of ids of subsets that has been populated into
     *                                 resultEntityMap. The content of this map can be changed
     *                                 in this method.
     */
    private Map<Long, TopologyEntity> populateResultMap(@Nonnull final Set<TopologyEntity> ent,
                                    @Nonnull final Map<Integer, Set<TopologyEntity>> entitySubsetMap,
                                    @Nonnull final Set<Integer> alreadyIncludedSubsetIds) {
        Map<Long, TopologyEntity> resultEntityMap = new HashMap<>();
        for (TopologyEntity e : ent) {
            for (Entry<Integer, Set<TopologyEntity>> set : entitySubsetMap.entrySet()) {
                if (alreadyIncludedSubsetIds.contains(set.getKey())) {
                    continue;
                } else if (set.getValue().contains(e)){
                    logger.trace("adding {} entities into scope", set.getValue().size());
                    set.getValue().forEach(entity -> {
                        resultEntityMap.put(entity.getOid(), entity);
                        logger.trace(entity.getDisplayName());
                    });
                    alreadyIncludedSubsetIds.add(set.getKey());
                    break;
                }
            }
        }
        return resultEntityMap;
    }
    /**
     * Parse entities into a list of entity sets. Entities in each set will have a provider/consumer
     * relationship between each other, or have a connectedTo/connectedFrom relationship if the entity
     * type exists in inScopeConnectedEntityTypes set. An entity in one set does not have provider
     * or consumer relationship with an entity from another set, no matter such relationship is a
     * direct association or an indirect association.
     *
     * @param entitySet the set of entities to be parsed
     * @return a list of subsets
     */
    private List<Set<TopologyEntity>> segregateEntities(@Nonnull final Set<TopologyEntity> entitySet) {
        List<Set<TopologyEntity>> subsetList = new ArrayList<>();
        Set<TopologyEntity> visitedEntity = new HashSet<TopologyEntity>();
        Iterator<TopologyEntity> iterator = entitySet.iterator();
        while (iterator.hasNext()) {
            // find any entity that is not yet visited, this will be the starting point
            // of a new subset
            TopologyEntity currentEntity = iterator.next();
            Queue<TopologyEntity> queue = new LinkedList<TopologyEntity>();
            if (visitedEntity.contains(currentEntity)) {
                continue;
            } else {
                queue.add(currentEntity);
            }
            // A set containing entities that are connected with each other by
            // consumer-provider relationship.
            Set<TopologyEntity> subGraph = new HashSet<TopologyEntity>();
            // use BFS to search for all entities that are either a consumer or a provider of the
            // current entity. The queue will be empty if current entity has no connection with any
            // other entity, or if all consumers and providers of the current entity are visited.
            while (!queue.isEmpty()) {
                TopologyEntity currentNode = queue.poll();
                if (visitedEntity.contains(currentNode)) { // skip the already visited entity
                    continue;
                }
                subGraph.add(currentNode);
                visitedEntity.add(currentNode);
                logger.trace("current node is {}", currentNode.getDisplayName());
                List<TopologyEntity> connectedEntities = new ArrayList<TopologyEntity>();
                connectedEntities.addAll(currentNode.getConsumers());
                connectedEntities.addAll(currentNode.getProviders());
                // we want to keep entities that are connectedTo/connectedFrom current node
                // if the entity type present in inScopeConnectedEntityTypes. Typically,
                // a virtual volume is not a consumer nor a provider, yet it is referenced
                // during move action interpretation, that is why we need to include it in scope.
                Stream.concat(currentNode.getConnectedFromEntities().stream(),
                    currentNode.getConnectedToEntities().stream())
                    .forEach(c -> {
                        // we need to populate all connected entities of a virtual volume,
                        // and the virtual volume itself. For example, if the scope starts
                        // from a virtual volume, we should associate it with vm so that
                        // it will be part of the topology. If the scope starts from a vm
                        // we should associate the vm with vv by getting the vm's connected
                        // entities.
                        if (inScopeConnectedEntityTypes.contains(currentNode.getEntityType()) ||
                            inScopeConnectedEntityTypes.contains(c.getEntityType())) {
                            connectedEntities.add(c);
                        }
                    });
                List<TopologyEntity> unvisitedEntities = connectedEntities.stream()
                    .filter(c -> !visitedEntity.contains(c) && entitySet.contains(c))
                    .collect(Collectors.toList());
                queue.addAll(unvisitedEntities);
                subGraph.addAll(unvisitedEntities);
                logger.trace("connected are {}", unvisitedEntities);
            }
            // a subset of entities with consumer-provider relationship is formed,
            // add it to our result list
            subsetList.add(subGraph);
            Map<Long, TopologyEntity.Builder> topologyEntityMap = subGraph.stream()
                    .map(TopologyEntity::getTopologyEntityDtoBuilder)
                    .map(TopologyEntity::newBuilder)
                    .collect(Collectors.toMap(TopologyEntity.Builder::getOid, Function.identity()));
            logger.debug("Subset contains {}", topologyEntityMap.values().iterator().next().getDisplayName());
            Map<Integer, Long> entityCountByType = entityByType(topologyEntityMap);
            entityCountByType.entrySet().forEach(count -> {
                logger.debug("Entity type {} with {} entities", count.getKey(), count.getValue());
            });
            logger.trace(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
            subGraph.forEach(e -> logger.trace("{}", e.getDisplayName()));
            logger.trace("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
        }
        return subsetList;
    }

    /**
     * Resolves the plan scope and returns a map of entity set by entity type.
     *
     * @param groupResolver the group resolver
     * @param planScope user defined plan scope
     * @param graph the topology entity graph
     * @return a set of topology entities that are grouped by entity type
     * @throws PipelineStageException
     */
    private Map<EntityType, Set<TopologyEntity>> getSeedEntities(@Nonnull final GroupResolver groupResolver,
                                                                 @Nonnull final PlanScope planScope,
                                                                 @Nonnull final TopologyGraph<TopologyEntity> graph)
                                                                 throws PipelineStageException {
        // create seed entity set by adding scope entries representing individual entities.
        Set<Long> seedEntityIdSet = planScope.getScopeEntriesList().stream()
                .filter(s -> !s.getClassName().equals(StringConstants.CLUSTER)
                        && !s.getClassName().equals(StringConstants.GROUP))
                .map(PlanScopeEntry::getScopeObjectOid)
                .collect(Collectors.toSet());
        // get the group or cluster id in the scope, resolve their members and
        // add into seedEntityIdSet
        Set<Long> groupIds = planScope.getScopeEntriesList().stream()
                .filter(s -> s.getClassName().equals(StringConstants.CLUSTER)
                        || s.getClassName().equals(StringConstants.GROUP))
                .map(PlanScopeEntry::getScopeObjectOid)
                .collect(Collectors.toSet());
        groupServiceClient.getGroups(GetGroupsRequest.newBuilder().addAllId(groupIds)
                .setResolveClusterSearchFilters(true).build())
                .forEachRemaining(g -> {
                    Set<Long> groupMembers = groupResolver.resolve(g, graph);
                    seedEntityIdSet.addAll(groupMembers);
                });
        logger.debug("Seed entity ids : {}", seedEntityIdSet);
        Map<EntityType, Set<TopologyEntity>> seedByEntityType = graph.getEntities(seedEntityIdSet)
                .collect(Collectors.groupingBy(entity -> EntityType.forNumber(entity.getEntityType()),
                        Collectors.toSet()));
        return seedByEntityType;
    }

}
