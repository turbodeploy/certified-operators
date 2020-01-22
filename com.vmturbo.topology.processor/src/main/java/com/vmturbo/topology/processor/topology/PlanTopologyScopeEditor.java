package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;

public class PlanTopologyScopeEditor {

    private static final Logger logger = LogManager.getLogger();

    // a set of on-prem application entity type
    private static final Set<Integer> APPLICATION_ENTITY_TYPES = Stream
                    .of(EntityType.APPLICATION_VALUE, EntityType.APPLICATION_SERVER_VALUE,
                            EntityType.BUSINESS_APPLICATION_VALUE, EntityType.DATABASE_SERVER_VALUE,
                            EntityType.DATABASE_VALUE, EntityType.CONTAINER_VALUE,
                            EntityType.CONTAINER_POD_VALUE)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));
    // a set of entity type which indicates the entities to be preserved in scope
    private static final Set<Integer> IN_SCOPE_CONNECTED_ENTITY_TYPES = Stream
                    .of(EntityType.VIRTUAL_VOLUME_VALUE)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));
    // The overlapping entity type in infrastructure and application layer. Currently, VM is
    // used to find application entities if user scoped in infrastructure layer, or is used
    // to find infrastructure entities if user scoped in application layer.
    private static final int OVERLAPPING_ENTITY_TYPE_VALUE = EntityType.VIRTUAL_MACHINE_VALUE;
    private static final Set<EntityType> CLOUD_SCOPE_ENTITY_TYPES = Stream.of(EntityType.REGION,
                             EntityType.BUSINESS_ACCOUNT, EntityType.VIRTUAL_MACHINE,
                             EntityType.DATABASE, EntityType.DATABASE_SERVER,
                             EntityType.VIRTUAL_VOLUME)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private final GroupServiceBlockingStub groupServiceClient;

    public PlanTopologyScopeEditor(@Nonnull final GroupServiceBlockingStub groupServiceClient) {
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
    }

    /**
     * In cloud plans, filter entities based on user defined scope.
     *
     * @param topologyInfo the topologyInfo which contains topology relevant properties
     * @param graph the topology entity graph
     * @return {@link TopologyGraph} topology entity graph after applying scope.
     */
    public TopologyGraph<TopologyEntity> scopeCloudTopology(@Nonnull final TopologyInfo topologyInfo,
                                                            @Nonnull final TopologyGraph<TopologyEntity> graph) {
        // from the seed list keep accounts, regions, and workloads only
        final Set<Long> seedIds =
            topologyInfo.getScopeSeedOidsList().stream()
                .filter(oid -> graph.getEntity(oid)
                                    .map(e -> CLOUD_SCOPE_ENTITY_TYPES.contains(
                                                    EntityType.forNumber(e.getEntityType())))
                                    .orElse(false))
                .collect(Collectors.toCollection(HashSet::new));

        // for all VMs in the seed, we should bring the connected volumes to the seed
        // and for all volumes in the seed, we should bring the connected VMs
        final Set<Long> connectedVMsAndVVIds = new HashSet<>();
        for (long oid : seedIds) {
            final TopologyEntity entity = graph.getEntity(oid).orElse(null);
            if (entity == null) {
                continue;
            }
            if (entity.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE) {
                connectedVMsAndVVIds.addAll(entity.getInboundAssociatedEntities().stream()
                                                .filter(e -> e.getEntityType()
                                                            == EntityType.VIRTUAL_MACHINE_VALUE)
                                                .map(TopologyEntity::getOid)
                                                .collect(Collectors.toList()));
            } else if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                connectedVMsAndVVIds.addAll(entity.getOutboundAssociatedEntities().stream()
                                                .filter(e -> e.getEntityType()
                                                             == EntityType.VIRTUAL_VOLUME_VALUE)
                                                .map(TopologyEntity::getOid)
                                                .collect(Collectors.toList()));
            }
        }
        seedIds.addAll(connectedVMsAndVVIds);

        // targets of the seeds
        final Collection<Long> targetIds = graph.getEntities(seedIds)
                                                .flatMap(TopologyEntity::getDiscoveringTargetIds)
                                                .filter(Objects::nonNull)
                                                .collect(Collectors.toSet());

        // applying inclusion transitively will fetch all workloads associated
        // with a business account or region
        // if scoping on a region, it will also bring availability zones
        final List<TopologyEntity> workloadsAndZones =
            TopologyGraphEntity.applyTransitively(graph.getEntities(seedIds)
                                                       .collect(Collectors.toList()),
                                                  TopologyEntity::getAggregatedAndOwnedEntities)
                .stream()
                .filter(e -> !TopologyDTOUtil.isTierEntityType(e.getEntityType()))
                .distinct()
                .collect(Collectors.toList());

        // applying getConsumers transitively will fetch all transitive consumers
        // of all workloads (e.g., applications, business applications, etc.)
        // if scoping on an account, we now have:
        //   - all sub-accounts
        //   - all workloads owned by the account and its sub-accounts
        //   - all transitive consumers of those workloads
        // if scoping on a region, we now have:
        //   - all zones
        //   - all workloads in the region, except virtual volumes
        //   - all transitive consumers of those workloads
        // if scoping on a single workload, we now have:
        //   - the workload and all its transitive consumers
        final List<TopologyEntity> workLoadsZonesAndConsumers =
            TopologyGraphEntity.applyTransitively(workloadsAndZones, TopologyEntity::getConsumers);

        // get all the entities aggregating the workloads
        // if scoping on an account, we now have:
        //   - all sub-accounts
        //   - the account owning this account
        //   - all workloads owned by the account and its sub-accounts
        //   - the virtual volumes connected to those workloads (for VMs)
        //   - all transitive consumers of those workloads
        //   - all zones and all regions in which those workloads live
        // if scoping on a region, we now have:
        //   - all zones in the region
        //   - all workloads in the region
        //   - all transitive consumers of those workloads
        //   - the virtual volumes connected to those workloads (for VMs)
        //   - all accounts owning those workloads and consumers
        //   - the accounts owning those accounts
        // if scoping on a single workload, we now have:
        //   - the workload and all its transitive consumers
        //   - the virtual volumes connected to the workload (for VMs)
        //   - the zone and region in which the workload lives
        //   - the account owning this workload and the account owning that account
        final Set<TopologyEntity> cloudConsumers =
            TopologyGraphEntity.applyTransitively(workLoadsZonesAndConsumers,
                                                  TopologyEntity::getAggregatorsAndOwner)
                .stream()
                .collect(Collectors.toSet());

        // calculate all tiers associated with all the regions in cloudConsumers
        // add the owning services
        final Set<TopologyEntity> regions = cloudConsumers.stream()
                                                .filter(e -> e.getEntityType() == EntityType.REGION_VALUE)
                                                .collect(Collectors.toSet());
        final Set<TopologyEntity> tiers = regions.stream()
                                            .flatMap(e -> e.getAggregatedEntities().stream())
                                            .filter(e -> TopologyDTOUtil.isTierEntityType(e.getEntityType()))
                                            .collect(Collectors.toSet());
        final Set<TopologyEntity> services = tiers.stream()
                                                .map(TopologyEntity::getOwner)
                                                .filter(Optional::isPresent)
                                                .map(Optional::get)
                                                .collect(Collectors.toSet());
        final List<TopologyEntity> cloudProviders = new ArrayList<>(regions);
        cloudProviders.addAll(tiers);
        cloudProviders.addAll(services);

        // union consumers and producers and filter by target
        final Map<Long, TopologyEntity.Builder> resultEntityMap =
            Stream.concat(cloudConsumers.stream(), cloudProviders.stream())
                    .distinct()
                    .filter(e -> discoveredBy(e, targetIds))
                    .map(TopologyEntity::getTopologyEntityDtoBuilder)
                    .collect(Collectors.toMap(Builder::getOid, TopologyEntity::newBuilder));
        return new TopologyGraphCreator<>(resultEntityMap).build();
    }

    private static boolean discoveredBy(@Nonnull TopologyEntity topologyEntity,
                                        @Nonnull Collection<Long> targetIds) {
        return topologyEntity.getDiscoveringTargetIds().anyMatch(targetIds::contains);
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
            } else if (APPLICATION_ENTITY_TYPES.contains(e.getEntityType())) {
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
                .filter(e -> APPLICATION_ENTITY_TYPES.contains(e.getKey().getNumber()))
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
     * type exists in IN_SCOPE_CONNECTED_ENTITY_TYPES set. An entity in one set does not have provider
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
                // if the entity type present in IN_SCOPE_CONNECTED_ENTITY_TYPES. Typically,
                // a virtual volume is not a consumer nor a provider, yet it is referenced
                // during move action interpretation, that is why we need to include it in scope.
                currentNode.getAllConnectedEntities().stream()
                    .forEach(c -> {
                        // we need to populate all connected entities of a virtual volume,
                        // and the virtual volume itself. For example, if the scope starts
                        // from a virtual volume, we should associate it with vm so that
                        // it will be part of the topology. If the scope starts from a vm
                        // we should associate the vm with vv by getting the vm's connected
                        // entities.
                        if (IN_SCOPE_CONNECTED_ENTITY_TYPES.contains(currentNode.getEntityType()) ||
                            IN_SCOPE_CONNECTED_ENTITY_TYPES.contains(c.getEntityType())) {
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
            if (logger.isTraceEnabled()) {
                logger.trace(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>");
                subGraph.forEach(e -> logger.trace("{}", e.getDisplayName()));
                logger.trace("<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<");
            }
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
                .filter(s -> !StringConstants.GROUP_TYPES.contains(s.getClassName()))
                .map(PlanScopeEntry::getScopeObjectOid)
                .collect(Collectors.toSet());
        // get the group or cluster or storage_cluster id in the scope, resolve their members and
        // add into seedEntityIdSet
        Set<Long> groupIds = planScope.getScopeEntriesList().stream()
                .filter(s -> StringConstants.GROUP_TYPES.contains(s.getClassName()))
                .map(PlanScopeEntry::getScopeObjectOid)
                .collect(Collectors.toSet());
        groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
            .setGroupFilter(GroupFilter.newBuilder()
                .addAllId(groupIds))
            .setReplaceGroupPropertyWithGroupMembershipFilter(true)
            .build())
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
