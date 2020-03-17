package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.processor.group.GroupResolver;
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipeline.PipelineStageException;

public class PlanTopologyScopeEditor {

    private static final Logger logger = LogManager.getLogger();

    private static final Set<EntityType> CLOUD_SCOPE_ENTITY_TYPES = Stream.of(EntityType.REGION,
                             EntityType.BUSINESS_ACCOUNT, EntityType.VIRTUAL_MACHINE,
                             EntityType.DATABASE, EntityType.DATABASE_SERVER,
                             EntityType.VIRTUAL_VOLUME)
                    .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    private static final Set<Integer> ENTITY_TYPES_TO_SKIP =
            new HashSet<>(Collections.singletonList(EntityType.BUSINESS_APPLICATION_VALUE));

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

    /**
     * Returns an instance of the InvertedIndex
     *
     * @return newly created {@link InvertedIndex}
     **/
    public InvertedIndex<TopologyEntity,
            TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider> createInvertedIndex() {
        // create an instance of the InvertedIndex
        return new InvertedIndex(32, new TopologyInvertedIndexTranslator());
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
     * @param index the inverted index constructed based on the sellers
     * @param topology the topology entity graph the topology entity graph
     * @param groupResolver the group resolver
     * @param planScope the user defined plan scope
     * @param planProjectType the type of plan.
     * @return scoped {@link TopologyGraph}
     * @throws PipelineStageException if the pipeline has errors.
     **/
    public TopologyGraph<TopologyEntity> indexBasedScoping(
            @Nonnull final InvertedIndex<TopologyEntity,
                    CommoditiesBoughtFromProvider> index,
            @Nonnull final TopologyGraph<TopologyEntity> topology,
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final PlanScope planScope,
            PlanProjectType planProjectType) throws PipelineStageException {

        logger.info("Entering scoping stage for on-prem topology .....");
        // record a count of the number of entities by their entity type in the context.
        logger.info("Initial entity graph total count is {}", topology.size());
        entityCountMsg(topology.entities().collect(Collectors.toSet()));

        Map<EntityType, Set<TopologyEntity>> allSeed = getSeedEntities(groupResolver, planScope, topology);
        // a "work queue" of entities to expand; any given OID is only ever added once -
        // if already in 'scopedTopologyOIDs' it has been considered and won't be re-expanded
        List<Long> seedOids = allSeed.values().stream()
                .flatMap(s -> s.stream())
                .mapToLong(TopologyEntity::getOid)
                .boxed().collect(Collectors.toList());
        // When the seed is a set of entities, we start traversing upwards till top of the supply-chain by successively
        // adding all the customers of one entity into the suppliersToExpand and recursively epanding upwards
        Queue<Long> suppliersToExpand = Lists.newLinkedList(seedOids);

        // the queue of entities to expand "downwards".
        Queue<Long> buyersToSatisfy = Lists.newLinkedList();
        Set<Long> visited = new HashSet<>();

        // the resulting scoped topology - contains at least the seed OIDs
        final Set<Long> scopedTopologyOIDs = Sets.newHashSet(suppliersToExpand);

        // starting with the seed, expand "up"
        while (!suppliersToExpand.isEmpty()) {
            long traderOid = suppliersToExpand.remove();
            Optional<TopologyEntity> optionalEntity = topology.getEntity(traderOid);
            if (!optionalEntity.isPresent()) {
                // not all entities are guaranteed to be in the traders set -- the
                // market will exclude entities based on factors such as entitystate, for example.
                // If we encounter an entity that is not in the market, don't expand it any further.
                logger.debug("Skipping OID {}, as it is not in the market.", traderOid);
                continue;
            }

            if (logger.isTraceEnabled()) {
                logger.trace("expand OID {}: {}", traderOid, optionalEntity.get().getDisplayName());
            }
            TopologyEntity entity = optionalEntity.get();
            // remember the trader for this OID in the scoped topology & continue expanding "up"
            scopedTopologyOIDs.add(traderOid);
            // add OIDs of traders THAT buy from this entity which we have not already added
            List<Long> relatedEntityOids = entity.getConsumers().stream()
                    .map(trader -> trader.getOid())
                    .filter(customerOid -> !scopedTopologyOIDs.contains(customerOid) &&
                            !suppliersToExpand.contains(customerOid))
                    .collect(Collectors.toList());
            // For reservation plan we remove all vms..so the cluster is empty.
            // so we have to add the storages explicitly.
            if (planProjectType == PlanProjectType.RESERVATION_PLAN &&
                    entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                boolean ishostEmpty =
                        !entity.getConsumers().stream().anyMatch(a -> (a.getEntityType()
                                == EntityType.VIRTUAL_MACHINE_VALUE));
                if (ishostEmpty) {
                    scopedTopologyOIDs
                            .addAll(entity.getProviders().stream()
                                    .filter(a -> a.getEntityType()
                                            == EntityType.STORAGE_VALUE)
                                    .map(b -> b.getOid())
                                    .collect(Collectors.toSet()));
                }
            }



            // pull in inboundAssociatedEntities of VMs so as to not skip entities like vVolume
            // that doesnt buy/sell commodities
            if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                entity.getOutboundAssociatedEntities().stream()
                        .map(trader -> trader.getOid())
                        .filter(entityOid -> !scopedTopologyOIDs.contains(entityOid) &&
                                !suppliersToExpand.contains(entityOid))
                        .collect(Collectors.toCollection(() -> relatedEntityOids));
            }
            if (relatedEntityOids.size() == 0) {
                // if no customers, then "start downwards" from here
                if (!visited.contains(traderOid)) {
                    buyersToSatisfy.add(traderOid);
                    visited.add(traderOid);
                }
            } else {
                // otherwise keep expanding upwards
                suppliersToExpand.addAll(relatedEntityOids);
                if (logger.isTraceEnabled()) {
                    logger.trace("add supplier oids ");
                    relatedEntityOids.forEach(oid -> logger.trace("{}: {}", oid, entity.getDisplayName()));
                }
            }
        }
        logger.trace("top OIDs: {}", buyersToSatisfy);
        // record the 'providers' we've expanded on the way down so we don't re-expand unnecessarily
        Set<Long> providersExpanded = new HashSet<>();
        // starting with buyersToSatisfy, expand "downwards"
        while (!buyersToSatisfy.isEmpty()) {
            long traderOid = buyersToSatisfy.remove();
            TopologyEntity buyer = topology.getEntity(traderOid).get();
            providersExpanded.add(traderOid);
            TopologyEntity thisTrader = topology.getEntity(traderOid).get();
            // build list of potential sellers for the commodities this Trader buys; omit Traders already expanded
            Set<TopologyEntity> potentialSellers = getPotentialSellers(index, thisTrader);
            List<Long> sellersAndConnectionsOids = potentialSellers.stream()
                    .map(trader -> trader.getOid())
                    .filter(buyerOid -> !providersExpanded.contains(buyerOid))
                    .collect(Collectors.toList());
            // pull in inboundAssociatedEntities of VMs so as to not skip entities like vVolume
            // that doesnt buy/sell commodities
            if (buyer.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                buyer.getOutboundAssociatedEntities().stream()
                        .map(trader -> trader.getOid())
                        .filter(entityOid -> !scopedTopologyOIDs.contains(entityOid) &&
                                !suppliersToExpand.contains(entityOid))
                        .collect(Collectors.toCollection(() -> sellersAndConnectionsOids));
            }

            // For reservation plan we remove all vms..so the cluster is empty.
            // so we have to add the storages explicitly.
            if (planProjectType == PlanProjectType.RESERVATION_PLAN &&
                    buyer.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                boolean ishostEmpty =
                        !buyer.getConsumers().stream().anyMatch(a -> (a.getEntityType()
                                == EntityType.VIRTUAL_MACHINE_VALUE));
                if (ishostEmpty) {
                    scopedTopologyOIDs
                            .addAll(buyer.getProviders().stream()
                                    .filter(a -> a.getEntityType()
                                            == EntityType.STORAGE_VALUE)
                                    .map(b -> b.getOid())
                                    .collect(Collectors.toSet()));
                }
            }

            // if thisTrader is a bapp that is not a seed, bring in just the apps that we have already scoped in
            if (ENTITY_TYPES_TO_SKIP.contains(thisTrader.getEntityType())) {
                if (!seedOids.contains(traderOid)) {
                    sellersAndConnectionsOids.retainAll(scopedTopologyOIDs);
                }
            }
            scopedTopologyOIDs.addAll(sellersAndConnectionsOids);
            for (Long buyerOid : sellersAndConnectionsOids) {
                if (visited.contains(buyerOid)) {
                    continue;
                }
                visited.add(buyerOid);
                buyersToSatisfy.add(buyerOid);
            }

            if (logger.isTraceEnabled()) {
                if (sellersAndConnectionsOids.size() > 0) {
                    logger.trace("add buyer oids: ");
                    sellersAndConnectionsOids.forEach(oid -> logger.trace("{}: {}",
                            oid, topology.getEntity(oid).get().getDisplayName()));
                }
            }
        }
        Map<Long, TopologyEntity.Builder> scopingResult = new HashMap<>();
        // return the subset of the original TraderTOs that correspond to the scoped topology
        scopedTopologyOIDs.stream().forEach(oid -> scopingResult.put(oid,
                TopologyEntity.newBuilder(topology.getEntity(oid).get().getTopologyEntityDtoBuilder())
                    .setClonedFromEntityOid(topology.getEntity(oid).get().getClonedFromEntityOid())));
        // including addedEntities into the scope
        topology.entities().filter(entity -> entity.getTopologyEntityDtoBuilder().hasOrigin() &&
                (entity.getTopologyEntityDtoBuilder().getOrigin().hasPlanScenarioOrigin() ||
                entity.getTopologyEntityDtoBuilder().getOrigin().hasReservationOrigin()))
                .forEach(entity -> scopingResult.put(entity.getOid(),
                        entity.getTopologyEntityDtoBuilder().getOrigin().hasPlanScenarioOrigin() ?
                        TopologyEntity.newBuilder(entity.getTopologyEntityDtoBuilder())
                            .setClonedFromEntityOid(entity.getClonedFromEntityOid())
                        : TopologyEntity.newBuilder(entity.getTopologyEntityDtoBuilder())));
        logger.info("Completed scoping stage for on-prem topology .....");
        return new TopologyGraphCreator<>(scopingResult).build();
    }

    /**
     * Returns a set of potential providers.
     *
     * @param index contains the mapping of commodity to sellers.
     * @return list of potential sellers selling the list of commoditiesBought.
     */
    public Set<TopologyEntity> getPotentialSellers(@Nonnull final InvertedIndex<TopologyEntity,
            TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider> index,
                               @Nonnull final TopologyEntity entity) {
        Set<TopologyEntity> potentialSellers = new HashSet<>();
        entity.getTopologyEntityDtoBuilder().getCommoditiesBoughtFromProvidersList().stream()
                // vCenter PMs buy latency and iops with active=false from underlying DSs.
                // Bring in only providers for baskets with atleast 1 active commodity
                .filter(cbp -> cbp.getCommodityBoughtList().stream().anyMatch(c -> c.getActive() == true))
                .forEach(cbp -> potentialSellers.addAll(index.getSatisfyingSellers(cbp).collect(Collectors.toList())));
        return potentialSellers;
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
        if (!groupIds.isEmpty()) {
            groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder()
                            .addAllId(groupIds))
                    .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                    .build())
                    .forEachRemaining(g -> {
                        Set<Long> groupMembers = groupResolver.resolve(g, graph);
                        seedEntityIdSet.addAll(groupMembers);
                    });
        }
        logger.debug("Seed entity ids : {}", seedEntityIdSet);
        Map<EntityType, Set<TopologyEntity>> seedByEntityType = graph.getEntities(seedEntityIdSet)
                .collect(Collectors.groupingBy(entity -> EntityType.forNumber(entity.getEntityType()),
                        Collectors.toSet()));
        return seedByEntityType;
    }

}
