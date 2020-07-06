package com.vmturbo.topology.processor.topology;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Streams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import gnu.trove.TLongCollection;
import gnu.trove.list.TLongList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.linked.TLongLinkedList;
import gnu.trove.set.TLongSet;
import gnu.trove.set.hash.TLongHashSet;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;

public class PlanTopologyScopeEditor {

    private static final Logger logger = LogManager.getLogger();

    private static final Set<EntityType> CLOUD_SCOPE_ENTITY_TYPES = Stream.of(
            EntityType.REGION,
            EntityType.BUSINESS_ACCOUNT,
            EntityType.VIRTUAL_MACHINE,
            EntityType.DATABASE,
            EntityType.DATABASE_SERVER,
            EntityType.VIRTUAL_VOLUME,
            EntityType.AVAILABILITY_ZONE)
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
                Streams.concat(entity.getInboundAssociatedEntities().stream(), entity.getConsumers().stream())
                        .filter(e -> e.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE)
                        .map(TopologyEntity::getOid)
                        .forEach(connectedVMsAndVVIds::add);
            } else if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                Streams.concat(entity.getOutboundAssociatedEntities().stream(), entity.getProviders().stream())
                        .filter(e -> e.getEntityType() == EntityType.VIRTUAL_VOLUME_VALUE)
                        .map(TopologyEntity::getOid)
                        .forEach(connectedVMsAndVVIds::add);
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
        final Long2ObjectMap<TopologyEntity.Builder> resultEntityMap =
                new Long2ObjectOpenHashMap<>(cloudConsumers.size() + cloudProviders.size());
        Stream.concat(cloudConsumers.stream(), cloudProviders.stream())
                .distinct()
                .filter(e -> discoveredBy(e, targetIds))
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .forEach(bldr -> resultEntityMap.put(bldr.getOid(), TopologyEntity.newBuilder(bldr)));
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
     * @throws GroupResolutionException if the pipeline has errors.
     **/
    public TopologyGraph<TopologyEntity> indexBasedScoping(
            @Nonnull final InvertedIndex<TopologyEntity,
                    CommoditiesBoughtFromProvider> index,
            @Nonnull final TopologyGraph<TopologyEntity> topology,
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final PlanScope planScope,
            PlanProjectType planProjectType) throws GroupResolutionException {
        final Stopwatch stopwatch = Stopwatch.createStarted();

        logger.info("Entering scoping stage for on-prem topology .....");
        // record a count of the number of entities by their entity type in the context.
        entityCountMsg(topology.entities().collect(Collectors.toSet()));

        Map<EntityType, Set<TopologyEntity>> allSeed = getSeedEntities(groupResolver, planScope, topology);

        final TLongSet seedOids = new TLongHashSet(allSeed.values().stream().mapToInt(Set::size).sum());
        allSeed.values().forEach(entities -> entities.forEach(e -> seedOids.add(e.getOid())));

        // a "work queue" of entities to expand; any given OID is only ever added once -
        // if already in 'scopedTopologyOIDs' it has been considered and won't be re-expanded
        // When the seed is a set of entities, we start traversing upwards till top of the supply-chain by successively
        // adding all the customers of one entity into the suppliersToExpand and recursively epanding upwards
        final FastLookupQueue suppliersToExpand = new FastLookupQueue(seedOids);

        // the queue of entities to expand "downwards".
        final FastLookupQueue buyersToSatisfy = new FastLookupQueue();
        final TLongSet visited = new TLongHashSet();

        // the resulting scoped topology - contains at least the seed OIDs
        final TLongSet scopedTopologyOIDs = new TLongHashSet(seedOids);

        // starting with the seed, expand "up"
        while (!suppliersToExpand.isEmpty()) {
            // the traversal is going to be in a random order in this version.
            // We do not lose any functionality or performance by having a random order.
            final long traderOid = suppliersToExpand.remove();
            final Optional<TopologyEntity> optionalEntity = topology.getEntity(traderOid);
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
            final TopologyEntity entity = optionalEntity.get();
            // remember the trader for this OID in the scoped topology & continue expanding "up"
            scopedTopologyOIDs.add(traderOid);

            // Used to track how many suppliers we added while processing this trader.
            final int beforeSuppliers = suppliersToExpand.size();

            // add OIDs of traders THAT buy from this entity which we have not already added
            entity.getConsumers().forEach(e -> {
                final long oid = e.getOid();
                if (!scopedTopologyOIDs.contains(oid)) {
                    suppliersToExpand.tryAdd(oid);
                }
            });

            // For reservation plan we remove all vms..so the cluster is empty.
            // so we have to add the storages explicitly.
            if (planProjectType == PlanProjectType.RESERVATION_PLAN &&
                    entity.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                final boolean isHostEmpty = entity.getConsumers().stream()
                    .noneMatch(a -> (a.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE));
                if (isHostEmpty) {
                    entity.getProviders().stream()
                        .filter(a -> a.getEntityType() == EntityType.STORAGE_VALUE)
                        .forEach(e -> scopedTopologyOIDs.add(e.getOid()));
                }
            }

            // Pull in outBoundAssociatedEntities for VMs and inBoundAssociatedEntities for Storage
            // so as to not skip entities like vVolume that don't buy/sell commodities.
            if (entity.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                entity.getOutboundAssociatedEntities().forEach(e -> {
                    final long oid = e.getOid();
                    if (!scopedTopologyOIDs.contains(oid)) {
                        suppliersToExpand.tryAdd(oid);
                    }
                });
            } else if (entity.getEntityType() == EntityType.STORAGE_VALUE) {
                entity.getInboundAssociatedEntities().forEach(e -> {
                    final long oid = e.getOid();
                    if (!scopedTopologyOIDs.contains(oid)) {
                        suppliersToExpand.tryAdd(oid);
                    }
                });
            }

            final int numSuppliersAdded = suppliersToExpand.size() - beforeSuppliers;
            // if no customers, then "start downwards" from here
            // otherwise keep expanding upwards
            if (numSuppliersAdded == 0) {
                if (!visited.contains(traderOid)) {
                    buyersToSatisfy.tryAdd(traderOid);
                    visited.add(traderOid);
                }
            }
        }

        logger.info("Completed consumer traversal in {} millis.", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        stopwatch.reset();
        stopwatch.start();

        logger.trace("top OIDs: {}", buyersToSatisfy);
        // record the 'providers' we've expanded on the way down so we don't re-expand unnecessarily
        final TLongSet providersExpanded = new TLongHashSet();
        // starting with buyersToSatisfy, expand "downwards"
        while (!buyersToSatisfy.isEmpty()) {
            final long traderOid = buyersToSatisfy.remove();
            final TopologyEntity buyer = topology.getEntity(traderOid).get();
            final boolean skipEntityType = ENTITY_TYPES_TO_SKIP.contains(buyer.getEntityType());
            providersExpanded.add(traderOid);
            // build list of potential sellers for the commodities this Trader buys; omit Traders already expanded
            // also omit traders of the type pulled in as seed members
            final Stream<TopologyEntity> potentialSellers = getPotentialSellers(index, buyer.getTopologyEntityDtoBuilder()
                            .getCommoditiesBoughtFromProvidersList().stream());
            final Stream<TopologyEntity> associatedEntities;
            if (buyer.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE) {
                associatedEntities = Stream.concat(potentialSellers, buyer.getOutboundAssociatedEntities().stream());
            } else if (buyer.getEntityType() == EntityType.STORAGE_VALUE) {
                associatedEntities = Stream.concat(potentialSellers, buyer.getInboundAssociatedEntities().stream());
            } else {
                associatedEntities = potentialSellers;
            }

            associatedEntities.forEach(seller -> {
                final long sellerId = seller.getOid();
                if (!providersExpanded.contains(sellerId)) {
                    // if thisTrader is "skipped", and is not a seed, bring in just the sellers that we have already scoped in.
                    // This logic is for business applications.
                    if (!skipEntityType || (seedOids.contains(traderOid) || scopedTopologyOIDs.contains(seller.getOid())) ) {
                        if (!allSeed.containsKey(EntityType.forNumber(topology.getEntity(sellerId).get().getEntityType()))) {
                            scopedTopologyOIDs.add(sellerId);
                        }
                        if (!visited.contains(sellerId)) {
                            visited.add(sellerId);
                            buyersToSatisfy.tryAdd(sellerId);
                        }
                    }
                }
            });

            // For reservation plan we remove all vms..so the cluster is empty.
            // so we have to add the storages explicitly.
            if (planProjectType == PlanProjectType.RESERVATION_PLAN &&
                    buyer.getEntityType() == EntityType.PHYSICAL_MACHINE_VALUE) {
                final boolean isHostEmpty = buyer.getConsumers().stream()
                    .noneMatch(a -> (a.getEntityType() == EntityType.VIRTUAL_MACHINE_VALUE));
                if (isHostEmpty) {
                    buyer.getProviders().forEach(e -> {
                        if (e.getEntityType() == EntityType.STORAGE_VALUE) {
                            scopedTopologyOIDs.add(e.getOid());
                        }
                    });
                }
            }
        }

        logger.info("Completed buyer traversal in {} millis", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        stopwatch.stop();

        final TopologyGraphCreator<TopologyEntity.Builder, TopologyEntity> graphCreator =
            new TopologyGraphCreator<>(scopedTopologyOIDs.size());
        topology.entities().forEach(entity -> {
            // Make sure to add the plan/reservation entities, even if they're not in scope.
            final boolean planOrReservation =
                entity.getTopologyEntityDtoBuilder().getOrigin().hasPlanScenarioOrigin() ||
                entity.getTopologyEntityDtoBuilder().getOrigin().hasReservationOrigin();
            if (planOrReservation || scopedTopologyOIDs.contains(entity.getOid())) {
                TopologyEntity.Builder eBldr = TopologyEntity.newBuilder(entity.getTopologyEntityDtoBuilder());
                entity.getClonedFromEntity().ifPresent(eBldr::setClonedFromEntity);
                graphCreator.addEntity(eBldr);
            }
        });

        final TopologyGraph<TopologyEntity> retGraph = graphCreator.build();

        logger.info("Completed scoping stage for on-prem topology. {} scoped entities", retGraph.size());
        return retGraph;
    }

    /**
     * Returns a set of potential providers.
     *
     * @param index contains the mapping of commodity to sellers.
     * @param commBoughtFromProviders list of {@link CommoditiesBoughtFromProvider}
     * @return list of potential sellers selling the list of commoditiesBought.
     */
    private Stream<TopologyEntity> getPotentialSellers(@Nonnull final InvertedIndex<TopologyEntity,
            TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider> index,
                       @Nonnull final Stream<CommoditiesBoughtFromProvider> commBoughtFromProviders) {
        // vCenter PMs buy latency and iops with active=false from underlying DSs.
        // Bring in only providers for baskets with atleast 1 active commodity
        return commBoughtFromProviders
            .filter(cbp -> cbp.getCommodityBoughtList().stream().anyMatch(CommodityBoughtDTO::getActive))
            .flatMap(index::getSatisfyingSellers);
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
     * Resolves the plan scope and returns a map of entity set by entity type.
     *
     * @param groupResolver the group resolver
     * @param planScope user defined plan scope
     * @param graph the topology entity graph
     * @return a set of topology entities that are grouped by entity type
     * @throws GroupResolutionException If there is an error resolving one of the scope groups.
     */
    private Map<EntityType, Set<TopologyEntity>> getSeedEntities(@Nonnull final GroupResolver groupResolver,
                                                                 @Nonnull final PlanScope planScope,
                                                                 @Nonnull final TopologyGraph<TopologyEntity> graph)
            throws GroupResolutionException {
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
            final Iterator<Grouping> groups = groupServiceClient.getGroups(GetGroupsRequest.newBuilder()
                    .setGroupFilter(GroupFilter.newBuilder()
                            .addAllId(groupIds))
                    .setReplaceGroupPropertyWithGroupMembershipFilter(true)
                    .build());
            while (groups.hasNext()) {
                final Grouping group = groups.next();
                Set<Long> groupMembers = groupResolver.resolve(group, graph).getAllEntities();
                seedEntityIdSet.addAll(groupMembers);
            }
        }
        logger.debug("Seed entity ids : {}", seedEntityIdSet);
        Map<EntityType, Set<TopologyEntity>> seedByEntityType = graph.getEntities(seedEntityIdSet)
                .collect(Collectors.groupingBy(entity -> EntityType.forNumber(entity.getEntityType()),
                        Collectors.toSet()));
        return seedByEntityType;
    }


    /**
     * A helper object to provide queue-like semantics for traversal, while supporting quick
     * membership checks (to avoid adding same entities multiple times).
     */
    @VisibleForTesting
    static class FastLookupQueue {
        private final TLongList queue;
        private final TLongSet lookupSet;

        FastLookupQueue() {
            queue = new TLongArrayList();
            lookupSet = new TLongHashSet();
        }

        FastLookupQueue(TLongCollection seedOids) {
            queue = new TLongLinkedList(seedOids.size());
            queue.addAll(seedOids);
            lookupSet = new TLongHashSet(seedOids);
        }

        /**
         * Remove and return the first element of the queue.
         * Throws an exception if called with an empty queue. Always call
         * {@link FastLookupQueue#isEmpty()} to check.
         *
         * @return The element.
         */
        long remove() {
            long val = queue.removeAt(0);
            lookupSet.remove(val);
            return val;
        }

        /**
         * Add an oid to the queue if it's not already in the queue.
         *
         * @param oid The element to add.
         */
        void tryAdd(long oid) {
            if (lookupSet.add(oid)) {
                queue.add(oid);
            }
        }

        boolean contains(long oid) {
            return lookupSet.contains(oid);
        }

        int size() {
            return queue.size();
        }

        public boolean isEmpty() {
            return queue.isEmpty();
        }
    }
}
