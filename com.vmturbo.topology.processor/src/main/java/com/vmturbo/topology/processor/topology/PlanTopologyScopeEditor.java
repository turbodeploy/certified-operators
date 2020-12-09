package com.vmturbo.topology.processor.topology;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE;

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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
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

import com.vmturbo.common.protobuf.common.EnvironmentTypeEnum.EnvironmentType;
import com.vmturbo.common.protobuf.group.GroupDTO.GetGroupsRequest;
import com.vmturbo.common.protobuf.group.GroupDTO.GroupFilter;
import com.vmturbo.common.protobuf.group.GroupDTO.Grouping;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectType;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScope;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.PlanScopeEntry;
import com.vmturbo.common.protobuf.topology.TopologyDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommodityBoughtDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.CommoditySoldDTO;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.Builder;
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
import com.vmturbo.topology.processor.topology.pipeline.TopologyPipelineContext;

/**
 * Class to build a list of entities to be included in a plan based on a list of seed entities.
 */
public class PlanTopologyScopeEditor {

    private static final Logger logger = LogManager.getLogger();

    /**
     * List of entity types to be included in the seed ID list.
     */
    private static final Set<EntityType> CLOUD_SCOPE_SEED_ENTITY_TYPES = Stream.of(
            EntityType.REGION,
            EntityType.BUSINESS_ACCOUNT,
            EntityType.VIRTUAL_MACHINE,
            EntityType.DATABASE,
            EntityType.DATABASE_SERVER,
            EntityType.VIRTUAL_VOLUME,
            EntityType.AVAILABILITY_ZONE)
            .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    /*
     * List of entity types to be included in cloud plans. We maintain separate lists so
     * that we can use the seeds types to discover the related entities, but later omit the original
     * entity.  This was added to handle the case where we want to include the VMs, volumes, and
     * databases related to a database server, but to exclude the database server itself.
     */

    /**
     * Entity types to include in optimize cloud plans.
     */
    private static final Set<EntityType> OPTIMIZE_CLOUD_SCOPE_ENTITY_TYPES = Stream.of(
            EntityType.REGION,
            EntityType.BUSINESS_ACCOUNT,
            EntityType.VIRTUAL_MACHINE,
            EntityType.DATABASE,
            EntityType.DATABASE_SERVER,
            EntityType.VIRTUAL_VOLUME,
            EntityType.AVAILABILITY_ZONE,
            EntityType.SERVICE_PROVIDER,
            EntityType.APPLICATION_COMPONENT)
            .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    /**
     * Entity types to include in optimize cloud plans.
     */
    private static final Set<EntityType> MIGRATE_CLOUD_SCOPE_ENTITY_TYPES = Stream.of(
            EntityType.REGION,
            EntityType.BUSINESS_ACCOUNT,
            EntityType.VIRTUAL_MACHINE,
            EntityType.DATABASE,
            EntityType.VIRTUAL_VOLUME,
            EntityType.AVAILABILITY_ZONE,
            EntityType.SERVICE_PROVIDER,
            EntityType.APPLICATION_COMPONENT)
            .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));

    /**
     * We don't include potential providers of the following entities into scope.
     * Current provider will still be included in the scope.
     */
    private static final Set<Integer> PROVIDER_ENTITY_TYPES_TO_SKIP =
            ImmutableSet.of(EntityType.BUSINESS_APPLICATION_VALUE, EntityType.BUSINESS_TRANSACTION_VALUE,
                EntityType.SERVICE_VALUE, EntityType.APPLICATION_COMPONENT_VALUE);

    /**
     * We don't include consumers of the following entities into scope.
     */
    private static final Set<Integer> CONSUMER_ENTITY_TYPES_TO_SKIP =
            ImmutableSet.of(EntityType.VIRTUAL_DATACENTER_VALUE);

    private final GroupServiceBlockingStub groupServiceClient;

    /**
     * Constructor.
     * @param groupServiceClient gRPC handle to group service
     */
    public PlanTopologyScopeEditor(@Nonnull final GroupServiceBlockingStub groupServiceClient) {
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
    }

    /**
     * Scopes topology based on source entities. If source entities includes on-prem workloads
     * also (needed for cloud migration), that is also scoped. Any cloud source entities are
     * included in cloud scoping.
     *
     * @param topologyInfo Plan topology info.
     * @param graph Topology graph.
     * @param context Plan pipeline context.
     * @return {@link TopologyGraph} topology entity graph after applying scope.
     */
    public TopologyGraph<TopologyEntity> scopeTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final TopologyPipelineContext context) {

        final Set<Long> cloudSourceEntities = new HashSet<>();
        final Set<Long> onPremSourceEntities = new HashSet<>();
        context.getSourceEntities()
                .stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(entity -> {
                    // Split source entities into cloud and on-prem.
                    // Env type doesn't seem to be set for some reason, so check owner instead.
                    if (entity.getOwner().isPresent()
                            && entity.getOwner().get().getEntityType() == BUSINESS_ACCOUNT_VALUE) {
                        cloudSourceEntities.add(entity.getOid());
                    } else if (EnvironmentType.ON_PREM == entity.getEnvironmentType()) {
                        onPremSourceEntities.add(entity.getOid());
                    } else {
                        logger.debug("Entity {} oid: {} not cloud or on-prem - excluding from plan",
                                entity.getDisplayName(), entity.getOid());
                    }
                });

        final Long2ObjectMap<TopologyEntity.Builder> resultEntityMap =
                new Long2ObjectOpenHashMap<>(cloudSourceEntities.size() + onPremSourceEntities.size());
        scopeCloudTopology(topologyInfo, graph, cloudSourceEntities, resultEntityMap);
        scopeOnPremTopology(graph, onPremSourceEntities, resultEntityMap);
        return new TopologyGraphCreator<>(resultEntityMap).build();
    }

    /**
     * In cloud plans, filter entities based on user defined scope.
     *
     * @param topologyInfo the topologyInfo which contains topology relevant properties
     * @param graph the topology entity graph
     * @param cloudSourceEntities Optional source entities to expand scope for.
     * @param resultEntityMap Map that is updated to later convert to scoped graph.
     */
    private void scopeCloudTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final Set<Long> cloudSourceEntities,
            @Nonnull final Map<Long, TopologyEntity.Builder> resultEntityMap) {
        // from the seed list keep accounts, regions, and workloads only
        final Set<Long> seedIds =
            topologyInfo.getScopeSeedOidsList().stream()
                .filter(oid -> graph.getEntity(oid)
                                    .map(e -> CLOUD_SCOPE_SEED_ENTITY_TYPES.contains(
                                                    EntityType.forNumber(e.getEntityType())))
                                    .orElse(false))
                .collect(Collectors.toCollection(HashSet::new));

        seedIds.addAll(cloudSourceEntities);

        // for all VMs in the seed, we should bring the connected volumes to the seed
        // and for all volumes in the seed, we should bring the connected VMs
        final Set<Long> connectedVMsAndVVIds = new HashSet<>();
        for (long oid : seedIds) {
            final TopologyEntity entity = graph.getEntity(oid).orElse(null);
            if (entity == null) {
                continue;
            }
            if (entity.getEntityType() == VIRTUAL_VOLUME_VALUE) {
                Streams.concat(entity.getInboundAssociatedEntities().stream(), entity.getConsumers().stream())
                        .filter(e -> e.getEntityType() == VIRTUAL_MACHINE_VALUE)
                        .map(TopologyEntity::getOid)
                        .forEach(connectedVMsAndVVIds::add);
            } else if (entity.getEntityType() == VIRTUAL_MACHINE_VALUE) {
                Streams.concat(entity.getOutboundAssociatedEntities().stream(), entity.getProviders().stream())
                        .filter(e -> e.getEntityType() == VIRTUAL_VOLUME_VALUE)
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
                new HashSet<>(TopologyGraphEntity.applyTransitively(workLoadsZonesAndConsumers,
                        TopologyEntity::getAggregatorsAndOwner));

        // calculate all tiers associated with all the regions in cloudConsumers
        // add the owning services
        final Set<TopologyEntity> tiers = cloudConsumers.stream()
                .filter(e -> e.getEntityType() == EntityType.REGION_VALUE)
                .flatMap(e -> e.getAggregatedEntities().stream())
                .filter(e -> TopologyDTOUtil.isTierEntityType(e.getEntityType()))
                .collect(Collectors.toSet());
        final Set<TopologyEntity> services = tiers.stream()
                .map(TopologyEntity::getOwner)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toSet());

        final Set<EntityType> validEntityTypes =
                topologyInfo.getPlanInfo().getPlanProjectType() == PlanProjectType.CLOUD_MIGRATION
                        ? MIGRATE_CLOUD_SCOPE_ENTITY_TYPES
                        : OPTIMIZE_CLOUD_SCOPE_ENTITY_TYPES;
        Set<TopologyEntity> validCloudConsumersForPlanType = cloudConsumers.stream()
                .filter(e -> e.getEnvironmentType().equals(EnvironmentType.CLOUD)
                        && validEntityTypes.contains(EntityType.forNumber(e.getEntityType())))
                .collect(Collectors.toSet());
        resultEntityMap.putAll(Stream.concat(Stream.of(validCloudConsumersForPlanType, tiers, services)
                        .flatMap(Collection::stream)
                        .filter(e -> discoveredBy(e, targetIds)),
                graph.entitiesOfType(BUSINESS_ACCOUNT_VALUE))
                .distinct()
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toMap(Builder::getOid, TopologyEntity::newBuilder)));
    }

    /**
     * Scopes on-prem topology (needed for cloud migration case).
     *
     * @param graph Topology graph.
     * @param sourceEntityOids Source on-prem entities, if empty, no changes are made.
     * @param resultEntityMap Map is updated with any on-prem entities to include in scope.
     */
    private void scopeOnPremTopology(
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final Set<Long> sourceEntityOids,
            @Nonnull final Map<Long, TopologyEntity.Builder> resultEntityMap) {
        final Set<TopologyEntity> sourceEntities = new HashSet<>();
        sourceEntityOids
                .stream()
                .map(graph::getEntity)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .forEach(sourceEntities::add);
        if (sourceEntities.isEmpty()) {
            return;
        }
        final Set<TopologyEntity> allEntities = new HashSet<>(sourceEntities);
        final Set<TopologyEntity> providers =
                new HashSet<>(TopologyGraphEntity.applyTransitively(
                        new ArrayList<>(sourceEntities),
                        TopologyEntity::getProviders));
        allEntities.addAll(providers);

        final Set<TopologyEntity> volumes = sourceEntities
                .stream()
                .map(TopologyEntity::getOutboundAssociatedEntities)
                .flatMap(Collection::stream)
                .filter(e -> e.getEntityType() == VIRTUAL_VOLUME_VALUE)
                .collect(Collectors.toSet());
        allEntities.addAll(volumes);

        final Collection<Long> targetIds = sourceEntities
                .stream()
                .flatMap(TopologyEntity::getDiscoveringTargetIds)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        // Finally filter by target and update result map.
        resultEntityMap.putAll(allEntities
                .stream()
                .filter(e -> discoveredBy(e, targetIds))
                .map(TopologyEntity::getTopologyEntityDtoBuilder)
                .collect(Collectors.toMap(Builder::getOid, TopologyEntity::newBuilder)));
    }

    /**
     * Returns an instance of the InvertedIndex.
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
                logger.trace("Skipping OID {}, as it is not in the market.", traderOid);
                continue;
            }

            logger.trace("Traversing up from {}:{}", traderOid, optionalEntity.get().getDisplayName());

            final TopologyEntity entity = optionalEntity.get();
            // Skip VDC to avoid bringing its consumers to plan scope so that plan doesn't contain
            // extra workloads from VDC that spanning across clusters. The VDC itself would still
            // be brought into the scope while we expand the topology by traversing down buyersToSatisfy.
            if (CONSUMER_ENTITY_TYPES_TO_SKIP.contains(entity.getEntityType())) {
                logger.trace("Not expanding {}:{} because its type {} is skipped",
                    traderOid, entity.getDisplayName(), entity.getEntityType());
                continue;
            }
            // remember the trader for this OID in the scoped topology & continue expanding "up"
            scopedTopologyOIDs.add(traderOid);

            // Used to track how many suppliers we added while processing this trader.
            final int beforeSuppliers = suppliersToExpand.size();

            // add OIDs of traders THAT buy from this entity which we have not already added
            final TLongSet consumersToExpand = new TLongHashSet();
            entity.getConsumers().forEach(e -> {
                if (!CONSUMER_ENTITY_TYPES_TO_SKIP.contains(e.getEntityType())) {
                    final long oid = e.getOid();
                    if (!scopedTopologyOIDs.contains(oid)) {
                        consumersToExpand.add(oid);
                    }
                }
            });
            logger.trace("Consumers of {}:{} to expand upwards - {}", entity.getOid(),
                entity.getDisplayName(), consumersToExpand);
            consumersToExpand.forEach(consumer -> {
                suppliersToExpand.tryAdd(consumer);
                return true;
            });

            // Pull in outBoundAssociatedEntities for VMs and inBoundAssociatedEntities for Storage
            // so as to not skip entities like vVolume that don't buy/sell commodities.
            if (entity.getEntityType() == VIRTUAL_MACHINE_VALUE) {
                final TLongSet outboundAssociatesToExpand = new TLongHashSet();
                entity.getOutboundAssociatedEntities().forEach(e -> {
                    final long oid = e.getOid();
                    if (!scopedTopologyOIDs.contains(oid)) {
                        outboundAssociatesToExpand.add(oid);
                    }
                });
                logger.trace("Outbound associates of {}:{} to expand upwards - {}", entity.getOid(),
                    entity.getDisplayName(), outboundAssociatesToExpand);
                outboundAssociatesToExpand.forEach(outbound -> {
                    suppliersToExpand.tryAdd(outbound);
                    return true;
                });
            } else if (entity.getEntityType() == STORAGE_VALUE) {
                final TLongSet inboundAssociatesToExpand = new TLongHashSet();
                entity.getInboundAssociatedEntities().forEach(e -> {
                    final long oid = e.getOid();
                    if (!scopedTopologyOIDs.contains(oid)) {
                        inboundAssociatesToExpand.add(oid);
                    }
                });
                logger.trace("Inbound associates of {}:{} to expand upwards - {}",
                    entity.getOid(), entity.getDisplayName(), inboundAssociatesToExpand);
                inboundAssociatesToExpand.forEach(inbound -> {
                    suppliersToExpand.tryAdd(inbound);
                    return true;
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
        logger.trace("Top level buyers: {}", buyersToSatisfy.lookupSet);
        // record the 'providers' we've expanded on the way down so we don't re-expand unnecessarily
        final TLongSet providersExpanded = new TLongHashSet();
        final TLongSet topLevelBuyers = new TLongHashSet();
        topLevelBuyers.addAll(buyersToSatisfy.lookupSet);
        // starting with buyersToSatisfy, expand "downwards"
        while (!buyersToSatisfy.isEmpty()) {
            final long traderOid = buyersToSatisfy.remove();
            final TopologyEntity buyer = topology.getEntity(traderOid).get();
            logger.trace("Traversing down from {}:{}", traderOid, buyer.getDisplayName());
            final boolean skipEntityType = PROVIDER_ENTITY_TYPES_TO_SKIP.contains(buyer.getEntityType());
            providersExpanded.add(traderOid);
            // build list of potential sellers for the commodities this Trader buys; omit Traders already expanded
            // also omit traders of the type pulled in as seed members
            final Set<TopologyEntity> potentialSellers = getPotentialSellers(index, buyer.getTopologyEntityDtoBuilder()
                            .getCommoditiesBoughtFromProvidersList().stream());
            logger.trace("Adding potential sellers as associated entities for {}:{} - {}",
                () -> traderOid, () -> buyer.getDisplayName(),
                () -> potentialSellers.stream().map(TopologyEntity::getOid).map(String::valueOf)
                    .collect(Collectors.joining(",")));
            final Set<TopologyEntity> associatedEntities = Sets.newHashSet();
            associatedEntities.addAll(potentialSellers);
            if (buyer.getEntityType() == VIRTUAL_MACHINE_VALUE) {
                logger.trace("Adding outbound associates as associated entities for {}:{} - {}",
                    () -> traderOid, () -> buyer.getDisplayName(),
                    () -> buyer.getOutboundAssociatedEntities().stream().map(TopologyEntity::getOid)
                        .map(String::valueOf).collect(Collectors.joining(",")));
                associatedEntities.addAll(buyer.getOutboundAssociatedEntities());
            } else if (buyer.getEntityType() == STORAGE_VALUE) {
                logger.trace("Adding inbound associates as associated entities for {}:{} - {}",
                    () -> traderOid, () -> buyer.getDisplayName(),
                    () -> buyer.getInboundAssociatedEntities().stream().map(TopologyEntity::getOid)
                        .map(String::valueOf).collect(Collectors.joining(",")));
                associatedEntities.addAll(buyer.getInboundAssociatedEntities());
                // In case of an empty storage cluster, Storages will not have VMs as customers.
                // And we rely on VMs to pull in the PMs. But we still need to pull in PMs even if
                // there are no VMs. Hence, PMs are pulled in using accesses relation.
                associatedEntities.addAll(getAccessesForTopLevelBuyer(buyer, topology, topLevelBuyers));
            } else if (buyer.getEntityType() == PHYSICAL_MACHINE_VALUE) {
                // In case of an empty cluster, PMs will not have VMs as customers. And we rely on
                // VMs to pull in the Storages. But we still need to pull in Storages even if
                // there are no VMs. Hence, Storages are pulled in using accesses relation.
                associatedEntities.addAll(getAccessesForTopLevelBuyer(buyer, topology, topLevelBuyers));
            }

            associatedEntities.forEach(seller -> {
                final long sellerId = seller.getOid();
                if (!providersExpanded.contains(sellerId)) {
                    // if thisTrader is "skipped", and is not a seed, bring in just the sellers that we have already scoped in.
                    // This logic is for ENTITY_TYPES_TO_SKIP.
                    if (!skipEntityType || (seedOids.contains(traderOid) || scopedTopologyOIDs.contains(seller.getOid())) ) {
                        if (!allSeed.containsKey(EntityType.forNumber(seller.getEntityType()))
                                || seedOids.contains(sellerId)) {
                            scopedTopologyOIDs.add(sellerId);
                            if (!visited.contains(sellerId)) {
                                visited.add(sellerId);
                                buyersToSatisfy.tryAdd(sellerId);
                            }
                        }
                    }
                }
            });
        }
        logger.trace("Scoped topology entities are - {}", scopedTopologyOIDs);
        logger.info("Completed buyer traversal in {} millis", stopwatch.elapsed(TimeUnit.MILLISECONDS));
        stopwatch.stop();

        final TopologyGraphCreator<TopologyEntity.Builder, TopologyEntity> graphCreator =
            new TopologyGraphCreator<>(scopedTopologyOIDs.size());
        topology.entities().forEach(entity -> {
            // Make sure to add the plan/reservation entities, even if they're not in scope.
            final boolean planEntities =
                entity.getTopologyEntityDtoBuilder().getOrigin().hasPlanScenarioOrigin();
            if (planEntities || scopedTopologyOIDs.contains(entity.getOid())) {
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
     * Get all the accesses relations of an entity if it is a top level buyer.
     * We only want the accesses relations for empty clusters. So if a host/storage is at the top
     * level, we should get its accesses. But we don't explicitly check for Host/Storage to keep
     * the code generic. And this is ok because the Accesses relations are only present for hosts
     * and storages today.
     * @param entity TopologyEntity to get the accesses relations of
     * @param topology the topology graph
     * @param topLevelBuyers the oids of entities at the top of the supply chain
     * @return all the accesses relations of the entity if it is a top level entity.
     */
    private Set<TopologyEntity> getAccessesForTopLevelBuyer(TopologyEntity entity,
                                                            TopologyGraph<TopologyEntity> topology,
                                                            TLongSet topLevelBuyers) {
        // Only get the accesses if the entity is a top level buyer.
        if (topLevelBuyers.contains(entity.getOid())) {
            Set<TopologyEntity> accessedEntities =
                entity.getTopologyEntityDtoBuilder().getCommoditySoldListBuilderList()
                    .stream()
                    .filter(CommoditySoldDTO.Builder::hasAccesses)
                    .map(CommoditySoldDTO.Builder::getAccesses)
                    .map(accesses -> topology.getEntity(accesses))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toSet());
            logger.trace("Adding accessed entities as associated entities for {}:{} - {}",
                () -> entity.getOid(), () -> entity.getDisplayName(),
                () -> accessedEntities.stream().map(TopologyEntity::getOid)
                    .map(String::valueOf).collect(Collectors.joining(",")));
            return accessedEntities;
        } else {
            return Collections.emptySet();
        }
    }

    /**
     * Returns a set of potential providers.
     *
     * @param index contains the mapping of commodity to sellers.
     * @param commBoughtFromProviders list of {@link CommoditiesBoughtFromProvider}
     * @return list of potential sellers selling the list of commoditiesBought.
     */
    private Set<TopologyEntity> getPotentialSellers(@Nonnull final InvertedIndex<TopologyEntity,
            TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider> index,
                       @Nonnull final Stream<CommoditiesBoughtFromProvider> commBoughtFromProviders) {
        // vCenter PMs buy latency and iops with active=false from underlying DSs.
        // Bring in only providers for baskets with atleast 1 active commodity
        return commBoughtFromProviders
            .filter(cbp -> cbp.getCommodityBoughtList().stream().anyMatch(CommodityBoughtDTO::getActive))
            .flatMap(index::getSatisfyingSellers)
            .collect(Collectors.toSet());
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
