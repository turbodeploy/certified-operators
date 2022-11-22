package com.vmturbo.topology.processor.topology;

import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.AVAILABILITY_ZONE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.BUSINESS_ACCOUNT_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.CONTAINER_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.PHYSICAL_MACHINE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.REGION_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.STORAGE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_MACHINE_VALUE;
import static com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType.VIRTUAL_VOLUME_VALUE;

import java.util.ArrayList;
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
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.CommoditiesBoughtFromProvider;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO.ConnectedEntity.ConnectionType;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTOUtil;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommodityBoughtView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.CommoditySoldImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.CommoditiesBoughtFromProviderView;
import com.vmturbo.common.protobuf.topology.TopologyPOJO.TopologyEntityImpl.ConnectedEntityImpl;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.commons.analysis.InvertedIndex;
import com.vmturbo.components.common.featureflags.FeatureFlags;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.stitching.TopologyEntity;
import com.vmturbo.topology.graph.TopologyGraph;
import com.vmturbo.topology.graph.TopologyGraphCreator;
import com.vmturbo.topology.graph.TopologyGraphEntity;
import com.vmturbo.topology.processor.group.GroupResolutionException;
import com.vmturbo.topology.processor.group.GroupResolver;

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
            EntityType.APPLICATION_COMPONENT,
            EntityType.VIRTUAL_MACHINE_SPEC)
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
            EntityType.APPLICATION_COMPONENT,
            EntityType.VIRTUAL_MACHINE_SPEC)
            .collect(Collectors.collectingAndThen(Collectors.toSet(),
                                                          Collections::unmodifiableSet));
    /**
     *  Entity types to include in container plans to get costs for VMs and Volumes.
     */
    private static final Set<EntityType> COST_COMPUTATION_SEED_ENTITY_TYPES = Stream.of(
            EntityType.AVAILABILITY_ZONE,
            EntityType.REGION,
            EntityType.BUSINESS_ACCOUNT,
            EntityType.SERVICE_PROVIDER)
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
     * @param sourceEntities The source entities for the plan.
     * @return {@link TopologyGraph} topology entity graph after applying scope.
     */
    public TopologyGraph<TopologyEntity> scopeTopology(
            @Nonnull final TopologyInfo topologyInfo,
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final Set<Long> sourceEntities) {

        final Set<Long> cloudSourceEntities = new HashSet<>();
        final Set<Long> onPremSourceEntities = new HashSet<>();
        sourceEntities.stream()
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
        final Stream<TopologyEntity> scopedEntityStream = getScopedCloudEntities(
                FeatureFlags.CROSS_TARGET_LINKING.isEnabled(),
                graph, cloudConsumers, seedIds, topologyInfo.getPlanInfo().getPlanProjectType());

        resultEntityMap.putAll(Stream.concat(scopedEntityStream,
                        graph.entitiesOfType(BUSINESS_ACCOUNT_VALUE))
                .distinct()
                .map(TopologyEntity::getTopologyEntityImpl)
                .collect(Collectors.toMap(TopologyEntityImpl::getOid, TopologyEntity::newBuilder)));
    }

    /**
     * Gets stream of cloud entities that are in plan scope. Refactored some method calls from
     * scopeCloudTopology() into here. Separated out enable unit test.
     *
     * @param linkTargets Whether targets are linked as part of static infrastructure probes.
     *      If so, we don't filter based on targets, default false initially.
     * @param graph Topology graph.
     * @param cloudConsumers Workloads, regions, zones, accounts etc. that are part of the plan.
     * @param seedIds Initially selected seed entities in the plan.
     * @param planProjectType Type of project - either MCP or not (then assumes OCP).
     * @return Stream of entities that will be added to the plan scope for processing.
     */
    @VisibleForTesting
    @Nonnull
    static Stream<TopologyEntity> getScopedCloudEntities(boolean linkTargets,
            @Nonnull final TopologyGraph<TopologyEntity> graph,
            @Nonnull final Set<TopologyEntity> cloudConsumers,
            @Nonnull final Set<Long> seedIds,
            @Nonnull PlanProjectType planProjectType) {
        // targets of the seeds
        final Collection<Long> targetIds = linkTargets
                ? Collections.emptyList() : graph.getEntities(seedIds)
                .flatMap(TopologyEntity::getDiscoveringTargetIds)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

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

        final Set<EntityType> validEntityTypes = planProjectType == PlanProjectType.CLOUD_MIGRATION
                        ? MIGRATE_CLOUD_SCOPE_ENTITY_TYPES
                        : OPTIMIZE_CLOUD_SCOPE_ENTITY_TYPES;
        Set<TopologyEntity> validCloudConsumersForPlanType = cloudConsumers.stream()
                .filter(e -> e.getEnvironmentType().equals(EnvironmentType.CLOUD)
                        && validEntityTypes.contains(EntityType.forNumber(e.getEntityType())))
                .collect(Collectors.toSet());

        final Stream<TopologyEntity> allEntities = Stream.of(validCloudConsumersForPlanType, tiers, services)
                .flatMap(Collection::stream);
        // If we are cross-linking targets, then return as is without checking discoveredBy.
        return linkTargets ? allEntities : allEntities.filter(e -> discoveredBy(e, targetIds));
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

        // Add in any Hypervisor Servers from associated targets and
        // make VMs be "Aggregated By" Hypervisor Servers from their targets.
        // We decided not to have the VC probe discover this relationship due to
        // the overhead in realtime for large topologies. Since we only need this
        // information in Migrate to Public Cloud plans, we synthesize it here.

        Map<Long, TopologyEntity> hypervisorServersByTargetId = new HashMap<>();
        graph.entitiesOfType(EntityType.HYPERVISOR_SERVER)
            .forEach(hvs -> hvs.getDiscoveringTargetIds()
                .filter(targetIds::contains)
                .forEach(targetId -> hypervisorServersByTargetId.put(targetId, hvs))
            );

        allEntities.addAll(hypervisorServersByTargetId.values());

        sourceEntities.stream()
            .filter(entity -> entity.getEntityType() == VIRTUAL_MACHINE_VALUE)
            .forEach(vm -> {
                vm.getDiscoveringTargetIds()
                    .map(hypervisorServersByTargetId::get)
                    .filter(Objects::nonNull)
                    .forEach(hvs -> {
                        vm.getTopologyEntityImpl().addConnectedEntityList(
                            new ConnectedEntityImpl()
                                .setConnectionType(ConnectionType.AGGREGATED_BY_CONNECTION)
                                .setConnectedEntityId(hvs.getOid())
                                .setConnectedEntityType(hvs.getEntityType()));
                    });
            });

        // Finally filter by target and update result map.
        resultEntityMap.putAll(allEntities
                .stream()
                .filter(e -> discoveredBy(e, targetIds))
                .map(TopologyEntity::getTopologyEntityImpl)
                .collect(Collectors.toMap(TopologyEntityImpl::getOid, TopologyEntity::newBuilder)));
    }

    /**
     * Returns an instance of the InvertedIndex.
     *
     * @return newly created {@link InvertedIndex}
     **/
    public InvertedIndex<TopologyEntity,
            CommoditiesBoughtFromProviderView> createInvertedIndex() {
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
            @Nonnull final InvertedIndex<TopologyEntity, CommoditiesBoughtFromProviderView> index,
            @Nonnull TopologyInfo topologyInfo,
            @Nonnull final TopologyGraph<TopologyEntity> topology,
            @Nonnull final GroupResolver groupResolver,
            @Nonnull final PlanScope planScope,
            PlanProjectType planProjectType) throws GroupResolutionException {
        final String logPrefix = String.format("%s topology [ID=%d, context=%d, plan=%s]:",
                topologyInfo.getTopologyType(), topologyInfo.getTopologyId(),
                topologyInfo.getTopologyContextId(), topologyInfo.getPlanInfo().getPlanType());

        final Stopwatch stopwatch = Stopwatch.createStarted();

        logger.info("{} Entering scoping stage for on-prem/container platform topology .....", logPrefix);
        // record a count of the number of entities by their entity type in the context.
        entityCountMsg(topology.entities().collect(Collectors.toSet()), logPrefix);

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

            // Pull in vVolumes using outBoundAssociatedEntities (usually for VMs) and inBoundAssociatedEntities (usually for Storage).
            addVirtualVolumeToPlanScope(entity, scopedTopologyOIDs);

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
        logger.info("{} Completed consumer traversal in {} millis.", logPrefix, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        stopwatch.reset();

        stopwatch.start();
        logger.trace("{} Top level buyers: {}", logPrefix, buyersToSatisfy.lookupSet);
        // record the 'providers' we've expanded on the way down so we don't re-expand unnecessarily
        final TLongSet providersExpanded = new TLongHashSet();
        final TLongSet topLevelBuyers = new TLongHashSet();

        // the seed OIDs for finding the entities needed for entity cost computation
        final FastLookupQueue costEntitiesSeeds = new FastLookupQueue();

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
            final Set<Long> potentialSellers = new HashSet<>();

            // Potential sellers for cloud virtual volumes wrongly include all the other volumes
            // from the storage tiers providers. For cloud virtual volumes, the storage tiers
            // are fetched from the Region entities while scoping the cost related entities.
            // For On-prem topology, the InboundAssociatedEntities and OutboundAssociatedEntities attributes
            // are used to pull in VirtualVolumes
            // If the cost feature flag is disabled, the region entity and tiers will not be included in the scope,
            // so revert back to fetching storage tiers for the VV entities too
            if (buyer.getEntityType() != VIRTUAL_VOLUME_VALUE) {
                potentialSellers.addAll(getPotentialSellers(topology, index,
                        buyer.getTopologyEntityImpl()
                                .getCommoditiesBoughtFromProvidersList()
                                .stream()));
            }

            logger.trace("Adding potential sellers as associated entities for {}:{} - {}",
                () -> traderOid, () -> buyer.getDisplayName(),
                () -> potentialSellers.stream().map(String::valueOf).collect(Collectors.joining(",")));

            // This is needed for on-prem VV. When on-prem VV feature flag is on, all commodities that VM buys from VV are inactive.
            // So without following line, VV won't be potential seller of VM and won't be pulled into scope.
            scopedTopologyOIDs.addAll(buyer.getTopologyEntityImpl().getCommoditiesBoughtFromProvidersList().stream()
                .filter(commBoughtGrouping -> commBoughtGrouping.getProviderEntityType() == VIRTUAL_VOLUME_VALUE)
                .map(CommoditiesBoughtFromProviderView::getProviderId).collect(Collectors.toList()));

            // Pull in vVolumes using outBoundAssociatedEntities (usually for VMs) and inBoundAssociatedEntities (usually for Storage).
            addVirtualVolumeToPlanScope(buyer, scopedTopologyOIDs);

            final TLongSet associatedEntities = new TLongHashSet();
            associatedEntities.addAll(potentialSellers);

            if (buyer.getEntityType() == VIRTUAL_MACHINE_VALUE) {
                // To compute costs for cloud VMs we need the BA and Region associated with them in the scope
                // Azure VMs are not associated with any AZ, so we get Region using the Aggregator relationship
                if (!buyer.getAggregators().isEmpty()) {
                    buyer.getAggregators().stream().filter(a -> a.getEntityType() == REGION_VALUE)
                            .forEach(a -> costEntitiesSeeds.tryAdd(a.getOid()));
                }
                if (buyer.getOwner().isPresent()
                        && buyer.getOwner().get().getEntityType() == BUSINESS_ACCOUNT_VALUE) {
                    costEntitiesSeeds.tryAdd(buyer.getOwner().get().getOid());
                }
            } else if (buyer.getEntityType() == STORAGE_VALUE) {
                // In case of an empty storage cluster, Storages will not have VMs as customers.
                // And we rely on VMs to pull in the PMs. But we still need to pull in PMs even if
                // there are no VMs. Hence, PMs are pulled in using accesses relation.
                associatedEntities.addAll(getAccessesForTopLevelBuyer(buyer, topology, topLevelBuyers));
            } else if (buyer.getEntityType() == PHYSICAL_MACHINE_VALUE) {
                // In case of an empty cluster, PMs will not have VMs as customers. And we rely on
                // VMs to pull in the Storages. But we still need to pull in Storages even if
                // there are no VMs. Hence, Storages are pulled in using accesses relation.
                associatedEntities.addAll(getAccessesForTopLevelBuyer(buyer, topology, topLevelBuyers));
            } else if (buyer.getEntityType() == CONTAINER_VALUE) {
                // Because container history is actually stored on ContainerSpec entities, we need to ensure
                // that we pull in the related ContainerSpecs even though they don't have any buy/sell
                // relations to the entities in plan scope.
                // ContainerSpecs are aggregators of Containers for kubeturbo 8.2.0 and before
                // ContainerSpecs are controllers of Containers for kubeturbo 8.2.1 and after
                // We check both connections here to maintain backward compatibility
                associatedEntities.addAll(buyer.getAggregators().stream().map(TopologyEntity::getOid).collect(Collectors.toSet()));
                associatedEntities.addAll(buyer.getControllers().stream().map(TopologyEntity::getOid).collect(Collectors.toSet()));
            } else if (buyer.getEntityType() == AVAILABILITY_ZONE_VALUE) {
                // We have encountered AZ as a provider, it can serve as the seed to obtain
                // Region, tiers and service providers to compute costs for entities in the scope
                // AWS VMs are connected to AZ, Azure VMs are not connected to AZ
                // so we obtain the Region for those VMs
                costEntitiesSeeds.tryAdd(buyer.getOid());
            }

            associatedEntities.forEach(sellerId -> {
                if (!providersExpanded.contains(sellerId)) {
                    // if thisTrader is "skipped", and is not a seed, bring in just the sellers that we have already scoped in.
                    // This logic is for ENTITY_TYPES_TO_SKIP.
                    if (!skipEntityType || (seedOids.contains(traderOid) || scopedTopologyOIDs.contains(sellerId))) {
                        final Optional<TopologyEntity> sellerOptional = topology.getEntity(sellerId);
                        if (sellerOptional.isPresent()
                                && (!allSeed.containsKey(EntityType.forNumber(sellerOptional.get().getEntityType()))
                                || seedOids.contains(sellerId))) {
                            scopedTopologyOIDs.add(sellerId);
                            if (!visited.contains(sellerId)) {
                                visited.add(sellerId);
                                buyersToSatisfy.tryAdd(sellerId);
                            }
                        }
                    }
                }
                return true;
            });
        }
        logger.trace("{} Scoped topology entities are - {}", logPrefix, scopedTopologyOIDs);
        logger.info("{} Completed buyer traversal in {} millis", logPrefix, stopwatch.elapsed(TimeUnit.MILLISECONDS));
        stopwatch.stop();

        // Fetching the entities required for the entity calculations
        final Long2ObjectMap<TopologyEntity.Builder> resultEntityMap = new Long2ObjectOpenHashMap<>();
        scopeCostTopology(topology, costEntitiesSeeds, resultEntityMap);
        resultEntityMap.values().stream().forEach(e -> scopedTopologyOIDs.add(e.getOid()));

        final TopologyGraphCreator<TopologyEntity.Builder, TopologyEntity> graphCreator =
            new TopologyGraphCreator<>(scopedTopologyOIDs.size());
        topology.entities().forEach(entity -> {
            // Make sure to add the plan/reservation entities, even if they're not in scope.
            final boolean planEntities =
                entity.getTopologyEntityImpl().getOrigin().hasPlanScenarioOrigin();
            if (planEntities || scopedTopologyOIDs.contains(entity.getOid())) {
                TopologyEntity.Builder eBldr = TopologyEntity.newBuilder(entity.getTopologyEntityImpl());
                entity.getClonedFromEntity().ifPresent(eBldr::setClonedFromEntity);
                graphCreator.addEntity(eBldr);
            }
        });

        final TopologyGraph<TopologyEntity> retGraph = graphCreator.build();

        logger.info("{} Completed scoping stage for on-prem topology. {} scoped entities", logPrefix, retGraph.size());
        entityCountMsg(new HashSet<>(retGraph.entities().collect(Collectors.toSet())), logPrefix);
        return retGraph;
    }

    /**
     * Add virtual volumes to plan scope using outBoundAssociatedEntities and inBoundAssociatedEntities.
     *
     * @param entity an entity
     * @param scopedTopologyOIDs scopedTopologyOIDs
     */
    private void addVirtualVolumeToPlanScope(final TopologyEntity entity,
                                             final TLongSet scopedTopologyOIDs) {
        addVirtualVolumeToPlanScope(entity, scopedTopologyOIDs, entity.getInboundAssociatedEntities());
        addVirtualVolumeToPlanScope(entity, scopedTopologyOIDs, entity.getOutboundAssociatedEntities());
    }

    /**
     * Add virtual volumes to plan scope using outBoundAssociatedEntities and inBoundAssociatedEntities.
     * Virtual volumes need to be included in plan scope in order to generate delete volume actions.
     * When VV feature flag is off, virtual volumes don't buy/sell commodities. VVs are outBoundAssociatedEntities of VMs.
     * When VV feature flag is on, VMs buy from VVs. Storages are outBoundAssociatedEntities of VMs.
     *
     * @param entity an entity
     * @param scopedTopologyOIDs scopedTopologyOIDs
     * @param associatedEntities inbound or outbound associatedEntities
     */
    private void addVirtualVolumeToPlanScope(final TopologyEntity entity,
                                             final TLongSet scopedTopologyOIDs,
                                             final List<TopologyEntity> associatedEntities) {
        TLongSet associatesToExpand = null;
        for (TopologyEntity associatedEntity : associatedEntities) {
            final long oid = associatedEntity.getOid();
            if (associatedEntity.getEntityType() == VIRTUAL_VOLUME_VALUE && !scopedTopologyOIDs.contains(oid)) {
                if (associatesToExpand == null) {
                    associatesToExpand = new TLongHashSet();
                }
                associatesToExpand.add(oid);
            }
        }

        if (associatesToExpand != null) {
            scopedTopologyOIDs.addAll(associatesToExpand);
            logger.trace("associated entities of {}:{} - {}",
                entity.getOid(), entity.getDisplayName(), associatesToExpand);
        }
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
    private Set<Long> getAccessesForTopLevelBuyer(TopologyEntity entity,
                                                  TopologyGraph<TopologyEntity> topology,
                                                  TLongSet topLevelBuyers) {
        // Only get the accesses if the entity is a top level buyer.
        if (topLevelBuyers.contains(entity.getOid())) {
            Set<Long> accessedEntities =
                entity.getTopologyEntityImpl().getCommoditySoldListImplList()
                    .stream()
                    .filter(CommoditySoldImpl::hasAccesses)
                    .map(CommoditySoldImpl::getAccesses)
                    .map(topology::getEntity)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(TopologyEntity::getOid)
                    .collect(Collectors.toSet());
            logger.trace("Adding accessed entities as associated entities for {}:{} - {}",
                entity::getOid, entity::getDisplayName,
                () -> accessedEntities.stream().map(String::valueOf).collect(Collectors.joining(",")));
            return accessedEntities;
        } else {
            return Collections.emptySet();
        }
    }

    /**
     * Returns a set of potential providers.
     *
     * @param topology The topology.
     * @param index contains the mapping of commodity to sellers.
     * @param commBoughtFromProviders list of {@link CommoditiesBoughtFromProvider}
     * @return list of potential sellers selling the list of commoditiesBought.
     */
    private Set<Long> getPotentialSellers(@Nonnull final TopologyGraph<TopologyEntity> topology,
                                          @Nonnull final InvertedIndex<TopologyEntity,
                                              CommoditiesBoughtFromProviderView> index,
                                          @Nonnull final Stream<CommoditiesBoughtFromProviderView> commBoughtFromProviders) {
        // vCenter PMs buy latency and iops with active=false from underlying DSs.
        // Bring in only providers for baskets with atleast 1 active commodity
        return commBoughtFromProviders
            .filter(cbp -> cbp.getCommodityBoughtList().stream().anyMatch(CommodityBoughtView::getActive))
            .flatMap(bought -> getSatisfyingSellers(topology, bought, index))
            .map(TopologyEntity::getOid)
            .collect(Collectors.toSet());
    }

    /**
     * Find the satisfying sellers for a particular set of commodities bought from a provider..
     *
     * @param commoditiesBought The {@link CommoditiesBoughtFromProvider} whose satisfying sellers should be found.
     * @param topology The topology.
     * @param index contains the mapping of commodity to sellers.
     * @return list of potential sellers selling the list of commoditiesBought.
     */
    private Stream<TopologyEntity> getSatisfyingSellers(@Nonnull final TopologyGraph<TopologyEntity> topology,
                                                        @Nonnull final CommoditiesBoughtFromProviderView commoditiesBought,
                                                        @Nonnull final InvertedIndex<TopologyEntity,
                                                            CommoditiesBoughtFromProviderView> index) {
        // If the commodities are immovable, the only possible provider for them is the current provider.
        if (commoditiesBought.hasMovable() && !commoditiesBought.getMovable()) {
            return topology.getEntity(commoditiesBought.getProviderId())
                .map(Stream::of)
                .orElse(Stream.empty());
        } else {
            // Search the inverted index for the satisfying sellers.
            return index.getSatisfyingSellers(commoditiesBought);
        }
    }

    private static boolean discoveredBy(@Nonnull TopologyEntity topologyEntity,
                                        @Nonnull Collection<Long> targetIds) {
        return topologyEntity.getDiscoveringTargetIds().anyMatch(targetIds::contains);
    }

    /**
     * A helper method to print the entities by type count in log.
     * @param entities the list of entities
     */
    private void entityCountMsg(Set<TopologyEntity> entities, String logPrefix) {
        Map<Integer, Long> originEntityByType = entities.stream()
                .collect(Collectors.groupingBy(e -> e.getEntityType(), Collectors.counting()));
        StringBuilder infoMsg = new StringBuilder().append(logPrefix).append(" Entity set contains the following :");
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
     * Scoping to include the entities from the cloud topology that are used to compute entity costs.
     * These entities include - Regions, Tiers, ServiceProviders and BusinessAccounts.
     * AZ --- OwnedBy --- Region   ------ OwnedBy ------ ServiceProvider
     *                      |
     *                   Aggregates
     *                      |
     *                    Tier (Compute, Storage)
     *
     * BA -- OwnedBy -- ServiceProvider
     *
     * @param graph             Topology graph
     * @param costTopologySeeds List of OIDs used as seed to traverse the topology
     *                              for Regions, Tiers, ServiceProviders
     * @param resultEntityMap Map containing the scoped entities
     */
    private void scopeCostTopology(@Nonnull final TopologyGraph<TopologyEntity> graph,
                                    FastLookupQueue costTopologySeeds,
                                    @Nonnull final Map<Long, TopologyEntity.Builder> resultEntityMap) {
        logger.info("Entering scoping stage for costs topology .....");
        if (costTopologySeeds.isEmpty()) {
            logger.trace("Empty seeds list for cost topology.");
            return;
        }

        // seed list consists of AZ, last provider for VMs in the the cloud topology
        final Set<Long> seedIds = new HashSet<>();
        costTopologySeeds.lookupSet.forEach(s -> seedIds.add(s));
        logger.trace("cost topology seeds: {} ", seedIds);

        // targets of the seeds
        final Collection<Long> targetIds = graph.getEntities(seedIds)
                .flatMap(TopologyEntity::getDiscoveringTargetIds)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());

        // the resulting scoped topology - contains at least the seed OIDs
        final TLongSet visited = new TLongHashSet();
        Set<TopologyEntity> scopeEntities = new HashSet<>();

        final TLongSet ownerExpanded = new TLongHashSet();
        while (!costTopologySeeds.isEmpty()) {
            final long seedOid = costTopologySeeds.remove();
            final TopologyEntity entity = graph.getEntity(seedOid).get();

            if (!COST_COMPUTATION_SEED_ENTITY_TYPES.contains(EntityType.forNumber(entity.getEntityType()))) {
                continue;
            }
            if (!ownerExpanded.contains(seedOid)) {
                scopeEntities.add(entity);
            }

            final Set<TopologyEntity> ownerEntities = Sets.newHashSet(entity.getAggregatorsAndOwner());
            ownerEntities.forEach(owner -> {
                final long ownerId = owner.getOid();
                if (!ownerExpanded.contains(ownerId)) {
                    if (!visited.contains(ownerId)) {
                        visited.add(ownerId);
                        costTopologySeeds.tryAdd(ownerId);
                    }
                    scopeEntities.add(owner);
                }
            });

            final Set<TopologyEntity> tiers = entity.getAggregatedEntities().stream()
                    .filter(e -> TopologyDTOUtil.isTierEntityType(e.getEntityType()))
                    .collect(Collectors.toSet());
            scopeEntities.addAll(tiers);
        }

        // Result map
        resultEntityMap.putAll(scopeEntities.stream().filter(e -> discoveredBy(e, targetIds))
                        .map(TopologyEntity::getTopologyEntityImpl)
                        .collect(Collectors.toMap(TopologyEntityImpl::getOid, TopologyEntity::newBuilder)));
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
