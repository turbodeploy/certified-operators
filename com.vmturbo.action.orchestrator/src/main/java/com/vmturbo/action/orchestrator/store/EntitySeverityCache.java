package com.vmturbo.action.orchestrator.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.action.orchestrator.topology.ActionGraphEntity;
import com.vmturbo.action.orchestrator.topology.ActionRealtimeTopology;
import com.vmturbo.action.orchestrator.topology.ActionTopologyStore;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.topology.graph.TopologyGraph;

/**
 * Maintain a cache of entity severities. Refreshing the entire cache causes the recomputation of
 * the severity for every entity in the cache. Refreshing the cache when a single action changes
 * updates causes the recomputation of only the "SeverityEntity" for that action (although this
 * operation is not that much faster than a full recomputation because it still requires an
 * examination of every action in the {@link ActionStore}.
 *
 * <p/>The severity for an entity is considered the maximum severity across all "visible" actions for
 * which the entity is a "SeverityEntity".
 *
 * <p>The cache should be invalidated and refreshed when:
 * <ol>
 * <li>A new action arrives in the system
 * <li>The "visibility" of an existing action in the system changes. (Visibility defined as whether
 * the user can see it in the UI).
 * <li>An action transitions from READY to any other state.
 * </ol>
 */
@ThreadSafe
public class EntitySeverityCache {
    /**
     * We calculate risk in the order of this list. As a result, any dependant risks like producers or consumers must come earlier in the list than the entity type that depends on them. For example, we must calculate PHYSICAL_MACHINE before VIRTUAL_MACHINE because VIRTUAL_MACHINE depends on its producers.
     */
    private static final List<TraversalConfig> RETRIEVAL_ORDER =
        ImmutableList.<TraversalConfig>builder()
            // entityType, includeSelf, persist, traverseProducers
            .add(new TraversalConfig(EntityType.STORAGE, true, false, false))
            // Do not traverse producers. A physical machine's producers has ALL storages useD by all
            // VMs hosted on that PM. As a result, unrelated storages would get counted if we
            // traversed the PM's producers.
            .add(new TraversalConfig(EntityType.PHYSICAL_MACHINE, true, false, false))
            // Storage must be processed before Virtual Volume, because Virtual Volume uses storage.
            // Do not traverse the producers because the storage producer will already be connected
            // to the VM. This double count doesn't make sense.
            .add(new TraversalConfig(EntityType.VIRTUAL_VOLUME, true, false, false))
            // VIRTUAL_DATACENTER not needed because it double counts.
            // There is always a link between VM->VDC->PM and VM->PM
            // Virtual Machine must process after STORAGE, PHYSICAL_MACHINE, and VIRTUAL_VOLUME
            // because those results are used in the Virtual Machine's calculation.
            .add(new TraversalConfig(EntityType.VIRTUAL_MACHINE, true, false, true, false,
                // VIRTUAL_VOLUME producer is not available in the producers list from the repository.
                // It's in the connected to list. All other entity types have the needed producers
                // in the api producers list.
                ImmutableSet.of(EntityType.VIRTUAL_VOLUME)))
            //
            // Traversal across the Container "square" is as follows:
            //
            //  Container <-------- ContainerSpec
            //      ^                      X
            //      |                      X
            // ContainerPod ----> WorkloadController
            //                             |
            //                             V
            //                         Namespace
            //
            // Notes:
            // 1. No traversal/accumulation between ContainerSpec and WorkloadController; they
            //    essentially have the same actions: the WorkloadController's actions are the
            //    aggregates of the ContainerSpec's.
            // 2. ContainerPod -> WorkloadController -> Namespace is traversing the consumers
            //    relationship. This way we get desired accumulation towards the Namespace.
            //
            .add(new TraversalConfig(EntityType.CONTAINER_POD, true, false, true))
            .add(new TraversalConfig(EntityType.WORKLOAD_CONTROLLER, true, false, false, true))
            .add(new TraversalConfig(EntityType.NAMESPACE, true, true, false, true))
            .add(new TraversalConfig(EntityType.CONTAINER_SPEC, true, false, false))
            .add(new TraversalConfig(EntityType.CONTAINER, true, false, true))
            .add(new TraversalConfig(EntityType.DATABASE_SERVER, true, false, true))
            // A database can be an instance running on a database server
            .add(new TraversalConfig(EntityType.DATABASE, true, false, true))
            // Application can have Database/Database Server as a producer
            .add(new TraversalConfig(EntityType.APPLICATION_COMPONENT, true, false, true))
            .add(new TraversalConfig(EntityType.SERVICE, false, true, true))
            .add(new TraversalConfig(EntityType.BUSINESS_TRANSACTION, false, true, true))
            .add(new TraversalConfig(EntityType.BUSINESS_APPLICATION, false, true, true))
            .build();

    private final Logger logger = LogManager.getLogger();

    private final Map<Long, Severity> severities = Collections.synchronizedMap(new HashMap<>());
    private final Map<Long, SeverityCount> entitySeverityBreakdowns =
        Collections.synchronizedMap(new HashMap<>());

    private final SeverityComparator severityComparator = new SeverityComparator();
    private final ActionTopologyStore actionTopologyStore;
    private final boolean isCalculatingBreakdowns;

    /**
     * Constructs the EntitySeverityCache that uses grpc to calculation risk propagation.
     *
     * @param actionTopologyStore Used to look up action-related information.
     * @param isCalculatingBreakdowns true for instances that calculate severity breakdowns. When
     *                                false, {@link #getSeverityBreakdown(long)} be empty and
     *                                {@link #getSeverityCounts(List)} will not consider severity
     *                                breakdowns.
     */
    public EntitySeverityCache(@Nonnull final ActionTopologyStore actionTopologyStore,
                               final boolean isCalculatingBreakdowns) {
        this.actionTopologyStore = actionTopologyStore;
        this.isCalculatingBreakdowns = isCalculatingBreakdowns;
        logger.debug("Property isCalculatingBreakdowns is set to " + isCalculatingBreakdowns);
    }

    /**
     * Invalidate and refresh the calculated severity based on the current
     * contents of the action store.
     *
     * @param actionStore the action store to use for the refresh
     */
    public void refresh(@Nonnull final ActionStore actionStore) {
        final Long2ObjectMap<Severity> newSeverities = new Long2ObjectOpenHashMap<>(severities.size());
        visibleActionViews(actionStore)
            .forEach(actionView -> handleActionSeverity(actionView, newSeverities));
        final Long2ObjectMap<SeverityCount> newSeverityBreakdowns =
                calculateSeverityBreakdowns(newSeverities);
        synchronized (severities) {
            severities.clear();
            severities.putAll(newSeverities);
            entitySeverityBreakdowns.clear();
            entitySeverityBreakdowns.putAll(newSeverityBreakdowns);
        }
    }

    /**
     * Refresh the calculated severity for the "SeverityEntity" for the given
     * action based on the current contents of the {@link ActionStore}.
     *
     * @param action The action whose "SeverityEntity" should have its severity recalculated.
     * @param actionStore The action store where the action view for this action is stored. The
     *     entity types map is taken from that action view.
     */
    public void refresh(@Nonnull final Action action, @Nonnull final ActionStore actionStore) {
        try {
            long severityEntity = ActionDTOUtil.getSeverityEntity(action);

            visibleActionViews(actionStore)
                    .filter(actionView -> matchingSeverityEntity(severityEntity, actionView))
                    .map(ActionView::getActionSeverity)
                    .max(severityComparator)
                    .ifPresent(severity -> severities.put(severityEntity, severity));
        } catch (UnsupportedActionException e) {
            logger.error("Unable to refresh severity cache for action {}", action, e);
        }
    }

    /**
     * A lock on {@link #severities} must be held when calling this method.
     * <ol>
     * <li>calculate the break downs of the entities in the order from the bottom of the topology (Storage and VM) to the top (BApp)</li>
     * <li>for each entity look take the break downs of the producers and combine them</li>
     * <li>also add in the severity of the entity itself</li>
     * </ol>
     * We must compute the breakdowns in this way for the following reasons:
     * <ol>
     *     <li>BusinessApplication, BusinessTransaction, and Service can directly contain
     *     Application Component, Database Server, Container, and Virtual Machine</li>
     *     <li>BusinessApplication can contain BusinessTransaction and Service</li>
     *     <li>BusinessTransaction can contain Service</li>
     *     <li>BusinessApplication, BusinessTransaction, and Service can have multiple paths to
     *     entities deeper in the supply chain like a host. All these paths must be counted.
     *     See the bottom for samples.</li>
     * </ol>
     * All potential relationships from BApp, BTxn, and Service are drawn below:
     * <pre>
     *     BusinessApplication --\    /----------------------------------------------------------
     *    /        |              |  |    ApplicationComponent---------\--------------------\    \
     *   /         v              v  |        ^                        v                    v    v
     *  |  BusinessTransaction -->o--/--------/--\-------------->Container----------->VirtualMachine
     *  |          |              ^              v                     ^                    ^
     *   \         V              |       DatabaseServer---------------/--------------------/
     *     ---> Service ---------/
     * </pre>
     * Here are some example edge cases that we need to support:
     * <pre>
     * Example 1: Services end up using the same host
     *      --> Service1 --> App1 --> VM1 ---
     *     /                                 \
     * BTxn1                                  --> Host1
     *     \                                 /
     *      --> Service2 --> App2 --> VM2 ---
     * So if Host1 is Critical, then BTxn1 double counts the critical entity host 1
     *     critical: 2 entities (Host1 thru Service1 and Host1 again through Service2)
     *     normal: 4 entities (App1, App2, VM 1 and VM2)
     * Example 2: Service can have multiple applications or databases:
     * Service1 ----> App1 -> VM1----
     *           \--> App2 -> VM2--  \
     *                             \  \
     *                              ------> Host1
     * Suppose Host1 has a critical action then the breakdown for Service1 is:
     *     critical: 2 entities (Host1 thru App1 and Host1 again through App2)
     *     normal: 4 entities (App1, App2, VM1 and VM2)
     * </pre>
     *
     * @param entitySeverities The entity severities to use for the breakdowns.
     * @return The new severity breakdowns, by entity OID.
     */
    @Nonnull
    public Long2ObjectMap<SeverityCount> calculateSeverityBreakdowns(Long2ObjectMap<Severity> entitySeverities) {
        final Long2ObjectMap<SeverityCount> retMap =
                new Long2ObjectOpenHashMap<>(entitySeverityBreakdowns.size());
        if (isCalculatingBreakdowns) {
            StopWatch stopWatch = new StopWatch();
            stopWatch.start();
            final Long2ObjectMap<SeverityCount> temporarySeverities = new Long2ObjectOpenHashMap<>();
            Optional<ActionRealtimeTopology> curTopology = actionTopologyStore.getSourceTopology();
            if (curTopology.isPresent()) {
                for (TraversalConfig traversalConfig : RETRIEVAL_ORDER) {
                    accumulateCounts(temporarySeverities, curTopology.get().entityGraph(),
                            traversalConfig, entitySeverities, retMap);
                }
            }
            stopWatch.stop();
            logger.info("completed calculateSeverityBreakdowns for {} entities in {}", retMap.size(), stopWatch.toString());
        } else {
            logger.debug("Skipping calculating breakdowns.");
        }
        return retMap;
    }

    /**
     * Accumulates the severity counts in temporarySeverities for only entities of type entityType.
     *
     * @param temporarySeverities The map to temporarily store the severity counts in.
     * @param topology The topology to use for traversals.
     * @param traversalConfig The type of the entities to compute the severity counts for and how
     *                        the traversal should happen. For instance, we should not traverse
     *                        a physical machines producers or else we will hit unrelated storages.
     * @param entitySeverities The per-entity severities to use for the accumulation.
     * @param newSeveritiesOutput The map where we track the new severity breakdowns. This method
     *       will add some entries to this map.
     */
    private void accumulateCounts(Long2ObjectMap<SeverityCount> temporarySeverities,
                                  TopologyGraph<ActionGraphEntity> topology,
                                  TraversalConfig traversalConfig,
                                  Long2ObjectMap<Severity> entitySeverities,
                                  Long2ObjectMap<SeverityCount> newSeveritiesOutput) {
        StopWatch stopWatchMethod = new StopWatch();
        stopWatchMethod.start();


        topology.entitiesOfType(traversalConfig.entityType)
            .forEach(entity -> {
                // add the counts from all the entities before this one
                long oid = entity.getOid();
                SeverityCount severityBreakdown = new SeverityCount();

                if (traversalConfig.traverseProducers) {
                    for (ActionGraphEntity provider : entity.getProviders()) {
                        severityBreakdown.combine(temporarySeverities.get(provider.getOid()));
                    }
                }

                if (traversalConfig.traverseConsumers) {
                    for (ActionGraphEntity provider : entity.getConsumers()) {
                        severityBreakdown.combine(temporarySeverities.get(provider.getOid()));
                    }
                }

                if (!traversalConfig.connectedEntities.isEmpty()) {
                    for (ActionGraphEntity connectedEntity : entity.getOutboundAssociatedEntities()) {
                        if (traversalConfig.connectedEntities.contains(EntityType.forNumber(connectedEntity.getEntityType()))) {
                            severityBreakdown.combine(temporarySeverities.get(connectedEntity.getOid()));
                        }
                    }
                }

                // add the count from the entity itself
                if (traversalConfig.includeSelf) {
                    Severity entitySeverity = entitySeverities.get(oid);
                    if (entitySeverity == null) {
                        entitySeverity = Severity.NORMAL;
                    }
                    severityBreakdown.addSeverity(entitySeverity);
                }
                temporarySeverities.put(oid, severityBreakdown);

                // persist to in memory store
                if (traversalConfig.persist) {
                    newSeveritiesOutput.put(oid, severityBreakdown);
                }
            });

        stopWatchMethod.stop();
        logger.trace("completed accumulateCounts({}) in {}",
            traversalConfig.entityType.name(),
            stopWatchMethod.toString());

    }

    /**
     * Describes how the entities of TraversalConfig's entity type should be used in the
     * {@link EntitySeverityCache#accumulateCounts(Long2ObjectMap, TopologyGraph,
     * TraversalConfig, Long2ObjectMap, Long2ObjectMap)} traversal.
     */
    private static class TraversalConfig {

        @Nonnull
        public final EntityType entityType;
        public final boolean includeSelf;
        public final boolean persist;
        public final boolean traverseProducers;
        public final boolean traverseConsumers;
        public final Set<EntityType> connectedEntities;

        /**
         * Creates an instances that configures how we handle severity breakdown calculations for
         * entities of type {@link EntityType}.
         *
         * @param entityType the {@link EntityType} configured by this {@link TraversalConfig}.
         * @param includeSelf true if each entity with the {@link EntityType} should include it's
         *                    direct severity in the severity break down. For instance, Business
         *                    Application should only include the severities below it, not it's own
         *                    severity.
         * @param persist true if the severity breakdown of the entity should be persisted to
         *                the in memory store, making it visible through
         *                {@link com.vmturbo.action.orchestrator.rpc.EntitySeverityRpcService}.
         * @param traverseProducers true if we should accumulate the severity breakdowns from the
         *                          producers of the entity.
         * @param traverseConsumers true if we should accumulate the severity breakdowns from the
         *                          consumers of the entity. This allows accumulation for entities
         *                          such as namespace and cluster.
         * @param connectedEntities The types of the connected entities we should accumulate
         *                          severity breakdowns from. Sometimes the relationship we need
         *                          for gather severity breakdowns is not available in the
         *                          producers. Additionally, we must specify which type instead of
         *                          a boolean to traversal all to prevent double counting entities
         *                          that are already in the producer list.
         */
        private TraversalConfig(
            @Nonnull final EntityType entityType,
            final boolean includeSelf,
            final boolean persist,
            final boolean traverseProducers,
            final boolean traverseConsumers,
            @Nonnull Set<EntityType> connectedEntities) {
            this.entityType = entityType;
            this.includeSelf = includeSelf;
            this.persist = persist;
            this.traverseProducers = traverseProducers;
            this.traverseConsumers = traverseConsumers;
            this.connectedEntities = connectedEntities;
        }

        private TraversalConfig(
                final EntityType entityType,
                final boolean includeSelf,
                final boolean persist,
                final boolean traverseProducers,
                final boolean traverseConsumers) {
            this(entityType, includeSelf, persist, traverseProducers, traverseConsumers,
                    Collections.emptySet());
        }

        private TraversalConfig(
                final EntityType entityType,
                final boolean includeSelf,
                final boolean persist,
                final boolean traverseProducers) {
            this(entityType, includeSelf, persist, traverseProducers, false);
        }
    }

    /**
     * Class that holds the counts of severities.
     */
    public static class SeverityCount {

        private final Map<Severity, Integer> counts =
            Collections.synchronizedMap(new EnumMap<>(Severity.class));

        /**
         * Increments the provided severity.
         *
         * @param severity the count of the severity to increment.
         */
        public void addSeverity(Severity severity) {
            addSeverity(severity, 1);
        }

        /**
         * Increments the provided severity.
         *
         * @param severity the count of the severity to increment.
         * @param count the amount to increment by.
         */
        @VisibleForTesting
        void addSeverity(Severity severity, int count) {
            int currentCount = counts.getOrDefault(severity, 0);
            counts.put(severity, currentCount + count);
        }

        /**
         * Returns the count of the given severity.
         *
         * @param severity the count of the severity to search for.
         * @return the count of the given severity, or null if not found.
         */
        @Nullable
        public Integer getCountOfSeverity(@Nonnull Severity severity) {
            return counts.get(severity);
        }

        /**
         * Returns the severity counts in a set of Map.Entry.
         * @return the severity counts in a set of Map.Entry.
         */
        public Set<Entry<Severity, Integer>> getSeverityCounts() {
            return counts.entrySet();
        }

        /**
         * Adds the counts from another SeverityCount.
         *
         * @param severityCount the severity breakdown to add to this one.
         */
        public void combine(@Nullable SeverityCount severityCount) {
            if (severityCount != null) {
                severityCount.counts.forEach((severity, count) -> {
                    this.addSeverity(severity, count);
                });
            }
        }

        /**
         * Returns a human readable representation of SeverityCount.
         *
         * @return a human readable representation of SeverityCount.
         */
        public String toString() {
            return counts.toString();
        }
    }

    /**
     * Get the severity for a given entity by that entity's OID.
     *
     * @param entityOid The OID of the entity whose severity should be retrieved.
     * @return The severity  of the entity. Optional.empty() if the severity of the entity is
     *         unknown.
     */
    @Nonnull
    public Optional<Severity> getSeverity(long entityOid) {
        return Optional.ofNullable(severities.get(entityOid));
    }

    /**
     * Get the severity breakdown for a given entity by that entity's OID.
     *
     * @param entityOid The OID of the entity whose severity breakdown should be retrieved.
     * @return The severity breakdown of the entity. Optional.empty() if the severity breakdown of
     *         the entity is unknown.
     */
    @Nonnull
    public Optional<SeverityCount> getSeverityBreakdown(long entityOid) {
        return Optional.ofNullable(entitySeverityBreakdowns.get(entityOid));
    }

    /**
     * Get the severity counts for the entities in the stream.
     * Entities that are unknown to the cache are mapped to an {@link Optional#empty()} severity.
     *
     * <p/>Note that we calculate severity based on actions that apply to an entity and the AO only
     * knows about entities that have actions because it doesn't receive the topology that
     * contains the authoratitive list of all entities. So if no actions apply to an entity,
     * the AO won't know about that entity.
     *
     * <p/>So this creates the following problem: if you ask for the severity of a real entity that
     * has no actions, or if you ask for the severity of an entity that does not actually exist,
     * the AO has to respond with "I don't know" in both cases. If querying for real entities,
     * the AO response of "I don't know" means that there were no actions for an entity,
     * meaning nothing is wrong with it, meaning it's in good shape (ie NORMAL severity). However,
     * note well that it is UP TO THE CALLER to recognize if an unknown severity maps to NORMAL
     * or to something else given the broader context of what the caller is doing.
     *
     * @param entityOids The oids for the entities whose severities should be retrieved.
     *
     * @return A map of the severities and the number of entities that have that severity.
     *         An entity whose severity is not known by the cache will be mapped to empty.
     */
    @Nonnull
    public Map<Optional<Severity>, Long> getSeverityCounts(@Nonnull final List<Long> entityOids) {
        Map<Optional<Severity>, Long> accumulatedCounts = new HashMap<>();
        synchronized (severities) {
            for (Long oid : entityOids) {
                SeverityCount countForOid = entitySeverityBreakdowns.get(oid);
                if (countForOid != null) {
                    for (Entry<Severity, Integer> entry : countForOid.getSeverityCounts()) {
                        Optional<Severity> key = Optional.of(entry.getKey());
                        long previous = accumulatedCounts.getOrDefault(key, 0L);
                        accumulatedCounts.put(key, previous + entry.getValue());
                    }
                } else {
                    Optional<Severity> key = Optional.ofNullable(severities.get(oid));
                    long previous = accumulatedCounts.getOrDefault(key, 0L);
                    accumulatedCounts.put(key, previous + 1);
                }
            }

            return accumulatedCounts;
        }
    }

    /**
     * Sort given entity oids based on corresponding severity.
     * Note that this method is guarded by a lock
     * so that the severities of entities are in a consistent state when sorting.
     *
     * @param entityOids entity oids
     * @param ascending whether to sort in ascending order
     * @return sorted entity oids
     */
    public List<Long> sortEntityOids(
        @Nonnull final Collection<Long> entityOids,
        final boolean ascending) {
        synchronized (severities) {
            OrderOidBySeverity orderOidBySeverity = new OrderOidBySeverity(this);
            return entityOids.stream()
                .sorted(ascending ? orderOidBySeverity : orderOidBySeverity.reversed())
                .collect(Collectors.toList());
        }
    }

    /**
     * Set the cached value for the actionView's severityEntity to be the maximum of
     * the current value for the severityEntity and the severity associated with the actionView.
     *
     * @param actionView The action whose severity should be updated.
     * @param newSeverities The map of severities we are currently building up. This method will
     *                      modify this map based on the input action view.
     */
    private void handleActionSeverity(@Nonnull final ActionView actionView,
                                      @Nonnull final Long2ObjectMap<Severity> newSeverities) {
        try {
            final long severityEntity = ActionDTOUtil.getSeverityEntity(
                    actionView.getTranslationResultOrOriginal());
            final Severity nextSeverity = actionView.getActionSeverity();
            final Severity existingSeverity = newSeverities.get(severityEntity);
            final Severity newSeverity = maxSeverity(existingSeverity, nextSeverity);
            if (newSeverity != null) {
                newSeverities.put(severityEntity, newSeverity);
            }
        } catch (UnsupportedActionException e) {
            logger.warn("Unable to handle action severity for action {}", actionView);
        }
    }

    /**
     * Check if the severity entity for the ActionView matches the input severityEntity.
     *
     * @param severityEntity The oid of the severityEntity to check for matches.
     * @param actionView The actionView to check as a match.
     * @return True if the severityEntity for the actionView matches the input severityEntity.
     *         False if there is no match or the severity entity for the spec cannot be determined.
     */
    private boolean matchingSeverityEntity(
        long severityEntity,
        @Nonnull final ActionView actionView) {
        try {
            long specSeverityEntity = ActionDTOUtil.getSeverityEntity(
                actionView.getTranslationResultOrOriginal());
            return specSeverityEntity == severityEntity;
        } catch (UnsupportedActionException e) {
            return false;
        }
    }

    private Stream<ActionView> visibleActionViews(@Nonnull final ActionStore actionStore) {
        return actionStore.getActionViews()
                .get(ActionQueryFilter.newBuilder()
                        .setVisible(true)
                        .addAllStates(Arrays.asList(ActionState.READY, ActionState.ACCEPTED))
                        .build());
    }

    /**
     * Compare severities, returning the more severe {@link Severity}.
     *
     * @param s1 The first severity
     * @param s2 The second severity
     * @return The severity that is the more severe. In the case where they are equally
     *         severe no guarantee is made about which will be returned.
     */
    @Nullable
    private Severity maxSeverity(@Nullable Severity s1, @Nullable Severity s2) {
        return severityComparator.compare(s1, s2) > 0 ? s1 : s2;
    }

    /**
     * Compare severities. Higher severities are ordered before lower severities.
     */
    private static class SeverityComparator implements Comparator<Severity> {

        @Override
        public int compare(Severity s1, Severity s2) {
            return (s1 == null ? 0 : s1.getNumber()) - (s2 == null ? 0 : s2.getNumber());
        }
    }

    /**
     * The compartor that orderds oids, by considering their severity breakdown and entity level
     * severity according to the following rules.
     * 1. An entity without severity and without severity breakdown
     * 2. An entity with severity but without severity breakdown
     * 3. An entity with severity but with empty severity breakdown map
     * 4. An entity with lowest severity break down
     * 5. An entity with same proportion, but a higher count of that severity
     * 6. An entity with the same highest severity, but the proportion is higher
     * 7. An entity with a higher severity in the breakdown.
     * 8. An entity with an even higher severity in the breakdown but the entity does not have a
     *    a severity (edge case).
     */
    @VisibleForTesting
    static final class OrderOidBySeverity implements Comparator<Long> {

        private final EntitySeverityCache entitySeverityCache;

        @VisibleForTesting
        OrderOidBySeverity(EntitySeverityCache entitySeverityCache) {
            this.entitySeverityCache = entitySeverityCache;
        }

        @Override
        public int compare(final @Nullable Long oid1, final @Nullable Long oid2) {
            if (oid1 == null && oid2 == null) {
                return 0;
            }
            if (oid1 == null) {
                return -1;
            }
            if (oid2 == null) {
                return 1;
            }

            int severityBreakdownComparison = compareSeverityBreakdown(oid1, oid2);
            if (severityBreakdownComparison != 0) {
                return severityBreakdownComparison;
            }

            return compareDirectSeverity(oid1, oid2);
        }

        private int compareSeverityBreakdown(long oid1,
                                             long oid2) {
            SeverityCount breakdown1 = entitySeverityCache.getSeverityBreakdown(oid1)
                .orElse(null);
            SeverityCount breakdown2 = entitySeverityCache.getSeverityBreakdown(oid2)
                .orElse(null);

            if (breakdown1 == breakdown2) {
                return 0;
            }
            if (breakdown1 == null) {
                return -1;
            }
            if (breakdown2 == null) {
                return 1;
            }

            long breakdownTotal1 = calculateTotalSeverities(breakdown1);
            long breakdownTotal2 = calculateTotalSeverities(breakdown2);

            // Iterate from CRITICAL (highest priority) to UNKNOWN (lowest priority)
            for (int i = Severity.values().length - 1; i >= 0; i--) {
                Severity severity = Severity.values()[i];
                int severityCountComparison = compareSameSeverity(
                    breakdown1.getCountOfSeverity(severity),
                    breakdown2.getCountOfSeverity(severity),
                    breakdownTotal1,
                    breakdownTotal2
                );
                if (severityCountComparison != 0) {
                    return severityCountComparison;
                }
            }

            return 0;
        }

        private static long calculateTotalSeverities(@Nonnull SeverityCount breakdown) {
            return breakdown.getSeverityCounts().stream()
                .map(Entry::getValue)
                .filter(Objects::nonNull)
                .mapToLong(Integer::longValue).sum();
        }

        private static int compareSameSeverity(@Nullable Integer severityCount1,
                                               @Nullable Integer severityCount2,
                                               long breakdownTotal1,
                                               long breakdownTotal2) {
            if (severityCount1 == null) {
                severityCount1 = 0;
            }
            if (severityCount2 == null) {
                severityCount2 = 0;
            }
            // do not divided by 0
            if (breakdownTotal1 == 0) {
                breakdownTotal1 = 1;
            }
            if (breakdownTotal2 == 0) {
                breakdownTotal2 = 1;
            }

            int proportionalComparison = Double.compare(
                severityCount1.doubleValue() / (double)breakdownTotal1,
                severityCount2.doubleValue() / (double)breakdownTotal2
            );
            if (proportionalComparison != 0) {
                return proportionalComparison;
            }

            return Long.compare(severityCount1, severityCount2);
        }

        private int compareDirectSeverity(final long oid1, final long oid2) {
            return Integer.compare(
                entitySeverityCache.getSeverity(oid1).orElse(Severity.NORMAL).getNumber(),
                entitySeverityCache.getSeverity(oid2).orElse(Severity.NORMAL).getNumber());
        }
    }
}
