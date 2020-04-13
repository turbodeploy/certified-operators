package com.vmturbo.action.orchestrator.store;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.action.orchestrator.action.ActionView;
import com.vmturbo.common.protobuf.action.ActionDTO.Action;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionQueryFilter;
import com.vmturbo.common.protobuf.action.ActionDTO.ActionState;
import com.vmturbo.common.protobuf.action.ActionDTO.Severity;
import com.vmturbo.common.protobuf.action.ActionDTOUtil;
import com.vmturbo.common.protobuf.action.UnsupportedActionException;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsRequest;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.GetMultiSupplyChainsResponse;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainNode.MemberList;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainScope;
import com.vmturbo.common.protobuf.repository.SupplyChainProto.SupplyChainSeed;
import com.vmturbo.common.protobuf.repository.SupplyChainServiceGrpc.SupplyChainServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.ApiPartialEntity.RelatedEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.ApiEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Maintain a cache of entity severities. Refreshing the entire cache causes the recomputation of
 * the severity for every entity in the cache. Refreshing the cache when a single action changes
 * updates causes the recomputation of only the "SeverityEntity" for that action (although this
 * operation is not that much faster than a full recomputation because it still requires an
 * examination of every action in the {@link ActionStore}.
 *
 * The severity for an entity is considered the maximum severity across all "visible" actions for
 * which the entity is a "SeverityEntity".
 *
 * <p>The cache should be invalidated and refreshed when:
 * <ol>
 * <li>A new action arrives in the system
 * <li>The "visibility" of an existing action in the system changes. (Visibility defined as whether
 * the user can see it in the UI).
 * <li>An action transitions from READY to any other state.
 * </ol>
 *
 * <p>TODO(davidblinn): The work being done here is somewhat specific to the API. Consider
 * moving this responsibility to the API if possible.
 *
 * @see {@link ActionDTOUtil#getSeverityEntity(Action)}
 */
@ThreadSafe
public class EntitySeverityCache {

    /**
     * BusinessApp, BusinessTransaction, and Service can directly connect to ApplicationComponent,
     * DatabaseServer, Container, and VirtualMachine. In order to efficiently compute the
     * breakdowns, we must first compute the breakdowns for these entities. After computing
     * the breakdowns for these, we can use their breakdowns to build up the breakdowns for
     * Service then BusinessTransaction then BusinessApp.
     */
    private static final RetrieveTopologyEntitiesRequest GET_ENTITIES_DIRECTLY_ATTACHED_TO_BAPP =
        RetrieveTopologyEntitiesRequest.newBuilder()
            .addEntityType(EntityType.APPLICATION_COMPONENT_VALUE)
            .addEntityType(EntityType.DATABASE_SERVER_VALUE)
            .addEntityType(EntityType.CONTAINER_VALUE)
            .addEntityType(EntityType.VIRTUAL_MACHINE_VALUE)
            .setReturnType(Type.MINIMAL)
            .build();

    private final Logger logger = LogManager.getLogger();

    private final Map<Long, Severity> severities = Collections.synchronizedMap(new HashMap<>());
    private final Map<Long, SeverityCount> entitySeverityBreakdowns =
        Collections.synchronizedMap(new HashMap<>());

    private final SeverityComparator severityComparator = new SeverityComparator();
    private final SupplyChainServiceBlockingStub supplyChainService;
    private final RepositoryServiceBlockingStub repositoryService;

    /**
     * These are the entities that need to be retrieved underneath BusinessApp, BusinessTxn, and
     * Service.
     * <pre>
     * BApp -> BTxn -> Service -> AppComp -> Node
     *                                       VirtualMachine  --> VDC ----> Host  --------
     *                       DatabaseServer   \    \   \             ^                \
     *                                          \    \   \___________/                v
     *                                           \    -----> Volume   ------------->  Storage
     *                                            \                                   ^
     *                                             ----------------------------------/
     * </pre>
     */
    private static final List<String> PROPAGATED_ENTITY_TYPES = Arrays.asList(
        ApiEntityType.APPLICATION_COMPONENT.apiStr(),
        ApiEntityType.VIRTUAL_MACHINE.apiStr(),
        ApiEntityType.DATABASE_SERVER.apiStr(),
        ApiEntityType.VIRTUAL_VOLUME.apiStr(),
        ApiEntityType.STORAGE.apiStr(),
        ApiEntityType.VIRTUAL_VOLUME.apiStr(),
        ApiEntityType.PHYSICAL_MACHINE.apiStr());

    /**
     * Constructs the EntitySeverityCache that uses grpc to calculation risk propagation.
     *
     * @param supplyChainService the service that provides supply chain info used in risk
     *                           propagation.
     * @param repositoryService the service that provides entities by type used as the seeds in risk
     *                          propagation.
     */
    public EntitySeverityCache(@Nonnull final SupplyChainServiceBlockingStub supplyChainService,
                               @Nonnull final RepositoryServiceBlockingStub repositoryService) {
        this.supplyChainService = supplyChainService;
        this.repositoryService = repositoryService;
    }

    /**
     * Invalidate and refresh the calculated severity based on the current
     * contents of the action store.
     *
     * @param actionStore the action store to use for the refresh
     */
    public void refresh(@Nonnull final ActionStore actionStore) {
        synchronized (severities) {
            severities.clear();

            visibleReadyActionViews(actionStore)
                .forEach(this::handleActionSeverity);

            calculateSeverityBreakdowns();
        }
    }

    /**
     * A lock on {@link #severities} must be held when calling this method.
     * <ol>
     *     <li>Compute the breakdown for all entities that directly connect to BusinessApp,
     *         BusinessTxn, and Service. These can be: Application Component, Database Server,
     *         Container, and Virtual Machine</li>
     *     <li>Compute Service using the direct connections from the repository, using the counts
     *         from step 1.</li>
     *     <li>Compute Business Transactions using the direct connections from repository, using
     *         the counts accumulated from step 1 and 2.</li>
     *     <li>Compute Business Applications using the direct connections from repository, using
     *         the counts accumulated from step 1, 2 and 3.</li>
     *     <li>Remove break downs for Application Component, Database Server, Container, and
     *         Virtual Machine because they are not used anywhere. It's a waste to keep them
     *         around.</li>
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
     */
    @GuardedBy("severities")
    private void calculateSeverityBreakdowns() {
        entitySeverityBreakdowns.clear();
        final Iterator<PartialEntityBatch> batchedIterator =
            repositoryService.retrieveTopologyEntities(GET_ENTITIES_DIRECTLY_ATTACHED_TO_BAPP);

        final Set<Long> oids = Streams.stream(batchedIterator)
            .map(PartialEntityBatch::getEntitiesList)
            .flatMap(List::stream)
            .map(PartialEntity::getMinimal)
            .map(MinimalEntity::getOid)
            .collect(Collectors.toSet());

        final List<SupplyChainSeed> oidSeeds = oids.stream()
            .map(oid -> SupplyChainSeed.newBuilder()
                .setSeedOid(oid)
                .setScope(SupplyChainScope.newBuilder()
                    .addStartingEntityOid(oid)
                    .addAllEntityTypesToInclude(PROPAGATED_ENTITY_TYPES)
                    .build())
                .build())
            .collect(Collectors.toList());

        final GetMultiSupplyChainsRequest getMultiSupplyChainsRequest =
            GetMultiSupplyChainsRequest.newBuilder()
                .addAllSeeds(oidSeeds)
                .build();

        final Iterator<GetMultiSupplyChainsResponse> multiSupplyChainIterator =
            supplyChainService.getMultiSupplyChains(getMultiSupplyChainsRequest);

        multiSupplyChainIterator.forEachRemaining(getMultiSupplyChainsResponse -> {
            long oid = getMultiSupplyChainsResponse.getSeedOid();
            SeverityCount severityBreakdown = new SeverityCount();
            for (SupplyChainNode node : getMultiSupplyChainsResponse.getSupplyChain()
                .getSupplyChainNodesList()) {
                for (MemberList memberList : node.getMembersByStateMap().values()) {
                    for (long entityOid : memberList.getMemberOidsList()) {
                        Severity entitySeverity = severities.get(entityOid);
                        if (entitySeverity == null) {
                            entitySeverity = Severity.NORMAL;
                        }
                        severityBreakdown.addSeverity(entitySeverity);
                    }
                }
            }
            entitySeverityBreakdowns.put(oid, severityBreakdown);
        });
        // At this point we have the breakdowns for Application Component, Database Server,
        // Container, and Virtual Machine. We can use these for Service, BusinessTransaction, and
        // BusinessApplication.

        // Build up the severity breakdowns starting from Service. Since BTxn depends on Service, we
        // compute BTxn next. Finally we finish off with BApp. We build up in this way so that we
        // do not need to repeat the calculation for the lower levels of the supply chain.
        calculateSeverityBreakdown(EntityType.SERVICE);
        calculateSeverityBreakdown(EntityType.BUSINESS_TRANSACTION);
        calculateSeverityBreakdown(EntityType.BUSINESS_APPLICATION);

        // Remove the Application Component, Database Server, Container, and Virtual Machine because
        // they are not used. It would be a waste of memory to keep them.
        oids.forEach(entitySeverityBreakdowns::remove);
    }

    /**
     * Compute the breakdowns for all the entities that have the provided entity type, using the
     * already computed breakdowns. The breakdowns for the entities under the provided entityType
     * in the supply chain must have their breakdowns already computed.
     * For instance,
     * <ol>
     *     <li>Service requires ApplicationComponent, DatabaseServer, Container, and VirtualMachines
     *         to have their breakdown in entitySeverityBreakdowns.</li>
     *     <li>BusinessTransaction requires the same as Service and requires Service to be
     *         computed</li>
     *     <li>Finally, BusinessApplication requires the same as BusinessTransaction and requires
     *         BusinessTransaction to be computed.</li>
     * </ol>
     * You must grab the monitor for severities before calling this method.
     *
     * @param entityType Computes the severities for the provided entity type. Severity breakdowns
     *                  for all the types that this entity depends on must already be computed.
     */
    @GuardedBy("severities")
    private void calculateSeverityBreakdown(@Nonnull EntityType entityType) {
        Iterator<PartialEntityBatch> batchedIterator =
            repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .addEntityType(entityType.getNumber())
                .setReturnType(Type.API)
                .build());

        Streams.stream(batchedIterator)
            .map(PartialEntityBatch::getEntitiesList)
            .flatMap(List::stream)
            .filter(PartialEntity::hasApi)
            .map(PartialEntity::getApi)
            .forEach(apiPartialEntity -> {
                long oid = apiPartialEntity.getOid();
                SeverityCount severityBreakdown = new SeverityCount();
                for (RelatedEntity providerRelatedEntity : apiPartialEntity.getProvidersList()) {
                    severityBreakdown.combine(
                        entitySeverityBreakdowns.get(providerRelatedEntity.getOid()));
                }
                entitySeverityBreakdowns.put(oid, severityBreakdown);
            });
    }

    /**
     * Class that holds the counts of severities.
     */
    public static class SeverityCount {

        private final Map<Severity, Integer> counts =
            Collections.synchronizedMap(new HashMap<>());

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
     * Refresh the calculated severity for the "SeverityEntity" for the given
     * action based on the current contents of the {@link ActionStore}.
     *
     * TODO(davidblinn): This may need to be made more efficient when lots of actions are updated
     *
     * @param action The action whose "SeverityEntity" should have its severity recalculated.
     * @param actionStore The action store where the action view for this action is stored. The
     *     entity types map is taken from that action view.
     */
    public void refresh(@Nonnull final Action action, @Nonnull final ActionStore actionStore) {
        try {
            long severityEntity = ActionDTOUtil.getSeverityEntity(action);

            visibleReadyActionViews(actionStore)
                .filter(actionView -> matchingSeverityEntity(severityEntity, actionView))
                .map(actionView ->
                    ActionDTOUtil.mapActionCategoryToSeverity(actionView.getActionCategory()))
                .max(severityComparator)
                .ifPresent(severity -> severities.put(severityEntity, severity));
        } catch (UnsupportedActionException e) {
            logger.error("Unable to refresh severity cache for action {}", action, e);
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
     * Note that we calculate severity based on actions that apply to an entity and the AO only
     * knows about entities that have actions because it doesn't receive the topology that
     * contains the authoratitive list of all entities. So if no actions apply to an entity,
     * the AO won't know about that entity.
     *
     * So this creates the following problem: if you ask for the severity of a real entity that
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
            for (long oid : entityOids) {
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
     */
    private void handleActionSeverity(@Nonnull final ActionView actionView) {
        try {
            final long severityEntity = ActionDTOUtil.getSeverityEntity(
                actionView.getTranslationResultOrOriginal());
            final Severity nextSeverity = ActionDTOUtil.mapActionCategoryToSeverity(
                actionView.getActionCategory());

            final Severity maxSeverity = getSeverity(severityEntity)
                .map(currentSeverity -> maxSeverity(currentSeverity, nextSeverity))
                .orElse(nextSeverity);

            severities.put(severityEntity, maxSeverity);
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

    private Stream<ActionView> visibleReadyActionViews(@Nonnull final ActionStore actionStore) {
        return actionStore.getActionViews().get(ActionQueryFilter.newBuilder()
            .setVisible(true)
            .addStates(ActionState.READY)
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
    @Nonnull
    private Severity maxSeverity(Severity s1, Severity s2) {
        return severityComparator.compare(s1, s2) > 0 ? s1 : s2;
    }

    /**
     * Compare severities. Higher severities are ordered before lower severities.
     */
    private static class SeverityComparator implements Comparator<Severity> {

        @Override
        public int compare(Severity s1, Severity s2) {
            return s1.getNumber() - s2.getNumber();
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
