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
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntityBatch;
import com.vmturbo.common.protobuf.topology.UIEntityType;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;

/**
 * Maintain a cache of entity severities. Refreshing the entire cache causes the recomputation of the
 * severity for every entity in the cache. Refreshing the cache when a single action changes updates
 * causes the recomputation of only the "SeverityEntity"
 * for that action (although this operation is not that much faster than a full recomputation because
 * it still requires an examination of every action in the {@link ActionStore}.
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
    private final Logger logger = LogManager.getLogger();

    private final Map<Long, Severity> severities = Collections.synchronizedMap(new HashMap<>());
    private final Map<Long, SeverityCount> entitySeverityBreakdowns = Collections.synchronizedMap(new HashMap<>());

    private final SeverityComparator severityComparator = new SeverityComparator();
    private final SupplyChainServiceBlockingStub supplyChainService;
    private final RepositoryServiceBlockingStub repositoryService;

    /**
     * There are the entities that need to be retrieved underneath BusinessApp, BusinessTxn, and Service.
     * BusinessApp -> BusinessTxn -> Service -> ApplicationComponent -> Node
     *                                          ApplicationServer       VirtualMachine  --> VDC ----> Host  --------
     *                                          DatabaseServer                \    \   \             ^              \
     *                                          Database                       \    \   \___________/                v
     *                                                                          \    -----> Volume   ------------->  Storage
     *                                                                           \                                   ^
     *                                                                            ----------------------------------/
     */
    private static final List<String> PROPAGATED_ENTITY_TYPES = Arrays.asList(
        UIEntityType.BUSINESS_APPLICATION.apiStr(),
        UIEntityType.BUSINESS_TRANSACTION.apiStr(),
        UIEntityType.SERVICE.apiStr(),
        UIEntityType.APPLICATION_COMPONENT.apiStr(),
        UIEntityType.VIRTUAL_MACHINE.apiStr(),
        UIEntityType.APPLICATION_SERVER.apiStr(),
        UIEntityType.DATABASE_SERVER.apiStr(),
        UIEntityType.DATABASE.apiStr(),
        UIEntityType.VIRTUAL_VOLUME.apiStr(),
        UIEntityType.STORAGE.apiStr(),
        UIEntityType.PHYSICAL_MACHINE.apiStr());

    /**
     * Constructs the EntitySeverityCache that uses grpc to calculation risk propagation.
     * @param supplyChainService the service that provides supply chain info used in risk propagation.
     * @param repositoryService the service that provides entities by type used as the seeds in risk propagation.
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

            entitySeverityBreakdowns.clear();
            Iterator<PartialEntityBatch> batchedIterator = repositoryService.retrieveTopologyEntities(RetrieveTopologyEntitiesRequest.newBuilder()
                .addAllEntityType(Arrays.asList(EntityType.BUSINESS_APPLICATION_VALUE, EntityType.BUSINESS_TRANSACTION_VALUE, EntityType.SERVICE_VALUE))
                .setReturnType(Type.MINIMAL)
                .build());

            List<SupplyChainSeed> oidSeeds = Streams.stream(batchedIterator)
                .map(PartialEntityBatch::getEntitiesList)
                .flatMap(List::stream)
                .map(PartialEntity::getMinimal)
                .map(MinimalEntity::getOid)
                .map(oid -> SupplyChainSeed.newBuilder()
                    .setSeedOid(oid)
                    .setScope(SupplyChainScope.newBuilder()
                        .addStartingEntityOid(oid)
                        .addAllEntityTypesToInclude(PROPAGATED_ENTITY_TYPES)
                        .build())
                    .build())
                .collect(Collectors.toList());

            GetMultiSupplyChainsRequest getMultiSupplyChainsRequest = GetMultiSupplyChainsRequest.newBuilder()
                .addAllSeeds(oidSeeds)
                .build();

            Iterator<GetMultiSupplyChainsResponse> multiSupplyChainIterator =
                supplyChainService.getMultiSupplyChains(getMultiSupplyChainsRequest);

            multiSupplyChainIterator.forEachRemaining(getMultiSupplyChainsResponse -> {
                long oid = getMultiSupplyChainsResponse.getSeedOid();
                SeverityCount severityCount = new SeverityCount();
                for (SupplyChainNode node : getMultiSupplyChainsResponse.getSupplyChain().getSupplyChainNodesList()) {
                    for (MemberList memberList : node.getMembersByStateMap().values()) {
                        for (long entityOid : memberList.getMemberOidsList()) {
                            Severity entitySeverity = severities.get(entityOid);
                            if (entitySeverity == null) {
                                entitySeverity = Severity.NORMAL;
                            }
                            severityCount.addSeverity(entitySeverity);
                        }
                    }
                }
                entitySeverityBreakdowns.put(oid, severityCount);
            });
        }
    }

    /**
     * Class that holds the counts of severities.
     */
    public static class SeverityCount {

        private final Map<Severity, Long> severityCount = Collections.synchronizedMap(new HashMap<>());

        /**
         * Increments the provided severity.
         *
         * @param severity the count of the severity to increment.
         */
        public void addSeverity(Severity severity) {
            addSeverity(severity, 1L);
        }

        /**
         * Increments the provided severity.
         *
         * @param severity the count of the severity to increment.
         * @param count the amount to increment by.
         */
        @VisibleForTesting
        void addSeverity(Severity severity, long count) {
            long currentCount = severityCount.getOrDefault(severity, 0L);
            severityCount.put(severity, currentCount + count);
        }

        /**
         * Returns the count of the given severity.
         *
         * @param severity the count of the severity to search for.
         * @return the count of the given severity, or null if not found.
         */
        @Nullable
        public Long getCountOfSeverity(@Nonnull Severity severity) {
            return severityCount.get(severity);
        }

        /**
         * Returns the severity counts in a set of Map.Entry.
         * @return the severity counts in a set of Map.Entry.
         */
        public Set<Entry<Severity, Long>> getSeverityCounts() {
            return severityCount.entrySet();
        }

        /**
         * Returns a human readable representation of SeverityCount.
         *
         * @return a human readable representation of SeverityCount.
         */
        public String toString() {
            return severityCount.toString();
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
     * @return The severity  of the entity. Optional.empty() if the severity of the entity is unknown.
     */
    @Nonnull
    public Optional<Severity> getSeverity(long entityOid) {
        return Optional.ofNullable(severities.get(entityOid));
    }

    /**
     * Get the severity breakdown for a given entity by that entity's OID.
     *
     * @param entityOid The OID of the entity whose severity breakdown should be retrieved.
     * @return The severity breakdown of the entity. Optional.empty() if the severity breakdown of the entity is unknown.
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
                    for (Entry<Severity, Long> entry : countForOid.getSeverityCounts()) {
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
    public List<Long> sortEntityOids(@Nonnull final Collection<Long> entityOids, final boolean ascending) {
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
    private boolean matchingSeverityEntity(long severityEntity, @Nonnull final ActionView actionView) {
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
            SeverityCount breakdown1 = entitySeverityCache.getSeverityBreakdown(oid1).orElse(null);
            SeverityCount breakdown2 = entitySeverityCache.getSeverityBreakdown(oid2).orElse(null);

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
                .mapToLong(Long::longValue).sum();
        }

        private static int compareSameSeverity(@Nullable Long severityCount1,
                                               @Nullable Long severityCount2,
                                               long breakdownTotal1,
                                               long breakdownTotal2) {
            if (severityCount1 == null) {
                severityCount1 = 0L;
            }
            if (severityCount2 == null) {
                severityCount2 = 0L;
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
            return Integer.compare(entitySeverityCache.getSeverity(oid1).orElse(Severity.NORMAL).getNumber(),
                entitySeverityCache.getSeverity(oid2).orElse(Severity.NORMAL).getNumber());
        }
    }
}
