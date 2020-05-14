package com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.immutables.value.Value;

import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.db.tables.records.ComputeTierTypeHourlyByWeekRecord;
import com.vmturbo.cost.component.reserved.instance.ComputeTierDemandStatsStore;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisScope;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstancePurchaseConstraints;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.demand.AccountGroupingIdentifier.AccountGroupingType;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCache;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.RegionalRIMatcherCacheFactory;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.inventory.ReservedInstanceSpecMatcher.ReservedInstanceSpecData;
import com.vmturbo.group.api.GroupAndMembers;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * This class provides a wrapper around {@link ComputeTierDemandStatsStore}, in order to group
 * demand clusters by (account grouping ID, region OID, RI Spec ID). The RI spec ID attribute of the
 * grouping will be determined through {@link ReservedInstanceSpecMatcher}, in order to group clusters
 * by the RI Spec to purchase.
 */
@ThreadSafe
public class RIBuyAnalysisContextProvider {

    private static final Logger logger = LogManager.getLogger();

    // A counter used to generate unique tags for log entries
    private static AtomicInteger analysisTagCounter = new AtomicInteger();

    // An interface for obtaining historical demand data
    private final ComputeTierDemandStatsStore computeTierDemandStatsStore;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final RegionalRIMatcherCacheFactory regionalRIMatcherCacheFactory;

    private final long realtimeTopologyContextId;

    private final boolean allowStandaloneAccountAnalysisClusters;

    /**
     * Construct the {@link RIBuyAnalysisContextProvider} instance.
     *
     * @param computeTierDemandStatsStore   historical demand data store.
     * @param repositoryClient              repository client used for retrieving the latest topology
     *                                      in order to link it to recorded demand
     * @param cloudTopologyFactory          Used to create a cloud topology of the latest topology, in order
     *                                      to verify the referenced entities in the recorded demand still exist
     * @param regionalRIMatcherCacheFactory A factory to create a new {@link RegionalRIMatcherCache} for an
     *                                      analysis context.
     * @param realtimeTopologyContextId     The realtime topology context ID, used in querying the realtime
     *                                      topology to match to recorded demand.
     * @param allowStandaloneAccountAnalysisClusters A boolean flag indicating whether demand clusters, in which
     *                                               the associated account is not tied to a billing family, are
     *                                               allowed.
     */
    public RIBuyAnalysisContextProvider(@Nonnull ComputeTierDemandStatsStore computeTierDemandStatsStore,
                                     @Nonnull RepositoryServiceBlockingStub repositoryClient,
                                     @Nonnull TopologyEntityCloudTopologyFactory cloudTopologyFactory,
                                     @Nonnull RegionalRIMatcherCacheFactory regionalRIMatcherCacheFactory,
                                     long realtimeTopologyContextId,
                                     boolean allowStandaloneAccountAnalysisClusters) {
        this.computeTierDemandStatsStore = Objects.requireNonNull(computeTierDemandStatsStore);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
        this.regionalRIMatcherCacheFactory = Objects.requireNonNull(regionalRIMatcherCacheFactory);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
        this.allowStandaloneAccountAnalysisClusters = allowStandaloneAccountAnalysisClusters;
    }

    /**
     * Constructs an analysis context, based on stored demand and querying the latest topology from
     * the repository. The analysis context contains individual regional contexts representing the tuple
     * of (region, RI spec to purchase, account grouping ID), as well as the cloud topology and matcher
     * classes used to compute the regional contexts.
     *
     * <p>The RI spec to purchase for each {@link RIBuyRegionalContext} will be determined through the
     * {@link ReservedInstanceSpecMatcher} in order to determine whether unique demand clusters can
     * be covered by a single RI spec recommendation. Demand clusters represent the tuple of
     * (account OID, region OID, tier OID, location OID, OS, Tenancy) that are stored within
     * {@link ComputeTierDemandStatsStore}.
     *
     * <p>The account grouping identifier represents either a billing family or standalone account rhough
     * {@link AccountGroupingIdentifier}. This class is used in order to protect against collisions
     * of standalone account OIDs with billing family group IDs (adding a group type enum as part of
     * the identifier).
     *
     * @param scope The analysis scope, indicating which accounts, regions, tenancies, and operating systems
     *              are targets of this analysis. Any demand clusters that do no fit the analyssi scope will
     *              not be returned in teh analysis context.
     * @param purchaseConstraints The purchase constraints for an RI in this analysis. The constraints are
     *                            used by the RI spec matcher, in order to filter RI specs in scope of the
     *                            analysis and subsequently match them to demand clusters.
     * @return The analysis context info. The analysis context will contain a list of regional contexts,
     * which represent a (region, account grouping, RI spec) tuple.
     */
    @Nonnull
    public RIBuyAnalysisContextInfo computeAnalysisContexts(
            @Nonnull ReservedInstanceAnalysisScope scope,
            Map<String, ReservedInstancePurchaseConstraints> purchaseConstraints) {


        final CloudTopology<TopologyEntityDTO> cloudTopology = createCloudTopology();
        final RegionalRIMatcherCache regionalRIMatcherCache =
                regionalRIMatcherCacheFactory.createNewCache(cloudTopology, purchaseConstraints, scope.getTopologyInfo());


        final List<ComputeTierTypeHourlyByWeekRecord> demandClusters = computeTierDemandStatsStore
                .getUniqueDemandClusters().collect(Collectors.toList());

        final List<ScopedDemandCluster> translatedClusters = demandClusters.stream()
                // convert the DB records to a scoped demand cluster instance, checking that the
                // referenced topology entities in the demand cluster still exist in the latest topology.
                .map(demandRecord -> translateRecordToScopedDemandCluster(cloudTopology, demandRecord))
                // If any of the associated topology entities are missing or if the platform or
                // tenancy are invalid, filter out the record.
                .filter(Objects::nonNull).collect(Collectors.toList());

        logger.info("Read {} demandClusters, Translated {} clusters.", demandClusters.size(), translatedClusters.size());

        Map<AnnotatedRegionalScopeKey, List<ScopedDemandCluster>> demandClustersByScopeKey =
                        translatedClusters.stream()
                        // Verify the cluster matches the requested analysis scope (e.g. the account associated
                        // with the cluster is within the requested account list).
                        .filter(scopedCluster -> filterDemandContextByAnalysisScope(scope, scopedCluster))
                        .map(scopedCluster -> matchDemandClusterToRISpec(regionalRIMatcherCache, scopedCluster))
                        // filter out contexts which cannot be mapped to an RISpec
                        .filter(Objects::nonNull)
                        .collect(Collectors.groupingBy(
                                this::mapDemandContextToScopeKey,
                                Collectors.mapping(
                                        DemandContextRISpecMatch::scopedDemandCluster,
                                        Collectors.toList())));
        logger.info("Found {} demandClustersByScopeKey", demandClustersByScopeKey.size());

        // Convert the demand clusters, grouped by a regional scope key (which groups by region,
        // account grouping ID, and target Spec ID) to a regional context representing a single
        // iteration of RI buy analysis.
        List<RIBuyRegionalContext> regionalContexts = demandClustersByScopeKey.entrySet()
                .stream()
                .map(e -> mapClustersToRegionalContext(e.getKey(), e.getValue()))
                .collect(Collectors.toList());

        return ImmutableRIBuyAnalysisContextInfo.builder()
                .cloudTopology(cloudTopology)
                .regionalRIMatcherCache(regionalRIMatcherCache)
                .regionalContexts(regionalContexts)
                .build();
    }

    @Nullable
    private ScopedDemandCluster translateRecordToScopedDemandCluster(
            @Nonnull CloudTopology<TopologyEntityDTO> cloudTopology,
            @Nonnull ComputeTierTypeHourlyByWeekRecord demandRecord) {

        // Try to load all the referenced entities from the latest topology.
        final long accountOid = demandRecord.getAccountId();
        final Optional<TopologyEntityDTO> account =
                cloudTopology.getEntity(accountOid);
        final Optional<TopologyEntityDTO> computeTier =
                cloudTopology.getEntity(demandRecord.getComputeTierId());
        final long regionOrZoneId = demandRecord.getRegionOrZoneId();
        final Optional<TopologyEntityDTO> locationEntity =
                cloudTopology.getEntity(regionOrZoneId);
        final Optional<TopologyEntityDTO> connectedRegion =
                locationEntity.flatMap(loc ->
                        (loc.getEntityType() == EntityType.REGION_VALUE) ?
                                Optional.of(loc) : cloudTopology.getOwner(loc.getOid()));

        final OSType platform = OSType.forNumber(demandRecord.getPlatform());
        final Tenancy tenancy = Tenancy.forNumber(demandRecord.getTenancy());

        final Optional<GroupAndMembers> billingFamily =
                cloudTopology.getBillingFamilyForEntity(accountOid);
        final boolean isBillingGroupAllowed =
                billingFamily.isPresent() || allowStandaloneAccountAnalysisClusters;


        // If any of the referenced entities are no longer present, return null
        if (!account.isPresent() || !computeTier.isPresent() ||
                !locationEntity.isPresent() || !connectedRegion.isPresent() ||
                platform == null || tenancy == null || !isBillingGroupAllowed) {

            logger.warn("Unable to find topology entities for RI demand record " +
                            "(Account OID={} (isPresent={}), Compute Tier OID={} (isPresent={}), " +
                            "Location OID={} (isPresent={}), isConnectedRegionPresent={}, " +
                            "Platform={}, Tenancy={}, isBillingFamilyPresent={} (standaloneAccountAllowed={}))",
                    accountOid, account.isPresent(),
                    demandRecord.getComputeTierId(), computeTier.isPresent(),
                    regionOrZoneId, locationEntity.isPresent(), connectedRegion.isPresent(),
                    platform, tenancy, billingFamily.isPresent(), allowStandaloneAccountAnalysisClusters);

            return null;
        } else {

            return ImmutableScopedDemandCluster.builder()
                    .billingFamily(billingFamily.orElse(null))
                    .account(account.get())
                    .region(connectedRegion.get())
                    .computeTier(computeTier.get())
                    .regionOrZone(locationEntity.get())
                    .platform(platform)
                    .tenancy(tenancy)
                    .build();
        }
    }

    private boolean filterDemandContextByAnalysisScope(@Nonnull ReservedInstanceAnalysisScope scope,
                                                      @Nonnull ScopedDemandCluster demandCluster) {
        return scope.isAccountInScope(demandCluster.account().getOid()) &&
                scope.isRegionInScope(demandCluster.region().getOid()) &&
                scope.isPlatformInScope(demandCluster.platform()) &&
                scope.isTenancyInScope(demandCluster.tenancy());
    }

    @Nullable
    private DemandContextRISpecMatch matchDemandClusterToRISpec(
            @Nonnull RegionalRIMatcherCache regionalRIMatcherCache,
            @Nonnull ScopedDemandCluster demandCluster) {

        final ReservedInstanceSpecMatcher riSpecMatcher =
                regionalRIMatcherCache.getOrCreateRISpecMatchForRegion(
                        demandCluster.region().getOid());

        return riSpecMatcher
                .matchToPurchasingRISpecData(
                        demandCluster.region(),
                        demandCluster.computeTier(),
                        demandCluster.platform(),
                        demandCluster.tenancy())
                .map(riSpecData -> ImmutableDemandContextRISpecMatch.builder()
                        .riSpecData(riSpecData)
                        .scopedDemandCluster(demandCluster)
                        .build())
                .orElse(null);
    }

    @Nonnull
    private AnnotatedRegionalScopeKey mapDemandContextToScopeKey(
        @Nonnull DemandContextRISpecMatch demandContextRISpecMatch) {

        final ImmutableAnnotatedRegionalScopeKey.Builder scopeKeyBuilder =
                ImmutableAnnotatedRegionalScopeKey.builder();
        final ScopedDemandCluster demandCluster = demandContextRISpecMatch.scopedDemandCluster();
        final ReservedInstanceSpecData riSpecData = demandContextRISpecMatch.riSpecData();


        final GroupAndMembers billingFamily = demandCluster.billingFamily();
        final AccountGroupingIdentifier accountGroupingIdentifier;
        if (billingFamily != null) {

            final String accountGroupingTag = String.format("BILLING_FAMILY[%s]",
                    billingFamily.group().getDefinition().getDisplayName());
            accountGroupingIdentifier = ImmutableAccountGroupingIdentifier.builder()
                    .groupingType(AccountGroupingType.BILLING_FAMILY)
                    .id(billingFamily.group().getId())
                    .tag(accountGroupingTag)
                    .build();
        } else {
            final String accountGroupingTag = String.format("BUSINESS_ACCOUNT[%s]",
                    demandCluster.account().getDisplayName());
            accountGroupingIdentifier = ImmutableAccountGroupingIdentifier.builder()
                    .groupingType(AccountGroupingType.STANDALONE_ACCOUNT)
                    .id(demandCluster.account().getOid())
                    .tag(accountGroupingTag)
                    .build();
        }

        return scopeKeyBuilder
                .accountGroupingId(accountGroupingIdentifier)
                .regionOid(demandCluster.region().getOid())
                .region(demandCluster.region())
                .riSpecId(riSpecData.reservedInstanceSpec().getId())
                .riSpecData(riSpecData)
                .build();
    }

    private RIBuyRegionalContext mapClustersToRegionalContext(
            @Nonnull AnnotatedRegionalScopeKey scopeKey,
            @Nonnull Collection<ScopedDemandCluster> scopedDemandContexts) {

        final String contextTag = String.format("[Region=%s, RI Spec ID=%s, ComputeTier=%s, AccountGrouping=%s",
                scopeKey.region().getDisplayName(),
                scopeKey.riSpecId(),
                scopeKey.riSpecData().computeTier().getDisplayName(),
                scopeKey.accountGroupingId().tag());
        final String analysisTag = generateAnalysisTag();

        return ImmutableRIBuyRegionalContext.builder()
                .region(scopeKey.region())
                .riSpecToPurchase(scopeKey.riSpecData().reservedInstanceSpec())
                .computeTier(scopeKey.riSpecData().computeTier())
                .accountGroupingId(scopeKey.accountGroupingId())
                .analysisTag(analysisTag)
                .contextTag(contextTag)
                .addAllDemandClusters(
                        scopedDemandContexts.stream()
                                .map(scopedContext -> ImmutableRIBuyDemandCluster.builder()
                                        .accountOid(scopedContext.account().getOid())
                                        .regionOrZoneOid(scopedContext.regionOrZone().getOid())
                                        .computeTier(scopedContext.computeTier())
                                        .platform(scopedContext.platform())
                                        .tenancy(scopedContext.tenancy())
                                        .build())
                                .collect(Collectors.toList()))
                .build();

    }

    /**
     * Represents the scoped demand cluster.
     * TODO Add more details.
     */
    @Value.Immutable
    interface ScopedDemandCluster {

        @Nullable
        GroupAndMembers billingFamily();

        TopologyEntityDTO account();

        TopologyEntityDTO region();

        TopologyEntityDTO computeTier();

        TopologyEntityDTO regionOrZone();

        OSType platform();

        Tenancy tenancy();
    }

    /**
     * RISpecData to the corresponding scopedDemandCluster.
     * TODO Add more details.
     */
    @Value.Immutable
    interface DemandContextRISpecMatch {

        ReservedInstanceSpecData riSpecData();

        ScopedDemandCluster scopedDemandCluster();
    }

    /**
     * THe ReservedInstanceSpecData for a given specId, region and accounting group.
     * TODO Add more details.
     */
    @Value.Immutable
    interface AnnotatedRegionalScopeKey {

        AccountGroupingIdentifier accountGroupingId();

        long regionOid();

        @Value.Auxiliary
        TopologyEntityDTO region();

        long riSpecId();

        @Value.Auxiliary
        ReservedInstanceSpecData riSpecData();
    }

    /**
     * Compute the TopologyEntityCloudTopology.
     *
     * @return TopologyEntityCloudTopology
     */
    private TopologyEntityCloudTopology createCloudTopology() {

        Stream<TopologyEntityDTO> entities = RepositoryDTOUtil.topologyEntityStream(
                repositoryClient.retrieveTopologyEntities(
                        RetrieveTopologyEntitiesRequest.newBuilder()
                                .setTopologyContextId(realtimeTopologyContextId)
                                .setReturnType(Type.FULL)
                                .setTopologyType(TopologyType.SOURCE)
                                .build()))
                .map(PartialEntity::getFullEntity);
        TopologyEntityCloudTopology cloudTopology =
                cloudTopologyFactory.newCloudTopology(entities);
        return cloudTopology;
    }

    /**
     * Generate a unique string in each JRE invocation to use as a log entry tag. This differs from
     * the cluster tag, in that it will uniquely identify a single analysis for a cluster.
     *
     * @return A tag used to identify a particular round of analysis for a regional cluster
     */
    private static String generateAnalysisTag() {
        int count = analysisTagCounter.getAndIncrement();
        return String.format("RILT%04d: ", count);
    }

}
