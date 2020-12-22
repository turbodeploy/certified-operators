package com.vmturbo.cost.component.cca;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.CloudCommitmentAnalysisManager;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig.Builder;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory.CloudCommitment;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.ReservedInstancePurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassificationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassificationSettings.AllocatedClassificationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandScope;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.TopologyReference;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.search.Search.SearchEntitiesRequest;
import com.vmturbo.common.protobuf.search.Search.SearchParameters;
import com.vmturbo.common.protobuf.search.Search.SearchQuery;
import com.vmturbo.common.protobuf.search.Search.TraversalFilter.TraversalDirection;
import com.vmturbo.common.protobuf.search.SearchProtoUtil;
import com.vmturbo.common.protobuf.search.SearchServiceGrpc.SearchServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.MinimalEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;

/**
 * This class is used for constructing the Cloud commitment analysis request and actually invoking CCA.
 */
public class CloudCommitmentAnalysisRunner {

    private final Logger logger = LogManager.getLogger();

    private final CloudCommitmentSettingsFetcher cloudCommitmentSettingsFetcher;

    private final PlanReservedInstanceStore planReservedInstanceStore;

    private final CloudCommitmentAnalysisManager cloudCommitmentAnalysisManager;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final SearchServiceBlockingStub searchServiceStub;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    /**
     * Constructor for the CloudCommitmentAnalysisRunner.
     *
     * @param cloudCommitmentAnalysisManager The cloud commitment analysis manager invokes cca.
     * @param cloudCommitmentSettingsFetcher This is used to retrieve any global settings spec relevant to cca.
     * @param planReservedInstanceStore The plan reserved instance store is used to fetch specific plan
     * Cloud commitments to be included in the CCA request.
     * @param repositoryClient The repository client used to fetch any required entities from the Repository service.
     * @param searchServiceBlockingStub The search service blocking stub.
     * @param cloudTopologyFactory A cloud topology factory, used to resolve the relationship between regions and
     *                             service providers in determining RI purchase profiles for each region.
     */
    public CloudCommitmentAnalysisRunner(
            @Nonnull CloudCommitmentAnalysisManager cloudCommitmentAnalysisManager,
            @Nonnull CloudCommitmentSettingsFetcher cloudCommitmentSettingsFetcher,
            @Nonnull PlanReservedInstanceStore planReservedInstanceStore,
            @Nonnull final RepositoryServiceBlockingStub repositoryClient,
            @Nonnull final SearchServiceBlockingStub searchServiceBlockingStub,
            @Nonnull TopologyEntityCloudTopologyFactory cloudTopologyFactory) {
        this.cloudCommitmentAnalysisManager = cloudCommitmentAnalysisManager;
        this.cloudCommitmentSettingsFetcher = cloudCommitmentSettingsFetcher;
        this.planReservedInstanceStore = planReservedInstanceStore;
        this.repositoryClient = repositoryClient;
        this.searchServiceStub = Objects.requireNonNull(searchServiceBlockingStub);
        this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
    }

    /**
     * Converts the Buy RI analysis request to a CCA request and invokes it.
     *
     * @param request The input from the buy RI plan containing the scope.
     * @param responseObserver Contains the response of the cca run.
     */
    public void runCloudCommitmentAnalysis(StartBuyRIAnalysisRequest request, StreamObserver<StartBuyRIAnalysisResponse> responseObserver) {
        boolean logDetailedSummary = cloudCommitmentSettingsFetcher.logDetailedSummary();
        Builder cloudCommitmentAnalysisConfigBuilder = CloudCommitmentAnalysisConfig.newBuilder()
                .setAnalysisTag("CCA-" + request.getTopologyInfo().getTopologyContextId())
                .setAnalysisTopology(TopologyReference.newBuilder()
                        .setTopologyContextId(request.getTopologyInfo().getTopologyContextId())
                        .setTopologyId(request.getTopologyInfo().getTopologyId())
                        .build());

        final DemandSelection demandSelection = DemandSelection.newBuilder()
                .setIncludeSuspendedEntities(cloudCommitmentSettingsFetcher.allocationSuspended())
                .setIncludeTerminatedEntities(request.hasIncludeTerminatedEntityDemand()
                        ? request.getIncludeTerminatedEntityDemand()
                        : cloudCommitmentSettingsFetcher.includeTerminatedEntities())
                .setScope(createDemandScopeFromRequest(request))
                .build();

        // Set demand selection
        cloudCommitmentAnalysisConfigBuilder.setDemandSelection(
                HistoricalDemandSelection.newBuilder()
                        .setCloudTierType(CloudTierType.COMPUTE_TIER)
                        .setAllocatedSelection(AllocatedDemandSelection.newBuilder()
                                .setDemandSelection(cloudCommitmentSettingsFetcher.scopeHistoricalDemandSelection()
                                        ? demandSelection
                                        : createBaseScopeForRequest(request)))
                        .setLookBackStartTime(resolveAnalysisStartTimeMillis(request))
                        .setLogDetailedSummary(logDetailedSummary));

        // Set the demand classification settings
        cloudCommitmentAnalysisConfigBuilder.setDemandClassificationSettings(
                DemandClassificationSettings.newBuilder()
                        .setAllocatedClassificationSettings(
                                AllocatedClassificationSettings.newBuilder()
                                        .setMinStabilityMillis(cloudCommitmentSettingsFetcher.minSatbilityMilis())
                                        .build())
                        .setLogDetailedSummary(logDetailedSummary));

        List<Long> rIBoughtIdList = getCloudCommitmentsFromRequest(request);

        // Add all the Cloud Commitments in the scope of the plan to the request.
        if (rIBoughtIdList != null || !rIBoughtIdList.isEmpty()) {
            cloudCommitmentAnalysisConfigBuilder.setCloudCommitmentInventory(
                    CloudCommitmentInventory.newBuilder()
                            .addAllCloudCommitment(rIBoughtIdList.stream()
                                    .map(riBoughtId -> CloudCommitment.newBuilder()
                                            .setOid(riBoughtId)
                                            .setType(CloudCommitmentType.RESERVED_INSTANCE)
                                            .build())
                                    .collect(Collectors.toSet())));
        }



        cloudCommitmentAnalysisConfigBuilder.setPurchaseProfile(
                CommitmentPurchaseProfile.newBuilder()
                        .setAllocatedSelection(AllocatedDemandSelection
                                .newBuilder()
                                .setIncludeFlexibleDemand(cloudCommitmentSettingsFetcher.allocationFlexible())
                                .setDemandSelection(demandSelection))
                        .setRecommendationSettings(RecommendationSettings.newBuilder()
                                .setMaxDemandPercent(cloudCommitmentSettingsFetcher.maxDemandPercentage())
                                .setMinimumSavingsOverOnDemandPercent(cloudCommitmentSettingsFetcher.minimumSavingsOverOnDemand())
                                .build())
                        .setRiPurchaseProfile(ReservedInstancePurchaseProfile.newBuilder()
                                .putAllRiTypeByRegionOid(getRITypeByOid(request))
                                .build()));

        CloudCommitmentAnalysisConfig cloudCommitmentAnalysisConfig = cloudCommitmentAnalysisConfigBuilder.build();

        // Run the analysis.
        final CloudCommitmentAnalysisInfo analysisInfo = cloudCommitmentAnalysisManager
                .startAnalysis(cloudCommitmentAnalysisConfig);

        responseObserver.onNext(StartBuyRIAnalysisResponse.newBuilder().setCloudCommitmentAnalysisInfo(analysisInfo)
                .build());
        responseObserver.onCompleted();
    }

    private List<Long> getCloudCommitmentsFromRequest(StartBuyRIAnalysisRequest request) {
        List<ReservedInstanceBought> riBoughtList = planReservedInstanceStore.getReservedInstanceBoughtByPlanId(request.getTopologyInfo().getTopologyContextId());
        return riBoughtList.stream().map(s -> s.getId()).collect(Collectors.toList());
    }

    private long resolveAnalysisStartTimeMillis(@Nonnull StartBuyRIAnalysisRequest request) {

        final int lookbackDays = request.hasLookBackDurationDays()
                ? request.getLookBackDurationDays()
                : cloudCommitmentSettingsFetcher.historicalLookBackDays();

        final Instant analysisStartTime = Instant.now().minus(lookbackDays, ChronoUnit.DAYS);
        return analysisStartTime.toEpochMilli();
    }

    private DemandScope createDemandScopeFromRequest(StartBuyRIAnalysisRequest request) {
        return DemandScope.newBuilder()
                .addAllAccountOid(request.getAccountsList())
                .addAllRegionOid(request.getRegionsList())
                .addAllEntityOid(request.getEntitiesList())
                .build();
    }

    @Nonnull
    private Set<Long> searchForAggregatingEntities(@Nonnull Collection<Long> seedOids,
                                                   @Nonnull EntityType entityType) {
        final SearchParameters serviceProviderSearchParams =
                SearchParameters.newBuilder()
                        .setStartingFilter(SearchProtoUtil.idFilter(seedOids))
                        .addSearchFilter(SearchProtoUtil.searchFilterTraversal(
                                SearchProtoUtil.traverseToType(
                                        TraversalDirection.AGGREGATED_BY,
                                        entityType)))
                        .build();
        final SearchEntitiesRequest searchRequest = SearchEntitiesRequest.newBuilder()
                .setSearch(SearchQuery.newBuilder()
                        .addSearchParameters(serviceProviderSearchParams))
                .setReturnType(Type.MINIMAL)
                .build();
        return RepositoryDTOUtil.topologyEntityStream(searchServiceStub.searchEntitiesStream(searchRequest))
                .map(PartialEntity::getMinimal)
                .map(MinimalEntity::getOid)
                .collect(ImmutableSet.toImmutableSet());
    }

    /**
     * If the analyzed demand is not scoped to match the recommendation demand, this method scopes
     * the request to remove irrelevant data based on RI scoping. For example, if a group of VMs is
     * selected as the plan scope, the analysis demand selection will be limited to only those regions
     * in which the VMs are present. For account requests, the demand selection will be limited to the
     * associated service providers.
     * @param request The buy RI analysis request.
     * @return The demand selection.
     */
    @Nonnull
    private DemandSelection createBaseScopeForRequest(StartBuyRIAnalysisRequest request) {

        // It is assumed that only one scope type of the request will be set.
        final DemandScope demandScope;
        if (!request.getRegionsList().isEmpty()) {
            demandScope = DemandScope.newBuilder()
                    .addAllRegionOid(request.getRegionsList())
                    .build();
        } else if (!request.getAccountsList().isEmpty()) {
            final Set<Long> serviceProviderOids = searchForAggregatingEntities(
                    request.getAccountsList(),
                    EntityType.SERVICE_PROVIDER);

            demandScope = DemandScope.newBuilder()
                    .addAllServiceProviderOid(serviceProviderOids)
                    .build();
        } else if (!request.getEntitiesList().isEmpty()) {
            final Set<Long> regionOids = searchForAggregatingEntities(
                    request.getEntitiesList(),
                    EntityType.REGION);

            demandScope = DemandScope.newBuilder()
                    .addAllRegionOid(regionOids)
                    .build();
        } else { // default scope
            demandScope = DemandScope.getDefaultInstance();
        }

        return DemandSelection.newBuilder()
                .setScope(demandScope)
                .setIncludeSuspendedEntities(cloudCommitmentSettingsFetcher.allocationSuspended())
                .setIncludeTerminatedEntities(cloudCommitmentSettingsFetcher.includeTerminatedEntities())
                .build();
    }

    private Map<Long, ReservedInstanceType> getRITypeByOid(StartBuyRIAnalysisRequest request) {

        // Get a list of regions from the request. if the request does not contain any regions,
        // get all the regions from the repository service.
        final Stream<TopologyEntityDTO> entities = RepositoryDTOUtil.topologyEntityStream(
                repositoryClient.retrieveTopologyEntities(
                        RetrieveTopologyEntitiesRequest.newBuilder()
                                .setReturnType(Type.FULL)
                                .setTopologyType(TopologyType.SOURCE)
                                .addEntityType(EntityType.REGION_VALUE)
                                .addEntityType(EntityType.SERVICE_PROVIDER_VALUE)
                                .build()))
                .map(PartialEntity::getFullEntity);
        final TopologyEntityCloudTopology staticTopology =
                cloudTopologyFactory.newCloudTopology(entities);

        final Map<String, RIPurchaseProfile> normalizedPurchaseProfileByCloudType =
                request.getPurchaseProfileByCloudtypeMap().entrySet()
                        .stream()
                        .collect(ImmutableMap.toImmutableMap(
                                // convert provider to upper case
                                (e) -> e.getKey().toUpperCase(),
                                Map.Entry::getValue));

        final ImmutableMap.Builder<Long, ReservedInstanceType> purchaseProfileByRegionMap = ImmutableMap.builder();
        for (TopologyEntityDTO region : staticTopology.getAllEntitiesOfType(EntityType.REGION_VALUE)) {

            Optional<TopologyEntityDTO> serviceProvider = staticTopology.getServiceProvider(region.getOid());

            if (serviceProvider.isPresent()) {

                final RIPurchaseProfile purchaseProfile = normalizedPurchaseProfileByCloudType.get(
                        serviceProvider.get().getDisplayName().toUpperCase());
                if (purchaseProfile != null) {
                    purchaseProfileByRegionMap.put(region.getOid(), purchaseProfile.getRiType());
                } else {
                    logger.error("Unable to find purchase constraints for region (Region OID={} SP OID={})",
                            region.getOid(), serviceProvider.get().getOid());
                }
            } else {
                logger.error("Unable to find service provider connection for region (Region OID={})", region.getOid());
            }
        }

        return purchaseProfileByRegionMap.build();
    }
}
