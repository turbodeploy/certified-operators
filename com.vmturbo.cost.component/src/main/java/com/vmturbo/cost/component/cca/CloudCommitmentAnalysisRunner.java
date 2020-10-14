package com.vmturbo.cost.component.cca;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;

import io.grpc.stub.StreamObserver;

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
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisResponse;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.cost.component.reserved.instance.PlanReservedInstanceStore;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;

/**
 * This class is used for constructing the Cloud commitment analysis request and actually invoking CCA.
 */
public class CloudCommitmentAnalysisRunner {

    private CloudCommitmentSettingsFetcher cloudCommitmentSettingsFetcher;

    private PlanReservedInstanceStore planReservedInstanceStore;

    private CloudCommitmentAnalysisManager cloudCommitmentAnalysisManager;

    private RepositoryServiceBlockingStub repositoryClient;

    /**
     * Constructor for the CloudCommitmentAnalysisRunner.
     *
     * @param cloudCommitmentAnalysisManager The cloud commitment analysis manager invokes cca.
     * @param cloudCommitmentSettingsFetcher This is used to retrieve any global settings spec relevant to cca.
     * @param planReservedInstanceStore The plan reserved instance store is used to fetch specific plan
     * Cloud commitments to be included in the CCA request.
     * @param repositoryClient The repository client used to fetch any required entities from the Repository service.
     */
    public CloudCommitmentAnalysisRunner(
            @Nonnull CloudCommitmentAnalysisManager cloudCommitmentAnalysisManager,
            @Nonnull CloudCommitmentSettingsFetcher cloudCommitmentSettingsFetcher,
            @Nonnull PlanReservedInstanceStore planReservedInstanceStore,
            @Nonnull final RepositoryServiceBlockingStub repositoryClient) {
        this.cloudCommitmentAnalysisManager = cloudCommitmentAnalysisManager;
        this.cloudCommitmentSettingsFetcher = cloudCommitmentSettingsFetcher;
        this.planReservedInstanceStore = planReservedInstanceStore;
        this.repositoryClient = repositoryClient;
    }

    /**
     * Converts the Buy RI analysis request to a CCA request and invokes it.
     *
     * @param request The input from the buy RI plan containing the scope.
     * @param responseObserver Contains the response of the cca run.
     */
    public void runCloudCommitmentAnalysis(StartBuyRIAnalysisRequest request, StreamObserver<StartBuyRIAnalysisResponse> responseObserver) {
        cloudCommitmentSettingsFetcher.populateSettingsMap();
        boolean logDetailedSummary = cloudCommitmentSettingsFetcher.logDetailedSummary();
        Builder cloudCommitmentAnalysisConfigBuilder = CloudCommitmentAnalysisConfig.newBuilder();
        cloudCommitmentAnalysisConfigBuilder.setAnalysisTag("CCA-" + request.getTopologyInfo().getTopologyContextId());

        // Set demand selection
        cloudCommitmentAnalysisConfigBuilder.setDemandSelection(HistoricalDemandSelection.newBuilder()
                .setCloudTierType(CloudTierType.COMPUTE_TIER)
                .setAllocatedSelection(AllocatedDemandSelection.newBuilder()
                        .build())
                .setLookBackStartTime(cloudCommitmentSettingsFetcher.historicalLookBackPeriod())
                .setLogDetailedSummary(logDetailedSummary));

        // Set the demand classification settings
        cloudCommitmentAnalysisConfigBuilder.setDemandClassificationSettings(
                DemandClassificationSettings.newBuilder()
                        .setAllocatedClassificationSettings(
                                AllocatedClassificationSettings.newBuilder()
                                        .setMinStabilityMillis(cloudCommitmentSettingsFetcher.minSatbilityMilis()).build()).setLogDetailedSummary(logDetailedSummary));

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

        DemandSelection demandSelection = DemandSelection.newBuilder().setIncludeSuspendedEntities(cloudCommitmentSettingsFetcher.allocationSuspended())
                .setIncludeTerminatedEntities(cloudCommitmentSettingsFetcher.includeTerminatedEntities())
                .setScope(createDemandScopeFromRequest(request))
                .build();

        cloudCommitmentAnalysisConfigBuilder.setPurchaseProfile(
                CommitmentPurchaseProfile.newBuilder().setAllocatedSelection(AllocatedDemandSelection
                        .newBuilder().setIncludeFlexibleDemand(cloudCommitmentSettingsFetcher.allocationFlexible())
                        .setDemandSelection(demandSelection))
                        .setRecommendationSettings(RecommendationSettings.newBuilder().setMaxDemandPercent(cloudCommitmentSettingsFetcher.maxDemandPercentage())
                        .setMinimumSavingsOverOnDemandPercent(cloudCommitmentSettingsFetcher.minimumSavingsOverOnDemand()).build())
                        .setRiPurchaseProfile(ReservedInstancePurchaseProfile.newBuilder().putAllRiTypeByRegionOid(getRITypeByOid(request)).build()));

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

    private DemandScope createDemandScopeFromRequest(StartBuyRIAnalysisRequest request) {
        return DemandScope.newBuilder().addAllAccountOid(request.getAccountsList())
                .addAllRegionOid(request.getRegionsList()).build();
    }

    private Map<Long, ReservedInstanceType> getRITypeByOid(StartBuyRIAnalysisRequest request) {
        // Get a list of regions from the request. if the request does not contain any regions,
        // get all the regions from the repository service.
        List<Long> regionOidList;
        if (request.getRegionsList() != null) {
            regionOidList = request.getRegionsList();
        } else {
            final RetrieveTopologyEntitiesRequest.Builder retrieveTopologyRequest =
                    RetrieveTopologyEntitiesRequest.newBuilder()
                            .setReturnType(Type.FULL)
                            .setTopologyType(TopologyType.SOURCE)
                            .addEntityType(EntityType.REGION_VALUE);

            regionOidList = RepositoryDTOUtil.topologyEntityStream(
                    repositoryClient.retrieveTopologyEntities(retrieveTopologyRequest.build()))
                    .map(PartialEntity::getMinimal).map(s -> s.getOid()).collect(Collectors.toList());
        }

        Map<Long, ReservedInstanceType> regionByRITypeMap = new HashMap<>();

        // Get a list of RIPurchaseProfiles from the request.
        List<RIPurchaseProfile> riPurchaseProfileList = new ArrayList<>(request.getPurchaseProfileByCloudtypeMap().values());

        // For every region, map the oid to th RIType.
        for (Long regionOid: regionOidList) {
            for (RIPurchaseProfile riPurchaseProfile: riPurchaseProfileList) {
                regionByRITypeMap.put(regionOid, riPurchaseProfile.getRiType());
            }
        }
        return regionByRITypeMap;
    }
}
