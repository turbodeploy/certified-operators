package com.vmturbo.cost.component.rpc;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.ImmutableMap;

import io.grpc.stub.StreamObserver;

import com.vmturbo.cloud.commitment.analysis.CloudCommitmentAnalysisManager;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.AllocatedDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisConfig;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentAnalysisInfo;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CloudCommitmentInventory.CloudCommitment;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.RecommendationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.CommitmentPurchaseProfile.ReservedInstancePurchaseProfile;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.DemandClassificationSettings;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection;
import com.vmturbo.common.protobuf.cca.CloudCommitmentAnalysis.HistoricalDemandSelection.CloudTierType;
import com.vmturbo.common.protobuf.cloud.CloudCommitment.CloudCommitmentType;
import com.vmturbo.common.protobuf.cost.Cost.ReservedInstanceBought;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.CostDebug;
import com.vmturbo.common.protobuf.cost.CostDebug.DisableCostRecordingRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.DisableCostRecordingResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.EnableCostRecordingResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.GetBuyRIImpactCsvRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.GetBuyRIImpactCsvResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.GetRecordedCostsRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.RecordedCost;
import com.vmturbo.common.protobuf.cost.CostDebug.StartFullAllocatedRIBuyRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.StartFullAllocatedRIBuyResponse;
import com.vmturbo.common.protobuf.cost.CostDebug.TriggerBuyRIAlgorithmRequest;
import com.vmturbo.common.protobuf.cost.CostDebug.TriggerBuyRIAlgorithmResponse;
import com.vmturbo.common.protobuf.cost.CostDebugServiceGrpc.CostDebugServiceImplBase;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.cost.component.reserved.instance.BuyRIImpactReportGenerator;
import com.vmturbo.cost.component.reserved.instance.EntityReservedInstanceMappingStore;
import com.vmturbo.cost.component.reserved.instance.ReservedInstanceBoughtStore;
import com.vmturbo.cost.component.reserved.instance.filter.ReservedInstanceBoughtFilter;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisInvoker;
import com.vmturbo.cost.component.topology.CostJournalRecorder;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.OfferingClass;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType.PaymentOption;

/**
 * See cost/CostDebug.proto
 */
public class CostDebugRpcService extends CostDebugServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final CostJournalRecorder costJournalRecording;

    private final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore;

    private final ReservedInstanceAnalysisInvoker invoker;

    private final BuyRIImpactReportGenerator buyRIImpactReportGenerator;

    private final CloudCommitmentAnalysisManager ccaManager;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final ReservedInstanceBoughtStore riBoughtStore;

    public CostDebugRpcService(@Nonnull final CostJournalRecorder costJournalRecording,
                               @Nonnull final EntityReservedInstanceMappingStore entityReservedInstanceMappingStore,
                               @Nonnull final ReservedInstanceAnalysisInvoker invoker,
                               @Nonnull BuyRIImpactReportGenerator buyRIImpactReportGenerator,
                               @Nonnull CloudCommitmentAnalysisManager ccaManager,
                               @Nonnull RepositoryServiceBlockingStub repositoryClient,
                               @Nonnull ReservedInstanceBoughtStore riBoughtStore) {
        this.costJournalRecording = costJournalRecording;
        this.entityReservedInstanceMappingStore = entityReservedInstanceMappingStore;
        this.invoker = invoker;
        this.buyRIImpactReportGenerator = Objects.requireNonNull(buyRIImpactReportGenerator);
        this.ccaManager = Objects.requireNonNull(ccaManager);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.riBoughtStore = Objects.requireNonNull(riBoughtStore);
    }

    @Override
    public void startFullAllocatedRIBuyAnalysis(final StartFullAllocatedRIBuyRequest request, final StreamObserver<StartFullAllocatedRIBuyResponse> responseObserver) {
        try {
            final Set<Long> riBoughtIds = riBoughtStore.getReservedInstanceBoughtByFilter(ReservedInstanceBoughtFilter.SELECT_ALL_FILTER)
                    .stream()
                    .map(ReservedInstanceBought::getId)
                    .collect(Collectors.toSet());
            final CloudCommitmentAnalysisConfig analysisConfig = CloudCommitmentAnalysisConfig.newBuilder()
                    .setAnalysisTag("Test Analysis")
                    .setDemandSelection(HistoricalDemandSelection.newBuilder()
                            .setCloudTierType(CloudTierType.COMPUTE_TIER)
                            .setAllocatedSelection(AllocatedDemandSelection.newBuilder()
                                    .build())
                            .setLookBackStartTime(Instant.now().minus(30, ChronoUnit.DAYS).toEpochMilli())
                            .setLogDetailedSummary(true))
                    .setDemandClassificationSettings(DemandClassificationSettings.newBuilder()
                            .setLogDetailedSummary(true))
                    .setCloudCommitmentInventory(CloudCommitmentInventory.newBuilder()
                            .addAllCloudCommitment(riBoughtIds.stream()
                                    .map(riBoughtId -> CloudCommitment.newBuilder()
                                            .setOid(riBoughtId)
                                            .setType(CloudCommitmentType.RESERVED_INSTANCE)
                                            .build())
                                    .collect(Collectors.toSet())))
                    .setPurchaseProfile(CommitmentPurchaseProfile.newBuilder()
                            .setAllocatedSelection(AllocatedDemandSelection.newBuilder().build())
                            .setRecommendationSettings(RecommendationSettings.newBuilder())
                            .setRiPurchaseProfile(ReservedInstancePurchaseProfile.newBuilder()
                                    .putAllRiTypeByRegionOid(
                                            retrieveAllRegionsFromRepository()
                                                    .collect(ImmutableMap.toImmutableMap(
                                                            TopologyEntityDTO::getOid,
                                                            (t) -> ReservedInstanceType.newBuilder()
                                                                    .setOfferingClass(OfferingClass.CONVERTIBLE)
                                                                    .setPaymentOption(PaymentOption.ALL_UPFRONT)
                                                                    .setTermYears(1)
                                                                    .build()))))
                            .build())
                    .build();

            final CloudCommitmentAnalysisInfo analysisInfo = ccaManager.startAnalysis(analysisConfig);

            responseObserver.onNext(StartFullAllocatedRIBuyResponse.newBuilder()
                    .setCloudCommitmentAnalysisInfo(analysisInfo)
                    .build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            logger.error("Error during CCA:", e);
            throw e;
        }
    }

    @Override
    public void enableCostRecording(EnableCostRecordingRequest request,
                                  StreamObserver<EnableCostRecordingResponse> responseObserver) {
        this.costJournalRecording.overrideEntitySelector(request);
        responseObserver.onNext(EnableCostRecordingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void disableCostRecording(DisableCostRecordingRequest request,
                                     StreamObserver<DisableCostRecordingResponse> responseObserver) {
        this.costJournalRecording.resetEntitySelector();
        responseObserver.onNext(DisableCostRecordingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void getRecordedCosts(GetRecordedCostsRequest request,
                                 StreamObserver<RecordedCost> responseObserver) {

        final Set<Long> targetEntities = new HashSet<>();
        if (request.hasEntityId()) {
            targetEntities.add(request.getEntityId());
        }

        costJournalRecording.getJournalDescriptions(targetEntities)
                .map(cost -> RecordedCost.newBuilder()
                        .setCostJournalDescription(cost)
                        .build())
                .forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    @Override
    public void logEntityRIMapping(CostDebug.LogEntityRIMappingRequest request, StreamObserver<CostDebug.LogEntityRIMappingResponse> responseObserver) {
        final List<Long> entityIdList = request.getEntityIdList();
        entityReservedInstanceMappingStore.logEntityCoverage(entityIdList);
        responseObserver.onNext(CostDebug.LogEntityRIMappingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    @Override
    public void logRIEntityMapping(CostDebug.LogRIEntityMappingRequest request, StreamObserver<CostDebug.LogRIEntityMappingResponse> responseObserver) {
        final List<Long> riIdList = request.getRiIdList();
        entityReservedInstanceMappingStore.logRICoverage(riIdList);
        responseObserver.onNext(CostDebug.LogRIEntityMappingResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * {@inheritDoc}
     *
     * @param request {@inheritDoc}
     * @param responseObserver {@inheritDoc}
     */
    @Override
    public void triggerBuyRIAlgorithm(TriggerBuyRIAlgorithmRequest request,
                                      StreamObserver<TriggerBuyRIAlgorithmResponse> responseObserver) {
        final StartBuyRIAnalysisRequest startBuyRIAnalysisRequest = invoker.getStartBuyRIAnalysisRequest();
        if (startBuyRIAnalysisRequest.getAccountsList().isEmpty()) {
            logger.info("No BA's found. RI Buy analysis will not be triggered.");
            return;
        }
        invoker.invokeBuyRIAnalysis(startBuyRIAnalysisRequest);
        responseObserver.onNext(TriggerBuyRIAlgorithmResponse.getDefaultInstance());
        responseObserver.onCompleted();
    }

    /**
     * Provides a report (formatted as CSV) mapping entity -> buy RI coverage and the cost impact from
     * buy RI actions. This report is useful in debugging the projected on-demand compute cost, providing
     * the mappings generated by BuyRIImpactAnalysis in the market component.
     * <p>
     * This response can be converted to a csv file taking the curl command generated by swagger and piping
     * it to jq to extract the csv report string and send it to a file.
     * <p>
     * Ex: curl [...] | jq -r '.response.csvString' > [output_csv_file]
     * @param request {@inheritDoc}
     * @param responseObserver {@inheritDoc}
     */
    @Override
    public void getBuyRIImpactCsv(final GetBuyRIImpactCsvRequest request, final StreamObserver<GetBuyRIImpactCsvResponse> responseObserver) {
        responseObserver.onNext(GetBuyRIImpactCsvResponse.newBuilder()
                .setCsvString(buyRIImpactReportGenerator.generateCsvReportAsString(request))
                .build());
        responseObserver.onCompleted();
    }

    private Stream<TopologyEntityDTO> retrieveAllRegionsFromRepository() {
        final RetrieveTopologyEntitiesRequest.Builder retrieveTopologyRequest =
                RetrieveTopologyEntitiesRequest.newBuilder()
                        .setReturnType(Type.FULL)
                        .setTopologyType(TopologyType.SOURCE)
                        .addEntityType(EntityType.REGION_VALUE);

        return RepositoryDTOUtil.topologyEntityStream(
                repositoryClient.retrieveTopologyEntities(retrieveTopologyRequest.build()))
                    .map(PartialEntity::getFullEntity);
    }
}
