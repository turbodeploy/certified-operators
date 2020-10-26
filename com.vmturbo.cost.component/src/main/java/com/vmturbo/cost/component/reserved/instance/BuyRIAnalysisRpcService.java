package com.vmturbo.cost.component.reserved.instance;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.cloud.commitment.analysis.CloudCommitmentAnalysisManager;
import com.vmturbo.common.protobuf.RepositoryDTOUtil;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.RetrieveTopologyEntitiesRequest;
import com.vmturbo.common.protobuf.repository.RepositoryDTO.TopologyType;
import com.vmturbo.common.protobuf.setting.SettingServiceGrpc.SettingServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PartialEntity.Type;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyEntityDTO;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc.BuyRIAnalysisServiceImplBase;
import com.vmturbo.common.protobuf.cost.Cost.SetBuyRIAnalysisScheduleRequest;
import com.vmturbo.common.protobuf.cost.Cost.SetBuyRIAnalysisScheduleResponse;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisResponse;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.calculation.integration.CloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopology;
import com.vmturbo.cost.calculation.topology.TopologyEntityCloudTopologyFactory;
import com.vmturbo.cost.component.cca.CloudCommitmentAnalysisRunner;
import com.vmturbo.cost.component.cca.CloudCommitmentSettingsFetcher;
import com.vmturbo.cost.component.cca.configuration.CloudCommitmentAnalysisConfigurationHolder;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalysisScope;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceAnalyzer;
import com.vmturbo.cost.component.reserved.instance.recommendationalgorithm.ReservedInstanceHistoricalDemandDataType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.DemandType;

/**
 * This class is a rpc service for buy reserved instance analysis.
 */
public class BuyRIAnalysisRpcService extends BuyRIAnalysisServiceImplBase {

    private final Logger logger = LogManager.getLogger();

    private final BuyRIAnalysisScheduler buyRIAnalysisScheduler;

    private final ReservedInstanceAnalyzer reservedInstanceAnalyzer;

    private final ComputeTierDemandStatsStore computeTierDemandStatsStore;

    private final long realtimeTopologyContextId;

    private final RepositoryServiceBlockingStub repositoryClient;

    private final TopologyEntityCloudTopologyFactory cloudTopologyFactory;

    private final ExecutorService buyRIExecutor = Executors.newSingleThreadExecutor(
            new ThreadFactoryBuilder().setNameFormat("buy-ri-algorithm-%d").build());

    private static final Map<DemandType, ReservedInstanceHistoricalDemandDataType>
            demandTypeToHistoricalDemandType = ImmutableMap.of(
            DemandType.ALLOCATION, ReservedInstanceHistoricalDemandDataType.ALLOCATION,
            DemandType.CONSUMPTION, ReservedInstanceHistoricalDemandDataType.CONSUMPTION);

    private final CloudCommitmentAnalysisRunner cloudCommitmentAnalysisRunner;

    private final CloudCommitmentSettingsFetcher cloudCommitmentSettingsFetcher;

    public BuyRIAnalysisRpcService(
            @Nonnull final BuyRIAnalysisScheduler buyRIAnalysisScheduler,
            @Nonnull final RepositoryServiceBlockingStub repositoryClient,
            @Nonnull final TopologyEntityCloudTopologyFactory cloudTopologyFactory,
            @Nonnull final ReservedInstanceAnalyzer reservedInstanceAnalyzer,
            @Nonnull final ComputeTierDemandStatsStore computeTierDemandStatsStore,
            @Nonnull final CloudCommitmentSettingsFetcher cloudCommitmentSettingsFetcher,
            @Nonnull final CloudCommitmentAnalysisRunner cloudCommitmentAnalysisRunner,
            final long realtimeTopologyContextId) {
        this.buyRIAnalysisScheduler = Objects.requireNonNull(buyRIAnalysisScheduler);
        this.repositoryClient = Objects.requireNonNull(repositoryClient);
        this.cloudTopologyFactory = Objects.requireNonNull(cloudTopologyFactory);
        this.reservedInstanceAnalyzer = Objects.requireNonNull(reservedInstanceAnalyzer);
        this.computeTierDemandStatsStore = Objects.requireNonNull(computeTierDemandStatsStore);
        this.cloudCommitmentSettingsFetcher = Objects.requireNonNull(cloudCommitmentSettingsFetcher);
        this.cloudCommitmentAnalysisRunner = Objects.requireNonNull(cloudCommitmentAnalysisRunner);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    /**
     * RI analysis task future.
     */
    private Future<?> buyRIFuture;

    /**
     * Set a new interval for buy reserved instance analysis scheduler. Note that after call
     * this function, it will trigger buy RI analysis immediately.
     *
     * @param request {@link SetBuyRIAnalysisScheduleRequest}.
     * @param responseObserver {@link StreamObserver}.
     */
    public void setBuyReservedInstanceSchedule(SetBuyRIAnalysisScheduleRequest request,
                                               StreamObserver<SetBuyRIAnalysisScheduleResponse> responseObserver) {
        if (!request.hasIntervalHours() || request.getIntervalHours() <= 0) {
            logger.error("Invalid interval hours parameter {}.", request.getIntervalHours());
            responseObserver.onError(Status.INVALID_ARGUMENT
                    .withDescription("Must provide a new positive interval value.").asException());
        }
        final long scheduleIntervalHours = request.getIntervalHours();
        final long intervalValue = buyRIAnalysisScheduler.setBuyRIAnalysisSchedule(
                scheduleIntervalHours, TimeUnit.HOURS);
        SetBuyRIAnalysisScheduleResponse response = SetBuyRIAnalysisScheduleResponse.newBuilder()
                    .setIntervalHours(intervalValue)
                    .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    /**
     * Start the Buy RI Analysis algorithm
     *
     * @param request {@link StartBuyRIAnalysisRequest}.
     * @param responseObserver {@link StreamObserver}.
     */
    public void startBuyRIAnalysis(StartBuyRIAnalysisRequest request,
                                   StreamObserver<StartBuyRIAnalysisResponse> responseObserver) {
        try {
            if (request.hasTopologyInfo() && request.getTopologyInfo().hasPlanInfo()) {
                if (StringConstants.OPTIMIZE_CLOUD_PLAN.equals(request.getTopologyInfo().getPlanInfo().getPlanType())
                        && StringConstants.OPTIMIZE_CLOUD_PLAN__RIBUY_ONLY.equals(request.getTopologyInfo().getPlanInfo().getPlanSubType())
                && cloudCommitmentSettingsFetcher.runCloudCommitmentAnalysis()) {
                    cloudCommitmentAnalysisRunner.runCloudCommitmentAnalysis(request, responseObserver);
                } else {
                    runPlanBuyRIAnalysis(request, responseObserver);
                }
            } else {
                runRealtimeBuyRIAnalysis(request, responseObserver);
            }
        } catch (Exception e) {
            logger.error("Unexpected exception occurred in buy RI analysis.", e);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Run Buy RI analysis for plan and send Buy RI action plan to Action Orchestrator.
     *
     * @param request StartBuyRIAnalysisRequest
     * @param responseObserver StartBuyRIAnalysisResponse
     */
    private void runPlanBuyRIAnalysis(StartBuyRIAnalysisRequest request,
                                      StreamObserver<StartBuyRIAnalysisResponse> responseObserver) {
        long planId = request.getTopologyInfo().getTopologyContextId();
        DemandType demandType = request.getDemandType();
        ReservedInstanceHistoricalDemandDataType historicalDemandDataType =
                demandTypeToHistoricalDemandType.get(demandType);
        try {
            logger.info("Executing buy RI algorithm for plan {} using {}.",
                    planId, historicalDemandDataType);

            final CloudTopology<TopologyEntityDTO> cloudTopology = createCloudTopology();

            ReservedInstanceAnalysisScope reservedInstanceAnalysisScope =
                    new ReservedInstanceAnalysisScope(request);
            reservedInstanceAnalyzer.runRIAnalysisAndSendActions(planId, cloudTopology,
                    reservedInstanceAnalysisScope, historicalDemandDataType);
            responseObserver.onNext(StartBuyRIAnalysisResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (CommunicationException | InterruptedException e) {
            logger.error("Failed to send buy RI action plan to action orchestrator for plan {}", planId);
            responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asException());
        } catch (RuntimeException ex) {
            logger.error("Unexpected run time exception occurs in buy RI analysis for plan {}", planId);
            responseObserver.onError(Status.ABORTED.withDescription(ex.getMessage()).asException());
        }
    }

    /**
     * Compute the TopologyEntityCloudTopology.
     *
     * @return TopologyEntityCloudTopology
     */
    private TopologyEntityCloudTopology createCloudTopology() {
        final Stream<TopologyEntityDTO> entities = RepositoryDTOUtil.topologyEntityStream(
                repositoryClient.retrieveTopologyEntities(
                        RetrieveTopologyEntitiesRequest.newBuilder()
                                .setTopologyContextId(realtimeTopologyContextId)
                                .setReturnType(Type.FULL)
                                .setTopologyType(TopologyType.SOURCE)
                                .build()))
                .map(PartialEntity::getFullEntity);
        final TopologyEntityCloudTopology cloudTopology =
                cloudTopologyFactory.newCloudTopology(entities);
        return cloudTopology;
    }

    /**
     * Run buy ri analysis in realtime and send buy ri action plan to action orchestrator.
     *
     * @param responseObserver StartBuyRIAnalysisResponse
     */
    private void runRealtimeBuyRIAnalysis(StartBuyRIAnalysisRequest request, StreamObserver<StartBuyRIAnalysisResponse> responseObserver) {
        // return if there is an outstanding buyRI algorithm execution.
        if (buyRIFuture != null && !buyRIFuture.isDone()) {
            logger.warn("A previous Buy RI algorithm execution is still in-progress.");
        }

        // We queue the requests. But there is only one RI analysis execution at a time.
        // Right now, the analysis is only for real-time. But when we add support for plans,
        // there could be multiple concurrent executions of the RI analysis algorithm.
        buyRIFuture = buyRIExecutor.submit( () -> {
            logger.info("Executing buy RI algorithm.");
            ReservedInstanceAnalysisScope reservedInstanceAnalysisScope =
                            new ReservedInstanceAnalysisScope(request);
            try {
                final CloudTopology<TopologyEntityDTO> cloudTopology = createCloudTopology();

                reservedInstanceAnalyzer.runRIAnalysisAndSendActions(realtimeTopologyContextId,
                        cloudTopology, reservedInstanceAnalysisScope,
                        ReservedInstanceHistoricalDemandDataType.CONSUMPTION);
                responseObserver.onNext(StartBuyRIAnalysisResponse.getDefaultInstance());
                responseObserver.onCompleted();
            } catch (InterruptedException ie) {
                logger.error("Interrupted publishing of buy RI actions", ie);
                Thread.currentThread().interrupt();
            } catch (CommunicationException ce) {
                logger.error("Exception while publishing buy RI actions", ce);
                responseObserver.onError(Status.INTERNAL.withDescription(ce.getMessage()).asException());
            }
        });
    }
}
