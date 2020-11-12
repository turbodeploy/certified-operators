package com.vmturbo.cost.component.rpc;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.google.common.collect.ImmutableMap;

import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.vmturbo.common.protobuf.action.ActionDTO;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisRequest.MigratedWorkloadPlacement;
import com.vmturbo.common.protobuf.cost.Cost.MigratedWorkloadCloudCommitmentAnalysisResponse;
import com.vmturbo.common.protobuf.cost.MigratedWorkloadCloudCommitmentAnalysisServiceGrpc.MigratedWorkloadCloudCommitmentAnalysisServiceImplBase;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.Scenario;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange.RIProviderSetting;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.search.CloudType;
import com.vmturbo.commons.idgen.IdentityGenerator;
import com.vmturbo.communication.CommunicationException;
import com.vmturbo.cost.component.plan.PlanService;
import com.vmturbo.cost.component.reserved.instance.action.ReservedInstanceActionsSender;
import com.vmturbo.cost.component.reserved.instance.migratedworkloadcloudcommitmentalgorithm.MigratedWorkloadCloudCommitmentAlgorithmStrategy;

/**
 * Performs cloud commitment (Buy RI) analysis for migrated workloads, such as from a migrate-to-cloud plan.
 */
@Service
public class MigratedWorkloadCloudCommitmentAnalysisService extends MigratedWorkloadCloudCommitmentAnalysisServiceImplBase {
    private static final Logger logger = LogManager.getLogger(MigratedWorkloadCloudCommitmentAnalysisService.class);

    /**
     * Used to publish an action plan to the action orchestrator.
     */
    private final ReservedInstanceActionsSender actionsSender;

    /**
     * Used for retrieving the PlanInstance from the plan service.
     */
    private final PlanService planService;

    /**
     * Creates a new MigratedWorkloadCloudCommitmentAnalysisService.
     *
     * @param actionsSender The action sender that is used to publish our action plan to the action orchestrator
     * @param planService   The plan service that is used for retrieving the plan with the plan ID in the MigratedWorkloadCloudCommitmentAnalysisRequest
     */
    public MigratedWorkloadCloudCommitmentAnalysisService(ReservedInstanceActionsSender actionsSender, PlanService planService) {
        this.actionsSender = actionsSender;
        this.planService = planService;
    }

    /**
     * The strategy that implements our cloud commitment algorithm.
     */
    @Autowired
    private MigratedWorkloadCloudCommitmentAlgorithmStrategy migratedWorkloadCloudCommitmentAlgorithmStrategy;

    /**
     * gRPC handler to start the cloud commitment analysis.  This builds the VM list from streaming
     * chunks sent from the client before sending the request off to the actual analysis start
     * logic.
     *
     * @param responseObserver The gRPC response observer
     * @return StreamObserver used by the gRPC code to build the request.  It is not meant to be
     * directly called by user code.
     */
    @Override
    public StreamObserver<MigratedWorkloadCloudCommitmentAnalysisRequest> startAnalysis(
            StreamObserver<MigratedWorkloadCloudCommitmentAnalysisResponse> responseObserver) {
        return new StreamObserver<MigratedWorkloadCloudCommitmentAnalysisRequest>() {
            MigratedWorkloadCloudCommitmentAnalysisRequest request = null;
            List<MigratedWorkloadPlacement> workloadsList = new ArrayList<>();
            @Override
            public void onNext(MigratedWorkloadCloudCommitmentAnalysisRequest
                    migratedWorkloadCloudCommitmentAnalysisRequest) {
                if (request == null) {
                    request = migratedWorkloadCloudCommitmentAnalysisRequest;
                }
                workloadsList.addAll(migratedWorkloadCloudCommitmentAnalysisRequest.getVirtualMachinesList());
            }

            @Override
            public void onError(Throwable throwable) {
                logger.error("Client error starting migrated workload cloud commitment analysis: {}",
                        throwable.getMessage());
            }

            @Override
            public void onCompleted() {
                doStartAnalysis(request, responseObserver, workloadsList);
            }
        };
    }

    /**
     * Executes the cloud commitment analysis.
     *
     * @param request          The gRPC request
     * @param responseObserver The gRPC response observer
     * @param workloadsList    Full list of workloads specified by the gRPC startAnalysis request
     */
    private void doStartAnalysis(MigratedWorkloadCloudCommitmentAnalysisRequest request,
              StreamObserver<MigratedWorkloadCloudCommitmentAnalysisResponse> responseObserver,
            List<MigratedWorkloadPlacement> workloadsList) {
        logger.info("MigratedWorkloadCloudCommitmentAnalysisService::startAnalysis for topology: {}",
                request.getTopologyContextId());

        // Respond back to our client that we've received the message
        responseObserver.onNext(MigratedWorkloadCloudCommitmentAnalysisResponse.getDefaultInstance());
        responseObserver.onCompleted();

        // Record our start time
        final long analysisStartTime = System.currentTimeMillis();

        // Retrieve the plan and its scenarios
        Optional<Map<CloudType, RIProviderSetting>> riProviderSettingMap = getRIProviderSetting(request.getTopologyContextId());
        List<ActionDTO.Action> actions = new ArrayList<>();
        if (riProviderSettingMap.isPresent() && !riProviderSettingMap.get().isEmpty() && request.hasBusinessAccount()) {
            // Only perform the analysis if we have valid RI provider settings
            CloudType cloudType = riProviderSettingMap.get().keySet().stream().findFirst().get();
            RIProviderSetting riProviderSetting = riProviderSettingMap.get().get(cloudType);

            // Analyze the topology
            actions = migratedWorkloadCloudCommitmentAlgorithmStrategy.analyze(workloadsList,
                    request.getBusinessAccount(),
                    cloudType,
                    riProviderSetting,
                    request.getTopologyContextId());
        }

        // Create an ActionPlan
        ActionDTO.ActionPlan actionPlan = ActionDTO.ActionPlan.newBuilder()
                .setId(IdentityGenerator.next())
                .setAnalysisStartTimestamp(analysisStartTime)
                .setAnalysisCompleteTimestamp(System.currentTimeMillis())
                .setInfo(ActionDTO.ActionPlanInfo.newBuilder()
                        .setBuyRi(ActionDTO.ActionPlanInfo.BuyRIActionPlanInfo.newBuilder()
                                .setTopologyContextId(request.getTopologyContextId())
                                .build())
                        .build())
                .addAllAction(actions)
                .build();

        try {
            // Publish the ActionPlan to Kafka
            actionsSender.notifyActionsRecommended(actionPlan);
        } catch (CommunicationException | InterruptedException e) {
            logger.error("An error occurred while publishing ActionPlan: {}", e.getMessage(), e);
        }
    }

    /**
     * Returns the RIProviderSettings defined in the plan scenario.
     *
     * @param planId The ID of the plan for which to retrieve the RIProviderSettings
     * @return The RIProviderSettings
     */
    private Optional<Map<CloudType, RIProviderSetting>> getRIProviderSetting(long planId) {
        PlanInstance planInstance = planService.getPlan(planId);
        if (planInstance != null) {
            Scenario scenario = planInstance.getScenario();
            if (scenario != null) {
                ScenarioInfo scenarioInfo = scenario.getScenarioInfo();
                if (scenarioInfo != null) {
                    List<ScenarioChange> changeList = scenarioInfo.getChangesList();
                    if (changeList != null && !changeList.isEmpty()) {
                        for (ScenarioOuterClass.ScenarioChange change : changeList) {
                            if (change.hasRiSetting()) {
                                ScenarioOuterClass.ScenarioChange.RISettingOrBuilder riSettings = change.getRiSetting();
                                Map<String, ScenarioOuterClass.ScenarioChange.RIProviderSetting> settingMap = riSettings.getRiSettingByCloudtypeMap();
                                logger.debug("RI Provider Setting Map: {}", settingMap);
                                Optional<Map.Entry<String, RIProviderSetting>> setting = settingMap.entrySet().stream().findFirst();
                                if (setting.isPresent()) {
                                    Optional<CloudType> cloudType = CloudType.fromString(setting.get().getKey());
                                    if (cloudType.isPresent()) {
                                        return Optional.of(ImmutableMap.of(cloudType.get(), setting.get().getValue()));
                                    }
                                }
                            }
                        }

                    }
                }
            }
        }

        return Optional.empty();
    }
}
