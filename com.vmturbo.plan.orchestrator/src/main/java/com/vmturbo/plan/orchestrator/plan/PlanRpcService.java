package com.vmturbo.plan.orchestrator.plan;

import java.util.List;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.authorization.UserContextUtils;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.auth.api.authorization.jwt.SecurityConstant;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc.BuyRIAnalysisServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Cost.RIPurchaseProfile;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScopeEntry;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioChange.RISetting;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceImplBase;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;
import com.vmturbo.plan.orchestrator.api.PlanUtils;
import com.vmturbo.common.protobuf.topology.TopologyDTO.PlanTopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyInfo;
import com.vmturbo.common.protobuf.topology.TopologyDTO.TopologyType;
import com.vmturbo.components.common.utils.StringConstants;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO;
import com.vmturbo.platform.common.dto.CommonDTO.EntityDTO.EntityType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.OSType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.ReservedInstanceType;
import com.vmturbo.platform.sdk.common.CloudCostDTO.Tenancy;

/**
 * Plan gRPC service implementation.
 */
public class PlanRpcService extends PlanServiceImplBase {

    private final PlanDao planDao;

    private final AnalysisServiceBlockingStub analysisService;

    private final BuyRIAnalysisServiceBlockingStub buyRIService;

    private final Logger logger = LogManager.getLogger();

    private final PlanNotificationSender planNotificationSender;

    private final ExecutorService analysisExecutor;

    private final UserSessionContext userSessionContext;

    public PlanRpcService(@Nonnull final PlanDao planDao,
                          @Nonnull final AnalysisServiceBlockingStub analysisService,
                          @Nonnull final PlanNotificationSender planNotificationSender,
                          @Nonnull final ExecutorService analysisExecutor,
                          @Nonnull final UserSessionContext userSessionContext,
                          @Nonnull final BuyRIAnalysisServiceBlockingStub buyRIService) {
        this.planDao = Objects.requireNonNull(planDao);
        this.analysisService = Objects.requireNonNull(analysisService);
        this.planNotificationSender = Objects.requireNonNull(planNotificationSender);
        this.analysisExecutor = analysisExecutor;
        this.userSessionContext = userSessionContext;
        this.buyRIService = buyRIService;
    }

    @Override
    public void createPlan(CreatePlanRequest request, StreamObserver<PlanInstance> responseObserver) {
        logger.debug("Creating a plan {}", request::toString);
        try {
            final PlanInstance plan = planDao.createPlanInstance(request);
            responseObserver.onNext(plan);
            responseObserver.onCompleted();
            logger.info("Plan {} successfully created", plan.getPlanId());
        } catch (IntegrityException e) {
            logger.warn("Error creating a plan " + request, e);
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void cancelPlan(PlanId request, StreamObserver<PlanInstance> responseObserver) {
        // stop analysis
        analysisExecutor.submit(() -> {
            try {
                logger.info("Triggering plan cancellation for plan {}.", request.getPlanId());
                planDao.updatePlanInstance(request.getPlanId(), oldInstance ->
                        oldInstance.setStatus(PlanStatus.STOPPED));
            } catch (NoSuchObjectException e) {
                // This could happen in the rare case where the plan got deleted
                // between queueing the analysis and starting it.
                logger.warn("Failed to stop analysis for plan " + request.getPlanId() +
                        ". Did the plan get deleted?", e);
            } catch (IntegrityException e) {
                // This could happen in the rare case where some of the plan's
                // dependencies got deleted between queueing the analysis and starting it.
                logger.warn("Failed to stop analysis of plan " + request.getPlanId() +
                        " due to integrity exception.", e);
            } catch (StatusRuntimeException e) {
                logger.error("Failed to stop analysis of plan {}  because the gRPC " +
                                "call to the Analysis Service failed with status: {}",
                        request.getPlanId(),
                        e.getStatus());
            } catch (RuntimeException e) {
                logger.error("Failed to stop analysis of plan " + request.getPlanId() + " due " +
                        "to unexpected runtime exception.", e);
            }
        });
    }

    @Override
    public void updatePlanScenario(PlanScenario request,
                            StreamObserver<PlanInstance> responseObserver) {
        logger.debug("Updating plan scenario info to existing plan {}", request);
        try {
            PlanInstance modifiedPlan = planDao.updatePlanScenario(request.getPlanId(),
                    request.getScenarioId());
            responseObserver.onNext(modifiedPlan);
            responseObserver.onCompleted();
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Plan ID (" + request.getPlanId() + ") not found.")
                    .asException());
        } catch (IntegrityException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Scenario ID (" + request.getScenarioId() + ") not found.")
                    .asException());
        }
    }

    @Override
    public void getPlan(PlanId request, StreamObserver<OptionalPlanInstance> responseObserver) {
        logger.debug("Getting a plan {}", () -> request.toString());
        final Optional<PlanInstance> instance = planDao.getPlanInstance(request.getPlanId());
        final OptionalPlanInstance.Builder builder = OptionalPlanInstance.newBuilder();
        instance.ifPresent(builder::setPlanInstance);
        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    @Override
    public void deletePlan(PlanId request, StreamObserver<PlanInstance> responseObserver) {
        logger.debug("Deleting a plan {}", () -> request.toString());
        try {
            final long planId = request.getPlanId();
            final PlanInstance planInstance = planDao.deletePlan(planId);
            responseObserver.onNext(planInstance);
            responseObserver.onCompleted();
            logger.info("Plan {} successfully deleted", planId);
        } catch (NoSuchObjectException e) {
            logger.warn("Plan not found while requested to be removed: " + request, e);
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        }
    }

    @Override
    public void runPlan(PlanId request, StreamObserver<PlanInstance> responseObserver) {
        logger.debug("Triggering a plan {}", () -> request.toString());
        final long planId = request.getPlanId();
        try {
            // TODO (roman, Dec 27 2016): This leads to a race condition where the
            // caller may get the status change notification before getting the response
            // from this RPC. For now it's benign because we also return
            // the instance.
            PlanInstance planInstance = planDao.getPlanInstance(planId)
                    .orElseThrow(() -> new NoSuchObjectException("Invalid Plan ID: " + planId));
            Optional<PlanInstance> queuedPlanInstance = planDao.queuePlanInstance(planInstance);
            if (queuedPlanInstance.isPresent()) {
                runQueuedPlan(queuedPlanInstance.get(), responseObserver);
            } else {
                // The plan was not queued. It may still be in READY state if the maximum number
                // of concurrent plans has reached, or it may be executed by another process.
                // In this case, just return the instance without initiating the analysis.
                responseObserver.onNext(planInstance);
                responseObserver.onCompleted();
            }
        } catch (NoSuchObjectException e) {
            logger.warn("Plan not found while requested to trigger: " + request, e);
            responseObserver.onError(
                    Status.NOT_FOUND.withDescription(e.getMessage()).asException());
        } catch (IntegrityException e) {
            logger.warn(
                    "Referential integrity violated while requesting to trigger a plan " + request,
                    e);
            responseObserver.onError(
                    Status.FAILED_PRECONDITION.withDescription(e.getMessage()).asException());
        }
    }

    /**
     * Start execution of a plan instance that is already in QUEUED state. This method is package
     * visible for PlanInstanceQueue to invoke, but it's not intended to be a public API.
     *
     * @param planInstance the plan instance to run.
     * @param responseObserver the response observer.
     */
    void runQueuedPlan(PlanInstance planInstance, StreamObserver<PlanInstance> responseObserver) {
        Preconditions.checkArgument(planInstance.getStatus().equals(PlanStatus.QUEUED));
        @Nullable ScenarioInfo scenarioInfo = planInstance.hasScenario() ?
                planInstance.getScenario().getScenarioInfo() : null;
        long planId = planInstance.getPlanId();
        if (scenarioInfo != null && scenarioInfo.getType().equals(StringConstants.OPTIMIZE_CLOUD_PLAN_TYPE)) {
            List<ScenarioChange> riScenario = scenarioInfo.getChangesList()
                    .stream()
                    .filter(c -> c.hasRiSetting())
                    .collect(Collectors.toList());
            if (!riScenario.isEmpty()) {
                // trigger buy RI for optimize cloud buy RI only or buy RI and optimize workload
                // if the optimize plan is for optimize workload only, then this block will be skipped
                // and we directly go to triggerAnalysis
                triggerBuyRI(scenarioInfo, riScenario.get(0), planId);
                responseObserver.onNext(planInstance);
                responseObserver.onCompleted();
                return;
            }
        }
        triggerAnalysis(planInstance);
        responseObserver.onNext(planInstance);
        responseObserver.onCompleted();
    }

    /**
     * Trigger buy RI recommendation algorithm in cost component
     *
     * @param scenarioInfo the scenarioInfo of plan instance
     * @param riScenario the scenario change related with RI 
     * @param planId the plan id
     */
    @VisibleForTesting
    void triggerBuyRI(@Nonnull ScenarioInfo scenarioInfo,
                              @Nonnull ScenarioChange riScenario, long planId) {
        try {
            StartBuyRIAnalysisRequest request = PlanRpcServiceUtil.createBuyRIRequest(scenarioInfo,
                    riScenario, planId);
            buyRIService.startBuyRIAnalysis(request);
            planDao.updatePlanInstance(planId, oldInstance ->
                    oldInstance.setStatus(PlanStatus.STARTING_BUY_RI));
            logger.info("Started buy RI for plan {} on region {} account {}", planId,
                    request.getRegionsList(), request.getAccountsList());
        } catch (IntegrityException | NoSuchObjectException e) {
            logger.warn("Failed to update status after run buy RI for plan {}", planId);
        } catch (StatusRuntimeException statusException) {
            try {
                logger.error("Failed to start buy RI for plan {}  because the gRPC call failed with status: {}",
                        planId, statusException.getStatus());
                planDao.updatePlanInstance(planId, oldInstance -> {
                        oldInstance.setStatus(PlanStatus.FAILED);});
            } catch (IntegrityException integrityException) {
                logger.warn("Referential integrity violated while changing plan status to failed for plan {}"
                            + " after starting buy RI", planId);
            } catch (NoSuchObjectException noObjectException) {
                // This could happen in the rare case where the plan got deleted
                // between queueing the analysis and starting it.
                logger.warn("Can not find plan object while changing plan status to failed for plan {}."
                            + " Did the plan get deleted?", planId);
            }
        } catch (RuntimeException runtimeEx) {
            logger.error("Failed to start buy RI analysis for plan " + planId +
                        "due to unexpected runtime exception.", runtimeEx);
        }
    }
    public void triggerAnalysis(@Nonnull PlanInstance planInstance) {
        final StartAnalysisRequest.Builder builder = StartAnalysisRequest.newBuilder();
        ScenarioInfo scenarioInfo = planInstance.getScenario().getScenarioInfo();
        builder.setPlanId(planInstance.getPlanId());

        if (planInstance.hasTopologyId()) {
            builder.setTopologyId(planInstance.getTopologyId());
        }
        if (planInstance.hasScenario()) {
            builder.addAllScenarioChange(scenarioInfo.getChangesList());
            if (scenarioInfo.hasScope()) {
                builder.setPlanScope(scenarioInfo.getScope());
            }
            if (scenarioInfo.hasType()) {
                builder.setPlanType(scenarioInfo.getType());
            }
        }
        builder.setPlanProjectType(planInstance.getProjectType());
        startAnalysis(builder.build());
    }

    @Override
    public void createPlanOverPlan(PlanScenario planSpec, StreamObserver<PlanInstance> responseObserver) {
        try {
            // plan must be completed PlanStatus.SUCCEEDED
            PlanInstance planInstance = planDao.getPlanInstance(planSpec.getPlanId())
                    .orElseThrow( () -> new NoSuchObjectException(
                            Long.toString(planSpec.getPlanId())));
            if (!planInstance.getStatus().equals(PlanStatus.SUCCEEDED)) {
                throw new IntegrityException("plan status = " + planInstance.getStatus());
            }
            // ensure that the projectedTopology is valid (i.e. non-zero)
            if (planInstance.getProjectedTopologyId() == 0) {
                responseObserver.onError(Status.INTERNAL
                        .withDescription("Plan ID (" + planSpec.getPlanId() +
                                ") has invalid projected topology.")
                        .asException());
                return;
            }
            // ensure the user can access the plan
            if (! PlanUtils.canCurrentUserAccessPlan(planInstance)) {
                throw new UserAccessException("User cannot access requested plan.");
            }

            // update the previous scenario to reflect the given scenario
            planDao.updatePlanScenario(planSpec.getPlanId(),
                    planSpec.getScenarioId());
            // Reset the planInstance to a "READY" state, using the previous projectedTopologyId as the
            // new topologyId, so it can be queued and executed in runPlan method.
            PlanInstance updatedPlanInstance = planDao.updatePlanInstance(planSpec.getPlanId(),
                    planInstanceBuilder -> {
                        planInstanceBuilder
                                .setTopologyId(planInstance.getProjectedTopologyId())
                                .setProjectedTopologyId(0)
                                .addActionPlanId(0)
                                .clearStatsAvailable()
                                .setStatus(PlanStatus.READY)
                                .setStartTime(System.currentTimeMillis())
                                .setEndTime(0);
                        // the user who is running the new plan will become the creator of the plan.
                        UserContextUtils.getCurrentUserId()
                                .ifPresent(planInstanceBuilder::setCreatedByUser);
                    });
            responseObserver.onNext(updatedPlanInstance);
            responseObserver.onCompleted();
        } catch (IntegrityException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Plan ID (" + planSpec.getPlanId() + ") cannot be run: " +
                            e.getMessage())
                    .asException());
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Plan ID (" + planSpec.getPlanId() + ") not found.")
                    .asException());
        }

    }

    @Override
    public void getAllPlans(GetPlansOptions request, StreamObserver<PlanInstance> responseObserver) {
        logger.debug("Retrieving all the existing plans...");
        planDao.getAllPlanInstances().stream()
            // When listing plans, return only USER-created plans.
            .filter(planInstance -> planInstance.getProjectType() == PlanProjectType.USER)
            .filter(PlanUtils::canCurrentUserAccessPlan) // filter plans for non-admin users
            .forEach(responseObserver::onNext);
        responseObserver.onCompleted();
    }

    /**
     * Queue a request to the Analysis Service (in Topology Processor) to edit + broadcast
     * a topology to the market.
     *
     * @param request The full-formed request to send.
     */
    private void startAnalysis(@Nonnull final StartAnalysisRequest request) {
        Objects.requireNonNull(request);
        // TODO (roman, Dec. 27 2016): We may want to do additional filtering here.
        // For example, disallow running the same plan more than once, or disallow
        // running more than one plan under a plan project (once we have those) or
        // a scenario.
        analysisExecutor.submit(() -> {
            // TODO (roman, Dec. 29 2016): If this try block throws an exception
            // we should send a notification indicating that the plan has failed.
            try {
                planDao.updatePlanInstance(request.getPlanId(), oldInstance ->
                        oldInstance.setStatus(PlanStatus.CONSTRUCTING_TOPOLOGY));

                final StartAnalysisResponse response = analysisService.startAnalysis(request);
                logger.info("Started analysis for plan {} on topology {} with {} entities", request.getPlanId(),
                        request.getTopologyId(), response.getEntitiesBroadcast());

                // TODO (roman, Dec. 29 2016): Consider doing this
                // to a notification from the market instead of assuming
                // that once startAnalysis returns the analysis has actually started.
                // For instance, the plan could be queued in the market component and not
                // running yet at the time startAnalysis returns.
                planDao.updatePlanInstance(request.getPlanId(),
                        oldInstance -> {
                            oldInstance.setStatus(PlanStatus.RUNNING_ANALYSIS);
                        });
            } catch (NoSuchObjectException e) {
                // This could happen in the rare case where the plan got deleted
                // between queueing the analysis and starting it.
                logger.warn("Failed to start analysis for plan " + request.getPlanId() +
                        ". Did the plan get deleted?", e);
            } catch (IntegrityException e) {
                // This could happen in the rare case where some of the plan's
                // dependencies got deleted between queueing the analysis and starting it.
                logger.warn("Failed to start analysis for plan " + request.getPlanId() +
                        " due to integrity exception.", e);
            } catch (StatusRuntimeException e) {
                logger.error("Failed to start analysis for plan {}  because the gRPC " +
                                "call to the Analysis Service failed with status: {}",
                        request.getPlanId(),
                        e.getStatus());
            } catch (RuntimeException e) {
                logger.error("Failed to start analysis for plan " + request.getPlanId() + " due " +
                        "to unexpected runtime exception.", e);
            }
        });
    }
}
