package com.vmturbo.plan.orchestrator.plan;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.auth.api.authorization.AuthorizationException.UserAccessException;
import com.vmturbo.auth.api.authorization.UserContextUtils;
import com.vmturbo.auth.api.authorization.UserSessionContext;
import com.vmturbo.common.protobuf.PlanDTOUtil;
import com.vmturbo.common.protobuf.cost.BuyRIAnalysisServiceGrpc.BuyRIAnalysisServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.Cost.StartBuyRIAnalysisRequest;
import com.vmturbo.common.protobuf.cost.PlanReservedInstanceServiceGrpc.PlanReservedInstanceServiceBlockingStub;
import com.vmturbo.common.protobuf.cost.ReservedInstanceBoughtServiceGrpc.ReservedInstanceBoughtServiceBlockingStub;
import com.vmturbo.common.protobuf.group.GroupServiceGrpc.GroupServiceBlockingStub;
import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.UpdatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceImplBase;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioChange;
import com.vmturbo.common.protobuf.plan.ScenarioOuterClass.ScenarioInfo;
import com.vmturbo.common.protobuf.repository.RepositoryServiceGrpc.RepositoryServiceBlockingStub;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;
import com.vmturbo.components.api.RetriableOperation;
import com.vmturbo.components.api.RetriableOperation.RetriableOperationFailedException;
import com.vmturbo.common.protobuf.utils.StringConstants;
import com.vmturbo.plan.orchestrator.api.PlanUtils;

/**
 * Plan gRPC service implementation.
 */
public class PlanRpcService extends PlanServiceImplBase {

    private final PlanDao planDao;

    private final AnalysisServiceBlockingStub analysisService;

    private final BuyRIAnalysisServiceBlockingStub buyRIService;

    private final RepositoryServiceBlockingStub repositoryServiceClient;

    private final GroupServiceBlockingStub groupServiceClient;

    private final Logger logger = LogManager.getLogger();

    private final PlanNotificationSender planNotificationSender;

    private final ExecutorService analysisExecutor;

    private final UserSessionContext userSessionContext;

    private final long startAnalysisRetryMs;

    private final ReservedInstanceBoughtServiceBlockingStub reservedInstanceBoughtService;

    private final PlanReservedInstanceServiceBlockingStub planRIService;

    private Long realtimeTopologyContextId;

    public PlanRpcService(@Nonnull final PlanDao planDao,
                          @Nonnull final AnalysisServiceBlockingStub analysisService,
                          @Nonnull final PlanNotificationSender planNotificationSender,
                          @Nonnull final ExecutorService analysisExecutor,
                          @Nonnull final UserSessionContext userSessionContext,
                          @Nonnull final BuyRIAnalysisServiceBlockingStub buyRIService,
                          @Nonnull final GroupServiceBlockingStub groupServiceClient,
                          @Nonnull final RepositoryServiceBlockingStub repositoryServiceClient,
                          @Nonnull final PlanReservedInstanceServiceBlockingStub planRIService,
                          @Nonnull final ReservedInstanceBoughtServiceBlockingStub reservedInstanceBoughtService,
                          final long startAnalysisRetryTimeout,
                          @Nonnull final TimeUnit startAnalysisRetryTimeUnit,
                          final Long realtimeTopologyContextId) {
        this.planDao = Objects.requireNonNull(planDao);
        this.analysisService = Objects.requireNonNull(analysisService);
        this.planNotificationSender = Objects.requireNonNull(planNotificationSender);
        this.analysisExecutor = analysisExecutor;
        this.userSessionContext = userSessionContext;
        this.buyRIService = buyRIService;
        this.groupServiceClient = Objects.requireNonNull(groupServiceClient);
        this.repositoryServiceClient = Objects.requireNonNull(repositoryServiceClient);
        this.planRIService = planRIService;
        this.reservedInstanceBoughtService = reservedInstanceBoughtService;
        this.startAnalysisRetryMs = startAnalysisRetryTimeUnit.toMillis(startAnalysisRetryTimeout);
        this.realtimeTopologyContextId = realtimeTopologyContextId;
    }

    @Override
    public void createPlan(CreatePlanRequest request, StreamObserver<PlanInstance> responseObserver) {
        logger.debug("Creating a plan {}", request::toString);
        try {
            final PlanInstance plan = planDao.createPlanInstance(request);
            responseObserver.onNext(plan);
            responseObserver.onCompleted();
            // save the user selected RI/Coupons included in the plan.
            PlanReservedInstanceClient planRIClient = new PlanReservedInstanceClient(
                         planRIService, reservedInstanceBoughtService, realtimeTopologyContextId);
            planRIClient.savePlanIncludedCoupons(plan, PlanRpcServiceUtil.getScopeSeedIds(plan,
                    this.groupServiceClient, this.repositoryServiceClient));
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
        final long planId = request.getPlanId();
        analysisExecutor.submit(() -> {
            try {
                logger.info("Triggering plan cancellation for plan {}.", planId);
                planDao.updatePlanInstance(planId, oldInstance ->
                        oldInstance.setStatus(PlanStatus.STOPPED));
                PlanInstance planInstance = planDao.getPlanInstance(planId)
                    .orElseThrow(() -> new NoSuchObjectException("Invalid Plan ID: " + planId));
                responseObserver.onNext(planInstance);
                responseObserver.onCompleted();
            } catch (NoSuchObjectException e) {
                // This could happen in the rare case where the plan got deleted
                // between queueing the analysis and starting it.
                logger.warn("Failed to stop analysis for plan " + planId +
                        ". Did the plan get deleted?", e);
                responseObserver.onError(
                        Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            } catch (IntegrityException e) {
                // This could happen in the rare case where some of the plan's
                // dependencies got deleted between queueing the analysis and starting it.
                logger.warn("Failed to stop analysis of plan " + planId +
                        " due to integrity exception.", e);
                responseObserver.onError(
                        Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            } catch (StatusRuntimeException e) {
                logger.error("Failed to stop analysis of plan {}  because the gRPC " +
                                "call to the Analysis Service failed with status: {}",
                                planId,
                        e.getStatus());
                responseObserver.onError(
                        Status.NOT_FOUND.withDescription(e.getMessage()).asException());
            } catch (RuntimeException e) {
                logger.error("Failed to stop analysis of plan " + planId + " due " +
                        "to unexpected runtime exception.", e);
                responseObserver.onError(
                        Status.NOT_FOUND.withDescription(e.getMessage()).asException());
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
        if (scenarioInfo != null && scenarioInfo.getType().equals(StringConstants.OPTIMIZE_CLOUD_PLAN)) {
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
        analysisExecutor.submit(() -> {
            try {
                StartBuyRIAnalysisRequest request = PlanRpcServiceUtil.createBuyRIRequest(scenarioInfo,
                        riScenario, planId, groupServiceClient, repositoryServiceClient);
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
                        oldInstance.setStatus(PlanStatus.FAILED);
                    });
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
        });
    }

    public void triggerAnalysis(@Nonnull PlanInstance planInstance) {
        final StartAnalysisRequest.Builder builder = StartAnalysisRequest.newBuilder();
        ScenarioInfo scenarioInfo = planInstance.getScenario().getScenarioInfo();
        builder.setPlanId(planInstance.getPlanId());

        if (planInstance.hasSourceTopologyId()) {
            builder.setTopologyId(planInstance.getSourceTopologyId());
        }
        if (planInstance.hasScenario()) {
            builder.addAllScenarioChange(scenarioInfo.getChangesList());
            if (scenarioInfo.hasScope()) {
                builder.setPlanScope(scenarioInfo.getScope());
            }
            if (scenarioInfo.hasType()) {
                builder.setPlanType(scenarioInfo.getType());
            }
            if (PlanRpcServiceUtil.hasPlanSubType(scenarioInfo)) {
                builder.setPlanSubType(PlanRpcServiceUtil.getCloudPlanSubType(scenarioInfo));
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
                                // In Plan over plan, the projected topology from the previous plan
                                // becomes the source topology for the next plan.
                                .setSourceTopologyId(planInstance.getProjectedTopologyId())
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
            // When listing plans, return only user displayable plans.
            .filter(planInstance -> PlanDTOUtil.isDisplayablePlan(planInstance.getProjectType()))
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
            try {
                planDao.updatePlanInstance(request.getPlanId(), oldInstance ->
                        oldInstance.setStatus(PlanStatus.CONSTRUCTING_TOPOLOGY));

                try {
                    final StartAnalysisResponse response = RetriableOperation.newOperation(
                            () -> analysisService.startAnalysis(request))
                        // Retry if unavailable.
                        .retryOnException(e -> (e instanceof StatusRuntimeException) &&
                            ((StatusRuntimeException)e).getStatus().getCode() == Code.UNAVAILABLE)
                        .run(startAnalysisRetryMs, TimeUnit.MILLISECONDS);
                    logger.info("Started analysis for plan {} on topology {}",
                        request.getPlanId(), response.getTopologyId());
                } catch (InterruptedException | TimeoutException | RetriableOperationFailedException e) {
                    if (e instanceof InterruptedException) {
                        // Reset interrupt status.
                        Thread.currentThread().interrupt();
                    }
                    // This can happen if there is an error calling the topology processor. In this
                    // case we should mark the plan as failed.
                    logger.error("Failed to start analysis for plan " + request.getPlanId()
                            + " because the gRPC call to the Analysis Service failed.", e);

                    // Set the plan status to failed, since it didn't even get out of the gate.
                    planDao.updatePlanInstance(request.getPlanId(),
                        oldInstance -> {
                            oldInstance.setStatus(PlanStatus.FAILED);
                            oldInstance.setStatusMessage("Failed to start analysis due to " +
                                "Topology Processor error: " + e.getMessage());
                        });
                }
            } catch (NoSuchObjectException e) {
                // This could happen in the rare case where the plan got deleted
                // between queueing the analysis and starting it.
                logger.error("Failed to start analysis for plan " + request.getPlanId() +
                        ". Did the plan get deleted?", e);
            } catch (IntegrityException e) {
                // This could happen in the rare case where some of the plan's
                // dependencies got deleted between queueing the analysis and starting it.
                logger.error("Failed to start analysis for plan " + request.getPlanId() +
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

    @Override
    public void updatePlan(UpdatePlanRequest request, StreamObserver<PlanInstance> responseObserver) {
        try {
            PlanInstance planInstance = planDao.updatePlanInstance(request.getPlanId(),
                    oldInstance -> oldInstance.setName(request.getName()));

            responseObserver.onNext(planInstance);
            responseObserver.onCompleted();
        } catch (IntegrityException e) {
            responseObserver.onError(Status.FAILED_PRECONDITION
                    .withDescription("Plan ID (" + request.getPlanId() + ") cannot be run: " +
                            e.getMessage())
                    .asException());
        } catch (NoSuchObjectException e) {
            responseObserver.onError(Status.NOT_FOUND
                    .withDescription("Plan ID (" + request.getPlanId() + ") not found.")
                    .asException());
        }
    }
}
