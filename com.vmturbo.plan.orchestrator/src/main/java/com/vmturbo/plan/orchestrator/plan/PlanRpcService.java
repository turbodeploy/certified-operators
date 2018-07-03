package com.vmturbo.plan.orchestrator.plan;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO.CreatePlanRequest;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlansOptions;
import com.vmturbo.common.protobuf.plan.PlanDTO.OptionalPlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanId;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanInstance.PlanStatus;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectType;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanScenario;
import com.vmturbo.common.protobuf.plan.PlanDTO.ScenarioInfo;
import com.vmturbo.common.protobuf.plan.PlanServiceGrpc.PlanServiceImplBase;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisRequest;
import com.vmturbo.common.protobuf.topology.AnalysisDTO.StartAnalysisResponse;
import com.vmturbo.common.protobuf.topology.AnalysisServiceGrpc.AnalysisServiceBlockingStub;

/**
 * Plan gRPC service implementation.
 */
public class PlanRpcService extends PlanServiceImplBase {

    private final PlanDao planDao;

    private final AnalysisServiceBlockingStub analysisService;

    private final Logger logger = LogManager.getLogger();

    private final PlanNotificationSender planNotificationSender;

    private final ExecutorService analysisExecutor;

    public PlanRpcService(@Nonnull final PlanDao planDao,
                          @Nonnull final AnalysisServiceBlockingStub analysisService,
                          @Nonnull final PlanNotificationSender planNotificationSender,
                          @Nonnull final ExecutorService analysisExecutor) {
        this.planDao = Objects.requireNonNull(planDao);
        this.analysisService = Objects.requireNonNull(analysisService);
        this.planNotificationSender = Objects.requireNonNull(planNotificationSender);
        this.analysisExecutor = analysisExecutor;
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
        final StartAnalysisRequest.Builder builder = StartAnalysisRequest.newBuilder();
        builder.setPlanId(planInstance.getPlanId());
        if (planInstance.hasTopologyId()) {
            builder.setTopologyId(planInstance.getTopologyId());
        }
        if (planInstance.hasScenario()) {
            ScenarioInfo scenarioInfo = planInstance.getScenario().getScenarioInfo();
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

        responseObserver.onNext(planInstance);
        responseObserver.onCompleted();
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
                                .setActionPlanId(0)
                                .clearStatsAvailable()
                                .setStatus(PlanStatus.READY)
                                .setStartTime(System.currentTimeMillis())
                                .setEndTime(0);
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
