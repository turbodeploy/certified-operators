package com.vmturbo.plan.orchestrator.project;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.DeletePlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.DeletePlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetAllPlanProjectsRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetAllPlanProjectsResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetPlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.GetPlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProject;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.RunPlanProjectRequest;
import com.vmturbo.common.protobuf.plan.PlanProjectOuterClass.RunPlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanProjectServiceGrpc.PlanProjectServiceImplBase;

/**
 * Implements CRUD functionality for plan project.
 */
public class PlanProjectRpcService extends PlanProjectServiceImplBase {
    private final Logger logger = LogManager.getLogger();
    private final PlanProjectDao planProjectDao;
    private final PlanProjectExecutor planProjectExecutor;

    public PlanProjectRpcService(@Nonnull final PlanProjectDao planProjectDao,
                                 @Nonnull final PlanProjectExecutor planProjectExecutor) {
        this.planProjectDao = Objects.requireNonNull(planProjectDao);
        this.planProjectExecutor = Objects.requireNonNull(planProjectExecutor);
    }

    @Override
    public void createPlanProject(PlanProjectInfo info,
                                  StreamObserver<PlanProjectOuterClass.PlanProject> responseObserver) {
        PlanProjectOuterClass.PlanProject planProject = planProjectDao.createPlanProject(info);
        responseObserver.onNext(planProject);
        responseObserver.onCompleted();
    }

    @Override
    public void deletePlanProject(DeletePlanProjectRequest request,
                                  StreamObserver<DeletePlanProjectResponse> responseObserver) {

        Optional<PlanProjectOuterClass.PlanProject> planProject = planProjectDao.deletePlan(request.getProjectId());

        if (planProject.isPresent()) {
            responseObserver.onNext(DeletePlanProjectResponse.newBuilder()
                    .setProjectId(request.getProjectId())
                    .build());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(
                    Status.NOT_FOUND
                            .withDescription(Long.toString(request.getProjectId()))
                            .asException());
        }
    }

    @Override
    public void getPlanProject(GetPlanProjectRequest request,
                               StreamObserver<GetPlanProjectResponse> responseObserver) {
        Optional<PlanProjectOuterClass.PlanProject> project = planProjectDao.getPlanProject(request.getProjectId());
        if (project.isPresent()) {
            responseObserver.onNext(
                    GetPlanProjectResponse.newBuilder()
                            .setProject(project.get())
                            .build());
        } else {
            responseObserver.onNext(
                    GetPlanProjectResponse.newBuilder()
                            .build());
        }
        responseObserver.onCompleted();

    }

    @Override
    public void getAllPlanProjects(GetAllPlanProjectsRequest request,
                                   StreamObserver<GetAllPlanProjectsResponse> responseObserver) {
        GetAllPlanProjectsResponse response = GetAllPlanProjectsResponse.newBuilder()
                .addAllProjects(planProjectDao.getAllPlanProjects())
                .build();
        responseObserver.onNext(response);
        responseObserver.onCompleted();
    }

    @Override
    public void runPlanProject(RunPlanProjectRequest request,
                               StreamObserver<RunPlanProjectResponse> responseObserver) {
        final Optional<PlanProject> planProject = planProjectDao.getPlanProject(request.getId());
        if (planProject.isPresent()) {
            planProjectExecutor.executePlan(planProject.get());
            responseObserver.onNext(RunPlanProjectResponse.getDefaultInstance());
            responseObserver.onCompleted();
        } else {
            responseObserver.onError(Status.NOT_FOUND.withDescription(
                    "Project not found: " + request.getId()).asException());
        }
    }
}
