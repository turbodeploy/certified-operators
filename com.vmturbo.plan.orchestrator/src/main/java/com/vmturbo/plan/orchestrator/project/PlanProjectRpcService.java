package com.vmturbo.plan.orchestrator.project;

import java.util.Objects;
import java.util.Optional;

import javax.annotation.Nonnull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;

import com.vmturbo.common.protobuf.plan.PlanDTO;
import com.vmturbo.common.protobuf.plan.PlanDTO.DeletePlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.GetPlanProjectResponse;
import com.vmturbo.common.protobuf.plan.PlanDTO.PlanProjectInfo;
import com.vmturbo.common.protobuf.plan.PlanProjectServiceGrpc.PlanProjectServiceImplBase;

/**
 * Implements CRUD functionality for plan project.
 */
public class PlanProjectRpcService extends PlanProjectServiceImplBase {
    private final Logger logger = LogManager.getLogger();
    private final PlanProjectDao planProjectDao;

    public PlanProjectRpcService(@Nonnull final PlanProjectDao planProjectDao) {
        this.planProjectDao = Objects.requireNonNull(planProjectDao);
    }

    @Override
    public void createPlanProject(PlanProjectInfo info,
                                  StreamObserver<PlanDTO.PlanProject> responseObserver) {
        PlanDTO.PlanProject planProject = planProjectDao.createPlanProject(info);
        responseObserver.onNext(planProject);
        responseObserver.onCompleted();
    }

    @Override
    public void deletePlanProject(PlanDTO.DeletePlanProjectRequest request,
                                  StreamObserver<PlanDTO.DeletePlanProjectResponse> responseObserver) {

        Optional<PlanDTO.PlanProject> planProject = planProjectDao.deletePlan(request.getProjectId());

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
    public void getPlanProject(PlanDTO.GetPlanProjectRequest request,
                               StreamObserver<PlanDTO.GetPlanProjectResponse> responseObserver) {
        Optional<PlanDTO.PlanProject> project = planProjectDao.getPlanProject(request.getProjectId());
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
}
